//! Soloist's local WebSocket API: commands out, events folded into the
//! engine's [`LocalState`].
//!
//! Soloist pushes a full `playback_state` when a client connects and small
//! events after that. Each one is applied to the shared state, and a
//! snapshot goes to the interface only when something it shows changed.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

use crate::api::models::ArtistRef;
use crate::player::{EngineEvent, LocalState, LocalTrack, Notify, Playback, RepeatMode};
use crate::sink::AudioControl;

/// A command for Soloist, sent in order.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Command {
    Play(Option<String>),
    Pause,
    Next,
    Previous,
    Seek(u32),
    /// Percent, 0 to 100.
    Volume(u32),
    Shuffle(bool),
    Repeat(RepeatMode),
    AddToQueue(String),
    Activate,
}

impl Command {
    /// The WebSocket messages for this command; repeat takes two.
    fn messages(&self) -> Vec<Value> {
        let command = |name: &str| json!({ "type": "command", "command": name });
        let with = |name: &str, key: &str, value: Value| {
            let mut message = command(name);
            message[key] = value;
            message
        };
        match self {
            Self::Play(None) => vec![command("play")],
            Self::Play(Some(uri)) => vec![with("play", "uri", json!(uri))],
            Self::Pause => vec![command("pause")],
            Self::Next => vec![command("skip_next")],
            Self::Previous => vec![command("skip_prev")],
            Self::Seek(position_ms) => vec![with("seek", "position_ms", json!(position_ms))],
            Self::Volume(percent) => vec![with("set_volume", "volume", json!(percent.min(&100)))],
            Self::Shuffle(enabled) => vec![with("set_shuffle", "enabled", json!(enabled))],
            // Soloist documents this order for each mode, so the two flags
            // are never both on.
            Self::Repeat(mode) => {
                let track = with(
                    "set_repeat_track",
                    "enabled",
                    json!(*mode == RepeatMode::Track),
                );
                let context = with(
                    "set_repeat_context",
                    "enabled",
                    json!(*mode == RepeatMode::Context),
                );
                if *mode == RepeatMode::Track {
                    vec![context, track]
                } else {
                    vec![track, context]
                }
            }
            Self::AddToQueue(uri) => vec![with("add_to_queue", "uri", json!(uri))],
            Self::Activate => vec![command("activate")],
        }
    }
}

/// Sends commands to the connection task.
#[derive(Clone)]
pub(crate) struct Control {
    commands: mpsc::UnboundedSender<Command>,
}

impl Control {
    pub(crate) fn send(&self, command: Command) -> Result<()> {
        self.commands
            .send(command)
            .map_err(|_| anyhow::anyhow!("Spotify Soloist is not running"))
    }
}

/// What the connection task updates and whom it tells.
pub(crate) struct Shared {
    pub state: Arc<Mutex<LocalState>>,
    pub notify: Notify,
    pub audio: Arc<AudioControl>,
}

/// Connects to Soloist's API and runs until it closes. The returned future
/// completes when the connection ends, which means Soloist went away.
pub(crate) async fn connect(
    port: u16,
    shared: Arc<Shared>,
) -> Result<(Control, impl Future<Output = ()> + Send + 'static)> {
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}"))
        .await
        .context("Spotify Soloist's control port did not answer")?;
    let (commands, receiver) = mpsc::unbounded_channel();
    Ok((Control { commands }, run(socket, receiver, shared)))
}

async fn run<S>(
    socket: tokio_tungstenite::WebSocketStream<S>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    shared: Arc<Shared>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let (mut outgoing, mut incoming) = socket.split();
    let mut events = Events::default();
    loop {
        tokio::select! {
            message = incoming.next() => match message {
                Some(Ok(Message::Text(text))) => {
                    let Ok(event) = serde_json::from_str::<Value>(text.as_str()) else {
                        log::debug!("soloist: unreadable event");
                        continue;
                    };
                    let now = Instant::now();
                    let snapshot = {
                        let mut state = shared.state.lock().unwrap_or_else(PoisonError::into_inner);
                        let applied = events.apply(&mut state, &event, now, epoch_ms());
                        if applied.track_changed {
                            shared.audio.track_changed();
                        }
                        applied.changed.then(|| state.clone())
                    };
                    if let Some(snapshot) = snapshot {
                        (shared.notify)(EngineEvent::State(snapshot));
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => {
                    log::warn!("soloist: control connection failed: {error}");
                    break;
                }
            },
            command = commands.recv() => {
                let Some(command) = command else { break };
                for message in command.messages() {
                    if let Err(error) = outgoing.send(Message::text(message.to_string())).await {
                        log::warn!("soloist: command not sent: {error}");
                    }
                }
            }
        }
    }
    let _ = outgoing.close().await;
}

fn epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

/// What applying one event did.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Applied {
    /// Something the interface shows changed.
    pub changed: bool,
    /// A new track started, including another play of the same one.
    pub track_changed: bool,
}

/// Folds Soloist's events into a [`LocalState`]. Keeps what the state does
/// not: whether the next position report belongs to a new track.
#[derive(Debug, Default)]
pub(crate) struct Events {
    fresh_track: bool,
}

/// A position jump this far from where playback should be is a seek.
const SEEK_THRESHOLD_MS: i64 = 1500;

impl Events {
    pub(crate) fn apply(
        &mut self,
        state: &mut LocalState,
        event: &Value,
        now: Instant,
        epoch_ms: i64,
    ) -> Applied {
        let mut applied = Applied::default();
        match event
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "auth_state" => {
                let logged_in = flag(event, "logged_in");
                applied.changed |= set(&mut state.connected, logged_in);
            }
            "playback_state" => {
                if let Some(item) = event.get("item") {
                    applied = self.track(state, item, false);
                }
                if let Some(status) = event.get("status").and_then(Value::as_str) {
                    applied.changed |= status_into(state, status, now);
                }
                if let Some(position) = event.get("position") {
                    applied.changed |= self.position(state, position, now, epoch_ms);
                }
                if let Some(volume) = event.get("volume").and_then(Value::as_u64) {
                    applied.changed |= volume_into(state, volume);
                }
                if let Some(options) = event.get("options") {
                    applied.changed |= options_into(state, options);
                }
            }
            "track_changed" => {
                if let Some(item) = event.get("item") {
                    applied = self.track(state, item, true);
                }
            }
            "playback_changed" => {
                if let Some(status) = event.get("status").and_then(Value::as_str) {
                    applied.changed = status_into(state, status, now);
                }
            }
            "position_sync" => {
                if let Some(position) = event.get("position") {
                    applied.changed = self.position(state, position, now, epoch_ms);
                }
            }
            "volume_changed" => {
                if let Some(volume) = event.get("volume").and_then(Value::as_u64) {
                    applied.changed = volume_into(state, volume);
                }
            }
            "options_changed" => {
                if let Some(options) = event.get("options") {
                    applied.changed = options_into(state, options);
                }
            }
            "device_changed" => {
                if !flag(event, "is_active") {
                    applied.changed = set(&mut state.active_client, String::new());
                }
            }
            "error" => {
                let message = event
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("Spotify Soloist reported an error");
                log::warn!("soloist: {message}");
                applied.changed = set(&mut state.loading, false);
            }
            _ => {}
        }
        applied
    }

    /// A track Soloist reports. `track_changed` always starts a new play,
    /// another play of the same track included; a `playback_state` that
    /// names the track already shown only refreshes its details.
    fn track(&mut self, state: &mut LocalState, item: &Value, new_play: bool) -> Applied {
        let Some(track) = local_track(item) else {
            return Applied::default();
        };
        let same_uri = state
            .track
            .as_ref()
            .is_some_and(|previous| previous.uri == track.uri);
        if same_uri && !new_play {
            return Applied {
                changed: set(&mut state.track, Some(track)),
                track_changed: false,
            };
        }
        state.replay_pending = same_uri;
        state.track = Some(track);
        state.error = None;
        state.track_sequence = state.track_sequence.wrapping_add(1);
        self.fresh_track = true;
        Applied {
            changed: true,
            track_changed: true,
        }
    }

    fn position(
        &mut self,
        state: &mut LocalState,
        position: &Value,
        now: Instant,
        epoch_ms: i64,
    ) -> bool {
        let Some(at_ms) = position.get("position_ms").and_then(Value::as_i64) else {
            return false;
        };
        let speed = position.get("speed").and_then(Value::as_f64).unwrap_or(0.0);
        let stamp = position
            .get("timestamp_ms")
            .and_then(Value::as_i64)
            .unwrap_or(epoch_ms);
        let elapsed = (epoch_ms - stamp).max(0);
        let mut position_ms = at_ms;
        if speed > 0.0 {
            position_ms += (elapsed as f64 * speed) as i64;
        }
        if let Some(track) = &state.track
            && track.duration_ms > 0
        {
            position_ms = position_ms.min(i64::from(track.duration_ms));
        }
        let position_ms = position_ms.max(0) as u32;
        let expected = i64::from(state.position_now());
        let fresh = std::mem::take(&mut self.fresh_track);
        if std::mem::take(&mut state.replay_pending)
            || (!fresh && (i64::from(position_ms) - expected).abs() > SEEK_THRESHOLD_MS)
        {
            state.seek_sequence = state.seek_sequence.wrapping_add(1);
        }
        state.position_ms = position_ms;
        state.position_at = (speed > 0.0 && state.playback == Playback::Playing).then_some(now);
        true
    }
}

fn flag(event: &Value, key: &str) -> bool {
    event.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn set<T: PartialEq>(target: &mut T, value: T) -> bool {
    if *target == value {
        false
    } else {
        *target = value;
        true
    }
}

fn status_into(state: &mut LocalState, status: &str, now: Instant) -> bool {
    match status {
        "playing" => {
            let mut changed = set(&mut state.playback, Playback::Playing);
            changed |= set(&mut state.loading, false);
            if state.position_at.is_none() {
                state.position_at = Some(now);
                changed = true;
            }
            changed
        }
        "paused" => {
            let position_ms = state.position_now();
            let mut changed = set(&mut state.playback, Playback::Paused);
            changed |= set(&mut state.loading, false);
            changed |= set(&mut state.position_ms, position_ms);
            changed |= set(&mut state.position_at, None);
            changed
        }
        "buffering" => {
            let mut changed = set(&mut state.loading, true);
            if state.playback == Playback::Stopped {
                changed |= set(&mut state.playback, Playback::Loading);
            }
            changed
        }
        "idle" => {
            let mut changed = set(&mut state.playback, Playback::Stopped);
            changed |= set(&mut state.loading, false);
            changed |= set(&mut state.position_ms, 0);
            changed |= set(&mut state.position_at, None);
            changed
        }
        _ => false,
    }
}

/// Soloist's percentage as the engine's level. A level that rounds to the
/// same percentage is kept, so a level set here is not nudged by its echo.
fn volume_into(state: &mut LocalState, percent: u64) -> bool {
    let percent = percent.min(100) as u32;
    if super::pcm::percent(state.volume) == percent {
        return false;
    }
    set(&mut state.volume, (percent * 65535 / 100) as u16)
}

fn options_into(state: &mut LocalState, options: &Value) -> bool {
    let mut changed = false;
    if let Some(shuffle) = options.get("shuffle").and_then(Value::as_bool) {
        changed |= set(&mut state.shuffle, shuffle);
    }
    if let Some(repeat) = options.get("repeat").and_then(Value::as_str) {
        changed |= set(&mut state.repeat, RepeatMode::from_api(repeat));
    }
    changed
}

fn name(entity: &Value) -> &str {
    entity
        .pointer("/decorations/identity/name")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// A Soloist entity as the track the interface shows.
pub(crate) fn local_track(item: &Value) -> Option<LocalTrack> {
    let uri = item.get("uri").and_then(Value::as_str)?;
    if uri.is_empty() {
        return None;
    }
    let decorations = item.get("decorations");
    let parent = decorations
        .and_then(|d| d.pointer("/parent/entity"))
        .map(name)
        .unwrap_or_default()
        .to_string();
    let is_episode = item.get("entity_type").and_then(Value::as_str) == Some("episode");
    let artists = if is_episode {
        vec![ArtistRef {
            name: parent.clone(),
            ..ArtistRef::default()
        }]
    } else {
        decorations
            .and_then(|d| d.get("creators"))
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|creator| creator.get("entity"))
            .map(|entity| {
                let uri = entity
                    .get("uri")
                    .and_then(Value::as_str)
                    .filter(|uri| !uri.is_empty())
                    .map(str::to_string);
                ArtistRef {
                    id: uri
                        .as_deref()
                        .and_then(crate::util::uri_id)
                        .map(str::to_string),
                    name: name(entity).to_string(),
                    uri,
                }
            })
            .collect()
    };
    let covers: Vec<(&str, &str)> = decorations
        .and_then(|d| d.pointer("/visual_identity/cover"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|cover| {
            Some((
                cover
                    .get("size")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                cover.get("url").and_then(Value::as_str)?,
            ))
        })
        .collect();
    let cover = |sizes: &[&str]| {
        sizes.iter().find_map(|size| {
            covers
                .iter()
                .find(|(found, _)| found == size)
                .map(|(_, url)| (*url).to_string())
        })
    };
    Some(LocalTrack {
        uri: uri.to_string(),
        title: name(item).to_string(),
        artists,
        album: parent,
        // 640 px is what the interface draws at most; xlarge only costs
        // a bigger download and decode.
        art_url: cover(&["large", "default", "xlarge", "small"]),
        art_small_url: cover(&["small", "default", "large", "xlarge"]),
        duration_ms: decorations
            .and_then(|d| d.pointer("/playback/duration_ms"))
            .and_then(Value::as_u64)
            .unwrap_or_default() as u32,
        is_episode,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    const NOW_MS: i64 = 1_747_654_321_000;

    fn item(uri: &str, duration_ms: u64) -> Value {
        json!({
            "uri": uri,
            "entity_type": "track",
            "decorations": {
                "identity": { "name": "My Song" },
                "visual_identity": { "cover": [
                    { "url": "https://i.scdn.co/small", "size": "small" },
                    { "url": "https://i.scdn.co/large", "size": "large" },
                    { "url": "https://i.scdn.co/xlarge", "size": "xlarge" }
                ] },
                "parent": { "entity": {
                    "uri": "spotify:album:a",
                    "decorations": { "identity": { "name": "The Album" } }
                } },
                "creators": [ { "entity": {
                    "uri": "spotify:artist:r",
                    "decorations": { "identity": { "name": "The Artist" } }
                } } ],
                "playback": { "duration_ms": duration_ms }
            }
        })
    }

    fn snapshot(uri: &str, status: &str, position_ms: i64) -> Value {
        json!({
            "type": "playback_state",
            "status": status,
            "item": item(uri, 210_000),
            "position": { "position_ms": position_ms, "timestamp_ms": NOW_MS, "speed": 1.0 },
            "volume": 65,
            "is_active": true,
            "options": { "shuffle": true, "repeat": "context" }
        })
    }

    fn sync(position_ms: i64) -> Value {
        json!({
            "type": "position_sync",
            "position": { "position_ms": position_ms, "timestamp_ms": NOW_MS, "speed": 1.0 }
        })
    }

    #[test]
    fn a_snapshot_fills_the_whole_state() {
        let mut events = Events::default();
        let mut state = LocalState::default();
        let now = Instant::now();
        let applied = events.apply(
            &mut state,
            &snapshot("spotify:track:t", "playing", 45_000),
            now,
            NOW_MS,
        );
        assert!(applied.changed && applied.track_changed);
        let track = state.track.as_ref().unwrap();
        assert_eq!(track.title, "My Song");
        assert_eq!(track.album, "The Album");
        assert_eq!(track.artist_names(), "The Artist");
        assert_eq!(track.artists[0].id.as_deref(), Some("r"));
        assert_eq!(track.art_url.as_deref(), Some("https://i.scdn.co/large"));
        assert_eq!(
            track.art_small_url.as_deref(),
            Some("https://i.scdn.co/small")
        );
        assert_eq!(track.duration_ms, 210_000);
        assert_eq!(state.playback, Playback::Playing);
        assert_eq!(state.position_ms, 45_000);
        assert_eq!(state.position_at, Some(now));
        assert_eq!(super::super::pcm::percent(state.volume), 65);
        assert!(state.shuffle);
        assert_eq!(state.repeat, RepeatMode::Context);
        assert_eq!(state.seek_sequence, 0, "a first position is not a seek");
    }

    #[test]
    fn the_position_moves_on_from_the_timestamp() {
        let mut events = Events::default();
        let mut state = LocalState::default();
        events.apply(
            &mut state,
            &snapshot("spotify:track:t", "playing", 45_000),
            Instant::now(),
            NOW_MS + 300,
        );
        assert_eq!(state.position_ms, 45_300);
    }

    #[test]
    fn a_jump_counts_as_a_seek_and_drift_does_not() {
        let mut events = Events::default();
        let mut state = LocalState::default();
        let now = Instant::now();
        events.apply(
            &mut state,
            &snapshot("spotify:track:t", "playing", 10_000),
            now,
            NOW_MS,
        );
        events.apply(&mut state, &sync(10_400), now, NOW_MS);
        assert_eq!(state.seek_sequence, 0);
        events.apply(&mut state, &sync(90_000), now, NOW_MS);
        assert_eq!(state.seek_sequence, 1);
        assert_eq!(state.position_ms, 90_000);
    }

    #[test]
    fn playing_the_same_track_again_is_a_new_play() {
        let mut events = Events::default();
        let mut state = LocalState::default();
        let now = Instant::now();
        events.apply(
            &mut state,
            &snapshot("spotify:track:t", "playing", 100_000),
            now,
            NOW_MS,
        );
        let first = state.track_sequence;
        // A snapshot of the same track only refreshes it.
        let refreshed = events.apply(
            &mut state,
            &snapshot("spotify:track:t", "playing", 100_500),
            now,
            NOW_MS,
        );
        assert!(!refreshed.track_changed);
        assert_eq!(state.track_sequence, first);
        let replay = events.apply(
            &mut state,
            &json!({ "type": "track_changed", "item": item("spotify:track:t", 210_000) }),
            now,
            NOW_MS,
        );
        assert!(replay.track_changed);
        assert_eq!(state.track_sequence, first + 1);
        assert!(state.replay_pending);
        // Its start position counts as a seek for media controls (#587).
        events.apply(&mut state, &sync(0), now, NOW_MS);
        assert!(!state.replay_pending);
        assert_eq!(state.seek_sequence, 1);
    }

    #[test]
    fn pause_freezes_the_position_and_idle_stops() {
        let mut events = Events::default();
        let mut state = LocalState::default();
        let started = Instant::now() - Duration::from_millis(250);
        events.apply(
            &mut state,
            &snapshot("spotify:track:t", "playing", 1_000),
            started,
            NOW_MS,
        );
        let status = |status: &str| json!({ "type": "playback_changed", "status": status });
        events.apply(&mut state, &status("paused"), Instant::now(), NOW_MS);
        assert_eq!(state.playback, Playback::Paused);
        assert_eq!(state.position_at, None);
        assert!(state.position_ms >= 1_250, "{}", state.position_ms);
        events.apply(&mut state, &status("buffering"), Instant::now(), NOW_MS);
        assert!(state.loading);
        events.apply(&mut state, &status("idle"), Instant::now(), NOW_MS);
        assert_eq!(state.playback, Playback::Stopped);
        assert!(!state.loading);
        assert_eq!(state.position_ms, 0);
    }

    #[test]
    fn a_volume_echo_keeps_the_exact_level_set_here() {
        let mut events = Events::default();
        let mut state = LocalState {
            volume: 40_000,
            ..LocalState::default()
        };
        let percent = super::super::pcm::percent(40_000);
        let echo = json!({ "type": "volume_changed", "volume": percent });
        assert!(
            !events
                .apply(&mut state, &echo, Instant::now(), NOW_MS)
                .changed
        );
        assert_eq!(state.volume, 40_000);
        let moved = json!({ "type": "volume_changed", "volume": 10 });
        assert!(
            events
                .apply(&mut state, &moved, Instant::now(), NOW_MS)
                .changed
        );
        assert_eq!(super::super::pcm::percent(state.volume), 10);
    }

    #[test]
    fn login_and_episodes() {
        let mut events = Events::default();
        let mut state = LocalState::default();
        let login = json!({ "type": "auth_state", "logged_in": true });
        assert!(
            events
                .apply(&mut state, &login, Instant::now(), NOW_MS)
                .changed
        );
        assert!(state.connected);
        let episode = local_track(&json!({
            "uri": "spotify:episode:e",
            "entity_type": "episode",
            "decorations": {
                "identity": { "name": "Episode 1" },
                "parent": { "entity": { "decorations": { "identity": { "name": "The Show" } } } }
            }
        }))
        .unwrap();
        assert!(episode.is_episode);
        assert_eq!(episode.artist_names(), "The Show");
        assert_eq!(episode.album, "The Show");
        assert!(local_track(&json!({ "uri": "" })).is_none());
    }

    #[test]
    fn commands_use_the_api_names_and_repeat_order() {
        let sent = |command: Command| -> Vec<(String, Value)> {
            command
                .messages()
                .into_iter()
                .map(|message| {
                    assert_eq!(message["type"], "command");
                    (message["command"].as_str().unwrap().to_string(), message)
                })
                .collect()
        };
        assert_eq!(sent(Command::Next)[0].0, "skip_next");
        assert_eq!(sent(Command::Seek(5_000))[0].1["position_ms"], 5_000);
        assert_eq!(sent(Command::Volume(250))[0].1["volume"], 100);
        assert_eq!(
            sent(Command::Play(Some("spotify:album:a".into())))[0].1["uri"],
            "spotify:album:a"
        );
        let track = sent(Command::Repeat(RepeatMode::Track));
        assert_eq!(track[0].0, "set_repeat_context");
        assert_eq!(track[0].1["enabled"], false);
        assert_eq!(track[1].0, "set_repeat_track");
        assert_eq!(track[1].1["enabled"], true);
        let context = sent(Command::Repeat(RepeatMode::Context));
        assert_eq!(context[0].0, "set_repeat_track");
        assert_eq!(context[1].1["enabled"], true);
    }
}
