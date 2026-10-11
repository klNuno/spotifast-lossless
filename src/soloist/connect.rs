//! Commands Soloist's local API lacks, sent as Spotify Connect commands over
//! the librespot session: a load that starts mid-context or mid-track, a
//! paused load, and episodes in the queue.

use anyhow::{Result, anyhow, bail};
use librespot_core::session::Session;
use serde_json::{Map, Value, json};

use crate::player::{LoadSpec, RepeatMode};

/// A load Soloist's own `play` does as well: one URI, from its start,
/// playing, with nothing to override.
pub(crate) fn simple_play(spec: &LoadSpec) -> Option<String> {
    if !spec.play
        || spec.autoplay
        || spec.position_ms > 0
        || spec.offset_uri.is_some()
        || spec.offset_index.is_some_and(|index| index > 0)
        || spec.shuffle.is_some()
        || spec.repeat.is_some()
    {
        return None;
    }
    match (&spec.context_uri, spec.uris.as_slice()) {
        (Some(context), []) => Some(context.clone()),
        (None, [uri]) => Some(uri.clone()),
        _ => None,
    }
}

/// The Connect `play` command for a load, as Spotify's own clients send it.
pub(crate) fn play_command(spec: &LoadSpec, current_repeat: RepeatMode) -> Result<Value> {
    let single = spec
        .context_uri
        .as_deref()
        .filter(|uri| matches!(crate::util::uri_kind(uri), Some("track" | "episode")));
    let context = match (single, &spec.context_uri) {
        (None, Some(uri)) => {
            json!({ "uri": uri, "url": format!("context://{uri}"), "metadata": {} })
        }
        _ => {
            let uris: Vec<&str> = match single {
                Some(uri) => vec![uri],
                None => spec.uris.iter().map(String::as_str).collect(),
            };
            if uris.is_empty() {
                bail!("nothing to play");
            }
            let tracks: Vec<Value> = uris.iter().map(|uri| json!({ "uri": uri })).collect();
            json!({ "uri": "", "url": "", "pages": [{ "tracks": tracks }], "metadata": {} })
        }
    };
    let mut skip_to = Map::new();
    if let Some(uri) = &spec.offset_uri {
        skip_to.insert("track_uri".into(), json!(uri));
    } else if let Some(index) = spec.offset_index {
        skip_to.insert("track_index".into(), json!(index));
    }
    // A load resets the player's options; carry the user's repeat over.
    let repeat = spec.repeat.unwrap_or(current_repeat);
    let mut options = json!({
        "license": "on-demand",
        "skip_to": Value::Object(skip_to),
        "player_options_override": {
            "shuffling_context": spec.shuffle.unwrap_or(false),
            "repeating_context": repeat == RepeatMode::Context,
            "repeating_track": repeat == RepeatMode::Track,
        },
    });
    if spec.position_ms > 0 {
        options["seek_to"] = json!(spec.position_ms);
    }
    if !spec.play {
        options["initially_paused"] = json!(true);
    }
    Ok(json!({
        "command": {
            "endpoint": "play",
            "context": context,
            "play_origin": { "feature_identifier": "spotifast" },
            "options": options,
        }
    }))
}

/// Queues an episode, which Soloist's local API refuses.
pub(crate) fn add_to_queue_command(uri: &str) -> Value {
    json!({ "command": { "endpoint": "add_to_queue", "track": { "uri": uri } } })
}

/// Sends a player command to Soloist. The librespot session is not a
/// Connect device, so the command names Soloist as its sender first, then
/// the session's own id.
pub(crate) async fn command(session: &Session, soloist: &str, body: &Value) -> Result<()> {
    let body = body.to_string();
    let own = session.device_id().to_string();
    let mut failure = None;
    for from in [soloist, own.as_str()] {
        let path = format!("/connect-state/v1/player/command/from/{from}/to/{soloist}");
        match session
            .spclient()
            .request(&reqwest::Method::POST, &path, None, Some(body.as_bytes()))
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => {
                log::debug!("soloist: Connect command from {from} failed: {error}");
                failure = Some(error);
            }
        }
    }
    Err(anyhow!(
        "Spotify did not pass the command to Soloist: {}",
        failure.map(|error| error.to_string()).unwrap_or_default()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plays_simple_loads_through_the_local_api() {
        let album = LoadSpec {
            context_uri: Some("spotify:album:a".into()),
            play: true,
            ..LoadSpec::default()
        };
        assert_eq!(simple_play(&album).as_deref(), Some("spotify:album:a"));
        let track = LoadSpec {
            uris: vec!["spotify:track:t".into()],
            play: true,
            ..LoadSpec::default()
        };
        assert_eq!(simple_play(&track).as_deref(), Some("spotify:track:t"));
        for spec in [
            LoadSpec {
                offset_uri: Some("spotify:track:t".into()),
                ..album.clone()
            },
            LoadSpec {
                position_ms: 1,
                ..album.clone()
            },
            LoadSpec {
                play: false,
                ..album.clone()
            },
            LoadSpec {
                shuffle: Some(true),
                ..album.clone()
            },
            LoadSpec {
                uris: vec!["spotify:track:a".into(), "spotify:track:b".into()],
                play: true,
                ..LoadSpec::default()
            },
        ] {
            assert_eq!(simple_play(&spec), None, "{spec:?}");
        }
    }

    #[test]
    fn starts_a_context_at_a_track_and_position() {
        let spec = LoadSpec {
            context_uri: Some("spotify:playlist:p".into()),
            offset_uri: Some("spotify:track:t".into()),
            position_ms: 61_000,
            play: false,
            shuffle: Some(true),
            ..LoadSpec::default()
        };
        let body = play_command(&spec, RepeatMode::Track).unwrap();
        let command = &body["command"];
        assert_eq!(command["endpoint"], "play");
        assert_eq!(command["context"]["uri"], "spotify:playlist:p");
        assert_eq!(command["context"]["url"], "context://spotify:playlist:p");
        let options = &command["options"];
        assert_eq!(options["skip_to"]["track_uri"], "spotify:track:t");
        assert_eq!(options["seek_to"], 61_000);
        assert_eq!(options["initially_paused"], true);
        assert_eq!(
            options["player_options_override"]["shuffling_context"],
            true
        );
        assert_eq!(options["player_options_override"]["repeating_track"], true);
        assert_eq!(
            options["player_options_override"]["repeating_context"],
            false
        );
    }

    #[test]
    fn plays_a_list_of_tracks_as_one_page() {
        let spec = LoadSpec {
            uris: vec!["spotify:track:a".into(), "spotify:episode:b".into()],
            offset_index: Some(1),
            play: true,
            ..LoadSpec::default()
        };
        let body = play_command(&spec, RepeatMode::Off).unwrap();
        let context = &body["command"]["context"];
        assert_eq!(context["uri"], "");
        assert_eq!(context["pages"][0]["tracks"][1]["uri"], "spotify:episode:b");
        assert_eq!(body["command"]["options"]["skip_to"]["track_index"], 1);
        assert!(body["command"]["options"].get("seek_to").is_none());
        assert!(body["command"]["options"].get("initially_paused").is_none());
        assert!(play_command(&LoadSpec::default(), RepeatMode::Off).is_err());
    }
}
