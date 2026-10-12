//! Lossless playback through Spotify Soloist.
//!
//! Soloist is Spotify's own Connect receiver for Linux. Unlike librespot, it
//! is given Spotify's FLAC streams on accounts that have lossless. Spotifast
//! runs it as a helper: a child process on Linux, a WSL1 distribution on
//! Windows. It is driven through its local WebSocket API, and its audio
//! comes back through `soloist/shim`, a stand-in `libpulse.so.0` that sends
//! the PCM over loopback TCP into Spotifast's own output chain, so the
//! equalizer, the visualisers and the output device behave as with
//! librespot.
//!
//! The librespot session stays: it serves lyrics, playlists and the Connect
//! commands Soloist's API lacks, such as a load that starts mid-context.

pub(crate) mod connect;
mod control;
mod install;
mod pcm;
mod prefs;
mod process;
#[cfg(windows)]
mod wsl;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use librespot_playback::mixer::Mixer;
use tokio::sync::{mpsc, watch};

pub(crate) use control::{Command, Shared};
pub(crate) use pcm::{SinkBuilder, percent};
use process::Report;

/// Whether this build can run Soloist: Spotify publishes it for x86_64
/// Linux, which Windows runs through WSL.
pub const AVAILABLE: bool = cfg!(all(
    target_arch = "x86_64",
    any(target_os = "linux", windows)
));

/// What the lossless engine needs from the settings.
#[derive(Clone)]
pub struct Config {
    /// The client key Spotify issues for Soloist, kept in protected storage
    /// and filled in by the backend just before Soloist starts.
    pub key: String,
    /// Where Soloist, the shim and Soloist's data live.
    pub dir: PathBuf,
}

/// Leaves the key out, so logging a configuration never prints it.
impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("key", &if self.key.is_empty() { "unset" } else { "set" })
            .field("dir", &self.dir)
            .finish()
    }
}

/// Installs or updates Soloist ahead of [`Soloist::start`], which then finds
/// it ready. On Windows this also creates its WSL distribution.
pub(crate) async fn install(config: &Config, proxy: &crate::settings::ProxyConfig) -> Result<()> {
    let (dir, proxy) = (config.dir.clone(), proxy.clone());
    tokio::task::spawn_blocking(move || prepare(&dir, &proxy).map(drop))
        .await
        .map_err(|error| anyhow!("{error}"))?
}

/// The libraries Soloist loads from Spotifast, built by
/// `soloist/shim/build.sh`; CI checks they match their sources.
#[cfg(all(target_arch = "x86_64", any(target_os = "linux", windows)))]
pub(crate) const LIBRARIES: [(&str, &[u8]); 2] = [
    (
        "libpulse.so.0",
        include_bytes!("../../soloist/shim/libpulse.so.0"),
    ),
    (
        "libatomic.so.1",
        include_bytes!("../../soloist/shim/libatomic.so.1"),
    ),
];

/// How long Soloist may take to open its ports.
const START_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a new pairing may take to log in.
const PAIR_TIMEOUT: Duration = Duration::from_secs(20);

/// Everything [`Soloist::start`] needs.
pub(crate) struct Start {
    pub config: Config,
    pub proxy: crate::settings::ProxyConfig,
    pub device_name: String,
    pub initial_volume: u16,
    pub sink: SinkBuilder,
    pub mixer: Arc<dyn Mixer>,
    pub held_ms: u32,
    pub shared: Shared,
    /// The account to hand Soloist when it has none yet.
    pub pairing: Option<crate::zeroconf::Credentials>,
}

/// A running Soloist and the connections to it.
pub(crate) struct Soloist {
    control: control::Control,
    device_id: String,
    process: Mutex<Option<process::Process>>,
    ended: watch::Receiver<bool>,
    notify: crate::player::Notify,
    /// Dropped after the process, so Soloist never writes into a closed port.
    _pcm: pcm::Receiver,
}

impl Soloist {
    pub(crate) async fn start(start: Start) -> Result<Self> {
        if !AVAILABLE {
            bail!("Spotify Soloist does not run on this system");
        }
        let Start {
            config,
            proxy,
            device_name,
            initial_volume,
            sink,
            mixer,
            held_ms,
            shared,
            pairing,
        } = start;
        let launch = {
            let dir = config.dir.clone();
            tokio::task::spawn_blocking(move || prepare(&dir, &proxy))
                .await
                .map_err(|error| anyhow!("{error}"))??
        };
        let pcm = pcm::Receiver::start(
            sink,
            pcm::Hooks {
                audio: Arc::clone(&shared.audio),
                mixer,
                held_ms,
            },
        )
        .context("cannot open the port Soloist's audio comes back on")?;

        let state = Arc::clone(&shared.state);
        let notify = Arc::clone(&shared.notify);
        let shared = Arc::new(shared);
        let mut attempt = 0;
        loop {
            attempt += 1;
            prefs::apply(&launch.data_host)?;
            let volume_percent = if attempt == 1 {
                pcm::percent(initial_volume)
            } else {
                pcm::percent(state.lock().unwrap_or_else(PoisonError::into_inner).volume)
            };
            let (process, mut reports) = process::spawn(
                &launch.process,
                &process::Args {
                    name: &device_name,
                    key: &config.key,
                    volume_percent,
                    pcm_port: pcm.port(),
                    cache_mb: 100,
                },
            )?;
            let mut process = Some(process);
            let ports = tokio::time::timeout(START_TIMEOUT, ports(&mut reports))
                .await
                .map_err(|_| anyhow!("Spotify Soloist did not start in time"))??;
            let (control, task) = control::connect(ports.control, Arc::clone(&shared)).await?;
            // Running from now on, so the login below reaches the state.
            let task = tokio::spawn(task);
            let mut info = pairing_info(ports.pairing).await?;
            // Soloist signs in with the account it kept a moment after it
            // starts listening, and reports no user until then.
            let mut signed_in = false;
            if info.active_user.is_empty() && prefs::has_account(&launch.data_host) {
                signed_in = wait_logged_in(&state).await.is_ok();
                if signed_in {
                    info = pairing_info(ports.pairing).await?;
                }
            }
            let wanted = pairing.as_ref().map(|credentials| &credentials.username);
            let other_user = !info.active_user.is_empty()
                && wanted.is_some_and(|user| !user.eq_ignore_ascii_case(&info.active_user));
            let unpaired = info.active_user.is_empty() && !signed_in;
            if (unpaired || other_user) && attempt == 1 {
                let Some(credentials) = &pairing else {
                    bail!("Spotify Soloist has no account yet");
                };
                pair(ports.pairing, &info, credentials, &device_name).await?;
                wait_user(ports.pairing, &credentials.username).await?;
                // Soloist wrote the new account's preferences when it
                // logged in; they take effect on its next start.
                if let Some(mut process) = process.take() {
                    process.stop();
                }
                drop(control);
                let _ = task.await;
                continue;
            }
            let (ended_tx, ended) = watch::channel(false);
            tokio::spawn(async move {
                tokio::select! {
                    _ = task => {}
                    () = exited(reports) => {}
                }
                let _ = ended_tx.send(true);
            });
            log::info!("soloist: running, Connect device {}", info.device_id);
            return Ok(Self {
                control,
                device_id: info.device_id,
                process: Mutex::new(process),
                ended,
                notify,
                _pcm: pcm,
            });
        }
    }

    pub(crate) fn send(&self, command: Command) -> Result<()> {
        self.control.send(command)
    }

    /// Soloist's Spotify Connect device id.
    pub(crate) fn device_id(&self) -> &str {
        &self.device_id
    }

    /// Completes when Soloist exits or its control connection closes.
    pub(crate) fn on_end(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut ended = self.ended.clone();
        async move {
            let _ = ended.wait_for(|ended| *ended).await;
        }
    }

    /// Where engine state goes, for reports from outside the control
    /// connection.
    pub(crate) fn report(&self) -> crate::player::Notify {
        Arc::clone(&self.notify)
    }

    pub(crate) fn shutdown(&self) {
        if let Some(mut process) = self
            .process
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            process.stop();
        }
    }
}

impl Drop for Soloist {
    fn drop(&mut self) {
        self.shutdown();
    }
}

struct Ports {
    control: u16,
    pairing: u16,
}

async fn ports(reports: &mut mpsc::UnboundedReceiver<Report>) -> Result<Ports> {
    let (mut control, mut pairing) = (None, None);
    while let Some(report) = reports.recv().await {
        match report {
            Report::Control(port) => control = Some(port),
            Report::Pairing(port) => pairing = Some(port),
            Report::ExpiresIn(days) => log::info!("soloist: this build expires in {days} days"),
            Report::Exited => break,
        }
        if let (Some(control), Some(pairing)) = (control, pairing) {
            return Ok(Ports { control, pairing });
        }
    }
    bail!("Spotify Soloist stopped while starting; its log is at debug level")
}

async fn exited(mut reports: mpsc::UnboundedReceiver<Report>) {
    while let Some(report) = reports.recv().await {
        if report == Report::Exited {
            return;
        }
    }
}

fn local_receiver(port: u16) -> crate::zeroconf::Receiver {
    crate::zeroconf::Receiver {
        name: String::new(),
        device_id: None,
        address: std::net::Ipv4Addr::LOCALHOST.into(),
        port,
        path: "/zc".into(),
    }
}

fn loopback_client() -> Result<reqwest::blocking::Client> {
    reqwest::blocking::Client::builder()
        .no_proxy()
        .build()
        .context("no HTTP client")
}

async fn pairing_info(port: u16) -> Result<crate::zeroconf::Info> {
    tokio::task::spawn_blocking(move || {
        crate::zeroconf::get_info(&loopback_client()?, &local_receiver(port))
    })
    .await
    .map_err(|error| anyhow!("{error}"))?
    .context("Spotify Soloist did not describe itself")
}

async fn pair(
    port: u16,
    info: &crate::zeroconf::Info,
    credentials: &crate::zeroconf::Credentials,
    name: &str,
) -> Result<()> {
    let (info, credentials, name) = (info.clone(), credentials.clone(), name.to_string());
    tokio::task::spawn_blocking(move || {
        crate::zeroconf::add_user(
            &loopback_client()?,
            &local_receiver(port),
            &info,
            &credentials,
            &name,
        )
    })
    .await
    .map_err(|error| anyhow!("{error}"))?
    .context("Spotify Soloist did not take the account")
}

async fn wait_logged_in(state: &Mutex<crate::player::LocalState>) -> Result<()> {
    let deadline = tokio::time::Instant::now() + PAIR_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        if state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connected
        {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    bail!("Spotify Soloist did not log in with this account")
}

/// Waits until Soloist reports `user` as its account, after a pairing.
async fn wait_user(port: u16, user: &str) -> Result<()> {
    let deadline = tokio::time::Instant::now() + PAIR_TIMEOUT;
    while tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(250)).await;
        if pairing_info(port)
            .await
            .is_ok_and(|info| info.active_user.eq_ignore_ascii_case(user))
        {
            return Ok(());
        }
    }
    bail!("Spotify Soloist did not log in with this account")
}

/// Soloist, ready to start, and where its data is from Spotifast's side.
struct Prepared {
    process: process::Launch,
    data_host: PathBuf,
}

#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
fn prepare(dir: &Path, proxy: &crate::settings::ProxyConfig) -> Result<Prepared> {
    let build = install::ensure(&dir.join("bin"), proxy)?;
    let lib = dir.join("lib");
    let data = dir.join("data");
    let cache = dir.join("cache");
    for folder in [&lib, &data, &cache] {
        std::fs::create_dir_all(folder)
            .with_context(|| format!("cannot create {}", folder.display()))?;
    }
    write_libraries(&lib)?;
    let text = |path: &Path| path.to_string_lossy().into_owned();
    Ok(Prepared {
        process: process::Launch {
            program: build.binary,
            prefix: Vec::new(),
            env: vec![("LD_LIBRARY_PATH".into(), text(&lib))],
            data: text(&data),
            cache: text(&cache),
            distro: None,
        },
        data_host: data,
    })
}

/// Puts the shim libraries in `lib`, rewriting only what differs.
#[cfg(all(target_arch = "x86_64", any(target_os = "linux", windows)))]
fn write_libraries(lib: &Path) -> Result<()> {
    for (name, bytes) in LIBRARIES {
        let path = lib.join(name);
        if std::fs::read(&path).ok().as_deref() != Some(bytes) {
            install::write_atomically(&path, bytes)?;
        }
    }
    Ok(())
}

#[cfg(all(target_arch = "x86_64", windows))]
fn prepare(dir: &Path, proxy: &crate::settings::ProxyConfig) -> Result<Prepared> {
    wsl::prepare(dir, proxy)
}

#[cfg(not(all(target_arch = "x86_64", any(target_os = "linux", windows))))]
fn prepare(_: &Path, _: &crate::settings::ProxyConfig) -> Result<Prepared> {
    bail!("Spotify Soloist does not run on this system")
}

/// Runs a real Soloist that is already paired, playing into a silent sink
/// paced like an output device. Nothing is heard. Set `SOLOIST_LAB_DIR` to a
/// Soloist folder whose `data` holds the paired account, and
/// `SOLOIST_LAB_KEY_FILE` to a file holding the client key. On Windows the
/// folder is a Windows path and Soloist runs in its WSL distribution:
///
/// `cargo test --lib soloist::lab -- --ignored --nocapture`
#[cfg(all(test, target_arch = "x86_64", any(target_os = "linux", windows)))]
mod lab {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    use librespot_playback::audio_backend::{Sink, SinkResult};
    use librespot_playback::convert::Converter;
    use librespot_playback::decoder::AudioPacket;
    use librespot_playback::mixer::{self, MixerConfig};

    use super::*;
    use crate::player::{EngineEvent, LocalState, Playback};

    #[derive(Default)]
    struct Heard {
        frames: AtomicU64,
        /// The loudest sample, as `f64` bits.
        peak: AtomicU64,
    }

    /// Takes audio as fast as a 44.1 kHz device would, and keeps nothing.
    struct Silent {
        heard: Arc<Heard>,
        clock: Option<(Instant, u64)>,
    }

    impl Sink for Silent {
        fn stop(&mut self) -> SinkResult<()> {
            self.clock = None;
            Ok(())
        }

        fn write(&mut self, packet: AudioPacket, _: &mut Converter) -> SinkResult<()> {
            let AudioPacket::Samples(samples) = packet else {
                return Ok(());
            };
            let frames = (samples.len() / 2) as u64;
            let peak = samples
                .iter()
                .fold(0f64, |peak, sample| peak.max(sample.abs()));
            self.heard.peak.fetch_max(peak.to_bits(), Ordering::Relaxed);
            let total = self.heard.frames.fetch_add(frames, Ordering::Relaxed) + frames;
            let (start, base) = *self.clock.get_or_insert((Instant::now(), total - frames));
            let due = start + Duration::from_secs_f64((total - base) as f64 / 44_100.0);
            // A device holds about 100 ms; write returns once that is full.
            if let Some(wait) =
                due.checked_duration_since(Instant::now() + Duration::from_millis(100))
            {
                std::thread::sleep(wait);
            }
            Ok(())
        }
    }

    fn state(shared: &Mutex<LocalState>) -> LocalState {
        shared
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Prints Spotifast's own log, with the client key masked.
    struct Redacted(String);

    impl log::Log for Redacted {
        fn enabled(&self, metadata: &log::Metadata) -> bool {
            metadata.target().starts_with("spotifast")
        }

        fn log(&self, record: &log::Record) {
            if self.enabled(record.metadata()) {
                let line = record.args().to_string().replace(&self.0, "<key>");
                eprintln!("{} {line}", record.level());
            }
        }

        fn flush(&self) {}
    }

    async fn until(what: &str, seconds: u64, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(seconds);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        eprintln!("lab: {what}");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs a paired Soloist and its client key"]
    async fn plays_through_a_paired_soloist() {
        let dir = PathBuf::from(std::env::var("SOLOIST_LAB_DIR").expect("SOLOIST_LAB_DIR"));
        let key = std::fs::read_to_string(
            std::env::var("SOLOIST_LAB_KEY_FILE").expect("SOLOIST_LAB_KEY_FILE"),
        )
        .unwrap()
        .trim()
        .to_string();
        assert!(key.len() > 8, "SOLOIST_LAB_KEY_FILE holds no key");
        if std::env::var_os("SOLOIST_LAB_LOG").is_some() {
            let _ = log::set_boxed_logger(Box::new(Redacted(key.clone())));
            log::set_max_level(log::LevelFilter::Trace);
        }
        let track = std::env::var("SOLOIST_LAB_TRACK")
            .unwrap_or_else(|_| "spotify:track:2tdNI4jmNQkOT41LSTSA17".into());
        let shared = Arc::new(Mutex::new(LocalState {
            volume: 30_000,
            ..LocalState::default()
        }));
        let heard = Arc::new(Heard::default());
        let mixer = mixer::find(Some("softvol")).unwrap()(MixerConfig::default()).unwrap();
        let sink: SinkBuilder = {
            let heard = Arc::clone(&heard);
            Box::new(move || Box::new(Silent { heard, clock: None }) as Box<dyn Sink>)
        };
        let notify: crate::player::Notify = Arc::new(|event| {
            if let EngineEvent::State(state) = event {
                log::trace!("lab: {:?} {:?}", state.playback, state.position_ms);
            }
        });
        let started = Instant::now();
        let soloist = Soloist::start(Start {
            config: Config { key, dir },
            proxy: crate::settings::ProxyConfig::Off,
            device_name: "Spotifast Lab".into(),
            initial_volume: 30_000,
            sink,
            mixer: Arc::clone(&mixer),
            held_ms: 230,
            shared: Shared {
                state: Arc::clone(&shared),
                notify,
                audio: crate::sink::AudioControl::new(100),
            },
            pairing: None,
        })
        .await
        .unwrap();
        eprintln!(
            "lab: started in {} ms, device {}",
            started.elapsed().as_millis(),
            soloist.device_id()
        );
        assert!(!soloist.device_id().is_empty());
        until("logged in", 10, || state(&shared).connected).await;

        soloist.send(Command::Play(Some(track.clone()))).unwrap();
        // Soloist can report that it plays a moment before it names the
        // track, so wait for both.
        until("playing the track", 20, || {
            let now = state(&shared);
            now.playback == Playback::Playing
                && now
                    .track
                    .as_ref()
                    .is_some_and(|playing| playing.uri == track)
        })
        .await;
        until("three seconds of audio", 20, || {
            heard.frames.load(Ordering::Relaxed) > 3 * 44_100
        })
        .await;
        let peak = f64::from_bits(heard.peak.load(Ordering::Relaxed));
        assert!(peak > 0.01, "silence: peak {peak}");

        soloist.send(Command::Pause).unwrap();
        until("paused", 10, || state(&shared).playback == Playback::Paused).await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        let paused_at = heard.frames.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(1)).await;
        let leaked = heard.frames.load(Ordering::Relaxed) - paused_at;
        assert!(leaked < 44_100 / 5, "{leaked} frames played while paused");

        let seeks = state(&shared).seek_sequence;
        soloist.send(Command::Seek(60_000)).unwrap();
        soloist.send(Command::Play(None)).unwrap();
        until("seeked and playing", 15, || {
            let now = state(&shared);
            now.seek_sequence > seeks
                && now.position_now() >= 60_000
                && now.playback == Playback::Playing
        })
        .await;

        soloist.send(Command::Volume(20)).unwrap();
        until("volume reached the mixer", 10, || {
            pcm::percent(mixer.volume()) == 20
        })
        .await;

        soloist.send(Command::Pause).unwrap();
        until("paused again", 10, || {
            state(&shared).playback == Playback::Paused
        })
        .await;
        let ended = soloist.on_end();
        soloist.shutdown();
        tokio::time::timeout(Duration::from_secs(10), ended)
            .await
            .expect("Soloist did not stop");
        eprintln!(
            "lab: {} frames heard, peak {peak:.3}",
            heard.frames.load(Ordering::Relaxed)
        );
    }
}
