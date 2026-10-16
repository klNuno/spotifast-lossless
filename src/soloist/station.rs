//! Lets a Spotify app pair Soloist, the one way Spotify accepts.
//!
//! Soloist keeps an account only when Spotify's own controller signed it in,
//! and its pairing endpoint stays on this computer (on Windows, inside the
//! machine), where no Spotify app can see it. While lossless waits for that
//! pairing, the station runs a Soloist with no account, relays its endpoint
//! to the local network and announces it over mDNS, as any Spotify Connect
//! speaker does. Picking it once in a Spotify app signs it in; Soloist keeps
//! that session in its data, the station closes, and the next engine plays
//! lossless.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use librespot_playback::audio_backend::{Sink, SinkResult};
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use librespot_playback::mixer::{Mixer, MixerConfig, softmixer::SoftMixer};
use tokio::sync::{OwnedMutexGuard, mpsc, oneshot};

use super::process::{self, Report};
use super::{Config, control, pcm};

/// What Spotify apps look for on the network.
const SERVICE: &str = "_spotify-connect._tcp.local.";
/// How often the station looks at whether an account signed in. Only the
/// control connection is read: every `getInfo` makes Soloist fetch a new
/// `authInfo`, which would void the one a Spotify app is pairing with.
const POLL: Duration = Duration::from_millis(250);

/// Everything a station needs.
pub(crate) struct StationStart {
    pub config: Config,
    pub proxy: crate::settings::ProxyConfig,
    /// What Spotify apps list it as.
    pub name: String,
    /// The account lossless plays for. Another account signing in is not a
    /// pairing for this one.
    pub user: String,
    /// Relay and announce on the local network. Off, the station stays on
    /// this computer.
    pub network: bool,
}

/// What a pairing station tells whoever opened it.
#[derive(Debug, PartialEq, Eq)]
pub enum StationEvent {
    /// Listening under `name`, its relay on `port`.
    Listed { name: String, port: u16 },
    /// The account signed in; Soloist kept it and has stopped.
    Paired,
    /// The station could not open, or Soloist stopped.
    Failed(String),
}

/// A running station. Dropping it closes the station.
pub(crate) struct Station {
    stop: Option<oneshot::Sender<()>>,
}

impl Station {
    /// Opens a station once `start` is ready; `report` hears what happens.
    pub(crate) fn open(
        start: impl Future<Output = Result<StationStart>> + Send + 'static,
        report: impl Fn(StationEvent) + Send + 'static,
    ) -> Self {
        let (stop, stopped) = oneshot::channel();
        tokio::spawn(run(start, stopped, report));
        Self { stop: Some(stop) }
    }
}

impl Drop for Station {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

async fn run(
    start: impl Future<Output = Result<StationStart>>,
    mut stopped: oneshot::Receiver<()>,
    report: impl Fn(StationEvent),
) {
    let opened = tokio::select! {
        biased;
        _ = &mut stopped => return,
        opened = open(start) => opened,
    };
    let mut open = match opened {
        Ok(open) => open,
        Err(error) => {
            log::warn!("soloist station: {error:#}");
            report(StationEvent::Failed(format!("{error:#}")));
            return;
        }
    };
    log::info!(
        "soloist station: listed as {:?} on port {}",
        open.name,
        open.relay_port
    );
    report(StationEvent::Listed {
        name: open.name.clone(),
        port: open.relay_port,
    });
    let signed_in = tokio::select! {
        biased;
        _ = &mut stopped => None,
        signed_in = open.signed_in() => Some(signed_in),
    };
    let dir = open.dir.clone();
    open.close().await;
    match signed_in {
        None => log::info!("soloist station: closed"),
        Some(Ok(())) => {
            log::info!("soloist station: paired");
            super::forget_refused(&dir);
            report(StationEvent::Paired);
        }
        Some(Err(error)) => {
            log::warn!("soloist station: {error:#}");
            report(StationEvent::Failed(format!("{error:#}")));
        }
    }
}

/// A station's Soloist and what makes it reachable. Fields drop in order:
/// the announcement and the relay go first, the slot last.
struct Open {
    name: String,
    user: String,
    dir: PathBuf,
    relay_port: u16,
    advert: Option<Advert>,
    relay: tokio::task::JoinHandle<()>,
    pairing: u16,
    state: Arc<Mutex<crate::player::LocalState>>,
    control: Option<control::Control>,
    reports: mpsc::UnboundedReceiver<Report>,
    process: Option<process::Process>,
    _pcm: pcm::Receiver,
    _slot: OwnedMutexGuard<()>,
}

async fn open(start: impl Future<Output = Result<StationStart>>) -> Result<Open> {
    let StationStart {
        config,
        proxy,
        name,
        user,
        network,
    } = start.await?;
    let slot = super::take_slot()
        .await
        .context("another Spotify Soloist is running")?;
    let launch = {
        let dir = config.dir.clone();
        tokio::task::spawn_blocking(move || super::prepare(&dir, &proxy))
            .await
            .map_err(|error| anyhow!("{error}"))??
    };
    let mixer: Arc<dyn Mixer> =
        Arc::new(SoftMixer::open(MixerConfig::default()).map_err(|error| anyhow!("{error}"))?);
    let pcm = pcm::Receiver::start(
        Box::new(|| Box::new(Discard::default()) as Box<dyn Sink>),
        pcm::Hooks {
            audio: crate::sink::AudioControl::new(100),
            mixer,
            held_ms: 0,
            lossless: Arc::new(|_| {}),
        },
    )
    .context("cannot open the port Soloist's audio comes back on")?;
    let (process, reports, ports) = launch
        .run(&process::Args {
            name: &name,
            key: &config.key,
            volume_percent: 50,
            pcm_port: pcm.port(),
            cache_mb: 100,
        })
        .await?;
    let state = Arc::new(Mutex::new(crate::player::LocalState::default()));
    let (control, task) = control::connect(
        ports.control,
        Arc::new(control::Shared {
            state: Arc::clone(&state),
            notify: Arc::new(|_| {}),
            audio: crate::sink::AudioControl::new(100),
        }),
    )
    .await?;
    tokio::spawn(task);
    let bind = if network {
        Ipv4Addr::UNSPECIFIED
    } else {
        Ipv4Addr::LOCALHOST
    };
    let listener = tokio::net::TcpListener::bind((bind, 0))
        .await
        .context("cannot open the pairing relay")?;
    let relay_port = listener.local_addr()?.port();
    let relay = tokio::spawn(relay(listener, ports.pairing));
    let advert = if network {
        Some(Advert::announce(&name, relay_port)?)
    } else {
        None
    };
    Ok(Open {
        name,
        user,
        dir: config.dir,
        relay_port,
        advert,
        relay,
        pairing: ports.pairing,
        state,
        control: Some(control),
        reports,
        process: Some(process),
        _pcm: pcm,
        _slot: slot,
    })
}

impl Open {
    /// Completes once this station's account signed in, or fails when
    /// Soloist stops. Another account signing in waits for it to leave.
    async fn signed_in(&mut self) -> Result<()> {
        let mut other = false;
        loop {
            tokio::select! {
                report = self.reports.recv() => match report {
                    None | Some(Report::Exited) => bail!("Spotify Soloist stopped"),
                    Some(_) => continue,
                },
                () = tokio::time::sleep(POLL) => {}
            }
            let connected = self
                .state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .connected;
            if !connected {
                other = false;
                continue;
            }
            if other {
                continue;
            }
            // Signed in: nobody is pairing now, so asking is safe.
            let info = super::pairing_info(self.pairing).await?;
            if info.active_user.eq_ignore_ascii_case(&self.user) {
                return Ok(());
            }
            if !info.active_user.is_empty() {
                log::info!("soloist station: another account signed in; waiting for this one");
                other = true;
            }
        }
    }

    /// Withdraws the announcement, then stops Soloist and lets its data be.
    async fn close(mut self) {
        if let Some(advert) = self.advert.take() {
            let _ = tokio::task::spawn_blocking(move || advert.withdraw()).await;
        }
        self.relay.abort();
        drop(self.control.take());
        if let Some(process) = self.process.take() {
            let _ = tokio::task::spawn_blocking(move || drop(process)).await;
        }
    }
}

/// Hands each connection on to Soloist's pairing endpoint.
async fn relay(listener: tokio::net::TcpListener, pairing: u16) {
    let target = SocketAddr::from((Ipv4Addr::LOCALHOST, pairing));
    loop {
        let Ok((mut outside, from)) = listener.accept().await else {
            return;
        };
        tokio::spawn(async move {
            let Ok(mut inside) = tokio::net::TcpStream::connect(target).await else {
                return;
            };
            log::debug!("soloist station: pairing request from {from}");
            let _ = tokio::io::copy_bidirectional(&mut outside, &mut inside).await;
        });
    }
}

/// The station's mDNS announcement.
struct Advert {
    daemon: mdns_sd::ServiceDaemon,
    fullname: String,
}

impl Advert {
    fn announce(name: &str, port: u16) -> Result<Self> {
        let daemon = mdns_sd::ServiceDaemon::new().context("cannot announce on the network")?;
        // The relay listens on IPv4 only.
        let _ = daemon.disable_interface(mdns_sd::IfKind::IPv6);
        let info = mdns_sd::ServiceInfo::new(
            SERVICE,
            name,
            &host_name(),
            "",
            port,
            &[("CPath", "/zc"), ("VERSION", "1.0"), ("Stack", "SP")][..],
        )
        .context("cannot describe the station")?
        .enable_addr_auto();
        let fullname = info.get_fullname().to_string();
        daemon
            .register(info)
            .context("cannot announce on the network")?;
        Ok(Self { daemon, fullname })
    }

    /// Says goodbye, so apps drop it now rather than when it times out.
    fn withdraw(self) {
        if let Ok(done) = self.daemon.unregister(&self.fullname) {
            let _ = done.recv_timeout(Duration::from_secs(2));
        }
        let _ = self.daemon.shutdown();
    }
}

/// A host name of the station's own, so two computers announcing at once
/// do not answer for each other's addresses.
fn host_name() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.subsec_nanos());
    format!("spotifast-{:x}-{nanos:x}.local.", std::process::id())
}

/// Takes the audio a Spotify app might start on the station at a device's
/// pace, and keeps none of it: nothing plays until lossless does.
#[derive(Default)]
struct Discard {
    clock: Option<(Instant, u64)>,
    frames: u64,
}

impl Sink for Discard {
    fn stop(&mut self) -> SinkResult<()> {
        self.clock = None;
        Ok(())
    }

    fn write(&mut self, packet: AudioPacket, _: &mut Converter) -> SinkResult<()> {
        let AudioPacket::Samples(samples) = packet else {
            return Ok(());
        };
        self.frames += (samples.len() / librespot_playback::NUM_CHANNELS as usize) as u64;
        let (start, base) = *self.clock.get_or_insert((Instant::now(), self.frames));
        let due = start
            + Duration::from_secs_f64(
                (self.frames - base) as f64 / f64::from(librespot_playback::SAMPLE_RATE),
            );
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            std::thread::sleep(wait);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The relay hands a request to the pairing endpoint and its answer back.
    #[tokio::test]
    async fn the_relay_reaches_the_pairing_endpoint() {
        let endpoint = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let pairing = endpoint.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = endpoint.accept().await.unwrap();
            let mut request = [0u8; 5];
            stream.read_exact(&mut request).await.unwrap();
            assert_eq!(&request, b"hello");
            stream.write_all(b"paired").await.unwrap();
        });
        let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let relay = tokio::spawn(relay(listener, pairing));

        let mut client = tokio::net::TcpStream::connect((Ipv4Addr::LOCALHOST, port))
            .await
            .unwrap();
        client.write_all(b"hello").await.unwrap();
        let mut answer = Vec::new();
        client.read_to_end(&mut answer).await.unwrap();
        assert_eq!(answer, b"paired");
        relay.abort();
    }

    /// The station's host name is a valid `.local.` name, and another
    /// process gets another one.
    #[test]
    fn the_host_name_is_local_and_its_own() {
        let name = host_name();
        assert!(name.starts_with("spotifast-") && name.ends_with(".local."));
        assert!(name.contains(&format!("{:x}", std::process::id())));
        let info = mdns_sd::ServiceInfo::new(
            SERVICE,
            "Spotifast Lossless",
            &name,
            "",
            4071,
            &[("CPath", "/zc"), ("VERSION", "1.0"), ("Stack", "SP")][..],
        )
        .unwrap();
        assert_eq!(
            info.get_fullname(),
            "Spotifast Lossless._spotify-connect._tcp.local."
        );
        assert_eq!(info.get_property_val_str("CPath"), Some("/zc"));
    }

    /// Dropping a station before it opens stops it without a word.
    #[tokio::test]
    async fn a_station_closed_before_it_opens_reports_nothing() {
        let heard = Arc::new(Mutex::new(Vec::new()));
        let (ready, wait) = oneshot::channel::<()>();
        let station = Station::open(
            async move {
                let _ = wait.await;
                bail!("never started")
            },
            {
                let heard = Arc::clone(&heard);
                move |event| heard.lock().unwrap().push(event)
            },
        );
        drop(station);
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = ready.send(());
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(heard.lock().unwrap().is_empty());
    }

    /// A station that cannot open says why.
    #[tokio::test]
    async fn a_station_that_cannot_open_says_why() {
        let (heard, mut events) = mpsc::unbounded_channel();
        let _station = Station::open(async { bail!("no key") }, move |event| {
            let _ = heard.send(event);
        });
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event, StationEvent::Failed("no key".into()));
    }
}

/// Opens a station on a real Soloist with no account and checks that it
/// answers as a Spotify Connect speaker would, then that closing it stops
/// Soloist. `SOLOIST_STATION_DIR` is a Soloist folder with no account (a
/// fresh one downloads Soloist), `SOLOIST_LAB_KEY_FILE` holds the client key,
/// and on Windows `SPOTIFAST_VM_DIR` names the machine's files.
/// `SOLOIST_STATION_NETWORK=1` also relays and announces on the network and
/// finds the station over mDNS, as a Spotify app would:
///
/// `cargo test --lib soloist::station::lab -- --ignored --nocapture`
#[cfg(all(test, target_arch = "x86_64", any(target_os = "linux", windows)))]
mod lab {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    #[ignore = "needs Soloist's client key"]
    async fn a_station_answers_like_a_speaker_and_closes() {
        let dir = PathBuf::from(std::env::var("SOLOIST_STATION_DIR").expect("SOLOIST_STATION_DIR"));
        let key = std::fs::read_to_string(
            std::env::var("SOLOIST_LAB_KEY_FILE").expect("SOLOIST_LAB_KEY_FILE"),
        )
        .unwrap()
        .trim()
        .to_string();
        assert!(key.len() > 8, "SOLOIST_LAB_KEY_FILE holds no key");
        if std::env::var_os("SOLOIST_LAB_LOG").is_some() {
            let _ = log::set_boxed_logger(Box::new(super::super::lab::Redacted(key.clone())));
            log::set_max_level(log::LevelFilter::Debug);
        }
        let network = std::env::var("SOLOIST_STATION_NETWORK").is_ok_and(|on| on == "1");
        let name = "Spotifast Lab Lossless".to_string();
        let (heard, mut events) = mpsc::unbounded_channel();
        let started = Instant::now();
        let station = Station::open(
            {
                let name = name.clone();
                async move {
                    Ok(StationStart {
                        config: Config { key, dir },
                        proxy: crate::settings::ProxyConfig::Off,
                        name,
                        user: "nobody-signs-in".into(),
                        network,
                    })
                }
            },
            move |event| {
                let _ = heard.send(event);
            },
        );
        let event = tokio::time::timeout(Duration::from_secs(120), events.recv())
            .await
            .expect("the station did not open in time")
            .unwrap();
        let StationEvent::Listed { name: listed, port } = event else {
            panic!("the station did not open: {event:?}");
        };
        eprintln!(
            "lab: listed as {listed:?} on port {port} after {} ms",
            started.elapsed().as_millis()
        );
        assert_eq!(listed, name);

        let info = tokio::task::spawn_blocking(move || {
            crate::zeroconf::get_info(
                &super::super::loopback_client()?,
                &super::super::local_receiver(port),
            )
        })
        .await
        .unwrap()
        .expect("getInfo through the relay");
        eprintln!(
            "lab: getInfo through the relay: {:?}, device {}, type {}, version {}, user {:?}",
            info.remote_name, info.device_id, info.device_type, info.version, info.active_user
        );
        assert_eq!(info.remote_name, name);
        assert!(
            info.active_user.is_empty(),
            "the fresh Soloist has an account"
        );
        assert!(!info.public_key.is_empty());

        if network {
            let found =
                tokio::task::spawn_blocking(|| crate::zeroconf::discover(Duration::from_secs(5)))
                    .await
                    .unwrap()
                    .expect("browse the network");
            let receiver = found
                .into_iter()
                .find(|receiver| receiver.name == name && !receiver.cast)
                .expect("the station was not found over mDNS");
            eprintln!(
                "lab: found over mDNS at {}:{}{}",
                receiver.address, receiver.port, receiver.path
            );
            assert!(
                !receiver.address.is_loopback(),
                "announced on loopback only"
            );
            let device_id = info.device_id.clone();
            let info = tokio::task::spawn_blocking(move || {
                crate::zeroconf::get_info(&super::super::loopback_client()?, &receiver)
            })
            .await
            .unwrap()
            .expect("getInfo at the announced address");
            assert_eq!(info.device_id, device_id);
        }

        let closing = Instant::now();
        drop(station);
        let slot = tokio::time::timeout(
            Duration::from_secs(15),
            Arc::clone(&super::super::SLOT).lock_owned(),
        )
        .await
        .expect("closing the station did not stop Soloist");
        drop(slot);
        eprintln!("lab: closed in {} ms", closing.elapsed().as_millis());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), events.recv())
                .await
                .is_ok_and(|event| event.is_none()),
            "a closed station kept talking"
        );
    }
}
