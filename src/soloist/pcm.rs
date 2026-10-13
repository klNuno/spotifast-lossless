//! Soloist's audio, played through Spotifast's own output chain.
//!
//! `soloist/shim` stands in for `libpulse.so.0` inside Soloist and sends the
//! stream it is given, unmixed and before volume, to this listener on
//! loopback. Each message is a type byte, a little-endian `u32` length and a
//! payload; the shim's header comment is the reference. The audio goes into
//! the same sink librespot would have fed, so the equalizer, the visualisers,
//! the volume, the limiter and the output device behave the same.
//!
//! Flow control: after each block reaches the sink, the receiver
//! acknowledges how many bytes it has taken. The sink blocks while its queue
//! is full, so the output device's clock paces Soloist and nothing drifts.
//!
//! The shim keeps up to three blocks in flight, and in a virtual machine
//! each block is 100 ms. A reader thread takes messages as they arrive, so a
//! flush drops the blocks still in flight at once instead of after they
//! played, and a cork pauses the output while the blocks behind it wait.

use std::collections::VecDeque;
use std::io::{self, BufReader, Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use librespot_playback::audio_backend::Sink;
use librespot_playback::convert::Converter;
use librespot_playback::decoder::AudioPacket;
use librespot_playback::mixer::Mixer;
use librespot_playback::{NUM_CHANNELS, SAMPLE_RATE};

use crate::sink::AudioControl;

const MSG_FORMAT: u8 = 1;
const MSG_PCM: u8 = 2;
const MSG_FLUSH: u8 = 3;
const MSG_CORK: u8 = 4;
const MSG_VOLUME: u8 = 5;
const MSG_ACK: u8 = 6;

/// libpulse's sample formats the shim can report.
const PA_SAMPLE_S16LE: u32 = 3;
const PA_SAMPLE_FLOAT32LE: u32 = 5;
/// libpulse's 100 %.
const PA_VOLUME_NORM: u32 = 0x10000;
/// The shim sends blocks of 250 ms at most; anything far larger is a broken
/// stream.
const MAX_MESSAGE: usize = 1 << 20;

/// What the receiver needs besides the sink.
pub(crate) struct Hooks {
    pub audio: Arc<AudioControl>,
    pub mixer: Arc<dyn Mixer>,
    /// Audio the output holds after a write returns, reported back so
    /// Soloist's position accounts for it.
    pub held_ms: u32,
}

pub(crate) type SinkBuilder = Box<dyn FnOnce() -> Box<dyn Sink> + Send>;

/// The loopback listener and the thread that plays what arrives on it.
pub(crate) struct Receiver {
    port: u16,
    stop: Arc<AtomicBool>,
    current: Arc<Mutex<Option<TcpStream>>>,
    thread: Option<JoinHandle<()>>,
}

impl Receiver {
    pub(crate) fn start(sink: SinkBuilder, hooks: Hooks) -> io::Result<Self> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))?;
        let port = listener.local_addr()?.port();
        let stop = Arc::new(AtomicBool::new(false));
        let current: Arc<Mutex<Option<TcpStream>>> = Arc::default();
        let thread = {
            let stop = Arc::clone(&stop);
            let current = Arc::clone(&current);
            thread::Builder::new()
                .name("soloist-pcm".into())
                .spawn(move || accept_loop(listener, sink, hooks, stop, current))?
        };
        Ok(Self {
            port,
            stop,
            current,
            thread: Some(thread),
        })
    }

    pub(crate) fn port(&self) -> u16 {
        self.port
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(stream) = self
            .current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = stream.shutdown(Shutdown::Both);
        }
        // Wakes the blocking accept.
        let _ = TcpStream::connect_timeout(
            &(Ipv4Addr::LOCALHOST, self.port).into(),
            Duration::from_millis(200),
        );
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn accept_loop(
    listener: TcpListener,
    builder: SinkBuilder,
    hooks: Hooks,
    stop: Arc<AtomicBool>,
    current: Arc<Mutex<Option<TcpStream>>>,
) {
    let mut output = Output {
        builder: Some(builder),
        sink: None,
        started: false,
    };
    for stream in listener.incoming() {
        if stop.load(Ordering::SeqCst) {
            break;
        }
        let stream = match stream {
            Ok(stream) => stream,
            Err(error) => {
                log::warn!("soloist audio: accept failed: {error}");
                thread::sleep(Duration::from_millis(100));
                continue;
            }
        };
        if let Ok(clone) = stream.try_clone() {
            *current.lock().unwrap_or_else(PoisonError::into_inner) = Some(clone);
        }
        match serve(stream, &mut output, &hooks) {
            Ok(()) => log::debug!("soloist audio: stream closed"),
            Err(error) => log::warn!("soloist audio: {error}"),
        }
        current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        // A gap until the shim reconnects; the output must not hang on to
        // the old stream's tail.
        output.stop();
    }
    output.stop();
}

/// The sink, built on the first audio so an engine that never plays opens
/// no output device.
struct Output {
    builder: Option<SinkBuilder>,
    sink: Option<Box<dyn Sink>>,
    started: bool,
}

impl Output {
    fn sink(&mut self) -> &mut Box<dyn Sink> {
        if self.sink.is_none() {
            let builder = self.builder.take().expect("the sink is built once");
            self.sink = Some(builder());
        }
        self.sink.as_mut().expect("built above")
    }

    fn start(&mut self) {
        if !self.started {
            if let Err(error) = self.sink().start() {
                log::warn!("soloist audio: output did not start: {error}");
            }
            self.started = true;
        }
    }

    fn stop(&mut self) {
        if self.started {
            if let Some(sink) = &mut self.sink
                && let Err(error) = sink.stop()
            {
                log::warn!("soloist audio: output did not stop: {error}");
            }
            self.started = false;
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    F32,
    S16,
}

impl Format {
    fn parse(payload: &[u8]) -> io::Result<Self> {
        let field = |at: usize| -> io::Result<u32> {
            payload
                .get(at..at + 4)
                .map(|bytes| u32::from_le_bytes(bytes.try_into().expect("four bytes")))
                .ok_or_else(|| invalid("short format message"))
        };
        let (format, rate, channels) = (field(0)?, field(4)?, field(8)?);
        if rate != SAMPLE_RATE || channels != u32::from(NUM_CHANNELS) {
            return Err(invalid(&format!(
                "unsupported stream: {rate} Hz, {channels} channels"
            )));
        }
        match format {
            PA_SAMPLE_FLOAT32LE => Ok(Self::F32),
            PA_SAMPLE_S16LE => Ok(Self::S16),
            other => Err(invalid(&format!("unsupported sample format {other}"))),
        }
    }

    fn bytes(self) -> usize {
        match self {
            Self::F32 => 4,
            Self::S16 => 2,
        }
    }

    /// Widens to the `f64` librespot's chain works in. Exact for both.
    fn decode(self, payload: &[u8]) -> Vec<f64> {
        match self {
            Self::F32 => payload
                .as_chunks::<4>()
                .0
                .iter()
                .map(|bytes| f64::from(f32::from_le_bytes(*bytes)))
                .collect(),
            Self::S16 => payload
                .as_chunks::<2>()
                .0
                .iter()
                .map(|bytes| f64::from(i16::from_le_bytes(*bytes)) / 32768.0)
                .collect(),
        }
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Soloist's sink-input volume as a mixer level. A level that rounds to the
/// same percentage is kept, so a level set here is not nudged by the echo.
fn mixer_level(pa: u32, current: u16) -> u16 {
    let level = (u64::from(pa.min(PA_VOLUME_NORM)) * 65535 / u64::from(PA_VOLUME_NORM)) as u16;
    if percent(level) == percent(current) {
        current
    } else {
        level
    }
}

pub(crate) fn percent(level: u16) -> u32 {
    (u32::from(level) * 100 + 32767) / 65535
}

/// What the reader has taken from the connection and the player has not
/// acted on yet.
#[derive(Default)]
struct Inbox {
    blocks: VecDeque<(Format, Vec<u8>)>,
    /// Cork changes in arrival order: `true` pauses.
    corks: VecDeque<bool>,
    /// A flush came after the player's last block.
    flushed: bool,
    /// The connection ended, cleanly or not.
    end: Option<io::Result<()>>,
}

struct Shared {
    inbox: Mutex<Inbox>,
    changed: Condvar,
    acks: Mutex<Acks>,
}

impl Shared {
    fn inbox(&self) -> std::sync::MutexGuard<'_, Inbox> {
        self.inbox.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Counts `bytes` more as taken and tells the shim.
    fn acknowledge(&self, bytes: u64) -> io::Result<()> {
        let mut acks = self.acks.lock().unwrap_or_else(PoisonError::into_inner);
        acks.taken += bytes;
        // Nothing is held before the first audio.
        let (taken, held_us) = (acks.taken, if acks.taken == 0 { 0 } else { acks.held_us });
        send_ack(&mut acks.stream, taken, held_us)
    }
}

struct Acks {
    stream: TcpStream,
    /// PCM bytes played or dropped since the connection opened.
    taken: u64,
    held_us: u32,
}

enum Next {
    Flush,
    Cork(bool),
    Block(Format, Vec<u8>),
    End(io::Result<()>),
}

fn serve(stream: TcpStream, output: &mut Output, hooks: &Hooks) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let shared = Shared {
        inbox: Mutex::default(),
        changed: Condvar::new(),
        acks: Mutex::new(Acks {
            stream: stream.try_clone()?,
            taken: 0,
            held_us: hooks.held_ms.saturating_mul(1000),
        }),
    };
    let closer = stream.try_clone()?;
    thread::scope(|scope| {
        scope.spawn(|| {
            let result = read_messages(stream, &shared, hooks);
            shared.inbox().end = Some(result);
            shared.changed.notify_all();
        });
        let result = play(output, &shared, hooks);
        // The reader stops when the connection does; it may still be
        // waiting for the shim after the player failed.
        let _ = closer.shutdown(Shutdown::Both);
        result
    })
}

/// Takes the shim's messages as they arrive. Volume applies at once; audio,
/// flushes and corks go to the player.
fn read_messages(stream: TcpStream, shared: &Shared, hooks: &Hooks) -> io::Result<()> {
    let mut reader = BufReader::with_capacity(64 * 1024, stream);
    let mut format = None;
    loop {
        let mut head = [0u8; 5];
        match reader.read_exact(&mut head) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        }
        let len = u32::from_le_bytes([head[1], head[2], head[3], head[4]]) as usize;
        if len > MAX_MESSAGE {
            return Err(invalid(&format!("message of {len} bytes")));
        }
        let mut payload = vec![0u8; len];
        reader.read_exact(&mut payload)?;
        match head[0] {
            MSG_FORMAT => {
                format = Some(Format::parse(&payload)?);
                // Tells the shim this receiver paces: from now on it sends
                // only what the acknowledgements make room for.
                shared.acknowledge(0)?;
            }
            MSG_PCM => {
                let format = format.ok_or_else(|| invalid("audio before its format"))?;
                let frame = format.bytes() * usize::from(NUM_CHANNELS);
                if !len.is_multiple_of(frame) {
                    return Err(invalid("audio block cut mid-frame"));
                }
                shared.inbox().blocks.push_back((format, payload));
                shared.changed.notify_all();
            }
            MSG_FLUSH => {
                let dropped: usize = {
                    let mut inbox = shared.inbox();
                    inbox.flushed = true;
                    inbox.blocks.drain(..).map(|(_, block)| block.len()).sum()
                };
                // What the sink still holds goes now; the player flushes
                // again after a write that was under way.
                hooks.audio.seeked();
                shared.changed.notify_all();
                shared.acknowledge(dropped as u64)?;
            }
            MSG_CORK => {
                let corked = match payload.first() {
                    Some(1) => true,
                    Some(_) => false,
                    None => return Err(invalid("empty cork message")),
                };
                shared.inbox().corks.push_back(corked);
                shared.changed.notify_all();
            }
            MSG_VOLUME => {
                let bytes: [u8; 4] = payload
                    .get(..4)
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| invalid("short volume message"))?;
                let level = mixer_level(u32::from_le_bytes(bytes), hooks.mixer.volume());
                hooks.mixer.set_volume(level);
            }
            // Newer shims may add messages; skipping them keeps this one working.
            other => log::debug!("soloist audio: skipped message type {other}"),
        }
    }
}

/// Plays what the reader queued: a flush first, then cork changes in order,
/// then audio unless corked. The blocks queued behind a cork wait for the
/// uncork, as a paused PulseAudio stream keeps its buffer.
fn play(output: &mut Output, shared: &Shared, hooks: &Hooks) -> io::Result<()> {
    let mut converter = Converter::new(None);
    let mut corked = false;
    let mut write_failed = false;
    loop {
        let next = {
            let mut inbox = shared.inbox();
            loop {
                if std::mem::take(&mut inbox.flushed) {
                    break Next::Flush;
                }
                if let Some(cork) = inbox.corks.pop_front() {
                    break Next::Cork(cork);
                }
                if !corked && let Some((format, block)) = inbox.blocks.pop_front() {
                    break Next::Block(format, block);
                }
                if let Some(end) = inbox.end.take() {
                    break Next::End(end);
                }
                inbox = shared
                    .changed
                    .wait(inbox)
                    .unwrap_or_else(PoisonError::into_inner);
            }
        };
        match next {
            Next::Flush => hooks.audio.seeked(),
            Next::Cork(true) => {
                corked = true;
                output.stop();
            }
            Next::Cork(false) => {
                corked = false;
                output.start();
            }
            Next::Block(format, block) => {
                output.start();
                let samples = format.decode(&block);
                let frames = samples.len() / usize::from(NUM_CHANNELS);
                match output
                    .sink()
                    .write(AudioPacket::Samples(samples), &mut converter)
                {
                    Ok(()) => write_failed = false,
                    Err(error) => {
                        if !write_failed {
                            log::warn!("soloist audio: {error}");
                        }
                        write_failed = true;
                        // A failing output returns at once; without this
                        // Soloist would race through the track.
                        thread::sleep(Duration::from_secs_f64(
                            frames as f64 / f64::from(SAMPLE_RATE),
                        ));
                    }
                }
                shared.acknowledge(block.len() as u64)?;
            }
            Next::End(result) => return result,
        }
    }
}

fn send_ack(stream: &mut TcpStream, taken: u64, held_us: u32) -> io::Result<()> {
    let mut message = [0u8; 17];
    message[0] = MSG_ACK;
    message[1..5].copy_from_slice(&12u32.to_le_bytes());
    message[5..13].copy_from_slice(&taken.to_le_bytes());
    message[13..17].copy_from_slice(&held_us.to_le_bytes());
    stream.write_all(&message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use librespot_playback::audio_backend::SinkResult;
    use librespot_playback::mixer::{MixerConfig, softmixer::SoftMixer};

    fn message(kind: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![kind];
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.extend_from_slice(payload);
        out
    }

    fn format(sample: u32, rate: u32, channels: u32) -> Vec<u8> {
        [sample, rate, channels]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect()
    }

    #[derive(Default)]
    struct Seen {
        samples: Vec<f64>,
        events: Vec<&'static str>,
    }

    struct Recorder(Arc<Mutex<Seen>>);

    impl Sink for Recorder {
        fn start(&mut self) -> SinkResult<()> {
            self.0.lock().unwrap().events.push("start");
            Ok(())
        }
        fn stop(&mut self) -> SinkResult<()> {
            self.0.lock().unwrap().events.push("stop");
            Ok(())
        }
        fn write(&mut self, packet: AudioPacket, _: &mut Converter) -> SinkResult<()> {
            let mut seen = self.0.lock().unwrap();
            seen.samples.extend_from_slice(packet.samples().unwrap());
            seen.events.push("write");
            Ok(())
        }
    }

    /// A sink whose writes wait until the test opens it, as a full output
    /// queue makes them wait for the device.
    struct Gated {
        seen: Arc<Mutex<Seen>>,
        open: Arc<(Mutex<bool>, std::sync::Condvar)>,
    }

    impl Sink for Gated {
        fn start(&mut self) -> SinkResult<()> {
            self.seen.lock().unwrap().events.push("start");
            Ok(())
        }
        fn stop(&mut self) -> SinkResult<()> {
            self.seen.lock().unwrap().events.push("stop");
            Ok(())
        }
        fn write(&mut self, packet: AudioPacket, _: &mut Converter) -> SinkResult<()> {
            self.seen
                .lock()
                .unwrap()
                .samples
                .extend_from_slice(packet.samples().unwrap());
            let (open, changed) = &*self.open;
            let mut open = open.lock().unwrap();
            while !*open {
                open = changed.wait(open).unwrap();
            }
            Ok(())
        }
    }

    fn gated_receiver(
        seen: &Arc<Mutex<Seen>>,
        open: &Arc<(Mutex<bool>, std::sync::Condvar)>,
    ) -> Receiver {
        let sink = Gated {
            seen: Arc::clone(seen),
            open: Arc::clone(open),
        };
        let mixer: Arc<dyn Mixer> = Arc::new(SoftMixer::open(MixerConfig::default()).unwrap());
        Receiver::start(
            Box::new(move || Box::new(sink) as Box<dyn Sink>),
            Hooks {
                audio: AudioControl::new(100),
                mixer,
                held_ms: 40,
            },
        )
        .unwrap()
    }

    fn pcm(values: &[f32]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[test]
    fn a_flush_drops_the_blocks_still_in_flight() {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let open = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let receiver = gated_receiver(&seen, &open);
        let mut shim = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port())).unwrap();
        shim.write_all(&message(MSG_FORMAT, &format(PA_SAMPLE_FLOAT32LE, 44100, 2)))
            .unwrap();
        read_ack(&mut shim);

        // The first block waits in the sink; two more are in flight when
        // Soloist seeks, and the next one is from the new position.
        shim.write_all(&message(MSG_PCM, &pcm(&[0.1, 0.1])))
            .unwrap();
        while seen.lock().unwrap().samples.is_empty() {
            thread::sleep(Duration::from_millis(5));
        }
        for block in [[0.2f32, 0.2], [0.3, 0.3]] {
            shim.write_all(&message(MSG_PCM, &pcm(&block))).unwrap();
        }
        shim.write_all(&message(MSG_FLUSH, &[])).unwrap();
        // The dropped blocks are acknowledged at once, before the sink frees.
        assert_eq!(read_ack(&mut shim).0, 16);
        shim.write_all(&message(MSG_PCM, &pcm(&[0.4, 0.4])))
            .unwrap();
        {
            let (flag, changed) = &*open;
            *flag.lock().unwrap() = true;
            changed.notify_all();
        }
        assert_eq!(read_ack(&mut shim).0, 24);
        assert_eq!(read_ack(&mut shim).0, 32);

        drop(shim);
        drop(receiver);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.samples, [0.1f32, 0.1, 0.4, 0.4].map(f64::from));
    }

    #[test]
    fn audio_behind_a_cork_waits_for_the_uncork() {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let open = Arc::new((Mutex::new(true), std::sync::Condvar::new()));
        let receiver = gated_receiver(&seen, &open);
        let mut shim = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port())).unwrap();
        shim.write_all(&message(MSG_FORMAT, &format(PA_SAMPLE_FLOAT32LE, 44100, 2)))
            .unwrap();
        read_ack(&mut shim);
        shim.write_all(&message(MSG_PCM, &pcm(&[0.1, 0.1])))
            .unwrap();
        assert_eq!(read_ack(&mut shim).0, 8);

        shim.write_all(&message(MSG_CORK, &[1])).unwrap();
        shim.write_all(&message(MSG_PCM, &pcm(&[0.2, 0.2])))
            .unwrap();
        thread::sleep(Duration::from_millis(300));
        assert_eq!(
            seen.lock().unwrap().samples.len(),
            2,
            "a corked block was played"
        );
        shim.write_all(&message(MSG_CORK, &[0])).unwrap();
        assert_eq!(read_ack(&mut shim).0, 16);

        drop(shim);
        drop(receiver);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.samples, [0.1f32, 0.1, 0.2, 0.2].map(f64::from));
        assert_eq!(seen.events, ["start", "stop", "start", "stop"]);
    }

    fn read_ack(stream: &mut TcpStream) -> (u64, u32) {
        let mut ack = [0u8; 17];
        stream.read_exact(&mut ack).unwrap();
        assert_eq!(ack[0], MSG_ACK);
        assert_eq!(u32::from_le_bytes(ack[1..5].try_into().unwrap()), 12);
        (
            u64::from_le_bytes(ack[5..13].try_into().unwrap()),
            u32::from_le_bytes(ack[13..17].try_into().unwrap()),
        )
    }

    fn receiver(seen: &Arc<Mutex<Seen>>, mixer: &Arc<dyn Mixer>) -> Receiver {
        let sink = Recorder(Arc::clone(seen));
        Receiver::start(
            Box::new(move || Box::new(sink) as Box<dyn Sink>),
            Hooks {
                audio: AudioControl::new(100),
                mixer: Arc::clone(mixer),
                held_ms: 40,
            },
        )
        .unwrap()
    }

    #[test]
    fn plays_float_audio_exactly_and_acknowledges_it() {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let mixer: Arc<dyn Mixer> = Arc::new(SoftMixer::open(MixerConfig::default()).unwrap());
        let receiver = receiver(&seen, &mixer);
        let mut shim = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port())).unwrap();

        shim.write_all(&message(MSG_FORMAT, &format(PA_SAMPLE_FLOAT32LE, 44100, 2)))
            .unwrap();
        assert_eq!(read_ack(&mut shim), (0, 0));

        // 24-bit values, as Soloist decodes a FLAC 24 track.
        let values = [0.5f32, -0.25, 8_388_607.0 / 8_388_608.0, -1.0];
        let pcm: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
        shim.write_all(&message(MSG_PCM, &pcm)).unwrap();
        assert_eq!(read_ack(&mut shim), (16, 40_000));

        drop(shim);
        drop(receiver);
        let seen = seen.lock().unwrap();
        let expected: Vec<f64> = values.iter().map(|v| f64::from(*v)).collect();
        assert_eq!(seen.samples, expected);
        assert_eq!(seen.events, ["start", "write", "stop"]);
    }

    #[test]
    fn cork_pauses_the_output_and_volume_reaches_the_mixer() {
        let seen = Arc::new(Mutex::new(Seen::default()));
        let mixer: Arc<dyn Mixer> = Arc::new(SoftMixer::open(MixerConfig::default()).unwrap());
        let receiver = receiver(&seen, &mixer);
        let mut shim = TcpStream::connect((Ipv4Addr::LOCALHOST, receiver.port())).unwrap();
        shim.write_all(&message(MSG_FORMAT, &format(PA_SAMPLE_S16LE, 44100, 2)))
            .unwrap();
        read_ack(&mut shim);
        let pcm: Vec<u8> = [16384i16, -32768]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        shim.write_all(&message(MSG_PCM, &pcm)).unwrap();
        read_ack(&mut shim);
        shim.write_all(&message(MSG_CORK, &[1])).unwrap();
        shim.write_all(&message(MSG_VOLUME, &(PA_VOLUME_NORM / 2).to_le_bytes()))
            .unwrap();
        shim.write_all(&message(MSG_CORK, &[0])).unwrap();
        shim.write_all(&message(MSG_PCM, &pcm)).unwrap();
        read_ack(&mut shim);

        assert_eq!(percent(mixer.volume()), 50);
        drop(shim);
        drop(receiver);
        let seen = seen.lock().unwrap();
        assert_eq!(seen.samples[..2], [0.5, -1.0]);
        assert_eq!(
            seen.events,
            ["start", "write", "stop", "start", "write", "stop"]
        );
    }

    #[test]
    fn refuses_a_stream_it_cannot_play_unconverted() {
        assert!(Format::parse(&format(PA_SAMPLE_FLOAT32LE, 48000, 2)).is_err());
        assert!(Format::parse(&format(PA_SAMPLE_FLOAT32LE, 44100, 6)).is_err());
        assert!(Format::parse(&format(9, 44100, 2)).is_err());
        assert!(Format::parse(&[1, 2, 3]).is_err());
    }

    #[test]
    fn a_level_set_here_survives_its_rounded_echo() {
        let set_here = 24_444; // 37.3 %
        let echo = 37 * PA_VOLUME_NORM / 100;
        assert_eq!(mixer_level(echo, set_here), set_here);
        assert_eq!(mixer_level(PA_VOLUME_NORM, 0), 65535);
        assert_eq!(mixer_level(PA_VOLUME_NORM * 2, 0), 65535);
        assert_eq!(mixer_level(0, 40_000), 0);
    }
}
