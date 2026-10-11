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

use std::io::{self, BufReader, Read, Write};
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
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
/// The shim sends 10 ms blocks; anything far larger is a broken stream.
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

fn serve(stream: TcpStream, output: &mut Output, hooks: &Hooks) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut acks = stream.try_clone()?;
    let mut reader = BufReader::with_capacity(64 * 1024, stream);
    let mut converter = Converter::new(None);
    let mut payload = Vec::with_capacity(16 * 1024);
    let mut format = None;
    let mut taken = 0u64;
    let held_us = hooks.held_ms.saturating_mul(1000);
    let mut write_failed = false;
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
        payload.resize(len, 0);
        reader.read_exact(&mut payload)?;
        match head[0] {
            MSG_FORMAT => {
                format = Some(Format::parse(&payload)?);
                // Tells the shim this receiver paces: from now on it sends
                // only what the acknowledgements make room for.
                send_ack(&mut acks, taken, 0)?;
            }
            MSG_PCM => {
                let format = format.ok_or_else(|| invalid("audio before its format"))?;
                let frame = format.bytes() * usize::from(NUM_CHANNELS);
                if !len.is_multiple_of(frame) {
                    return Err(invalid("audio block cut mid-frame"));
                }
                output.start();
                let samples = format.decode(&payload);
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
                taken += len as u64;
                send_ack(&mut acks, taken, held_us)?;
            }
            MSG_FLUSH => hooks.audio.seeked(),
            MSG_CORK => match payload.first() {
                Some(1) => output.stop(),
                Some(_) => output.start(),
                None => return Err(invalid("empty cork message")),
            },
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
