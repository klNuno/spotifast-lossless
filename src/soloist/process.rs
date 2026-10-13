//! Runs Soloist: as a child process on Linux, in Spotifast's own Linux
//! machine on Windows (see `vm.rs`). Either way it gets the shim as its
//! `libpulse.so.0` and verbose output that is read for the ports it opens
//! and never written to disk.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::mpsc;

/// How to start Soloist on this system.
#[derive(Debug)]
pub(crate) enum Launch {
    /// Soloist itself, with these extra environment variables and folders.
    #[cfg_attr(windows, allow(dead_code))]
    Native {
        program: std::path::PathBuf,
        env: Vec<(String, String)>,
        data: String,
        cache: String,
    },
    /// Soloist in its machine.
    #[cfg(windows)]
    Machine(super::vm::Machine),
}

/// What Soloist is started with.
pub(crate) struct Args<'a> {
    pub name: &'a str,
    pub key: &'a str,
    pub volume_percent: u32,
    pub pcm_port: u16,
    pub cache_mb: u32,
}

/// What Soloist's output reveals.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Report {
    /// The WebSocket API listens on this port.
    Control(u16),
    /// The Spotify Connect pairing endpoint, served under `/zc`.
    Pairing(u16),
    /// The build stops working in this many days.
    ExpiresIn(u32),
    /// Whether Soloist kept an account from an earlier run, as the machine
    /// found before starting it.
    Account(bool),
    /// The machine could not start Soloist.
    Failed(String),
    /// The output ended: Soloist exited.
    Exited,
}

/// The loopback ports QEMU forwards into the machine.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Forwarded {
    pub control: u16,
    pub pairing: u16,
    /// The pairing port inside the machine, which the shim pins.
    pub pairing_inside: u16,
}

/// How long the machine may take to stop Soloist, write its data and power
/// off before it is ended.
const STOP_GRACE: Duration = Duration::from_secs(5);

pub(crate) struct Process {
    child: Child,
    /// Standard input. On Linux it is never written to: closing it tells the
    /// shim Spotifast is gone. In the machine it is the console.
    input: Arc<Mutex<Option<ChildStdin>>>,
    machine: bool,
    #[cfg(windows)]
    _job: Option<super::vm::Job>,
}

/// Soloist's own arguments, all but the key.
pub(crate) fn soloist_args(
    args: &Args<'_>,
    data: &str,
    cache: &str,
    websocket: &str,
) -> Vec<String> {
    [
        "-n",
        args.name,
        "-D",
        data,
        "-C",
        cache,
        "-z",
        &args.cache_mb.max(100).to_string(),
        "-i",
        &args.volume_percent.min(100).to_string(),
        "-w",
        websocket,
        "-v",
    ]
    .map(str::to_string)
    .to_vec()
}

pub(crate) fn spawn(
    launch: &Launch,
    args: &Args<'_>,
) -> Result<(Process, mpsc::UnboundedReceiver<Report>)> {
    let (mut command, forwarded) = match launch {
        Launch::Native {
            program,
            env,
            data,
            cache,
        } => {
            let mut command = Command::new(program);
            command.args(soloist_args(args, data, cache, "127.0.0.1:0"));
            command.args(["-k", args.key]);
            command.envs(env.iter().cloned()).envs([
                (
                    "PULSE_SHIM_OUT".to_string(),
                    format!("tcp:127.0.0.1:{}", args.pcm_port),
                ),
                ("PULSE_SHIM_LIFELINE".into(), "1".into()),
                // The key has to be an argument, which any local user can
                // read in /proc. The shim blanks it once Soloist has it.
                ("PULSE_SHIM_HIDE".into(), "-k".into()),
            ]);
            (command, None)
        }
        #[cfg(windows)]
        Launch::Machine(machine) => {
            let (command, forwarded) = machine.command(args)?;
            (command, Some(forwarded))
        }
    };
    let (reader, writer) = std::io::pipe().context("no pipe for Soloist's output")?;
    command
        .stdin(Stdio::piped())
        .stdout(writer.try_clone()?)
        .stderr(writer);
    hide_window(&mut command);
    let mut child = command.spawn().context("cannot start Spotify Soloist")?;
    // The command still holds the pipe's write ends; without dropping it
    // the reader would never see the end of the output.
    drop(command);
    #[cfg(windows)]
    let job = match super::vm::Job::hold(&child) {
        Ok(job) => Some(job),
        Err(error) => {
            log::warn!("soloist: the machine may outlive Spotifast: {error}");
            None
        }
    };
    let input = Arc::new(Mutex::new(child.stdin.take()));
    let (reports, received) = mpsc::unbounded_channel();
    let mut output = Output {
        forwarded,
        key: forwarded.map(|_| args.key.to_string()),
        redact: args.key.to_string(),
        input: Arc::clone(&input),
        reports,
    };
    thread::Builder::new()
        .name("soloist-output".into())
        .spawn(move || {
            for line in BufReader::new(reader).lines() {
                let Ok(line) = line else { break };
                output.line(&line);
            }
            let _ = output.reports.send(Report::Exited);
        })?;
    Ok((
        Process {
            child,
            input,
            machine: forwarded.is_some(),
            #[cfg(windows)]
            _job: job,
        },
        received,
    ))
}

/// Reads Soloist's output, and the machine's, line by line.
struct Output {
    /// Set when Soloist runs in the machine.
    forwarded: Option<Forwarded>,
    /// The client key, until the machine asks for it.
    key: Option<String>,
    /// Never logged.
    redact: String,
    input: Arc<Mutex<Option<ChildStdin>>>,
    reports: mpsc::UnboundedSender<Report>,
}

impl Output {
    fn line(&mut self, line: &str) {
        let line = strip_ansi(line);
        let line = line.trim_end_matches('\r');
        let line = if self.redact.is_empty() {
            line.to_string()
        } else {
            line.replace(&self.redact, "<key>")
        };
        if let Some(said) = line.strip_prefix("spotifast-vm: ") {
            match machine_line(said) {
                Some(MachineLine::Ready) => self.send_key(),
                Some(MachineLine::Report(report)) => {
                    if let Report::Failed(why) = &report {
                        log::warn!("soloist: the machine: {why}");
                    }
                    let _ = self.reports.send(report);
                }
                None => log::debug!("soloist: the machine: {said}"),
            }
            return;
        }
        if let Some(report) = report(&line) {
            let _ = self.reports.send(on_host(self.forwarded, report));
        }
        if line_is_error(&line) {
            log::warn!("soloist: {}", line.trim());
        } else {
            log::trace!("soloist: {}", line.trim());
        }
    }

    /// The machine reads the key from its console once it is ready.
    fn send_key(&mut self) {
        let Some(key) = self.key.take() else { return };
        let mut input = self.input.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(console) = input.as_mut() {
            let sent = console
                .write_all(format!("{key}\n").as_bytes())
                .and_then(|()| console.flush());
            if let Err(error) = sent {
                log::warn!("soloist: cannot reach the machine's console: {error}");
            }
        }
    }
}

/// A port Soloist opened, as Spotifast reaches it.
fn on_host(forwarded: Option<Forwarded>, report: Report) -> Report {
    let Some(ports) = forwarded else {
        return report;
    };
    match report {
        Report::Control(_) => Report::Control(ports.control),
        Report::Pairing(port) => {
            if port != ports.pairing_inside {
                log::warn!("soloist: pairing opened on {port}, not the forwarded port");
            }
            Report::Pairing(ports.pairing)
        }
        other => other,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum MachineLine {
    Ready,
    Report(Report),
}

/// A line `soloist/vm/init` wrote for Spotifast, without its prefix.
fn machine_line(said: &str) -> Option<MachineLine> {
    match said {
        "ready" => Some(MachineLine::Ready),
        "account yes" => Some(MachineLine::Report(Report::Account(true))),
        "account no" => Some(MachineLine::Report(Report::Account(false))),
        _ => said
            .strip_prefix("error: ")
            .map(|why| MachineLine::Report(Report::Failed(why.to_string()))),
    }
}

impl Process {
    pub(crate) fn stop(&mut self) {
        let input = self
            .input
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if self.machine
            && let Some(mut console) = input
            && console
                .write_all(b"stop\n")
                .and_then(|()| console.flush())
                .is_ok()
        {
            // The machine stops Soloist, writes its data and powers off.
            let deadline = Instant::now() + STOP_GRACE;
            while Instant::now() < deadline && matches!(self.child.try_wait(), Ok(None)) {
                thread::sleep(Duration::from_millis(50));
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(windows)]
fn hide_window(command: &mut Command) {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    command.creation_flags(CREATE_NO_WINDOW);
}

#[cfg(not(windows))]
fn hide_window(_: &mut Command) {}

fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            // CSI: ESC [ parameters final-byte
            if chars.next() == Some('[') {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// Soloist's lines read `HH:MM:SS.mmm L [f:NNN ] message`, L the level.
fn line_is_error(line: &str) -> bool {
    line.get(12..15) == Some(" E ")
}

fn report(line: &str) -> Option<Report> {
    let number_after = |marker: &str| -> Option<u32> {
        let rest = &line[line.find(marker)? + marker.len()..];
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        digits.parse().ok()
    };
    if let Some(at) = line.find("websocket server listening on ") {
        let address = &line[at..];
        let port = address.rsplit(':').next()?;
        let digits: String = port.chars().take_while(char::is_ascii_digit).collect();
        return digits.parse().ok().map(Report::Control);
    }
    if line.contains("Started ZeroConf service on port ") {
        return number_after("Started ZeroConf service on port ")
            .and_then(|port| u16::try_from(port).ok())
            .map(Report::Pairing);
    }
    number_after("client expires in ").map(Report::ExpiresIn)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_the_ports_from_soloists_output() {
        let lines = [
            "22:18:40.839 I [f:353                           ] soloist 1.3.9.12 build 1791741672",
            "22:18:40.839 I [f:128                           ] client expires in 89 days\u{1b}[0m",
            "\u{1b}[32m22:18:40.870 I [f:1021                          ] api: websocket server listening on 127.0.0.1:39090\u{1b}[0m",
            "22:18:40.871 I [f:52                            ] Started ZeroConf service on port 52083 path /zc",
        ];
        let reports: Vec<Report> = lines
            .iter()
            .filter_map(|line| report(&strip_ansi(line)))
            .collect();
        assert_eq!(
            reports,
            [
                Report::ExpiresIn(89),
                Report::Control(39090),
                Report::Pairing(52083)
            ]
        );
    }

    #[test]
    fn reads_what_the_machine_says() {
        assert_eq!(machine_line("ready"), Some(MachineLine::Ready));
        assert_eq!(
            machine_line("account yes"),
            Some(MachineLine::Report(Report::Account(true)))
        );
        assert_eq!(
            machine_line("error: cannot read Soloist's disk"),
            Some(MachineLine::Report(Report::Failed(
                "cannot read Soloist's disk".into()
            )))
        );
        assert_eq!(machine_line("something else"), None);
    }

    #[test]
    fn gives_the_machines_ports_as_forwarded() {
        let forwarded = Some(Forwarded {
            control: 50_001,
            pairing: 50_002,
            pairing_inside: 4071,
        });
        assert_eq!(
            on_host(forwarded, Report::Control(4070)),
            Report::Control(50_001)
        );
        assert_eq!(
            on_host(forwarded, Report::Pairing(4071)),
            Report::Pairing(50_002)
        );
        assert_eq!(on_host(None, Report::Pairing(4071)), Report::Pairing(4071));
        assert_eq!(
            on_host(forwarded, Report::ExpiresIn(3)),
            Report::ExpiresIn(3)
        );
    }

    #[test]
    fn tells_errors_from_the_rest() {
        assert!(line_is_error("22:18:40.871 E [f:52 ] audio driver failed"));
        assert!(!line_is_error("22:18:40.871 I [f:52 ] fine"));
        assert!(!line_is_error("short"));
        assert_eq!(strip_ansi("\u{1b}[31mred\u{1b}[0m text"), "red text");
    }
}
