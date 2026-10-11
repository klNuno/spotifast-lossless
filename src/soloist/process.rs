//! Runs Soloist: as a child process on Linux, in a WSL1 distribution on
//! Windows. Either way it gets the shim as its `libpulse.so.0`, a pipe as
//! standard input that ends its life with Spotifast's, and verbose output
//! that is read for the ports it opens and never written to disk.

use std::io::{BufRead, BufReader};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::thread;

use anyhow::{Context, Result};
use tokio::sync::mpsc;

/// How to start Soloist on this system.
#[derive(Clone, Debug)]
pub(crate) struct Launch {
    /// The program to run and the arguments that come before Soloist's own.
    pub program: std::path::PathBuf,
    pub prefix: Vec<String>,
    /// Environment for Soloist, as `NAME=value`. Under WSL they travel
    /// through `WSLENV`.
    pub env: Vec<(String, String)>,
    /// Soloist's folders, as Soloist sees them.
    pub data: String,
    pub cache: String,
    /// The WSL distribution to stop with Soloist.
    pub distro: Option<String>,
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
    /// The output ended: Soloist exited.
    Exited,
}

pub(crate) struct Process {
    child: Child,
    /// Never written to: closing it tells the shim Spotifast is gone.
    lifeline: Option<ChildStdin>,
    distro: Option<String>,
}

pub(crate) fn spawn(
    launch: &Launch,
    args: &Args<'_>,
) -> Result<(Process, mpsc::UnboundedReceiver<Report>)> {
    let mut command = Command::new(&launch.program);
    command.args(&launch.prefix);
    command.args([
        "-n",
        args.name,
        "-k",
        args.key,
        "-D",
        &launch.data,
        "-C",
        &launch.cache,
        "-z",
        &args.cache_mb.max(100).to_string(),
        "-i",
        &args.volume_percent.min(100).to_string(),
        "-w",
        "127.0.0.1:0",
        "-v",
    ]);
    let mut env = launch.env.clone();
    env.extend([
        (
            "PULSE_SHIM_OUT".to_string(),
            format!("tcp:127.0.0.1:{}", args.pcm_port),
        ),
        ("PULSE_SHIM_LIFELINE".into(), "1".into()),
        // The key has to be an argument, which any local user can read in
        // /proc. The shim blanks it once Soloist has taken it in.
        ("PULSE_SHIM_HIDE".into(), "-k".into()),
    ]);
    if launch.distro.is_some() {
        let names: Vec<String> = env.iter().map(|(name, _)| format!("{name}/u")).collect();
        command.env("WSLENV", names.join(":"));
    }
    command.envs(env);
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
    let lifeline = child.stdin.take();
    let (reports, received) = mpsc::unbounded_channel();
    thread::Builder::new()
        .name("soloist-output".into())
        .spawn(move || {
            for line in BufReader::new(reader).lines() {
                let Ok(line) = line else { break };
                let line = strip_ansi(&line);
                if let Some(report) = report(&line) {
                    let _ = reports.send(report);
                }
                if line_is_error(&line) {
                    log::warn!("soloist: {}", line.trim());
                } else {
                    log::trace!("soloist: {}", line.trim());
                }
            }
            let _ = reports.send(Report::Exited);
        })?;
    Ok((
        Process {
            child,
            lifeline,
            distro: launch.distro.clone(),
        },
        received,
    ))
}

impl Process {
    pub(crate) fn stop(&mut self) {
        self.lifeline.take();
        if let Some(distro) = &self.distro {
            // Ending wsl.exe can leave the Linux side running; terminating
            // the distribution ends everything in it.
            let mut terminate = Command::new("wsl.exe");
            terminate
                .args(["--terminate", distro])
                .stdout(Stdio::null())
                .stderr(Stdio::null());
            hide_window(&mut terminate);
            let _ = terminate.status();
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
    fn tells_errors_from_the_rest() {
        assert!(line_is_error("22:18:40.871 E [f:52 ] audio driver failed"));
        assert!(!line_is_error("22:18:40.871 I [f:52 ] fine"));
        assert!(!line_is_error("short"));
        assert_eq!(strip_ansi("\u{1b}[31mred\u{1b}[0m text"), "red text");
    }
}
