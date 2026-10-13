//! Fetches Spotify Soloist from Spotify's CDN and keeps it current.
//!
//! Spotify may not be redistributed, so nothing of Soloist ships with
//! Spotifast: the first start downloads the official build onto the user's
//! machine. Builds stop working 90 days after they were made, so a build
//! older than [`REFRESH_AFTER`] is replaced when a newer one is out, and one
//! past [`EXPIRES_AFTER`] must be replaced before Soloist starts.

use std::fs::{self, File};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, bail};
use flate2::read::GzDecoder;

pub(crate) const URL: &str = "https://soloist-builds.spotifycdn.com/soloist_release_x86_64.tar.gz";

/// Spotify's builds expire this long after they are published.
const EXPIRES_AFTER: Duration = Duration::from_secs(90 * 24 * 3600);
/// Look for a newer build once the current one is this old.
const REFRESH_AFTER: Duration = Duration::from_secs(60 * 24 * 3600);
/// And then at most this often.
const CHECK_EVERY: Duration = Duration::from_secs(24 * 3600);
/// The archive is about 14 MB; this covers a slow connection.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);

/// The Soloist binary and what is known about its build.
#[derive(Debug)]
pub(crate) struct Build {
    pub binary: PathBuf,
}

/// What the last download recorded, next to the binary.
#[derive(Debug, Default, PartialEq, Eq)]
struct Stamp {
    /// The CDN's `Last-Modified`, when the build was published.
    published: Option<SystemTime>,
    etag: Option<String>,
    checked: Option<SystemTime>,
}

impl Stamp {
    fn path(binary: &Path) -> PathBuf {
        binary.with_extension("build")
    }

    fn read(binary: &Path) -> Self {
        let Ok(text) = fs::read_to_string(Self::path(binary)) else {
            return Self::default();
        };
        let mut stamp = Self::default();
        for line in text.lines() {
            let Some((key, value)) = line.split_once('=') else {
                continue;
            };
            match key {
                "published" => stamp.published = seconds(value),
                "checked" => stamp.checked = seconds(value),
                "etag" if !value.is_empty() => stamp.etag = Some(value.to_string()),
                _ => {}
            }
        }
        stamp
    }

    fn write(&self, binary: &Path) -> io::Result<()> {
        let secs = |time: Option<SystemTime>| {
            time.and_then(|time| time.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map_or(String::new(), |since| since.as_secs().to_string())
        };
        let text = format!(
            "published={}\netag={}\nchecked={}\n",
            secs(self.published),
            self.etag.as_deref().unwrap_or_default(),
            secs(self.checked),
        );
        write_atomically(&Self::path(binary), text.as_bytes())
    }

    /// The build's age; a binary without a stamp counts as its file's age.
    fn age(&self, binary: &Path, now: SystemTime) -> Duration {
        let published = self
            .published
            .or_else(|| fs::metadata(binary).and_then(|meta| meta.modified()).ok());
        published
            .and_then(|published| now.duration_since(published).ok())
            .unwrap_or_default()
    }
}

fn seconds(value: &str) -> Option<SystemTime> {
    value
        .parse::<u64>()
        .ok()
        .map(|secs| SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
}

/// What to do with the build on disk.
#[derive(Debug, PartialEq, Eq)]
enum Plan {
    Download,
    /// Use it, and look for a newer one first.
    Refresh,
    Use,
}

fn plan(binary_exists: bool, stamp: &Stamp, age: Duration, now: SystemTime) -> Plan {
    if !binary_exists || age >= EXPIRES_AFTER {
        return Plan::Download;
    }
    let checked_recently = stamp
        .checked
        .and_then(|checked| now.duration_since(checked).ok())
        .is_some_and(|since| since < CHECK_EVERY);
    if age >= REFRESH_AFTER && !checked_recently {
        Plan::Refresh
    } else {
        Plan::Use
    }
}

/// Makes sure `<dir>/soloist` is a build that still works. Blocking: run it
/// off the interface thread.
pub(crate) fn ensure(dir: &Path, proxy: &crate::settings::ProxyConfig) -> Result<Build> {
    let binary = dir.join("soloist");
    let now = SystemTime::now();
    let stamp = Stamp::read(&binary);
    let age = stamp.age(&binary, now);
    match plan(binary.is_file(), &stamp, age, now) {
        Plan::Use => {}
        Plan::Download => {
            fs::create_dir_all(dir).context("cannot create the Soloist folder")?;
            download(&binary, &Stamp::default(), proxy)
                .context("cannot download Spotify Soloist")?;
        }
        Plan::Refresh => {
            if let Err(error) = download(&binary, &stamp, proxy) {
                // The build on disk still works; try again tomorrow.
                log::warn!("soloist: no newer build: {error:#}");
                let _ = Stamp {
                    checked: Some(now),
                    ..stamp
                }
                .write(&binary);
            }
        }
    }
    Ok(Build { binary })
}

/// Downloads the archive and unpacks `soloist` from it as it arrives, so
/// neither the archive nor the binary is ever held in memory.
fn download(binary: &Path, stamp: &Stamp, proxy: &crate::settings::ProxyConfig) -> Result<()> {
    let client =
        crate::http::build_blocking(proxy, DOWNLOAD_TIMEOUT).map_err(anyhow::Error::msg)?;
    let mut request = client.get(URL);
    if let Some(etag) = &stamp.etag
        && binary.is_file()
    {
        request = request.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    let response = request.send().context("Spotify's CDN did not answer")?;
    let now = SystemTime::now();
    if response.status() == reqwest::StatusCode::NOT_MODIFIED {
        log::info!("soloist: the build on disk is the newest");
        Stamp {
            checked: Some(now),
            ..Stamp::read(binary)
        }
        .write(binary)?;
        return Ok(());
    }
    if !response.status().is_success() {
        bail!("Spotify's CDN answered {}", response.status());
    }
    let header = |name| {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_string)
    };
    let etag = header(reqwest::header::ETAG);
    let published = header(reqwest::header::LAST_MODIFIED).and_then(|date| httpdate(&date));
    let partial = binary.with_extension("part");
    let result = unpack(GzDecoder::new(response), "soloist", &partial);
    if let Err(error) = result {
        let _ = fs::remove_file(&partial);
        return Err(error);
    }
    make_executable(&partial)?;
    fs::rename(&partial, binary).context("cannot put the new Soloist in place")?;
    Stamp {
        published,
        etag,
        checked: Some(now),
    }
    .write(binary)?;
    log::info!("soloist: downloaded the current build");
    Ok(())
}

/// Copies the regular file named `wanted` out of a tar stream into `to`.
/// Only what Spotify's archive uses is understood: ustar headers, regular
/// files and directories.
fn unpack(mut tar: impl Read, wanted: &str, to: &Path) -> Result<()> {
    let mut header = [0u8; 512];
    loop {
        if !read_block(&mut tar, &mut header)? || header.iter().all(|&byte| byte == 0) {
            bail!("the archive has no {wanted}");
        }
        let size = octal(&header[124..136]).context("the archive is damaged")?;
        let name = tar_name(&header);
        let regular = matches!(header[156], b'0' | 0);
        let padded = size.div_ceil(512) * 512;
        if regular && name.trim_start_matches("./") == wanted {
            let mut out = BufWriter::new(File::create(to)?);
            let copied = io::copy(&mut (&mut tar).take(size), &mut out)?;
            if copied != size {
                bail!("the archive ends inside {wanted}");
            }
            out.flush()?;
            out.into_inner()
                .map_err(|error| error.into_error())?
                .sync_all()?;
            check_elf(to)?;
            return Ok(());
        }
        io::copy(&mut (&mut tar).take(padded), &mut io::sink())?;
    }
}

fn read_block(reader: &mut impl Read, block: &mut [u8; 512]) -> Result<bool> {
    let mut filled = 0;
    while filled < block.len() {
        match reader.read(&mut block[filled..])? {
            0 if filled == 0 => return Ok(false),
            0 => bail!("the archive ends inside a header"),
            read => filled += read,
        }
    }
    Ok(true)
}

fn octal(field: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(field).ok()?;
    let text = text.trim_matches(|c: char| c == '\0' || c == ' ');
    if text.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(text, 8).ok()
}

fn tar_name(header: &[u8; 512]) -> String {
    let field = |range: std::ops::Range<usize>| {
        let bytes = &header[range];
        let end = bytes
            .iter()
            .position(|&byte| byte == 0)
            .unwrap_or(bytes.len());
        String::from_utf8_lossy(&bytes[..end]).into_owned()
    };
    let name = field(0..100);
    let prefix = if &header[257..262] == b"ustar" {
        field(345..500)
    } else {
        String::new()
    };
    if prefix.is_empty() {
        name
    } else {
        format!("{prefix}/{name}")
    }
}

fn check_elf(path: &Path) -> Result<()> {
    let mut magic = [0u8; 4];
    File::open(path)?.read_exact(&mut magic)?;
    if magic != *b"\x7fELF" {
        bail!("the download is not a Linux program");
    }
    Ok(())
}

#[cfg(unix)]
fn make_executable(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o755))
}

/// On Windows, Soloist's disk in the machine marks it executable.
#[cfg(not(unix))]
fn make_executable(_: &Path) -> io::Result<()> {
    Ok(())
}

/// Writes through a temporary file and a rename, so a crash never leaves
/// half a file.
pub(crate) fn write_atomically(path: &Path, contents: &[u8]) -> io::Result<()> {
    let partial = path.with_extension("tmp");
    {
        let mut file = File::create(&partial)?;
        file.write_all(contents)?;
        file.sync_all()?;
    }
    fs::rename(&partial, path)
}

/// Parses an HTTP date such as `Sun, 11 Oct 2026 18:08:57 GMT`.
fn httpdate(text: &str) -> Option<SystemTime> {
    let parsed = jiff::fmt::rfc2822::parse(text).ok()?;
    let secs = u64::try_from(parsed.timestamp().as_second()).ok()?;
    Some(SystemTime::UNIX_EPOCH + Duration::from_secs(secs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar_entry(name: &str, kind: u8, contents: &[u8]) -> Vec<u8> {
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        let size = format!("{:011o}\0", contents.len());
        header[124..136].copy_from_slice(size.as_bytes());
        header[156] = kind;
        header[257..263].copy_from_slice(b"ustar\0");
        let mut out = header.to_vec();
        out.extend_from_slice(contents);
        out.resize(out.len().div_ceil(512) * 512, 0);
        out
    }

    fn archive(entries: &[Vec<u8>]) -> Vec<u8> {
        let mut out: Vec<u8> = entries.concat();
        out.extend_from_slice(&[0u8; 1024]);
        out
    }

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("spotifast-soloist-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn unpacks_the_binary_from_spotifys_layout() {
        let dir = scratch("unpack");
        let program = b"\x7fELF and the rest of a program".to_vec();
        let tar = archive(&[
            tar_entry("CHANGELOG.md", b'0', b"notes"),
            tar_entry("THIRD_PARTY_LICENSES.txt", b'0', &[b'x'; 700]),
            tar_entry("soloist", b'0', &program),
        ]);
        let out = dir.join("soloist");
        unpack(&tar[..], "soloist", &out).unwrap();
        assert_eq!(fs::read(&out).unwrap(), program);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn refuses_an_archive_without_a_program() {
        let dir = scratch("refuse");
        let out = dir.join("soloist");
        let missing = archive(&[tar_entry("README", b'0', b"hello")]);
        assert!(unpack(&missing[..], "soloist", &out).is_err());
        let not_elf = archive(&[tar_entry("soloist", b'0', b"#!/bin/sh")]);
        assert!(unpack(&not_elf[..], "soloist", &out).is_err());
        let cut = &archive(&[tar_entry("soloist", b'0', &[0x7f; 2000])])[..1000];
        assert!(unpack(cut, "soloist", &out).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn replaces_a_build_before_it_expires() {
        let now = SystemTime::UNIX_EPOCH + Duration::from_secs(400 * 24 * 3600);
        let day = Duration::from_secs(24 * 3600);
        let fresh = Stamp::default();
        assert_eq!(plan(false, &fresh, Duration::ZERO, now), Plan::Download);
        assert_eq!(plan(true, &fresh, day * 10, now), Plan::Use);
        assert_eq!(plan(true, &fresh, day * 61, now), Plan::Refresh);
        let checked = Stamp {
            checked: Some(now - day / 2),
            ..Stamp::default()
        };
        assert_eq!(plan(true, &checked, day * 61, now), Plan::Use);
        assert_eq!(plan(true, &checked, day * 90, now), Plan::Download);
    }

    #[test]
    fn keeps_what_the_last_download_recorded() {
        let dir = scratch("stamp");
        let binary = dir.join("soloist");
        let stamp = Stamp {
            published: httpdate("Sun, 11 Oct 2026 18:08:57 GMT"),
            etag: Some("\"44b35637\"".into()),
            checked: seconds("1791741672"),
        };
        stamp.write(&binary).unwrap();
        assert_eq!(Stamp::read(&binary), stamp);
        assert_eq!(
            stamp
                .published
                .unwrap()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            1_791_742_137
        );
        let _ = fs::remove_dir_all(dir);
    }
}
