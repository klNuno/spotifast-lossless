//! Soloist on Windows, in a WSL1 distribution of Spotifast's own.
//!
//! WSL1 runs Linux programs as Windows processes, with no virtual machine,
//! so Soloist costs about what it costs on Linux. The distribution holds
//! only what Soloist cannot do without: glibc from Debian's official image,
//! the CA certificates Windows trusts, and a few files in `/etc`. Soloist,
//! the shim and Soloist's data stay in Spotifast's folder on the Windows
//! side, reached through `/mnt`, so updating Soloist never touches the
//! distribution.

use std::ffi::OsStr;
use std::fs;
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use flate2::read::GzDecoder;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{Prepared, install, process};

/// The distribution's name, as `wsl.exe` lists it.
pub(crate) const DISTRO: &str = "Spotifast-Soloist";
/// Bumped when the distribution's contents change, so it is rebuilt.
const ROOTFS_VERSION: &str = "1";
/// Debian 13: glibc 2.41, newer than the 2.38 Soloist needs.
const IMAGE: &str = "library/debian";
const TAG: &str = "trixie-slim";
const REGISTRY: &str = "https://registry-1.docker.io";

/// The command that turns on what WSL1 needs. Spotifast never runs it: it
/// needs an administrator and a restart, so the user decides.
pub const ENABLE_COMMAND: &str = "dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart";

pub(crate) fn prepare(dir: &Path, proxy: &crate::settings::ProxyConfig) -> Result<Prepared> {
    if !wsl1_installed() {
        bail!(
            "Windows Subsystem for Linux is off. Run this in an administrator terminal, then restart Windows:\n{ENABLE_COMMAND}"
        );
    }
    ensure_distro(dir, proxy)?;
    let build = install::ensure(&dir.join("bin"), proxy)?;
    let lib = dir.join("lib");
    let data = dir.join("data");
    let cache = dir.join("cache");
    for folder in [&lib, &data, &cache] {
        fs::create_dir_all(folder)
            .with_context(|| format!("cannot create {}", folder.display()))?;
    }
    super::write_libraries(&lib)?;
    Ok(Prepared {
        process: process::Launch {
            program: wsl_exe(),
            prefix: vec![
                "-d".into(),
                DISTRO.into(),
                "-u".into(),
                "root".into(),
                "--exec".into(),
                linux_path(&build.binary)?,
            ],
            env: vec![("LD_LIBRARY_PATH".into(), linux_path(&lib)?)],
            data: linux_path(&data)?,
            cache: linux_path(&cache)?,
            distro: Some(DISTRO.into()),
        },
        data_host: data,
    })
}

fn wsl_exe() -> PathBuf {
    let root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
    Path::new(&root).join("System32").join("wsl.exe")
}

/// WSL1's kernel driver is present: the optional feature is on.
fn wsl1_installed() -> bool {
    let root = std::env::var_os("SystemRoot").unwrap_or_else(|| "C:\\Windows".into());
    Path::new(&root)
        .join("System32")
        .join("drivers")
        .join("lxcore.sys")
        .is_file()
        && wsl_exe().is_file()
}

/// `C:\Users\Ana\AppData` as `/mnt/c/Users/Ana/AppData`.
fn linux_path(path: &Path) -> Result<String> {
    let text = path
        .to_str()
        .context("the Soloist folder's path is not Unicode")?;
    let text = text.strip_prefix(r"\\?\").unwrap_or(text);
    let mut chars = text.chars();
    match (chars.next(), chars.next()) {
        (Some(drive), Some(':')) if drive.is_ascii_alphabetic() => {
            let rest = chars.as_str().replace('\\', "/");
            Ok(format!("/mnt/{}{}", drive.to_ascii_lowercase(), rest))
        }
        _ => bail!("Soloist's folder must be on a local drive, not {text}"),
    }
}

fn hidden(program: impl AsRef<OsStr>) -> Command {
    use std::os::windows::process::CommandExt;
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    let mut command = Command::new(program);
    command.creation_flags(CREATE_NO_WINDOW);
    command
}

/// `wsl.exe` writes UTF-16 to a pipe.
fn decode_wsl(bytes: &[u8]) -> String {
    if bytes.len() >= 2
        && bytes.len().is_multiple_of(2)
        && bytes.iter().skip(1).step_by(2).all(|&b| b == 0)
    {
        let units: Vec<u16> = bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u16::from_le_bytes(*pair))
            .collect();
        String::from_utf16_lossy(&units)
    } else {
        String::from_utf8_lossy(bytes).into_owned()
    }
}

fn distro_registered() -> Result<bool> {
    let output = hidden(wsl_exe())
        .args(["--list", "--quiet"])
        .stdin(Stdio::null())
        .output()
        .context("cannot run wsl.exe")?;
    // With no distribution at all, wsl.exe exits with an error.
    Ok(decode_wsl(&output.stdout)
        .lines()
        .any(|line| line.trim().trim_matches('\0') == DISTRO))
}

fn ensure_distro(dir: &Path, proxy: &crate::settings::ProxyConfig) -> Result<()> {
    let marker = dir.join("rootfs.version");
    let current = fs::read_to_string(&marker).unwrap_or_default();
    let registered = distro_registered()?;
    if registered && current.trim() == ROOTFS_VERSION {
        return Ok(());
    }
    if registered {
        let status = hidden(wsl_exe())
            .args(["--unregister", DISTRO])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .context("cannot run wsl.exe")?;
        if !status.success() {
            bail!("cannot remove the old Soloist distribution");
        }
    }
    let rootfs = build_rootfs(proxy)?;
    let tar = dir.join("rootfs.tar");
    let home = dir.join("wsl");
    fs::create_dir_all(&home)?;
    fs::write(&tar, &rootfs)?;
    let output = hidden(wsl_exe())
        .arg("--import")
        .arg(DISTRO)
        .arg(&home)
        .arg(&tar)
        .args(["--version", "1"])
        .stdin(Stdio::null())
        .output()
        .context("cannot run wsl.exe")?;
    let _ = fs::remove_file(&tar);
    if !output.status.success() {
        bail!(
            "WSL did not create Soloist's distribution: {}",
            decode_wsl(&output.stdout).trim()
        );
    }
    install::write_atomically(&marker, ROOTFS_VERSION.as_bytes())?;
    log::info!("soloist: created the {DISTRO} WSL distribution");
    Ok(())
}

/// Builds the distribution: glibc from Debian's image, then `/etc`.
fn build_rootfs(proxy: &crate::settings::ProxyConfig) -> Result<Vec<u8>> {
    let mut tar = TarWriter::default();
    for dir in [
        "etc",
        "etc/ssl",
        "etc/ssl/certs",
        "usr",
        "usr/lib",
        "usr/lib/x86_64-linux-gnu",
        "usr/lib64",
        "dev",
        "proc",
        "sys",
        "run",
        "mnt",
        "home",
    ] {
        tar.dir(dir, 0o755);
    }
    tar.dir("root", 0o700);
    tar.dir("tmp", 0o1777);
    tar.symlink("lib", "usr/lib");
    tar.symlink("lib64", "usr/lib64");
    tar.symlink(
        "usr/lib64/ld-linux-x86-64.so.2",
        "../lib/x86_64-linux-gnu/ld-linux-x86-64.so.2",
    );
    let glibc = fetch_glibc(proxy)?;
    if !glibc.iter().any(|entry| entry.name.ends_with("/libc.so.6")) {
        bail!("Debian's image has no glibc where expected");
    }
    for entry in glibc {
        match entry.link {
            Some(target) => tar.symlink(&entry.name, &target),
            None => tar.file(&entry.name, 0o755, &entry.data),
        }
    }
    let certificates = certificates();
    tar.file(
        "etc/ssl/certs/ca-certificates.crt",
        0o644,
        certificates.as_bytes(),
    );
    tar.symlink("etc/ssl/cert.pem", "certs/ca-certificates.crt");
    for (name, text) in [
        ("etc/passwd", "root:x:0:0:root:/root:/bin/false\n"),
        ("etc/group", "root:x:0:\n"),
        (
            "etc/nsswitch.conf",
            "passwd: files\ngroup: files\nhosts: files dns\n",
        ),
        ("etc/hosts", "127.0.0.1 localhost\n::1 localhost\n"),
        (
            "etc/os-release",
            "NAME=\"Spotifast Soloist\"\nID=spotifast-soloist\n",
        ),
        // No Windows programs from inside, nothing added to PATH; WSL
        // still writes resolv.conf for DNS.
        (
            "etc/wsl.conf",
            "[interop]\nenabled=false\nappendWindowsPath=false\n\n[network]\ngenerateResolvConf=true\n",
        ),
    ] {
        tar.file(name, 0o644, text.as_bytes());
    }
    Ok(tar.finish())
}

/// The trust store Windows uses, as the PEM bundle Soloist reads.
fn certificates() -> String {
    use base64::Engine as _;
    let found = rustls_native_certs::load_native_certs();
    for error in &found.errors {
        log::debug!("soloist: skipped a Windows certificate store: {error}");
    }
    let mut pem = String::new();
    for certificate in found.certs {
        let encoded = base64::engine::general_purpose::STANDARD.encode(certificate.as_ref());
        pem.push_str("-----BEGIN CERTIFICATE-----\n");
        for line in encoded.as_bytes().chunks(64) {
            pem.push_str(std::str::from_utf8(line).unwrap_or_default());
            pem.push('\n');
        }
        pem.push_str("-----END CERTIFICATE-----\n");
    }
    pem
}

struct Entry {
    name: String,
    link: Option<String>,
    data: Vec<u8>,
}

/// Streams Debian's image layer and keeps glibc's three libraries, checking
/// the layer against the digest its manifest names.
fn fetch_glibc(proxy: &crate::settings::ProxyConfig) -> Result<Vec<Entry>> {
    let client =
        crate::http::build_blocking(proxy, Duration::from_secs(300)).map_err(anyhow::Error::msg)?;
    let token: Value = client
        .get("https://auth.docker.io/token")
        .query(&[
            ("service", "registry.docker.io"),
            ("scope", &format!("repository:{IMAGE}:pull")),
        ])
        .send()
        .and_then(reqwest::blocking::Response::error_for_status)
        .context("Docker Hub did not answer")?
        .json()?;
    let token = token["token"]
        .as_str()
        .context("Docker Hub gave no token")?
        .to_string();
    let get = |path: &str, accept: &str| {
        client
            .get(format!("{REGISTRY}/v2/{IMAGE}/{path}"))
            .bearer_auth(&token)
            .header(reqwest::header::ACCEPT, accept)
            .send()
            .and_then(reqwest::blocking::Response::error_for_status)
    };
    let index: Value = get(
        &format!("manifests/{TAG}"),
        "application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json",
    )?
    .json()?;
    let manifest_digest = index["manifests"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|manifest| {
            manifest["platform"]["architecture"] == "amd64" && manifest["platform"]["os"] == "linux"
        })
        .and_then(|manifest| manifest["digest"].as_str())
        .context("Debian's image has no x86_64 build")?
        .to_string();
    let manifest: Value = get(
        &format!("manifests/{manifest_digest}"),
        "application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json",
    )?
    .json()?;
    let layer = manifest["layers"][0]["digest"]
        .as_str()
        .context("Debian's image has no layer")?
        .to_string();
    let expected = layer
        .strip_prefix("sha256:")
        .context("unknown digest kind")?
        .to_string();
    let response = get(&format!("blobs/{layer}"), "*/*")?;
    let mut hashing = Hashing {
        inner: response,
        hasher: Sha256::new(),
    };
    let entries = {
        let mut gz = GzDecoder::new(&mut hashing);
        let entries = glibc_entries(&mut gz)?;
        // Hash every byte, the archive's padding included.
        io::copy(&mut gz, &mut io::sink())?;
        entries
    };
    io::copy(&mut hashing, &mut io::sink())?;
    let actual = hex(&hashing.hasher.finalize());
    if actual != expected {
        bail!("Debian's image did not match its digest");
    }
    Ok(entries)
}

struct Hashing<R> {
    inner: R,
    hasher: Sha256,
}

impl<R: Read> Read for Hashing<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.hasher.update(&buf[..read]);
        Ok(read)
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The dynamic loader, libc and libm, as files or the links Debian makes.
fn wanted(name: &str) -> bool {
    let Some(file) = name.strip_prefix("usr/lib/x86_64-linux-gnu/") else {
        return false;
    };
    !file.contains('/')
        && (file.starts_with("libc.so")
            || file.starts_with("libm.so")
            || file.starts_with("ld-linux-x86-64.so"))
}

fn glibc_entries(tar: &mut impl Read) -> Result<Vec<Entry>> {
    let mut entries = Vec::new();
    let mut header = [0u8; 512];
    loop {
        if !read_exact_or_end(tar, &mut header)? || header.iter().all(|&byte| byte == 0) {
            break;
        }
        let size = octal(&header[124..136]).context("Debian's image is damaged")?;
        let name = field(&header, 0..100);
        let prefix = field(&header, 345..500);
        let name = if &header[257..262] == b"ustar" && !prefix.is_empty() {
            format!("{prefix}/{name}")
        } else {
            name
        };
        let name = name.trim_start_matches("./").to_string();
        let padded = size.div_ceil(512) * 512;
        let kind = header[156];
        if wanted(&name) && matches!(kind, b'0' | 0 | b'2') {
            if kind == b'2' {
                entries.push(Entry {
                    name,
                    link: Some(field(&header, 157..257)),
                    data: Vec::new(),
                });
                io::copy(&mut tar.take(padded), &mut io::sink())?;
            } else {
                let mut data = Vec::with_capacity(size as usize);
                tar.take(size).read_to_end(&mut data)?;
                io::copy(&mut tar.take(padded - size), &mut io::sink())?;
                entries.push(Entry {
                    name,
                    link: None,
                    data,
                });
            }
            continue;
        }
        io::copy(&mut tar.take(padded), &mut io::sink())?;
    }
    Ok(entries)
}

fn read_exact_or_end(reader: &mut impl Read, block: &mut [u8; 512]) -> Result<bool> {
    let mut filled = 0;
    while filled < block.len() {
        match reader.read(&mut block[filled..])? {
            0 if filled == 0 => return Ok(false),
            0 => return Err(anyhow!("an archive ends inside a header")),
            read => filled += read,
        }
    }
    Ok(true)
}

fn field(header: &[u8; 512], range: std::ops::Range<usize>) -> String {
    let bytes = &header[range];
    let end = bytes
        .iter()
        .position(|&byte| byte == 0)
        .unwrap_or(bytes.len());
    String::from_utf8_lossy(&bytes[..end]).into_owned()
}

fn octal(field: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(field).ok()?;
    let text = text.trim_matches(|c: char| c == '\0' || c == ' ');
    if text.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(text, 8).ok()
}

/// Writes a ustar archive in memory, owned by root.
#[derive(Default)]
struct TarWriter {
    out: Vec<u8>,
}

impl TarWriter {
    fn header(&mut self, name: &str, mode: u32, size: usize, kind: u8, link: &str) {
        let mut header = [0u8; 512];
        let (prefix, name) = if name.len() > 100 {
            let split = name[..name.len() - 100.min(name.len())]
                .rfind('/')
                .or_else(|| name.rfind('/'))
                .unwrap_or(0);
            (&name[..split], &name[split + 1..])
        } else {
            ("", name)
        };
        header[..name.len()].copy_from_slice(name.as_bytes());
        let put = |header: &mut [u8; 512], at: usize, width: usize, value: u64| {
            let text = format!("{value:0w$o}\0", w = width - 1);
            header[at..at + width].copy_from_slice(text.as_bytes());
        };
        put(&mut header, 100, 8, u64::from(mode));
        put(&mut header, 108, 8, 0);
        put(&mut header, 116, 8, 0);
        put(&mut header, 124, 12, size as u64);
        put(&mut header, 136, 12, 0);
        header[156] = kind;
        header[157..157 + link.len()].copy_from_slice(link.as_bytes());
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        header[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
        header[148..156].copy_from_slice(b"        ");
        let sum: u32 = header.iter().map(|&byte| u32::from(byte)).sum();
        let text = format!("{sum:06o}\0 ");
        header[148..156].copy_from_slice(text.as_bytes());
        self.out.extend_from_slice(&header);
    }

    fn dir(&mut self, name: &str, mode: u32) {
        self.header(&format!("{name}/"), mode, 0, b'5', "");
    }

    fn symlink(&mut self, name: &str, target: &str) {
        self.header(name, 0o777, 0, b'2', target);
    }

    fn file(&mut self, name: &str, mode: u32, data: &[u8]) {
        self.header(name, mode, data.len(), b'0', "");
        self.out.extend_from_slice(data);
        let padding = data.len().div_ceil(512) * 512 - data.len();
        self.out.extend(std::iter::repeat_n(0, padding));
    }

    fn finish(mut self) -> Vec<u8> {
        self.out.extend_from_slice(&[0u8; 1024]);
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_windows_folders_into_wsl() {
        assert_eq!(
            linux_path(Path::new(r"C:\Users\Ana\AppData\Local\spotifast\soloist")).unwrap(),
            "/mnt/c/Users/Ana/AppData/Local/spotifast/soloist"
        );
        assert_eq!(linux_path(Path::new(r"\\?\D:\x y")).unwrap(), "/mnt/d/x y");
        assert!(linux_path(Path::new(r"\\server\share\x")).is_err());
    }

    #[test]
    fn reads_wsl_output_in_either_encoding() {
        let utf16: Vec<u8> = "Spotifast-Soloist\r\n"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        assert_eq!(decode_wsl(&utf16).trim(), DISTRO);
        assert_eq!(decode_wsl(b"Ubuntu\n").trim(), "Ubuntu");
    }

    #[test]
    fn keeps_only_glibc_from_the_image() {
        let mut layer = TarWriter::default();
        layer.dir("usr/lib/x86_64-linux-gnu", 0o755);
        layer.file("usr/lib/x86_64-linux-gnu/libc.so.6", 0o755, b"libc");
        layer.file("usr/lib/x86_64-linux-gnu/libz.so.1", 0o644, &[1; 900]);
        layer.symlink("usr/lib/x86_64-linux-gnu/libm.so.6", "libm-2.41.so");
        layer.file(
            "usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2",
            0o755,
            &[7; 600],
        );
        layer.file("usr/lib/x86_64-linux-gnu/gconv/libc.so", 0o644, b"no");
        let bytes = layer.finish();
        let entries = glibc_entries(&mut &bytes[..]).unwrap();
        let names: Vec<_> = entries.iter().map(|entry| entry.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "usr/lib/x86_64-linux-gnu/libc.so.6",
                "usr/lib/x86_64-linux-gnu/libm.so.6",
                "usr/lib/x86_64-linux-gnu/ld-linux-x86-64.so.2"
            ]
        );
        assert_eq!(entries[0].data, b"libc");
        assert_eq!(entries[1].link.as_deref(), Some("libm-2.41.so"));
        assert_eq!(entries[2].data, [7; 600]);
    }
}
