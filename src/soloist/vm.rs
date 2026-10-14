//! Soloist on Windows, in a small Linux machine of Spotifast's own.
//!
//! Spotify publishes Soloist for Linux only. Spotifast ships QEMU, a Linux
//! kernel and a minimal user space (busybox and glibc from Debian, built by
//! `soloist/vm/build-bundle.sh`) and runs Soloist in a machine with one
//! processor, a little memory and QEMU's private user network. It needs no
//! administrator, no Windows feature and no restart: QEMU runs the machine
//! on Windows' hypervisor when the Windows Hypervisor Platform feature is
//! on, and translates its code itself otherwise.
//!
//! The machine gets three things from Spotifast's folder:
//! - `boot.cpio`, its first file system: the shipped `base.cpio`, then this
//!   start's `/init` (`soloist/vm/init`), Soloist's arguments and the
//!   certificates Windows trusts;
//! - `system.img`, a read-only disk with Soloist and the shim, written again
//!   when either changes;
//! - `data.img`, Soloist's account and cache, a sparse file the machine
//!   formats on first use.
//!
//! Spotifast reaches Soloist's WebSocket API and pairing endpoint through
//! QEMU's port forwarding on loopback, and the shim sends the audio to
//! [`HOST`], which QEMU connects to Spotifast's port on loopback. The client
//! key goes over the machine's console and is never written to disk.

use std::fs::{self, OpenOptions};
use std::io;
use std::net::{Ipv4Addr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

use super::cpio::Cpio;
use super::process::{self, Forwarded};
use super::{Prepared, ext2, install};

/// The machine's address on QEMU's user network.
const GUEST: &str = "10.0.2.15";
/// The address QEMU answers for Windows' loopback.
const HOST: &str = "10.0.2.2";
/// Soloist's WebSocket API and pairing endpoint inside the machine. The
/// shim pins the pairing endpoint's port (`PULSE_SHIM_LISTEN`).
const CONTROL_PORT: u16 = 4070;
const PAIRING_PORT: u16 = 4071;
/// Memory for Linux, Soloist and its file cache.
const MEMORY_MB: u32 = 160;
/// QEMU's cache of translated code, in MiB, when Windows' hypervisor is not
/// available. Soloist's code fills 32 MiB often enough that translating it
/// again costs more than the memory.
const CODE_CACHE_MB: u32 = 64;
/// Audio per message from the shim. Every message wakes the machine several
/// times; 100 ms instead of the shim's 10 ms cuts that tenfold. The 300 ms
/// in flight cost nothing on a seek or pause: the receiver reads ahead and
/// drops them (see `pcm.rs`).
const SHIM_CHUNK_MS: u32 = 100;
/// Soloist is told it may write only once this much of its 250 ms buffer is
/// free, so the shim does not wake it after every message.
const SHIM_REFILL_MS: u32 = 150;
/// The data disk's size. The file is sparse: Windows stores only what
/// Soloist writes, mostly its audio cache.
const DATA_DISK: u64 = 1 << 30;
/// What the installation ships next to Spotifast in `vm\`.
const BUNDLE: [&str; 6] = [
    "qemu-system-x86_64.exe",
    "vmlinuz",
    "base.cpio",
    "bios-256k.bin",
    "linuxboot_dma.bin",
    "kvmvapic.bin",
];
/// The machine's `/init`, its first process.
const INIT: &[u8] = include_bytes!("../../soloist/vm/init");
/// Bumped when the system disk's layout changes, so it is written again.
const SYSTEM_VERSION: &str = "1";

/// A machine ready to start, short of what each start chooses.
#[derive(Debug)]
pub(crate) struct Machine {
    bundle: PathBuf,
    /// Where the machine's files are kept.
    dir: PathBuf,
    /// Soloist's data from an installation that ran it outside the machine,
    /// copied in when the data disk is created.
    seed: PathBuf,
}

pub(crate) fn prepare(dir: &Path, proxy: &crate::settings::ProxyConfig) -> Result<Prepared> {
    let bundle = bundle()?;
    let build = install::ensure(&dir.join("bin"), proxy)?;
    let lib = dir.join("lib");
    let machine = dir.join("vm");
    for folder in [&lib, &machine] {
        fs::create_dir_all(folder)
            .with_context(|| format!("cannot create {}", folder.display()))?;
    }
    super::write_libraries(&lib)?;
    system_disk(&machine, &build.binary, &lib)?;
    Ok(Prepared {
        process: process::Launch::Machine(Machine {
            bundle,
            dir: machine,
            seed: dir.join("data"),
        }),
        data_host: None,
    })
}

/// The shipped machine: `vm\` beside Spotifast, or `SPOTIFAST_VM_DIR`.
fn bundle() -> Result<PathBuf> {
    let dir = match std::env::var_os("SPOTIFAST_VM_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => std::env::current_exe()
            .context("cannot find Spotifast's own folder")?
            .parent()
            .context("cannot find Spotifast's own folder")?
            .join("vm"),
    };
    if let Some(missing) = BUNDLE.iter().find(|name| !dir.join(name).is_file()) {
        bail!(
            "this installation lacks {missing}, part of the machine Soloist runs in; reinstall Spotifast"
        );
    }
    Ok(dir)
}

/// Writes `system.img` when Soloist or the shim changed since the last one.
fn system_disk(dir: &Path, soloist: &Path, lib: &Path) -> Result<()> {
    let image = dir.join("system.img");
    let marker = dir.join("system.version");
    let mut hasher = Sha256::new();
    hasher.update(SYSTEM_VERSION);
    let meta = fs::metadata(soloist).context("cannot read Soloist")?;
    hasher.update(meta.len().to_le_bytes());
    if let Ok(since) = meta
        .modified()
        .map(|time| time.duration_since(std::time::UNIX_EPOCH))
    {
        hasher.update(since.unwrap_or_default().as_nanos().to_le_bytes());
    }
    for (_, bytes) in super::LIBRARIES {
        hasher.update(bytes);
    }
    let version: String = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if image.is_file() && fs::read_to_string(&marker).is_ok_and(|text| text == version) {
        return Ok(());
    }
    let mut files = vec![("soloist", soloist.to_path_buf())];
    for (name, _) in super::LIBRARIES {
        files.push((name, lib.join(name)));
    }
    let files: Vec<(&str, &Path)> = files
        .iter()
        .map(|(name, path)| (*name, path.as_path()))
        .collect();
    let partial = image.with_extension("part");
    ext2::write(&partial, &files).context("cannot write Soloist's disk")?;
    fs::rename(&partial, &image).context("cannot put Soloist's disk in place")?;
    install::write_atomically(&marker, version.as_bytes())?;
    log::info!("soloist: wrote the machine's system disk");
    Ok(())
}

impl Machine {
    /// The QEMU command for one start of Soloist with `args`.
    pub(crate) fn command(&self, args: &process::Args<'_>) -> Result<(Command, Forwarded)> {
        let soloist = process::soloist_args(
            args,
            "/data/data",
            "/data/cache",
            &format!("{GUEST}:{CONTROL_PORT}"),
        );
        let env = [
            ("PULSE_SHIM_OUT", format!("tcp:{HOST}:{}", args.pcm_port)),
            ("PULSE_SHIM_LISTEN", format!("{GUEST}:{PAIRING_PORT}")),
            ("PULSE_SHIM_HIDE", "-k".into()),
            ("PULSE_SHIM_CHUNK_MS", SHIM_CHUNK_MS.to_string()),
            ("PULSE_SHIM_REFILL_MS", SHIM_REFILL_MS.to_string()),
        ];
        let data = self.dir.join("data.img");
        if !data.is_file() {
            sparse_file(&data, DATA_DISK).context("cannot create Soloist's data disk")?;
        }
        // The machine formats the disk on its first start; until then the
        // carried-over data goes with every start.
        let new_disk = !formatted(&data);
        let mut boot = fs::read(self.bundle.join("base.cpio")).context("cannot read base.cpio")?;
        boot.extend(self.start_archive(&soloist, &env, new_disk)?);
        let initrd = self.dir.join("boot.cpio");
        fs::write(&initrd, &boot).context("cannot write the machine's start-up files")?;

        let forwarded = Forwarded {
            control: free_port()?,
            pairing: free_port()?,
            pairing_inside: PAIRING_PORT,
        };
        let mut command = Command::new(self.bundle.join("qemu-system-x86_64.exe"));
        command.arg("-L").arg(&self.bundle);
        // QEMU's emulated HPET keeps its main loop and Windows' thread pool
        // waking about 2,300 times a second while the machine runs, even
        // though Linux times itself with the hypervisor's timers or the APIC:
        // 2 % of a core at idle without it, near 0 % with.
        command.args(["-nodefaults", "-no-user-config", "-M", "pc,hpet=off"]);
        match std::env::var("SPOTIFAST_VM_ACCEL") {
            Ok(accel) => {
                command.args(["-accel", &accel]);
            }
            // Windows' own hypervisor runs the machine's code directly when
            // the Windows Hypervisor Platform feature is on; QEMU falls back
            // to translating it when the feature is off.
            Err(_) => {
                command.args(["-accel", "whpx", "-accel"]);
                command.arg(format!("tcg,tb-size={CODE_CACHE_MB}"));
            }
        }
        command.args(["-cpu", "max", "-smp", "1", "-display", "none", "-no-reboot"]);
        command.args(["-m", &MEMORY_MB.to_string()]);
        command.arg("-kernel").arg(self.bundle.join("vmlinuz"));
        command.arg("-initrd").arg(&initrd);
        // A trusted TSC spares the idle machine the clocksource watchdog.
        command.args([
            "-append",
            "console=hvc0 quiet loglevel=1 panic=-1 init_on_alloc=0 mitigations=off tsc=reliable clocksource=tsc rdinit=/init",
            "-chardev",
            "stdio,id=console,signal=off",
            "-device",
            "virtio-serial-pci",
            "-device",
            "virtconsole,chardev=console",
            "-netdev",
        ]);
        command.arg(format!(
            "user,id=net,hostfwd=tcp:127.0.0.1:{}-{GUEST}:{CONTROL_PORT},hostfwd=tcp:127.0.0.1:{}-{GUEST}:{PAIRING_PORT}",
            forwarded.control, forwarded.pairing
        ));
        command.args(["-device", "virtio-net-pci,netdev=net,romfile="]);
        for (id, path, options) in [
            ("system", self.dir.join("system.img"), ",readonly=on"),
            ("data", data, ""),
        ] {
            command.arg("-drive").arg(format!(
                "if=none,id={id},format=raw,file={}{options}",
                qemu_value(&path)?
            ));
            command
                .arg("-device")
                .arg(format!("virtio-blk-pci,drive={id},serial={id}"));
        }
        command.args(["-device", "virtio-rng-pci"]);
        Ok((command, forwarded))
    }

    /// What this start adds to the machine's first file system.
    fn start_archive(
        &self,
        args: &[String],
        env: &[(&str, String)],
        new_disk: bool,
    ) -> Result<Vec<u8>> {
        let mut cpio = Cpio::new();
        cpio.file("init", 0o755, INIT);
        cpio.file(
            "etc/soloist.args",
            0o644,
            lines(args.iter().cloned()).as_bytes(),
        );
        cpio.file(
            "etc/soloist.env",
            0o644,
            lines(env.iter().map(|(name, value)| format!("{name}={value}"))).as_bytes(),
        );
        cpio.dir("etc/ssl");
        cpio.dir("etc/ssl/certs");
        cpio.file(
            "etc/ssl/certs/ca-certificates.crt",
            0o644,
            certificates().as_bytes(),
        );
        cpio.symlink("etc/ssl/cert.pem", "certs/ca-certificates.crt");
        if new_disk && self.seed.is_dir() {
            cpio.dir("seed");
            add_tree(&mut cpio, &self.seed, "seed")?;
            log::info!("soloist: carrying the account over into the machine");
        }
        Ok(cpio.finish())
    }
}

/// One item per line; a line break inside an item becomes a space.
fn lines(items: impl Iterator<Item = String>) -> String {
    items
        .map(|item| item.replace(['\n', '\r'], " ") + "\n")
        .collect()
}

/// Adds the files under `dir` as `name/...`.
fn add_tree(cpio: &mut Cpio, dir: &Path, name: &str) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let Some(file) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        let path = entry.path();
        let inside = format!("{name}/{file}");
        let kind = entry.file_type()?;
        if kind.is_dir() {
            cpio.dir(&inside);
            add_tree(cpio, &path, &inside)?;
        } else if kind.is_file() {
            cpio.file(&inside, 0o644, &fs::read(&path)?);
        }
    }
    Ok(())
}

/// Whether the disk image holds an ext2 file system: its superblock's magic
/// number.
fn formatted(path: &Path) -> bool {
    use std::io::{Read, Seek, SeekFrom};
    let mut magic = [0u8; 2];
    fs::File::open(path)
        .and_then(|mut file| {
            file.seek(SeekFrom::Start(1024 + 56))?;
            file.read_exact(&mut magic)
        })
        .is_ok_and(|()| magic == 0xEF53u16.to_le_bytes())
}

/// A loopback port no one listens on. QEMU takes it a moment later.
fn free_port() -> Result<u16> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).context("no free local port")?;
    Ok(listener.local_addr()?.port())
}

/// A path inside a QEMU option, where a comma has to be doubled.
fn qemu_value(path: &Path) -> Result<String> {
    let text = path
        .to_str()
        .context("the Soloist folder's path is not Unicode")?;
    Ok(text.replace(',', ",,"))
}

/// Creates `path` as a sparse file of `size` bytes, so the unwritten part
/// takes no room.
fn sparse_file(path: &Path, size: u64) -> io::Result<()> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::System::IO::DeviceIoControl;
    use windows_sys::Win32::System::Ioctl::FSCTL_SET_SPARSE;
    let partial = path.with_extension("part");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&partial)?;
    let mut returned = 0u32;
    // SAFETY: the handle is open for writing for the whole call, and the
    // control code takes no buffers.
    let sparse = unsafe {
        DeviceIoControl(
            file.as_raw_handle(),
            FSCTL_SET_SPARSE,
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            0,
            &raw mut returned,
            std::ptr::null_mut(),
        )
    };
    if sparse == 0 {
        // FAT and some network drives cannot hold sparse files; the disk
        // then takes its full size.
        log::debug!(
            "soloist: the data disk is not sparse: {}",
            io::Error::last_os_error()
        );
    }
    file.set_len(size)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&partial, path)
}

/// Ends QEMU with Spotifast, even when Spotifast is ended abruptly.
pub(crate) struct Job(windows_sys::Win32::Foundation::HANDLE);

// SAFETY: a job handle may be used and closed from any thread.
unsafe impl Send for Job {}

impl Job {
    pub(crate) fn hold(child: &std::process::Child) -> io::Result<Self> {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::System::JobObjects::{
            AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
            SetInformationJobObject,
        };
        // SAFETY: no name and no security attributes; the handle is checked.
        let handle = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        let job = Self(handle);
        // SAFETY: all-zero is a valid value for this plain C structure.
        let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = unsafe { std::mem::zeroed() };
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: the structure matches the information class and outlives
        // the call; both handles are open.
        let ok = unsafe {
            SetInformationJobObject(
                job.0,
                JobObjectExtendedLimitInformation,
                (&raw const limits).cast(),
                std::mem::size_of_val(&limits) as u32,
            ) != 0
                && AssignProcessToJobObject(job.0, child.as_raw_handle()) != 0
        };
        if ok {
            Ok(job)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // SAFETY: the handle came from CreateJobObjectW and is closed once.
        unsafe {
            windows_sys::Win32::Foundation::CloseHandle(self.0);
        }
    }
}

/// The PEM bundle Soloist reads: Mozilla's roots, which Linux distributions
/// ship, and whatever else Windows trusts (a company's own authority, a
/// proxy's). Windows alone is not enough: it downloads most roots only when
/// one is first needed, so a fresh installation lacks the ones Spotify uses.
fn certificates() -> String {
    use base64::Engine as _;
    let found = rustls_native_certs::load_native_certs();
    for error in &found.errors {
        log::debug!("soloist: skipped a Windows certificate store: {error}");
    }
    let mut seen = std::collections::HashSet::new();
    let mut pem = String::new();
    let mozilla = webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter();
    for certificate in mozilla.chain(found.certs.iter()) {
        if !seen.insert(certificate.as_ref()) {
            continue;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trusts_mozillas_roots_beside_windows_store() {
        let pem = certificates();
        let count = pem.matches("-----BEGIN CERTIFICATE-----").count();
        assert!(count >= webpki_root_certs::TLS_SERVER_ROOT_CERTS.len());
        assert_eq!(count, pem.matches("-----END CERTIFICATE-----").count());
    }

    #[test]
    fn doubles_commas_in_qemu_paths() {
        assert_eq!(
            qemu_value(Path::new(r"C:\Users\A,B\soloist\vm\data.img")).unwrap(),
            r"C:\Users\A,,B\soloist\vm\data.img"
        );
    }

    #[test]
    fn makes_a_sparse_disk_of_the_asked_size() {
        let dir = std::env::temp_dir().join(format!("spotifast-vm-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("data.img");
        sparse_file(&path, 1 << 30).unwrap();
        assert_eq!(fs::metadata(&path).unwrap().len(), 1 << 30);
        assert!(!formatted(&path));
        let file = dir.join("file");
        fs::write(&file, b"x").unwrap();
        ext2::write(&path, &[("file", &file)]).unwrap();
        assert!(formatted(&path));
        let _ = fs::remove_dir_all(dir);
    }
}
