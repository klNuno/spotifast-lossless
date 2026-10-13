//! newc archives, the format Linux unpacks as its first file system: the
//! initial RAM disk Spotifast hands its Soloist virtual machine. Linux takes
//! several archives one after the other, so the shipped `base.cpio` is
//! followed by what each start adds.

/// An archive being written in memory. Entries belong to root and are dated
/// 1970, so the same input gives the same bytes.
pub(crate) struct Cpio {
    out: Vec<u8>,
    inode: u32,
}

impl Cpio {
    pub(crate) fn new() -> Self {
        Self {
            out: Vec::new(),
            inode: 0,
        }
    }

    fn entry(&mut self, name: &str, mode: u32, data: &[u8]) {
        self.inode += 1;
        let fields = [
            self.inode,
            mode,
            0,
            0,
            if mode & 0o170_000 == 0o040_000 { 2 } else { 1 },
            0,
            u32::try_from(data.len()).unwrap_or(u32::MAX),
            0,
            0,
            0,
            0,
            u32::try_from(name.len() + 1).unwrap_or(u32::MAX),
            0,
        ];
        self.out.extend_from_slice(b"070701");
        for field in fields {
            self.out
                .extend_from_slice(format!("{field:08X}").as_bytes());
        }
        self.out.extend_from_slice(name.as_bytes());
        self.out.push(0);
        self.pad();
        self.out.extend_from_slice(data);
        self.pad();
    }

    fn pad(&mut self) {
        let padding = self.out.len().next_multiple_of(4) - self.out.len();
        self.out.extend(std::iter::repeat_n(0, padding));
    }

    pub(crate) fn dir(&mut self, name: &str) {
        self.entry(name, 0o040_755, &[]);
    }

    pub(crate) fn file(&mut self, name: &str, mode: u32, data: &[u8]) {
        self.entry(name, 0o100_000 | mode, data);
    }

    pub(crate) fn symlink(&mut self, name: &str, target: &str) {
        self.entry(name, 0o120_777, target.as_bytes());
    }

    pub(crate) fn finish(mut self) -> Vec<u8> {
        self.entry("TRAILER!!!", 0, &[]);
        self.out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Reads names, modes and contents back, as Linux would.
    fn entries(mut bytes: &[u8]) -> Vec<(String, u32, Vec<u8>)> {
        let mut out = Vec::new();
        let mut offset = 0;
        loop {
            assert_eq!(&bytes[..6], b"070701");
            let field = |index: usize| {
                let at = 6 + index * 8;
                u32::from_str_radix(std::str::from_utf8(&bytes[at..at + 8]).unwrap(), 16).unwrap()
                    as usize
            };
            let (mode, size, name_size) = (field(1), field(6), field(11));
            let name = std::str::from_utf8(&bytes[110..110 + name_size - 1])
                .unwrap()
                .to_string();
            let data_at = (offset + 110 + name_size).next_multiple_of(4) - offset;
            let data = bytes[data_at..data_at + size].to_vec();
            let next = (offset + data_at + size).next_multiple_of(4) - offset;
            offset += next;
            bytes = &bytes[next..];
            if name == "TRAILER!!!" {
                assert!(bytes.is_empty());
                return out;
            }
            out.push((name, mode as u32, data));
        }
    }

    #[test]
    fn writes_what_linux_reads() {
        let mut cpio = Cpio::new();
        cpio.dir("etc");
        cpio.file("etc/soloist.args", 0o644, b"-n\nKitchen\n");
        cpio.file("init", 0o755, b"#!/bin/sh\n");
        cpio.file("empty", 0o600, b"");
        cpio.symlink("etc/cert.pem", "certs/ca.crt");
        let bytes = cpio.finish();
        assert_eq!(bytes.len() % 4, 0);
        assert_eq!(
            entries(&bytes),
            [
                ("etc".to_string(), 0o040_755, Vec::new()),
                (
                    "etc/soloist.args".into(),
                    0o100_644,
                    b"-n\nKitchen\n".to_vec()
                ),
                ("init".into(), 0o100_755, b"#!/bin/sh\n".to_vec()),
                ("empty".into(), 0o100_600, Vec::new()),
                ("etc/cert.pem".into(), 0o120_777, b"certs/ca.crt".to_vec()),
            ]
        );
    }
}
