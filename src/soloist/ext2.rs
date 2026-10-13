//! A read-only ext2 file system holding a few files in its root directory:
//! the disk that carries Soloist and the shim into Spotifast's virtual
//! machine on Windows. On a disk, Linux reads the program as it needs it
//! and can drop what it read, where the initial RAM disk would hold the
//! whole program in the machine's memory, twice while it unpacks.
//!
//! The layout is the simplest Linux mounts: 4 KiB blocks, one block group
//! (so up to 128 MiB), 32 inodes, revision 1 with typed directory entries,
//! and no free space. Each file's data blocks are contiguous and followed by
//! the blocks that point to them.

use std::fs::File;
use std::io::{self, BufWriter, Read, Seek, Write};
use std::path::Path;

const BLOCK: u64 = 4096;
const POINTERS: u64 = BLOCK / 4;
const DIRECT: u64 = 12;
const BLOCKS_PER_GROUP: u64 = BLOCK * 8;
const INODE_SIZE: usize = 128;
const INODES: u32 = 32;
const ROOT: u32 = 2;
const LOST_AND_FOUND: u32 = 11;
/// Superblock, group descriptors, two bitmaps, the inode table, then the
/// root directory and lost+found.
const ROOT_BLOCK: u64 = 5;
const LOST_BLOCK: u64 = 6;
const FIRST_FREE: u64 = 7;

/// The blocks that point to `data` data blocks.
fn pointer_blocks(data: u64) -> io::Result<u64> {
    let mut rest = data.saturating_sub(DIRECT);
    let mut count = 0;
    if rest > 0 {
        count += 1;
        rest = rest.saturating_sub(POINTERS);
    }
    if rest > 0 {
        let singles = rest.div_ceil(POINTERS);
        if singles > POINTERS {
            return Err(io::Error::other("a file is too large for Soloist's disk"));
        }
        count += 1 + singles;
    }
    Ok(count)
}

/// Where one file lives.
struct Placed {
    inode: u32,
    size: u64,
    start: u64,
    data: u64,
    pointers: u64,
}

impl Placed {
    /// The inode's 15 block pointers.
    fn block_map(&self) -> [u32; 15] {
        let mut map = [0u32; 15];
        for (index, slot) in map.iter_mut().enumerate().take(DIRECT as usize) {
            if (index as u64) < self.data {
                *slot = (self.start + index as u64) as u32;
            }
        }
        if self.data > DIRECT {
            map[12] = (self.start + self.data) as u32;
        }
        if self.data > DIRECT + POINTERS {
            map[13] = (self.start + self.data + 1) as u32;
        }
        map
    }

    /// The pointer blocks, in the order they follow the data.
    fn pointer_contents(&self) -> Vec<Vec<u32>> {
        let mut blocks = Vec::new();
        if self.data > DIRECT {
            let end = self.data.min(DIRECT + POINTERS);
            blocks.push((DIRECT..end).map(|at| (self.start + at) as u32).collect());
        }
        if self.data > DIRECT + POINTERS {
            let rest = self.data - DIRECT - POINTERS;
            let singles = rest.div_ceil(POINTERS);
            let first_single = self.start + self.data + 2;
            blocks.push((0..singles).map(|k| (first_single + k) as u32).collect());
            for k in 0..singles {
                let from = DIRECT + POINTERS + k * POINTERS;
                let to = (from + POINTERS).min(self.data);
                blocks.push((from..to).map(|at| (self.start + at) as u32).collect());
            }
        }
        blocks
    }
}

/// Writes `files`, each `(name, source)`, as the image at `path`.
pub(crate) fn write(path: &Path, files: &[(&str, &Path)]) -> io::Result<()> {
    let mut placed = Vec::with_capacity(files.len());
    let mut next = FIRST_FREE;
    for (index, (name, source)) in files.iter().enumerate() {
        if name.is_empty() || name.len() > 255 || name.contains('/') {
            return Err(io::Error::other(format!("bad file name {name:?}")));
        }
        let size = std::fs::metadata(source)?.len();
        let data = size.div_ceil(BLOCK);
        let pointers = pointer_blocks(data)?;
        placed.push(Placed {
            inode: LOST_AND_FOUND + 1 + index as u32,
            size,
            start: next,
            data,
            pointers,
        });
        next += data + pointers;
    }
    let blocks = next;
    if blocks > BLOCKS_PER_GROUP || placed.len() as u32 > INODES - LOST_AND_FOUND {
        return Err(io::Error::other("too much for Soloist's disk"));
    }

    let mut image = BufWriter::new(File::create(path)?);
    let mut head = vec![0u8; (FIRST_FREE * BLOCK) as usize];
    superblock(&mut head[1024..2048], blocks as u32, placed.len() as u32);
    group_descriptor(&mut head[BLOCK as usize..], placed.len() as u32);
    // Every block is in use, and the bits past the last block count as used.
    head[2 * BLOCK as usize..3 * BLOCK as usize].fill(0xff);
    let inodes = &mut head[3 * BLOCK as usize..4 * BLOCK as usize];
    for bit in
        (0..LOST_AND_FOUND as usize + placed.len()).chain(INODES as usize..BLOCK as usize * 8)
    {
        inodes[bit / 8] |= 1 << (bit % 8);
    }
    let table = &mut head[4 * BLOCK as usize..5 * BLOCK as usize];
    let mut put_inode =
        |number: u32, mode: u16, links: u16, size: u64, blocks: u64, map: [u32; 15]| {
            let at = (number as usize - 1) * INODE_SIZE;
            let inode = &mut table[at..at + INODE_SIZE];
            inode[0..2].copy_from_slice(&mode.to_le_bytes());
            inode[4..8].copy_from_slice(&(size as u32).to_le_bytes());
            inode[26..28].copy_from_slice(&links.to_le_bytes());
            inode[28..32].copy_from_slice(&((blocks * (BLOCK / 512)) as u32).to_le_bytes());
            for (index, pointer) in map.iter().enumerate() {
                inode[40 + index * 4..44 + index * 4].copy_from_slice(&pointer.to_le_bytes());
            }
        };
    let one = |block: u64| {
        let mut map = [0u32; 15];
        map[0] = block as u32;
        map
    };
    put_inode(ROOT, 0o040_755, 3, BLOCK, 1, one(ROOT_BLOCK));
    put_inode(LOST_AND_FOUND, 0o040_700, 2, BLOCK, 1, one(LOST_BLOCK));
    for file in &placed {
        put_inode(
            file.inode,
            0o100_755,
            1,
            file.size,
            file.data + file.pointers,
            file.block_map(),
        );
    }
    let mut root = vec![
        (ROOT, ".", 2u8),
        (ROOT, "..", 2),
        (LOST_AND_FOUND, "lost+found", 2),
    ];
    for (file, (name, _)) in placed.iter().zip(files) {
        root.push((file.inode, *name, 1));
    }
    directory(&mut head[(ROOT_BLOCK * BLOCK) as usize..], &root)?;
    directory(
        &mut head[(LOST_BLOCK * BLOCK) as usize..],
        &[(LOST_AND_FOUND, ".", 2), (ROOT, "..", 2)],
    )?;
    image.write_all(&head)?;

    for (file, (_, source)) in placed.iter().zip(files) {
        let copied = io::copy(&mut File::open(source)?.take(file.size), &mut image)?;
        if copied != file.size {
            return Err(io::Error::other("a file changed while it was copied"));
        }
        let padding = file.data * BLOCK - file.size;
        image.write_all(&vec![0; padding as usize])?;
        for pointers in file.pointer_contents() {
            let mut block = vec![0u8; BLOCK as usize];
            for (index, pointer) in pointers.iter().enumerate() {
                block[index * 4..index * 4 + 4].copy_from_slice(&pointer.to_le_bytes());
            }
            image.write_all(&block)?;
        }
    }
    let mut file = image.into_inner().map_err(io::IntoInnerError::into_error)?;
    debug_assert_eq!(file.stream_position()?, blocks * BLOCK);
    file.sync_all()
}

fn superblock(sb: &mut [u8], blocks: u32, files: u32) {
    let mut put32 = |at: usize, value: u32| sb[at..at + 4].copy_from_slice(&value.to_le_bytes());
    put32(0, INODES);
    put32(4, blocks);
    put32(12, 0);
    put32(16, INODES - LOST_AND_FOUND - files);
    put32(24, 2); // 1024 << 2
    put32(28, 2);
    put32(32, BLOCKS_PER_GROUP as u32);
    put32(36, BLOCKS_PER_GROUP as u32);
    put32(40, INODES);
    put32(76, 1); // dynamic revision
    put32(84, LOST_AND_FOUND);
    put32(96, 0x0002); // directory entries carry the file type
    let mut put16 = |at: usize, value: u16| sb[at..at + 2].copy_from_slice(&value.to_le_bytes());
    put16(54, u16::MAX); // no mount count check
    put16(56, 0xEF53);
    put16(58, 1); // clean
    put16(60, 1); // continue on errors
    put16(88, INODE_SIZE as u16);
    sb[104..120].copy_from_slice(b"spotifast-solo\0\x01");
    sb[120..127].copy_from_slice(b"soloist");
}

fn group_descriptor(gd: &mut [u8], files: u32) {
    let mut put32 = |at: usize, value: u32| gd[at..at + 4].copy_from_slice(&value.to_le_bytes());
    put32(0, 2);
    put32(4, 3);
    put32(8, 4);
    let mut put16 = |at: usize, value: u16| gd[at..at + 2].copy_from_slice(&value.to_le_bytes());
    put16(12, 0);
    put16(14, (INODES - LOST_AND_FOUND - files) as u16);
    put16(16, 2);
}

fn directory(block: &mut [u8], entries: &[(u32, &str, u8)]) -> io::Result<()> {
    let mut at = 0;
    for (index, (inode, name, kind)) in entries.iter().enumerate() {
        let length = (8 + name.len()).next_multiple_of(4);
        let record = if index + 1 == entries.len() {
            BLOCK as usize - at
        } else {
            length
        };
        if at + length > BLOCK as usize {
            return Err(io::Error::other("too many files for one directory block"));
        }
        block[at..at + 4].copy_from_slice(&inode.to_le_bytes());
        block[at + 4..at + 6].copy_from_slice(&(record as u16).to_le_bytes());
        block[at + 6] = name.len() as u8;
        block[at + 7] = *kind;
        block[at + 8..at + 8 + name.len()].copy_from_slice(name.as_bytes());
        at += record;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("spotifast-ext2-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Follows an inode's pointers the way the kernel does.
    fn read_file(image: &[u8], inode: u32) -> Vec<u8> {
        let u32_at = |at: usize| u32::from_le_bytes(image[at..at + 4].try_into().unwrap()) as usize;
        let at = 4 * BLOCK as usize + (inode as usize - 1) * INODE_SIZE;
        let size = u32_at(at + 4);
        let block = |number: usize| &image[number * BLOCK as usize..(number + 1) * BLOCK as usize];
        let pointers = |number: usize| -> Vec<usize> {
            (0..POINTERS as usize)
                .map(|k| u32_at(number * BLOCK as usize + k * 4))
                .collect()
        };
        let mut numbers: Vec<usize> = (0..12).map(|k| u32_at(at + 40 + k * 4)).collect();
        if u32_at(at + 88) != 0 {
            numbers.extend(pointers(u32_at(at + 88)));
        }
        if u32_at(at + 92) != 0 {
            for single in pointers(u32_at(at + 92)).into_iter().filter(|&n| n != 0) {
                numbers.extend(pointers(single));
            }
        }
        let mut out = Vec::new();
        for number in numbers.into_iter().take_while(|&n| n != 0) {
            out.extend_from_slice(block(number));
        }
        out.truncate(size);
        out
    }

    #[test]
    fn counts_the_blocks_that_point() {
        assert_eq!(pointer_blocks(0).unwrap(), 0);
        assert_eq!(pointer_blocks(12).unwrap(), 0);
        assert_eq!(pointer_blocks(13).unwrap(), 1);
        assert_eq!(pointer_blocks(12 + 1024).unwrap(), 1);
        assert_eq!(pointer_blocks(12 + 1024 + 1).unwrap(), 3);
        assert_eq!(pointer_blocks(12 + 1024 + 2048).unwrap(), 4);
        assert!(pointer_blocks(12 + 1024 + 1024 * 1024 + 1).is_err());
    }

    #[test]
    fn holds_files_of_every_size() {
        let dir = scratch("sizes");
        // Empty, direct blocks only, and one past the double indirect block's
        // first child, so every kind of pointer block is read back.
        let sizes = [
            0usize,
            5000,
            (DIRECT + 2 * POINTERS + 3) as usize * 4096 + 7,
        ];
        let mut sources = Vec::new();
        for (index, size) in sizes.iter().enumerate() {
            let path = dir.join(format!("f{index}"));
            let bytes: Vec<u8> = (0..*size).map(|at| (at * 7 + index) as u8).collect();
            std::fs::write(&path, &bytes).unwrap();
            sources.push((format!("file-{index}"), path, bytes));
        }
        let files: Vec<(&str, &Path)> = sources
            .iter()
            .map(|(name, path, _)| (name.as_str(), path.as_path()))
            .collect();
        let image_path = dir.join("system.img");
        write(&image_path, &files).unwrap();
        let image = std::fs::read(&image_path).unwrap();
        assert_eq!(image.len() % BLOCK as usize, 0);
        assert_eq!(&image[1024 + 56..1024 + 58], &0xEF53u16.to_le_bytes());
        for (index, (_, _, bytes)) in sources.iter().enumerate() {
            assert_eq!(&read_file(&image, LOST_AND_FOUND + 1 + index as u32), bytes);
        }
        // e2fsprogs, where installed, checks the whole file system.
        if let Ok(output) = std::process::Command::new("e2fsck")
            .args(["-fn"])
            .arg(&image_path)
            .output()
        {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stdout)
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
