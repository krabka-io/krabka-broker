//! The two sparse per-segment indexes: the `.index` file, which maps a
//! relative offset to a byte position in the segment's log, and the
//! `.timeindex` file, which maps a timestamp to a relative offset.
//!
//! One submodule per on-disk artifact. `offset` holds the 8-byte `.index`
//! entry layout, its loader and its floor lookup; `time` holds the 12-byte
//! `.timeindex` entry layout, its loader and its timestamp lookup. Both
//! layouts are byte-compatible with Kafka's, so each entry's encode, decode
//! and binary-search paths stay beside the layout they read.

use std::{
    fs::{File, OpenOptions},
    io::Read,
    path::Path,
};

use crate::error::LogError;

// Both index files participate in the same segment flush.
macro_rules! flush_handle {
    () => {
        #[cfg(not(target_os = "wasi"))]
        pub(crate) fn flush_handle(&self) -> std::io::Result<File> {
            self.file.try_clone()
        }
    };
}

mod offset;
mod time;

pub(crate) use self::{offset::OFFSET_ENTRY_SIZE, time::TIME_ENTRY_SIZE};
pub use self::{offset::OffsetIndex, time::TimeIndex};

fn open_index_file(path: &Path) -> Result<(File, Vec<u8>), LogError> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    Ok((file, bytes))
}

/// Read a fixed-width sidecar, treating only a missing file as an empty index.
pub(crate) fn read_sidecar(
    path: &Path,
    entry_size: usize,
    name: &str,
) -> Result<Vec<u8>, LogError> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => return Err(error.into()),
    };
    if !bytes.len().is_multiple_of(entry_size) {
        return Err(LogError::Corrupt(format!(
            "{name} {} has length {} not divisible by {entry_size}",
            path.display(),
            bytes.len(),
        )));
    }
    Ok(bytes)
}
