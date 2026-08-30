//! Injectable writes and data syncs for the active `.log` file.

use std::{
    fmt::Debug,
    fs::File,
    io::{IoSlice, Write},
};

/// The narrow operating-system I/O boundary used by segment appends.
///
/// The default methods perform real file I/O. Tests can override only the
/// operation they need to fail, while reads continue to use the segment's
/// shared `Arc<File>` directly.
pub trait LogIo: Debug + Send + Sync {
    /// Write bytes at the file's current cursor.
    ///
    /// # Errors
    /// Returns the underlying write error.
    fn write(&self, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        (&*file).write(buf)
    }

    /// Write byte slices at the file's current cursor.
    ///
    /// # Errors
    /// Returns the underlying vectored-write error.
    fn write_vectored(&self, file: &File, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        (&*file).write_vectored(bufs)
    }

    /// Flush file data to stable storage.
    ///
    /// # Errors
    /// Returns the underlying data-sync error.
    fn sync_data(&self, file: &File) -> std::io::Result<()> {
        file.sync_data()
    }
}

#[derive(Debug)]
pub(crate) struct FileIo;

impl LogIo for FileIo {}
