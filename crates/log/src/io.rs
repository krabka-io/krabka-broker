//! Injectable file I/O for every durable write the log makes.
//!
//! The active `.log` file keeps the three handle-shaped methods the append
//! hot path uses. Every other durable write in the crate -- the sparse
//! indexes, the `.stampindex`, the producer snapshots, the leader-epoch
//! checkpoint, the compaction swap and the segment deletions retention
//! performs -- goes through the path-and-target methods below, so a test can
//! fail exactly one class of file and watch what recovery makes of it.

use std::{
    fmt::Debug,
    fs::File,
    io::{IoSlice, Write},
    path::Path,
};

/// Which on-disk file class an [`LogIo`] operation touches.
///
/// A fault injector matches on this to fail one durable write without
/// disturbing the rest of the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IoTarget {
    /// A segment's sparse `.index`.
    OffsetIndex,
    /// A segment's sparse `.timeindex`.
    TimeIndex,
    /// A segment's `.stampindex` sidecar.
    StampIndex,
    /// A `<offset>.snapshot` producer-state snapshot, or its `.tmp` staging
    /// file.
    ProducerSnapshot,
    /// The partition's `leader-epoch-checkpoint`, or its `.tmp` staging file.
    LeaderEpochCheckpoint,
    /// The partition's `log-start-offset-checkpoint`, or its `.tmp` staging
    /// file.
    LogStartOffsetCheckpoint,
    /// A compaction `.swap` file, or a segment file the swap replaces.
    CompactionSwap,
    /// A segment file being renamed to its `.deleted` tombstone or unlinked
    /// by retention.
    SegmentDeletion,
}

/// The operating-system I/O boundary every durable log write crosses.
///
/// The default methods perform real file I/O. Tests can override only the
/// operation they need to fail, while reads continue to use the segment's
/// shared `Arc<File>` directly.
pub trait LogIo: Debug + Send + Sync {
    /// Write bytes at the active `.log` file's current cursor.
    ///
    /// # Errors
    /// Returns the underlying write error.
    fn write(&self, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        (&*file).write(buf)
    }

    /// Write byte slices at the active `.log` file's current cursor.
    ///
    /// # Errors
    /// Returns the underlying vectored-write error.
    fn write_vectored(&self, file: &File, bufs: &[IoSlice<'_>]) -> std::io::Result<usize> {
        (&*file).write_vectored(bufs)
    }

    /// Flush the active `.log` file's data to stable storage.
    ///
    /// # Errors
    /// Returns the underlying data-sync error.
    fn sync_data(&self, file: &File) -> std::io::Result<()> {
        file.sync_data()
    }

    /// Write bytes at `file`'s current cursor on behalf of `target`.
    ///
    /// Like [`std::io::Write::write`], this may write fewer bytes than asked
    /// for; [`write_all`] is the loop that finishes the buffer.
    ///
    /// # Errors
    /// Returns the underlying write error.
    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        let _ = target;
        (&*file).write(buf)
    }

    /// Flush `target`'s file data to stable storage.
    ///
    /// # Errors
    /// Returns the underlying data-sync error.
    fn sync_file(&self, target: IoTarget, file: &File) -> std::io::Result<()> {
        let _ = target;
        file.sync_data()
    }

    /// `fsync` a directory so the names created or renamed inside it are
    /// durable.
    ///
    /// # Errors
    /// Returns the underlying open or sync error.
    fn sync_dir(&self, dir: &Path) -> std::io::Result<()> {
        real_sync_dir(dir)
    }

    /// Rename a file on behalf of `target`.
    ///
    /// # Errors
    /// Returns the underlying rename error.
    fn rename(&self, target: IoTarget, from: &Path, to: &Path) -> std::io::Result<()> {
        let _ = target;
        std::fs::rename(from, to)
    }

    /// Unlink a file on behalf of `target`.
    ///
    /// # Errors
    /// Returns the underlying unlink error.
    fn remove_file(&self, target: IoTarget, path: &Path) -> std::io::Result<()> {
        let _ = target;
        std::fs::remove_file(path)
    }
}

/// `fsync` `dir` for real.
///
/// Rust's standard directory-open path is supported on Unix, where syncing the
/// parent is what makes a rename or a fresh name durable. Windows offers no
/// equivalent through `std` (`File::open` on a directory fails with `EACCES`),
/// so the call is a no-op there and the file contents are still synced before
/// every rename.
pub(crate) fn real_sync_dir(dir: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

/// Write the whole of `buf` to `file` through `io`, looping over short writes.
///
/// A `LogIo` that writes part of the buffer and then fails leaves the file
/// torn at exactly that boundary, which is the shape a disk-full write has on
/// a real filesystem.
///
/// # Errors
/// Returns the first error the underlying writes produce, or `WriteZero` when
/// a write makes no progress.
pub(crate) fn write_all(
    io: &dyn LogIo,
    target: IoTarget,
    file: &File,
    mut buf: &[u8],
) -> std::io::Result<()> {
    while !buf.is_empty() {
        match io.write_at(target, file, buf) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(written) => buf = &buf[written..],
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) struct FileIo;

impl LogIo for FileIo {}

/// The shared handle every structure defaults to before a test installs its
/// own.
pub(crate) fn file_io() -> std::sync::Arc<dyn LogIo> {
    std::sync::Arc::new(FileIo)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use assert2::check;

    use super::*;

    /// A writer that replays a script of results, recording what each call was
    /// offered. A regular file cannot be persuaded to write short or to make
    /// no progress on demand, so the loop's two obligations -- resume where the
    /// last write stopped, and refuse to spin on a writer that writes nothing
    /// -- are only reachable through one of these.
    #[derive(Debug)]
    struct Scripted {
        script: Mutex<std::vec::IntoIter<std::io::Result<usize>>>,
        seen: Mutex<Vec<Vec<u8>>>,
    }

    impl Scripted {
        fn new(script: Vec<std::io::Result<usize>>) -> Self {
            Self {
                script: Mutex::new(script.into_iter()),
                seen: Mutex::new(Vec::new()),
            }
        }
    }

    impl LogIo for Scripted {
        fn write_at(&self, _target: IoTarget, _file: &File, buf: &[u8]) -> std::io::Result<usize> {
            self.seen.lock().unwrap().push(buf.to_vec());
            self.script
                .lock()
                .unwrap()
                .next()
                .unwrap_or(Ok(buf.len()))
                .map(|written| written.min(buf.len()))
        }
    }

    fn scratch_file() -> (tempfile::TempDir, File) {
        let dir = tempfile::tempdir().unwrap();
        let file = File::create(dir.path().join("scratch")).unwrap();
        (dir, file)
    }

    #[test]
    fn write_all_resumes_short_writes_retries_interruptions_and_stops_on_no_progress() {
        let (_dir, file) = scratch_file();

        // Three short writes finish a six-byte buffer, each offered only what
        // the last one left.
        let io = Scripted::new(vec![Ok(2), Ok(3), Ok(1)]);
        write_all(&io, IoTarget::OffsetIndex, &file, b"abcdef").unwrap();
        check!(
            *io.seen.lock().unwrap() == vec![b"abcdef".to_vec(), b"cdef".to_vec(), b"f".to_vec()]
        );

        // An interrupted write is retried with the same bytes, not skipped past.
        let io = Scripted::new(vec![
            Ok(2),
            Err(std::io::Error::from(std::io::ErrorKind::Interrupted)),
            Ok(4),
        ]);
        write_all(&io, IoTarget::OffsetIndex, &file, b"abcdef").unwrap();
        check!(
            *io.seen.lock().unwrap()
                == vec![b"abcdef".to_vec(), b"cdef".to_vec(), b"cdef".to_vec()]
        );

        // A writer that makes no progress is a `WriteZero`, not a spin.
        let io = Scripted::new(vec![Ok(0)]);
        let error = write_all(&io, IoTarget::OffsetIndex, &file, b"abcdef").unwrap_err();
        check!(error.kind() == std::io::ErrorKind::WriteZero);

        // Any other error is returned as it stands.
        let io = Scripted::new(vec![
            Ok(1),
            Err(std::io::Error::from(std::io::ErrorKind::StorageFull)),
        ]);
        let error = write_all(&io, IoTarget::OffsetIndex, &file, b"abcdef").unwrap_err();
        check!(error.kind() == std::io::ErrorKind::StorageFull);
    }
}
