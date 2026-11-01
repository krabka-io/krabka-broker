//! The page-cache write-back that the broker gives its partition logs.
//!
//! `krabka-log` paces the dirty data of an active segment, and drops the page
//! cache of a range, through one process-wide `WriteBack`. Without one, it
//! uses `fdatasync` for the write-back, and `posix_fadvise(POSIX_FADV_DONTNEED)`
//! with no wait for a drop. `sync_file_range(2)` does both jobs better on
//! Linux, but it is in `rustix` only through the git fork that the workspace
//! patches in. The published `krabka-log` cannot use that patch, so the
//! broker, which is not published, installs [`SyncFileRange`] with [`install`]
//! instead.

#[cfg(target_os = "linux")]
use {
    krabka_log::{WriteBack, set_write_back},
    rustix::fs::{Advice, SyncFileRangeFlags, fadvise, sync_file_range},
    std::{fs::File, io, num::NonZeroU64},
};

/// The Linux write-back through `sync_file_range(2)`.
///
/// A write-back is `sync_file_range` with `SYNC_FILE_RANGE_WRITE`. It starts
/// the write-out of the range and does not wait for it. It does no journal
/// commit and no disk-cache flush. `Log` paces the dirty data of the active
/// segment with it. Thus an `fdatasync` of the file, which stalls the appends
/// that run while it is in progress, occurs only at the roll and on request.
///
/// A drop waits for the write-out of the range with `sync_file_range`, then
/// gives `posix_fadvise(POSIX_FADV_DONTNEED)`. The kernel frees only clean
/// pages. A drop of a range whose write-out had only started would leave its
/// pages cached, and the page-cache ledger would no longer count them. The
/// wait is what makes a drop a drop. On pages written out long ago, which is
/// every drop of a sealed segment, the wait returns at once. No data is lost
/// either way: a later read of the range comes from the disk.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Default)]
pub struct SyncFileRange;

#[cfg(target_os = "linux")]
impl WriteBack for SyncFileRange {
    fn write_back(&self, file: &File, offset: u64, len: u64) -> io::Result<()> {
        sync_file_range(file, offset, len, SyncFileRangeFlags::WRITE)?;
        Ok(())
    }

    fn drop_cached(&self, file: &File, offset: u64, len: u64) {
        // A zero length means "to the end of the file" to both calls, which
        // is not what a caller of an empty range wants.
        if let Some(len) = NonZeroU64::new(len) {
            // The pages must be clean to go: wait for a write-out still in
            // flight, and start one for a page that is still dirty. A failure
            // here leaves the hint to do what it can.
            let _ = sync_file_range(
                file,
                offset,
                len.get(),
                SyncFileRangeFlags::WAIT_BEFORE
                    | SyncFileRangeFlags::WRITE
                    | SyncFileRangeFlags::WAIT_AFTER,
            );
            // A refused hint leaves the pages as they were.
            let _ = fadvise(file, offset, Some(len), Advice::DontNeed);
        }
    }
}

/// Installs the broker's write-back in `krabka-log` for the whole process.
///
/// On Linux this installs [`SyncFileRange`]. Call it once, before the first
/// log opens. If a write-back is already installed, `krabka-log` keeps that
/// one and this logs a warning. On other targets this does nothing, and the
/// default of `krabka-log` applies.
pub fn install() {
    #[cfg(target_os = "linux")]
    if set_write_back(Box::new(SyncFileRange)).is_err() {
        tracing::warn!(
            "a log write-back was already installed; krabka-log keeps it in place of \
             sync_file_range"
        );
    }
}

#[cfg(test)]
#[cfg(target_os = "linux")]
mod tests {
    use {
        super::SyncFileRange,
        assert2::assert,
        krabka_log::WriteBack,
        rustix::{
            fs::fstatfs,
            io::{Errno, ReadWriteFlags, preadv2},
        },
        std::{
            fs::File,
            io::{IoSliceMut, Write},
            os::unix::fs::FileExt,
        },
    };

    /// The length of the test file: sixteen 4 KiB pages.
    const LEN: u64 = 64 * 1024;

    /// The `f_type` of the filesystems that free the clean pages of a file on
    /// `POSIX_FADV_DONTNEED`: ext4, XFS and Btrfs. tmpfs and ramfs keep the
    /// only copy of a file in the page cache, so they cannot free it.
    const EVICTING_FILESYSTEMS: [i128; 3] = [0xEF53, 0x5846_5342, 0x9123_683E];

    /// A temporary file that holds `LEN` bytes, written but not synced, and a
    /// copy of the bytes.
    fn written_file() -> (File, Vec<u8>) {
        let bytes: Vec<u8> = (0..LEN).map(|i| u8::try_from(i % 251).unwrap()).collect();
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&bytes).unwrap();
        (file, bytes)
    }

    /// The whole content of `file`.
    fn read_back(file: &File) -> Vec<u8> {
        let mut bytes = vec![0; usize::try_from(LEN).unwrap()];
        file.read_exact_at(&mut bytes, 0).unwrap();
        bytes
    }

    /// Whether the page cache holds the byte at `offset`, or `None` when the
    /// filesystem cannot tell. This is the probe `krabka-log` reads with: a
    /// `preadv2(RWF_NOWAIT)` gives `EAGAIN` for a byte that is not cached.
    /// Linux 5.9 and 5.10 can read zero bytes instead.
    fn is_cached(file: &File, offset: u64) -> Option<bool> {
        let mut byte = [0u8; 1];
        match preadv2(
            file,
            &mut [IoSliceMut::new(&mut byte)],
            offset,
            ReadWriteFlags::NOWAIT,
        ) {
            Err(Errno::OPNOTSUPP | Errno::NOSYS) => None,
            Err(Errno::AGAIN) | Ok(0) => Some(false),
            _ => Some(true),
        }
    }

    #[test]
    fn write_back_of_a_written_range_succeeds() {
        let (file, bytes) = written_file();
        assert!(let Ok(()) = SyncFileRange.write_back(&file, 0, LEN));
        assert!(let Ok(()) = SyncFileRange.write_back(&file, LEN / 4, LEN / 2));
        assert!(read_back(&file) == bytes);
    }

    #[yare::parameterized(
        whole_file = { 0, LEN },
        inner_range = { LEN / 4, LEN / 2 },
        empty_range = { LEN / 2, 0 },
    )]
    fn drop_cached_keeps_the_bytes_readable_and_intact(offset: u64, len: u64) {
        let (file, bytes) = written_file();
        SyncFileRange.drop_cached(&file, offset, len);
        // yare's expansion makes the imported `assert` ambiguous with std's.
        assert2::assert!(read_back(&file) == bytes, "offset {offset}, len {len}");
    }

    /// The drop waits for the write-out of dirty pages, so it frees the pages
    /// of a range written a moment ago. The check runs only on a filesystem
    /// that frees clean pages and that the `RWF_NOWAIT` probe can ask.
    #[test]
    fn drop_cached_frees_a_dirty_range_where_the_filesystem_can() {
        let (file, bytes) = written_file();
        let f_type = i128::from(fstatfs(&file).unwrap().f_type);
        if !EVICTING_FILESYSTEMS.contains(&f_type) || is_cached(&file, 0).is_none() {
            return;
        }
        assert!(is_cached(&file, 0) == Some(true));
        SyncFileRange.drop_cached(&file, 0, LEN);
        for offset in [0, LEN / 2, LEN - 1] {
            assert!(is_cached(&file, offset) == Some(false), "offset {offset}");
        }
        assert!(read_back(&file) == bytes);
    }
}
