//! The page cache a process's logs keep for the segments they wrote.
//!
//! A segment written through the page cache leaves its pages behind after
//! the kernel has written them out, and the kernel keeps them until memory
//! runs short. In a container with a memory limit, that point is the limit
//! itself, and from then on every page an append charges to the cgroup is
//! reclaimed inside the append: the kernel scans the cgroup's own pages for
//! one to free before the write goes on. On the OMB 50K 1 KiB records/s RF3
//! workloads in 10 GiB containers, the three brokers' cgroups reached their
//! limit after four minutes, and the publish p50 then doubled for minutes at
//! a time while the followers' kernel time rose by half, until the kernel
//! evicted gigabytes in one burst. The same cell written through `O_DIRECT`,
//! which keeps no page cache, showed none of it.
//!
//! A [`Ledger`] keeps the logs under a budget instead. Every range a
//! write-back job has synced is recorded, oldest first, and while the
//! recorded bytes exceed the budget the oldest ranges are dropped from the
//! page cache with `posix_fadvise(POSIX_FADV_DONTNEED)`, off the appending
//! thread. The cgroup then stays below its limit and reclaims nothing in an
//! append. A consumer further behind than the budget reads from the disk, as
//! it does once the kernel has evicted the range. A recorded range holds its
//! segment's file open, so a segment that retention deleted keeps its blocks
//! until its ranges leave the ledger: at most the budget's worth.

use {
    crate::io::LogIo,
    std::{
        collections::VecDeque,
        fs::File,
        sync::{Arc, Mutex, MutexGuard, PoisonError},
    },
};

/// The page size the recorded ranges align to. Larger pages make a range
/// end short of a whole page, which stays in the cache.
pub(in crate::log) const PAGE: u64 = 4096;

/// The most bytes one recorded range grows to by taking in the range that
/// follows it in the same file. Dropping is by whole ranges from the front,
/// so this is the most the recorded bytes fall below the budget at once.
const MAX_RANGE: u64 = 32 * 1024 * 1024;

/// A range of a `.log` file whose pages the page cache holds.
#[derive(Debug)]
struct Range {
    file: Arc<File>,
    io: Arc<dyn LogIo>,
    offset: u64,
    len: u64,
}

#[derive(Debug, Default)]
struct State {
    /// Oldest first.
    ranges: VecDeque<Range>,
    /// The sum of the ranges' lengths.
    bytes: u64,
}

impl State {
    fn record(&mut self, file: Arc<File>, io: Arc<dyn LogIo>, offset: u64, len: u64) {
        self.bytes = self.bytes.saturating_add(len);
        if let Some(last) = self.ranges.back_mut()
            && Arc::ptr_eq(&last.file, &file)
            && last.offset.saturating_add(last.len) == offset
            && last.len.saturating_add(len) <= MAX_RANGE
        {
            last.len += len;
            return;
        }
        self.ranges.push_back(Range {
            file,
            io,
            offset,
            len,
        });
    }

    /// Take the oldest recorded bytes beyond `budget` out of the record, in
    /// whole pages: a range the excess ends inside loses its front only.
    fn take_beyond(&mut self, budget: u64) -> Vec<Range> {
        let mut taken = Vec::new();
        while self.bytes > budget {
            let Some(front) = self.ranges.front_mut() else {
                self.bytes = 0;
                break;
            };
            let excess = (self.bytes - budget).next_multiple_of(PAGE);
            if excess >= front.len {
                self.bytes -= front.len;
                if let Some(range) = self.ranges.pop_front() {
                    taken.push(range);
                }
            } else {
                taken.push(Range {
                    file: Arc::clone(&front.file),
                    io: Arc::clone(&front.io),
                    offset: front.offset,
                    len: excess,
                });
                front.offset += excess;
                front.len -= excess;
                self.bytes -= excess;
            }
        }
        taken
    }
}

/// The written-back ranges of the logs on one flush executor, oldest first,
/// and the bytes they add up to. See the module documentation.
#[derive(Debug, Default)]
pub(in crate::log) struct Ledger {
    state: Mutex<State>,
}

impl Ledger {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Record the `len` bytes of `file` from `offset` as written back, then
    /// drop the oldest recorded ranges from the page cache while more than
    /// `budget` bytes are recorded. The drops run on the calling thread,
    /// outside the ledger's lock. Nothing is recorded for `len` zero.
    pub fn retain(&self, file: Arc<File>, io: Arc<dyn LogIo>, offset: u64, len: u64, budget: u64) {
        if len == 0 {
            return;
        }
        let dropped = {
            let mut state = self.lock();
            state.record(file, io, offset, len);
            state.take_beyond(budget)
        };
        for range in dropped {
            range.io.drop_cached(&range.file, range.offset, range.len);
        }
    }

    /// The bytes recorded and not yet dropped.
    #[cfg(test)]
    pub fn recorded_bytes(&self) -> u64 { self.lock().bytes }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use {
        super::{Ledger, MAX_RANGE, PAGE},
        crate::io::LogIo,
        assert2::assert,
        std::{
            fs::File,
            os::fd::AsRawFd,
            sync::{Arc, Mutex},
        },
        tempfile::tempfile,
    };

    /// Records every drop as the file's descriptor, offset and length.
    #[derive(Debug, Default)]
    struct RecordingIo {
        drops: Mutex<Vec<(i32, u64, u64)>>,
    }

    impl LogIo for RecordingIo {
        fn drop_cached(&self, file: &File, offset: u64, len: u64) {
            self.drops
                .lock()
                .unwrap()
                .push((file.as_raw_fd(), offset, len));
        }
    }

    fn fixture() -> (Ledger, Arc<RecordingIo>, Arc<File>) {
        (
            Ledger::default(),
            Arc::new(RecordingIo::default()),
            Arc::new(tempfile().unwrap()),
        )
    }

    fn drops(io: &RecordingIo) -> Vec<(i32, u64, u64)> { io.drops.lock().unwrap().clone() }

    fn retain(
        ledger: &Ledger,
        io: &Arc<RecordingIo>,
        file: &Arc<File>,
        offset: u64,
        len: u64,
        budget: u64,
    ) {
        ledger.retain(
            Arc::clone(file),
            Arc::<RecordingIo>::clone(io),
            offset,
            len,
            budget,
        );
    }

    /// Ranges within the budget stay recorded and nothing is dropped.
    #[test]
    fn ranges_within_the_budget_are_kept() {
        let (ledger, io, file) = fixture();
        retain(&ledger, &io, &file, 0, 2 * PAGE, 8 * PAGE);
        retain(&ledger, &io, &file, 2 * PAGE, 2 * PAGE, 8 * PAGE);
        assert!(ledger.recorded_bytes() == 4 * PAGE);
        assert!(drops(&io).is_empty());
    }

    /// Beyond the budget the oldest bytes go first, as one drop per
    /// contiguous run of the same file.
    #[test]
    fn the_oldest_bytes_beyond_the_budget_are_dropped_first() {
        let (ledger, io, file) = fixture();
        let fd = file.as_raw_fd();
        for window in 0..6 {
            retain(&ledger, &io, &file, window * PAGE, PAGE, 3 * PAGE);
        }
        // Six pages recorded against three: the first three leave, one per
        // retain that exceeds the budget, from the front of one merged range.
        assert!(drops(&io) == vec![(fd, 0, PAGE), (fd, PAGE, PAGE), (fd, 2 * PAGE, PAGE)]);
        assert!(ledger.recorded_bytes() == 3 * PAGE);
    }

    /// A range the excess ends inside loses only its front, in whole pages.
    #[test]
    fn a_range_is_split_at_a_page_boundary() {
        let (ledger, io, file) = fixture();
        let fd = file.as_raw_fd();
        retain(&ledger, &io, &file, 0, 4 * PAGE, u64::MAX);
        // A budget of two and a half pages drops three pages: the excess
        // rounds up to the page that holds it.
        retain(&ledger, &io, &file, 4 * PAGE, PAGE, 2 * PAGE + PAGE / 2);
        assert!(drops(&io) == vec![(fd, 0, 3 * PAGE)]);
        assert!(ledger.recorded_bytes() == 2 * PAGE);
    }

    /// Ranges of different files keep their order and are dropped by file.
    #[test]
    fn ranges_of_two_files_are_dropped_in_order() {
        let (ledger, io, first) = fixture();
        let second = Arc::new(tempfile().unwrap());
        retain(&ledger, &io, &first, 0, 2 * PAGE, u64::MAX);
        retain(&ledger, &io, &second, 0, 2 * PAGE, u64::MAX);
        retain(&ledger, &io, &first, 2 * PAGE, PAGE, PAGE);
        assert!(
            drops(&io)
                == vec![
                    (first.as_raw_fd(), 0, 2 * PAGE),
                    (second.as_raw_fd(), 0, 2 * PAGE)
                ]
        );
        assert!(ledger.recorded_bytes() == PAGE);
    }

    /// A merged range stops growing at `MAX_RANGE`, so one drop is at most
    /// that long.
    #[test]
    fn a_merged_range_stops_at_the_maximum() {
        let (ledger, io, file) = fixture();
        let fd = file.as_raw_fd();
        let half = MAX_RANGE / 2;
        for part in 0..3 {
            retain(&ledger, &io, &file, part * half, half, u64::MAX);
        }
        retain(&ledger, &io, &file, 3 * half, half, 0);
        assert!(drops(&io) == vec![(fd, 0, MAX_RANGE), (fd, MAX_RANGE, MAX_RANGE)]);
        assert!(ledger.recorded_bytes() == 0);
    }

    /// An empty range is not recorded.
    #[test]
    fn an_empty_range_records_nothing() {
        let (ledger, io, file) = fixture();
        retain(&ledger, &io, &file, 0, 0, 0);
        assert!(ledger.recorded_bytes() == 0);
        assert!(drops(&io).is_empty());
    }
}
