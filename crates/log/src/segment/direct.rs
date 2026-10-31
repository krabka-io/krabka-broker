//! The `O_DIRECT` write path of an active segment's `.log` file.
//!
//! An `O_DIRECT` write skips the page cache, so the kernel puts three rules on
//! it: the buffer address is a multiple of the memory alignment, and the file
//! offset and the length are multiples of the block size. [`DioAlign`] holds
//! the two values for one file.
//!
//! Kafka record batches have arbitrary sizes and go to arbitrary byte
//! positions, so an append almost never starts or stops on a block boundary.
//! [`DirectWriter`] therefore keeps the partial last block of the file in
//! memory, and each append writes that block again together with the new
//! bytes. It writes `[tail_start, padded_end)`, where `tail_start` is the
//! logical end rounded down to a block and `padded_end` is the new logical end
//! rounded up to a block. Zeros fill the space after the logical end. The file
//! can thus be up to `block - 1` bytes longer than the segment. That is
//! expected: the segment knows its logical size and removes the padding when
//! it seals the file.
//!
//! An append does not wait for its write: [`DirectWriter`] queues it, and the
//! writes of every segment go to the disk from one `io_uring`, each submitted as
//! soon as it is queued. The produce path thus pays for a copy, not for the
//! device, as it does through the page cache. Where the kernel refuses an
//! `io_uring`, and for a test's [`LogIo`] that sees each write, a small pool of
//! threads shared by every segment makes the writes with synchronous
//! `pwrite`s instead. Both start a segment's writes in file order and put the
//! same bytes at the same offsets.
//!
//! The page cache does not hold what an `O_DIRECT` write put on disk, so a
//! consumer that reads right behind the producer would go to the device for
//! every fetch. [`TailCache`] keeps a bounded window of the most recently
//! appended bytes in memory to serve those reads, and every byte that is not
//! on the disk yet.

#[cfg(any(target_os = "linux", test))]
use std::mem;
use {
    super::tail_arena::{FIRST_SLAB, Headroom, SLAB, TailArena},
    crate::io::LogIo,
    bytes::{Bytes, BytesMut},
    std::{
        collections::{BTreeMap, VecDeque},
        fs::File,
        io::{self, Error, ErrorKind},
        panic::{self, AssertUnwindSafe},
        sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
        thread,
    },
};

#[cfg(target_os = "linux")]
mod ring;

/// The `O_DIRECT` alignment rules of one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DioAlign {
    /// Required alignment of a buffer's address, in bytes. A power of two, >= 1.
    pub(crate) mem: usize,
    /// Required alignment of a file offset and of a write's length, in bytes. A power of two, >= 1.
    pub(crate) block: usize,
}

impl DioAlign {
    fn validate(self) -> io::Result<()> {
        if self.mem.is_power_of_two() && self.block.is_power_of_two() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!("O_DIRECT alignment is not a power of two: {self:?}"),
            ))
        }
    }

    /// The unit of progress that keeps the next write legal: a write that
    /// starts this many bytes further into an aligned buffer, at an aligned
    /// offset, is aligned in both address and offset. Both values are powers
    /// of two, so the larger is a multiple of the smaller.
    fn step(self) -> usize { self.mem.max(self.block) }
}

/// A growable byte buffer whose contents start on an `align`-aligned address.
///
/// The buffer over-allocates a `Vec<u8>` by `align - 1` bytes and starts at
/// the first aligned byte in it, so it needs no `unsafe` allocator call. The
/// `Vec` never changes length after it is made, so its heap address, and with
/// it the alignment, stays fixed until the next growth. A growth makes a new
/// `Vec`, aligns it again, and copies the contents.
pub struct AlignedBuf {
    align: usize,
    storage: Vec<u8>,
    offset: usize,
    len: usize,
}

impl AlignedBuf {
    pub(crate) fn new(align: usize) -> Self {
        Self {
            align: align.max(1),
            storage: Vec::new(),
            offset: 0,
            len: 0,
        }
    }

    pub(crate) fn clear(&mut self) { self.len = 0; }

    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        let new_len = self.len + bytes.len();
        self.reserve(new_len);
        let at = self.offset + self.len;
        // `reserve` made room for `new_len` bytes after the aligned start.
        if let Some(room) = self.storage.get_mut(at..at + bytes.len()) {
            room.copy_from_slice(bytes);
            self.len = new_len;
        }
    }

    /// Set the length to `len`. New bytes are zero, also where an earlier
    /// use of the buffer left other data.
    pub(crate) fn resize(&mut self, len: usize) {
        if len > self.len {
            self.reserve(len);
            // `reserve` made room for `len` bytes after the aligned start.
            if let Some(fresh) = self
                .storage
                .get_mut(self.offset + self.len..self.offset + len)
            {
                fresh.fill(0);
            }
        }
        self.len = len;
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // The contents always lie inside the storage.
        self.storage
            .get(self.offset..self.offset + self.len)
            .unwrap_or_default()
    }

    /// Lend the storage out for a write that must own its buffer: the
    /// `Vec`, and where the contents start in it and how long they are.
    /// [`Self::restore`] takes it back; until then the buffer is empty, and
    /// a buffer whose storage never comes back stays a valid empty one.
    #[cfg(target_os = "linux")]
    fn lend(&mut self) -> (Vec<u8>, usize, usize) {
        let lent = (mem::take(&mut self.storage), self.offset, self.len);
        self.offset = 0;
        self.len = 0;
        lent
    }

    /// Take back what [`Self::lend`] lent. The `Vec` is the same allocation,
    /// so the contents are still aligned.
    #[cfg(target_os = "linux")]
    fn restore(&mut self, (storage, offset, len): (Vec<u8>, usize, usize)) {
        self.storage = storage;
        self.offset = offset;
        self.len = len;
    }

    fn capacity(&self) -> usize { self.storage.len() - self.offset }

    /// Make room for `needed` bytes after the aligned start.
    fn reserve(&mut self, needed: usize) {
        if needed <= self.capacity() {
            return;
        }
        let capacity = needed.max(self.capacity().saturating_mul(2));
        let storage = vec![0; capacity + self.align - 1];
        // The modulo works on the address alone, so it is right for every
        // allocation, where `align_offset` may report that it cannot help.
        let misalignment = storage.as_ptr().addr() % self.align;
        let offset = (self.align - misalignment) % self.align;
        let mut grown = Self {
            align: self.align,
            storage,
            offset,
            len: 0,
        };
        // `capacity >= needed > self.len`, so the contents fit.
        if let Some(room) = grown.storage.get_mut(offset..offset + self.len) {
            room.copy_from_slice(self.as_slice());
            grown.len = self.len;
        }
        *self = grown;
    }
}

/// How many bytes one segment may have queued for the disk and not yet
/// written. An append that would go past it waits for the writes before it,
/// so the bytes held in memory for them stay bounded. One append larger than
/// the limit still goes through on its own.
const QUEUE_LIMIT: usize = 8 * 1024 * 1024;

/// How many of one segment's writes may run at the same time. One write at a
/// time leaves the device idle between writes and caps one partition well
/// below what the device takes; the kernel's own writeback keeps several in
/// flight too.
const WRITES_IN_FLIGHT: usize = 4;

/// How many of one segment's writes may be submitted to `io_uring` at the same
/// time. A submission costs no thread, so the bound is the queue limit's,
/// not the pool's.
const RING_WRITES_IN_FLIGHT: usize = 16;

/// How many threads write queued appends to the disk, for every segment in
/// the process together.
const WRITE_THREADS: usize = 8;

/// The `O_DIRECT` side of an active segment: the partial last block, and the
/// queue of aligned writes that the shared write threads put on the disk.
///
/// An append returns once its write is queued, so the produce response does
/// not wait for the device, as it does not through the page cache. The write
/// covers only the whole blocks the append completes, so no two writes touch
/// the same block and several can run at once. The partial last block stays
/// in memory: the next append's write starts with it, and when the segment's
/// queue runs empty a write thread writes it alone, zero-padded, so the bytes
/// reach the disk without waiting for more appends.
///
/// The segment keeps every byte that is not yet on the disk in its tail
/// cache, so reads never see the gap. A failed write fails every later
/// append and every [`Self::drain`]: the bytes it held were already
/// acknowledged, as a page-cache writeback error is, so the segment must not
/// go on.
pub struct DirectWriter {
    align: DioAlign,
    end: u64,
    tail: Vec<u8>,
    queue_limit: usize,
    shared: Arc<Shared>,
}

/// What a segment's writer shares with the write threads.
struct Shared {
    /// Shared with the `io_uring` writes in flight, which hold it until the
    /// kernel is done with them.
    file: Arc<File>,
    align: DioAlign,
    io: Mutex<Arc<dyn LogIo>>,
    queue: Mutex<Queue>,
    changed: Condvar,
    /// The write threads, started when the writer was made.
    pool: Arc<Pool>,
}

struct Queue {
    /// Whole-block writes not started yet, in file order.
    jobs: VecDeque<Job>,
    /// Writes running now, including a write of the partial last block.
    in_flight: usize,
    /// The end of the last whole-block write queued. The bytes from
    /// `written` to it are queued, running, or finished out of order behind a
    /// gap: all of them stay in memory, so all of them count against the
    /// queue limit until the gap-free written mark passes them.
    queued_end: u64,
    /// The logical end of every queued append, and its partial last block.
    end: u64,
    tail: Vec<u8>,
    /// Whether a write of the partial last block is running. No whole-block
    /// write starts then, because the next one begins in that block.
    tail_writing: bool,
    /// Every byte below it is on the disk.
    written: u64,
    /// Finished writes past `written`, by start: writes finish out of order.
    finished: BTreeMap<u64, u64>,
    /// The first failed write, which fails everything after it.
    failure: Option<(ErrorKind, String)>,
    /// Whether the segment is on the write threads' ready list.
    ready: bool,
    /// Buffers of finished writes, for the next appends to fill.
    spare: Vec<AlignedBuf>,
    /// Whether the `io_uring` submitter makes this segment's writes, rather
    /// than the write threads.
    ring: bool,
}

/// One aligned write of whole blocks: `buf` at `at`.
struct Job {
    buf: AlignedBuf,
    at: u64,
}

/// What a write thread takes from a segment's queue.
enum Work {
    Blocks(Job),
    /// The partial last block, zero-padded, which takes the segment to `end`.
    Tail {
        buf: AlignedBuf,
        at: u64,
        end: u64,
    },
}

impl Work {
    /// The buffer to write and the file offset to write it at.
    #[cfg(target_os = "linux")]
    fn buf_mut(&mut self) -> (&mut AlignedBuf, u64) {
        match self {
            Self::Blocks(job) => (&mut job.buf, job.at),
            Self::Tail { buf, at, .. } => (buf, *at),
        }
    }
}

impl Queue {
    /// Bytes held in memory for writes: see `queued_end`.
    fn unwritten(&self) -> usize {
        usize::try_from(self.queued_end.saturating_sub(self.written)).unwrap_or(usize::MAX)
    }

    fn failed(&self) -> io::Result<()> {
        match &self.failure {
            Some((kind, message)) => Err(Error::new(*kind, message.clone())),
            None => Ok(()),
        }
    }

    /// Whether everything queued is on the disk, or nothing more will be.
    fn settled(&self) -> bool {
        self.in_flight == 0
            && (self.failure.is_some() || (self.jobs.is_empty() && self.written >= self.end))
    }

    /// Whether a write thread has something to start for this segment.
    fn has_work(&self) -> bool {
        if self.failure.is_some() || self.tail_writing {
            return false;
        }
        if !self.jobs.is_empty() {
            let limit = if self.ring {
                RING_WRITES_IN_FLIGHT
            } else {
                WRITES_IN_FLIGHT
            };
            return self.in_flight < limit;
        }
        self.in_flight == 0 && !self.tail.is_empty() && self.written < self.end
    }

    /// Record that `[at, end)` is on the disk.
    fn finish(&mut self, at: u64, end: u64) {
        if at > self.written {
            self.finished.insert(at, end);
            return;
        }
        self.written = self.written.max(end);
        while let Some(entry) = self.finished.first_entry() {
            if *entry.key() > self.written {
                break;
            }
            let end = entry.remove();
            self.written = self.written.max(end);
        }
    }

    /// Keep a finished write's buffer for the next appends, while the spares
    /// hold no more bytes than [`QUEUE_LIMIT`].
    ///
    /// A buffer grows to the largest append the segment saw, and a count
    /// alone let one segment keep sixteen buffers of a 1 MiB batch's size: a
    /// broker hosting a hundred such partitions held gigabytes of spares and
    /// reached its memory limit. The bytes a segment may hold for writes not
    /// yet on the disk are the queue limit, so its spares are bounded the
    /// same way.
    fn give_back(&mut self, buf: AlignedBuf) {
        let held: usize = self.spare.iter().map(AlignedBuf::capacity).sum();
        if self.spare.len() < RING_WRITES_IN_FLIGHT
            && held.saturating_add(buf.capacity()) <= QUEUE_LIMIT
        {
            self.spare.push(buf);
        }
    }
}

impl DirectWriter {
    /// Take over the `O_DIRECT` handle `file` on a segment's `.log`. `io`
    /// makes each write.
    ///
    /// `end` is the segment's logical size. `tail` holds the file's bytes in
    /// `[end - end % block, end)`; the caller reads them through its buffered
    /// handle.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when an alignment is not a power of two, or when
    /// `tail.len()` is not `end % block`. The error of starting the write
    /// threads, on a target that has none.
    pub(crate) fn new(
        file: File,
        align: DioAlign,
        end: u64,
        tail: Vec<u8>,
        io: Arc<dyn LogIo>,
    ) -> io::Result<Self> {
        align.validate()?;
        check_tail(align, end, &tail)?;
        let pool = Pool::shared()?;
        let ring = pool.takes(io.as_ref());
        Ok(Self {
            align,
            end,
            tail: tail.clone(),
            queue_limit: QUEUE_LIMIT,
            shared: Arc::new(Shared {
                file: Arc::new(file),
                align,
                io: Mutex::new(io),
                queue: Mutex::new(Queue {
                    jobs: VecDeque::new(),
                    in_flight: 0,
                    queued_end: end,
                    end,
                    tail,
                    tail_writing: false,
                    written: end,
                    finished: BTreeMap::new(),
                    failure: None,
                    ready: false,
                    spare: Vec::new(),
                    ring,
                }),
                changed: Condvar::new(),
                pool,
            }),
        })
    }

    pub(crate) fn end(&self) -> u64 { self.end }

    #[cfg(all(test, unix))]
    pub(crate) fn file(&self) -> &File { &self.shared.file }

    /// The block size that file offsets and write lengths align to.
    pub(crate) fn block(&self) -> usize { self.align.block }

    /// Write the next appends through `io`.
    pub(crate) fn set_io(&self, io: Arc<dyn LogIo>) {
        let ring = self.shared.pool.takes(io.as_ref());
        *self
            .shared
            .io
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = io;
        self.shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .ring = ring;
    }

    /// Every byte below this is on the disk.
    pub(crate) fn written(&self) -> u64 {
        self.shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .written
    }

    /// Whether one append fits without waiting for the disk. The owner keeps the segment lock from this check through the append; completion can only make more room in the queue.
    pub(crate) fn can_queue(&self, bytes: usize) -> bool {
        let Some(logical) = self.tail.len().checked_add(bytes) else {
            return false;
        };
        let whole = logical - logical % self.align.block;
        self.shared.queue.try_lock().is_ok_and(|queue| {
            queue.failure.is_none()
                && queue
                    .unwritten()
                    .checked_add(whole)
                    .is_some_and(|needed| needed <= self.queue_limit)
        })
    }

    /// Queue the concatenation of `parts` to be written at [`Self::end`].
    ///
    /// The whole blocks from the start of the partial last block to the last
    /// block the append fills go into one write; the bytes after them become
    /// the new partial last block. The call returns once the write is
    /// queued. It waits first when the segment's queue is full.
    ///
    /// An empty append writes nothing.
    ///
    /// # Errors
    ///
    /// The error of an earlier write. [`Self::end`] and the partial last
    /// block do not change then.
    pub(crate) fn append(&mut self, parts: &[&[u8]]) -> io::Result<()> {
        let total: usize = parts.iter().map(|part| part.len()).sum();
        let block = self.align.block;
        let logical = self.tail.len() + total;
        let whole = logical - logical % block;
        let mut buf = {
            let mut queue = self
                .shared
                .queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            queue.failed()?;
            if total == 0 {
                return Ok(());
            }
            while queue.unwritten() > 0
                && queue.unwritten() + whole > self.queue_limit
                && queue.failure.is_none()
            {
                queue = self
                    .shared
                    .changed
                    .wait(queue)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            queue.failed()?;
            queue
                .spare
                .pop()
                .unwrap_or_else(|| AlignedBuf::new(self.align.mem))
        };

        buf.clear();
        buf.extend_from_slice(&self.tail);
        for part in parts {
            buf.extend_from_slice(part);
        }
        // `buf` holds the old tail and every part: `logical` bytes.
        let tail = buf
            .as_slice()
            .get(whole..logical)
            .ok_or_else(|| Error::other("an O_DIRECT append lost bytes it was given"))?
            .to_vec();
        buf.resize(whole);
        let at = self.end - to_u64(self.tail.len());
        let end = self.end + to_u64(total);

        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        // A write may have failed while the buffer was filled. Its handler

        // dropped the queued jobs, and a job queued now would never run, so

        // the append must not be acknowledged.

        if let Err(error) = queue.failed() {
            queue.give_back(buf);

            return Err(error);
        }

        if whole > 0 {
            queue.queued_end = at + to_u64(whole);
            queue.jobs.push_back(Job { buf, at });
        } else {
            queue.give_back(buf);
        }
        queue.end = end;
        queue.tail.clone_from(&tail);
        self.shared.schedule(&mut queue);
        drop(queue);
        self.tail = tail;
        self.end = end;
        Ok(())
    }

    /// Wait until every queued byte is on the disk.
    ///
    /// # Errors
    ///
    /// The error of a failed write.
    pub(crate) fn drain(&self) -> io::Result<()> { self.settle().failed() }

    /// Wait until no write is queued or running and the bytes are on the
    /// disk, or a write failed, and return the queue then.
    fn settle(&self) -> MutexGuard<'_, Queue> {
        let mut queue = self
            .shared
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        self.shared.schedule(&mut queue);
        while !queue.settled() {
            queue = self
                .shared
                .changed
                .wait(queue)
                .unwrap_or_else(PoisonError::into_inner);
        }
        queue
    }

    /// Adopt `end` and its partial last block `tail` after the caller
    /// truncated the file to `end`. It waits for the queued writes first; the
    /// caller waits for them too, before it truncates.
    ///
    /// # Errors
    ///
    /// `InvalidInput` when `tail.len()` is not `end % block`. The writer does
    /// not change then.
    pub(crate) fn reset(&mut self, end: u64, tail: Vec<u8>) -> io::Result<()> {
        check_tail(self.align, end, &tail)?;
        let mut queue = self.settle();
        queue.written = end;
        queue.queued_end = end;
        queue.end = end;
        queue.tail.clone_from(&tail);
        queue.finished.clear();
        drop(queue);
        self.end = end;
        self.tail = tail;
        Ok(())
    }

    /// Cut the file to the end of the bytes it holds, once its queued writes
    /// are done: [`Self::end`], or after a failed write, the end of the bytes
    /// that reached the disk without a gap.
    pub(crate) fn cut_padding(&self) -> io::Result<()> {
        let queue = self.settle();
        let end = if queue.failure.is_some() {
            queue.written
        } else {
            self.end
        };
        drop(queue);
        self.shared.file.set_len(end)
    }
}

impl Drop for DirectWriter {
    /// The write threads hold the file and its buffers until the queue is
    /// empty, so a dropped writer still finishes what it queued.
    fn drop(&mut self) { drop(self.settle()); }
}

/// The threads that write every segment's queued appends, and the `io_uring`
/// submitter that writes them instead for the real file I/O.
struct Pool {
    ready: Mutex<VecDeque<Arc<Shared>>>,
    changed: Condvar,
    /// None where `io_uring` is unavailable: the kernel lacks it, or a
    /// seccomp filter or `kernel.io_uring_disabled` refuses it.
    #[cfg(target_os = "linux")]
    ring: Option<ring::Ring>,
}

static POOL: Mutex<Option<Arc<Pool>>> = Mutex::new(None);

impl Pool {
    fn shared() -> io::Result<Arc<Self>> {
        let mut pool = POOL.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(pool) = pool.as_ref() {
            return Ok(Arc::clone(pool));
        }
        let started = Arc::new(Self {
            ready: Mutex::new(VecDeque::new()),
            changed: Condvar::new(),
            #[cfg(target_os = "linux")]
            ring: ring::Ring::start(ring::ENTRIES),
        });
        for index in 0..WRITE_THREADS {
            let worker = Arc::clone(&started);
            // A thread that did start keeps waiting on an empty list, which
            // costs nothing, if a later one fails.
            thread::Builder::new()
                .name(format!("log-direct-write-{index}"))
                .spawn(move || worker.run())?;
        }
        *pool = Some(Arc::clone(&started));
        drop(pool);
        Ok(started)
    }

    /// Whether the `io_uring` submitter makes the writes of a segment under
    /// `io`.
    fn takes(&self, io: &dyn LogIo) -> bool {
        #[cfg(target_os = "linux")]
        let ring = self.ring.is_some();
        #[cfg(not(target_os = "linux"))]
        let ring = {
            let _ = self;
            false
        };
        ring && io.submits_direct_writes()
    }

    fn push(&self, segment: Arc<Shared>, ring: bool) {
        #[cfg(target_os = "linux")]
        if ring && let Some(submitter) = &self.ring {
            submitter.push(segment);
            return;
        }
        let _ = ring;
        self.ready
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push_back(segment);
        self.changed.notify_one();
    }

    fn run(&self) -> ! {
        loop {
            let segment = {
                let mut ready = self.ready.lock().unwrap_or_else(PoisonError::into_inner);
                loop {
                    if let Some(segment) = ready.pop_front() {
                        break segment;
                    }
                    ready = self
                        .changed
                        .wait(ready)
                        .unwrap_or_else(PoisonError::into_inner);
                }
            };
            segment.write_one();
        }
    }
}

impl Shared {
    /// Put the segment on the ready list when it has work and is not on it.
    fn schedule(self: &Arc<Self>, queue: &mut Queue) {
        if queue.ready || !queue.has_work() {
            return;
        }
        queue.ready = true;
        self.pool.push(Arc::clone(self), queue.ring);
    }

    /// Make one write on this thread, through [`LogIo::write_direct`].
    fn write_one(self: &Arc<Self>) {
        let Some(work) = self.take_work() else {
            return;
        };
        let (buf, at) = match &work {
            Work::Blocks(job) => (job.buf.as_slice(), job.at),
            Work::Tail { buf, at, .. } => (buf.as_slice(), *at),
        };
        let io = Arc::clone(&self.io.lock().unwrap_or_else(PoisonError::into_inner));
        let step = self.align.step();
        let result = panic::catch_unwind(AssertUnwindSafe(|| {
            write_aligned(&self.file, buf, at, step, |file, buf, offset| {
                io.write_direct(file, buf, offset)
            })
        }))
        .unwrap_or_else(|_| Err(Error::other("O_DIRECT write panicked")));
        self.complete(work, result);
    }

    /// Take the next write to start, and put the segment back on the ready
    /// list first if another can run beside it.
    fn take_work(self: &Arc<Self>) -> Option<Work> {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        queue.ready = false;
        if !queue.has_work() {
            return None;
        }
        let work = queue.jobs.pop_front().map_or_else(
            || {
                let block = self.align.block;
                let at = queue.end - to_u64(queue.tail.len());
                let mut buf = queue
                    .spare
                    .pop()
                    .unwrap_or_else(|| AlignedBuf::new(self.align.mem));
                buf.clear();
                buf.extend_from_slice(&queue.tail);
                buf.resize(block);
                queue.tail_writing = true;
                Work::Tail {
                    buf,
                    at,
                    end: queue.end,
                }
            },
            Work::Blocks,
        );
        queue.in_flight += 1;
        self.schedule(&mut queue);
        drop(queue);
        Some(work)
    }

    /// Record the outcome of a write [`Self::take_work`] handed out.
    fn complete(self: &Arc<Self>, work: Work, result: io::Result<()>) {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        queue.in_flight -= 1;
        let at = match &work {
            Work::Blocks(job) => job.at,
            Work::Tail { at, .. } => *at,
        };
        let buf = match work {
            Work::Blocks(job) => {
                let len = job.buf.as_slice().len();
                if result.is_ok() {
                    queue.finish(job.at, job.at + to_u64(len));
                }
                job.buf
            }
            Work::Tail { buf, at, end } => {
                queue.tail_writing = false;
                if result.is_ok() {
                    queue.finish(at, end);
                }
                buf
            }
        };
        queue.give_back(buf);
        if let Err(error) = result {
            tracing::error!(%error, at, "O_DIRECT write failed; the segment fails every later write");
            queue
                .failure
                .get_or_insert_with(|| (error.kind(), error.to_string()));
            queue.jobs.clear();
        }
        self.schedule(&mut queue);
        drop(queue);
        self.changed.notify_all();
    }
}

/// Put all of `buf` on the disk at `offset`.
///
/// `write(file, buf, offset)` is one positioned write; this function calls it
/// again after a short write and after `Interrupted`. A short write that stops
/// off an aligned position goes on from the last aligned position, a multiple
/// of `step`, because the next write must be aligned too. It writes some bytes
/// again, with the same contents.
///
/// # Errors
///
/// The first error of `write` other than `Interrupted`, or `WriteZero` when
/// `write` makes no aligned progress.
fn write_aligned(
    file: &File,
    buf: &[u8],
    offset: u64,
    step: usize,
    mut write: impl FnMut(&File, &[u8], u64) -> io::Result<usize>,
) -> io::Result<()> {
    let mut done = 0;
    while let Some(rest) = buf.get(done..).filter(|rest| !rest.is_empty()) {
        match write(file, rest, offset + to_u64(done)) {
            Ok(written) => done = aligned_progress(done, written, buf.len(), offset, step)?,
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// Where a write of `len` bytes at `offset` goes on after one positioned
/// write, started `done` bytes in, wrote `written`: the end, or the last
/// aligned position, a multiple of `step`, that it reached.
///
/// # Errors
///
/// `WriteZero` when that is no further than `done`.
fn aligned_progress(
    done: usize,
    written: usize,
    len: usize,
    offset: u64,
    step: usize,
) -> io::Result<usize> {
    let reached = done.saturating_add(written).min(len);
    // The end of the buffer is block-aligned, and a write that reaches it
    // ends the loop, so only a short write rounds.
    let next = if reached == len {
        reached
    } else {
        reached - reached % step
    };
    if next <= done {
        return Err(Error::new(
            ErrorKind::WriteZero,
            format!(
                "O_DIRECT write at {} made no aligned progress",
                offset + to_u64(done)
            ),
        ));
    }
    Ok(next)
}

fn check_tail(align: DioAlign, end: u64, tail: &[u8]) -> io::Result<()> {
    let expected = end % to_u64(align.block);
    if to_u64(tail.len()) == expected {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!(
                "partial last block of a segment that ends at {end} has {} bytes, not {expected}",
                tail.len()
            ),
        ))
    }
}

/// A `usize` widened to a `u64`; no supported target has a wider `usize`.
fn to_u64(n: usize) -> u64 { n as u64 }

/// The newest bytes of a segment that writes directly, `[start, end)`, as
/// shared slices: what a read takes from memory. A read view clones the part
/// it needs with [`Self::suffix`], which copies slice handles, not bytes.
#[derive(Debug, Clone, Default)]
pub struct TailWindow {
    start: u64,
    len: usize,
    chunks: VecDeque<Bytes>,
}

impl TailWindow {
    pub(crate) fn start(&self) -> u64 { self.start }

    pub(crate) fn end(&self) -> u64 { self.start + to_u64(self.len) }

    /// The window from the chunk that holds `pos` on: what a read that starts
    /// at or after `pos` can use. Chunks wholly before `pos` are left out, and
    /// the walk goes back from the end, so a reader near the end touches a few
    /// handles whatever the window's size: the plan that takes this view holds
    /// the log lock.
    pub(crate) fn suffix(&self, pos: u64) -> Self {
        let end = self.end();
        let mut start = end;
        let mut kept = 0;
        for chunk in self.chunks.iter().rev() {
            if start <= pos {
                break;
            }
            start -= to_u64(chunk.len());
            kept += 1;
        }
        Self {
            start,
            len: usize::try_from(end - start).unwrap_or(self.len),
            chunks: self
                .chunks
                .range(self.chunks.len() - kept..)
                .cloned()
                .collect(),
        }
    }

    /// The bytes from `pos` on: the rest of the chunk `pos` falls in, then
    /// whole chunks while the total stays within `max`. The first chunk comes
    /// whatever its size. One chunk is a shared slice, with no copy; several
    /// are copied into one buffer. `None` when `pos` is outside the window.
    pub(crate) fn bytes_from(&self, pos: u64, max: usize) -> Option<Bytes> {
        if pos < self.start || pos >= self.end() {
            return None;
        }
        let mut skip = usize::try_from(pos - self.start).ok()?;
        let mut taken: Vec<Bytes> = Vec::new();
        let mut total = 0;
        for chunk in &self.chunks {
            if skip >= chunk.len() {
                skip -= chunk.len();
                continue;
            }
            let part = chunk.slice(skip..);
            skip = 0;
            if !taken.is_empty() && total + part.len() > max {
                break;
            }
            total += part.len();
            taken.push(part);
        }
        if taken.len() == 1 {
            return taken.pop();
        }
        let mut joined = bytes::BytesMut::with_capacity(total);
        for part in &taken {
            joined.extend_from_slice(part);
        }
        Some(joined.freeze())
    }

    /// Copy `[pos, pos + out.len())` into `out` and return true when the whole
    /// range is in the window. Return false and leave `out` as it is when it
    /// is not.
    pub(crate) fn read(&self, pos: u64, out: &mut [u8]) -> bool {
        let Some(read_end) = pos.checked_add(to_u64(out.len())) else {
            return false;
        };
        if pos < self.start || read_end > self.end() {
            return false;
        }
        let Ok(mut skip) = usize::try_from(pos - self.start) else {
            return false;
        };
        let mut filled = 0;
        for chunk in &self.chunks {
            if filled == out.len() {
                break;
            }
            if skip >= chunk.len() {
                skip -= chunk.len();
                continue;
            }
            let take = (chunk.len() - skip).min(out.len() - filled);
            let (Some(dst), Some(src)) = (
                out.get_mut(filled..filled + take),
                chunk.get(skip..skip + take),
            ) else {
                return false;
            };
            dst.copy_from_slice(src);
            filled += take;
            skip = 0;
        }
        true
    }

    /// Forget every byte at or past `end`. When `end` is at or before the
    /// window's start, the window becomes empty and begins at `end`.
    fn truncate(&mut self, end: u64) {
        if end >= self.end() {
            return;
        }
        if end <= self.start {
            self.chunks.clear();
            self.len = 0;
            self.start = end;
            return;
        }
        // `end` is below the window's end, so `keep` is below `self.len`.
        let keep = usize::try_from(end - self.start).unwrap_or(self.len);
        let mut kept = 0;
        let mut chunks = 0;
        for chunk in &mut self.chunks {
            if kept == keep {
                break;
            }
            let take = chunk.len().min(keep - kept);
            chunk.truncate(take);
            kept += take;
            chunks += 1;
        }
        self.chunks.truncate(chunks);
        self.len = keep;
    }
}

/// A slab a cache copies its batches into, and the segment position its
/// last batch ends at.
#[derive(Debug)]
struct Slab {
    /// The slab's free space; its filled part was split off into chunks.
    free: BytesMut,
    /// What the arena counted for the slab.
    capacity: usize,
    /// The segment positions of its first byte and of the end of its last
    /// batch.
    start: u64,
    end: u64,
}

/// The most recently appended bytes of an active segment, kept in memory to
/// serve reads, and every byte at or past `written` whatever the size.
///
/// The window holds at least `floor` bytes when the segment has written that
/// many. It grows towards `max` while the process's [`TailArena`] has a
/// budget and the caches together are under it; at the budget a cache keeps
/// the size it has, and with no budget it holds `floor`, as a fixed window
/// did. Eviction drops the oldest chunk. A chunk that is not on the disk yet
/// is never evicted, so a read below the window's start can always go to the
/// file.
///
/// Each batch is copied into a slab from the arena, and the window holds
/// shared slices of the slabs: see [`super::tail_arena`].
#[derive(Debug)]
pub struct TailCache {
    floor: usize,
    max: usize,
    written: u64,
    window: TailWindow,
    slabs: VecDeque<Slab>,
    arena: Arc<TailArena>,
}

impl Drop for TailCache {
    fn drop(&mut self) {
        for slab in self.slabs.drain(..) {
            self.arena.give_back(slab.free, slab.capacity);
        }
    }
}

impl TailCache {
    /// An empty window of `cap` bytes that begins at `start`, the segment's
    /// logical end when the cache is made, in a test's own arena.
    #[cfg(test)]
    pub(crate) fn new(cap: usize, start: u64) -> Self {
        Self::in_arena(cap, cap, start, Arc::new(TailArena::default()))
    }

    /// An empty window that begins at `start`, the segment's logical end when
    /// the cache is made, holding `floor` to `max` bytes in `arena`.
    pub(crate) fn in_arena(floor: usize, max: usize, start: u64, arena: Arc<TailArena>) -> Self {
        Self {
            floor,
            max: max.max(floor),
            written: start,
            window: TailWindow {
                start,
                len: 0,
                chunks: VecDeque::new(),
            },
            slabs: VecDeque::new(),
            arena,
        }
    }

    /// Learn that every byte below `written` is on the disk, so the chunks
    /// below it can be evicted.
    pub(crate) fn set_written(&mut self, written: u64) { self.written = self.written.max(written); }

    pub(crate) fn start(&self) -> u64 { self.window.start }

    pub(crate) fn end(&self) -> u64 { self.window.end() }

    /// The bytes the cache holds, for reads.
    pub(crate) fn window(&self) -> &TailWindow { &self.window }

    /// Record the concatenation of `parts`, appended at [`Self::end`], as one
    /// chunk.
    ///
    /// Then drop the oldest chunks while the window is larger than it may be
    /// and the oldest chunk is on the disk. The newest chunk stays even when
    /// it alone is larger, so a reader right behind the producer always finds
    /// the last append.
    pub(crate) fn push(&mut self, parts: &[&[u8]]) {
        let len = parts.iter().map(|part| part.len()).sum();
        if len == 0 {
            return;
        }
        let before = self.window.len;
        let chunk = self.copy_in(parts, len);
        self.window.len += len;
        self.window.chunks.push_back(chunk);
        self.evict(before);
    }

    /// Record the batches of one coalesced write, `group[..ends[0]]`,
    /// `group[ends[0]..ends[1]]` and so on, appended at [`Self::end`], one
    /// chunk each.
    pub(crate) fn push_group(&mut self, group: &[u8], ends: &[usize]) {
        let mut start = 0;
        for &end in ends {
            if let Some(batch) = group.get(start..end) {
                self.push(&[batch]);
            }
            start = end;
        }
    }

    /// Copy `len` bytes, the concatenation of `parts`, into the newest slab,
    /// or a new one when it has no room, and return them as a shared slice.
    fn copy_in(&mut self, parts: &[&[u8]], len: usize) -> Bytes {
        let start = self.window.end();
        let end = start + to_u64(len);
        let room = self
            .slabs
            .back()
            .is_some_and(|slab| slab.free.capacity() - slab.free.len() >= len);
        if !room {
            let mut wanted = self
                .slabs
                .back()
                .map_or(FIRST_SLAB, |slab| slab.capacity.saturating_mul(2))
                .clamp(FIRST_SLAB, SLAB);
            while wanted < len && wanted < SLAB {
                wanted *= 2;
            }
            let free = self.arena.take(wanted.min(SLAB), len);
            let capacity = free.capacity();
            self.slabs.push_back(Slab {
                free,
                capacity,
                start,
                end,
            });
        }
        let Some(slab) = self.slabs.back_mut() else {
            return Bytes::new();
        };
        for part in parts {
            slab.free.extend_from_slice(part);
        }
        slab.end = end;
        slab.free.split().freeze()
    }

    /// The most bytes the window may hold after a push that found it holding
    /// `before`: `max` while the arena's budget has room, the larger of
    /// `floor` and `before` once the caches reach it, and `floor` with no
    /// budget.
    fn limit(&self, before: usize) -> usize {
        match self.arena.headroom() {
            Headroom::Unbudgeted => self.floor,
            Headroom::Room => self.max,
            Headroom::Full => self.floor.max(before),
        }
    }

    fn evict(&mut self, before: usize) {
        let limit = self.limit(before);
        while self.window.len > limit && self.window.chunks.len() > 1 {
            let Some(oldest) = self.window.chunks.front().map(Bytes::len) else {
                break;
            };
            if self.window.start + to_u64(oldest) > self.written {
                break;
            }
            self.window.chunks.pop_front();
            self.window.len -= oldest;
            self.window.start += to_u64(oldest);
        }
        self.release_slabs();
    }

    /// Give the arena back every slab, but the newest, that the window no
    /// longer reaches.
    fn release_slabs(&mut self) {
        while self.slabs.len() > 1
            && self
                .slabs
                .front()
                .is_some_and(|slab| slab.end <= self.window.start)
        {
            if let Some(slab) = self.slabs.pop_front() {
                self.arena.give_back(slab.free, slab.capacity);
            }
        }
    }

    /// Forget every byte at or past `end`, as a truncate of the segment does.
    ///
    /// When `end` is at or before the window's start, the window becomes
    /// empty and begins at `end`.
    pub(crate) fn truncate(&mut self, end: u64) {
        self.written = self.written.min(end);
        self.window.truncate(end);
        // A slab that starts at or past the cut holds only bytes the cut
        // dropped; the next push starts a slab of its own there.
        while let Some(slab) = self.slabs.pop_back_if(|slab| slab.start >= end) {
            self.arena.give_back(slab.free, slab.capacity);
        }
        for slab in &mut self.slabs {
            slab.end = slab.end.min(end);
        }
        self.written = self.written.min(self.window.end());
        self.release_slabs();
    }

    /// The bytes the cache's slabs hold, counted by the arena.
    #[cfg(test)]
    pub(crate) fn slab_bytes(&self) -> usize { self.slabs.iter().map(|slab| slab.capacity).sum() }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use {
        super::*,
        crate::io::FileIo,
        assert2::{assert, check},
        std::{
            cell::{Cell, RefCell},
            fs, io, mem,
            ops::Range,
            os::unix::fs::FileExt,
            sync::atomic::{AtomicBool, AtomicUsize, Ordering},
            thread,
            time::Duration,
        },
    };

    const MEM: usize = 512;

    /// The bytes `0..len` of a pattern that never repeats inside one block,
    /// tagged by `seed` so two appends differ.
    fn pattern(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| {
                seed.wrapping_mul(31)
                    .wrapping_add(u8::try_from(i % 251).unwrap())
            })
            .collect()
    }

    /// One `write` call a writer made: where in the file, and how many bytes.
    #[derive(Debug, PartialEq, Eq)]
    struct WriteCall {
        offset: u64,
        len: usize,
    }

    /// A [`LogIo`] whose direct writes check the `O_DIRECT` rules, record
    /// each call's offset and length, and write through `write_at`. It fails
    /// every write once `fail` is set, and holds every write back while
    /// `held` is set.
    #[derive(Debug)]
    struct RecordingIo {
        align: DioAlign,
        calls: Mutex<Vec<WriteCall>>,
        fail: AtomicBool,
        held: Mutex<bool>,
        released: Condvar,
    }

    impl RecordingIo {
        fn new(align: DioAlign) -> Arc<Self> {
            Arc::new(Self {
                align,
                calls: Mutex::default(),
                fail: false.into(),
                held: Mutex::new(false),
                released: Condvar::new(),
            })
        }

        fn take_calls(&self) -> Vec<WriteCall> { mem::take(&mut *self.calls.lock().unwrap()) }

        fn hold(&self, held: bool) {
            *self.held.lock().unwrap() = held;
            self.released.notify_all();
        }
    }

    impl LogIo for RecordingIo {
        fn write_direct(&self, file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
            let mut held = self.held.lock().unwrap();
            while *held {
                held = self.released.wait(held).unwrap();
            }
            drop(held);
            if self.fail.load(Ordering::SeqCst) {
                return Err(Error::other("disk on fire"));
            }
            assert!(buf.as_ptr().addr() % self.align.mem == 0);
            assert!(offset % to_u64(self.align.block) == 0);
            assert!(buf.len() % self.align.block == 0);
            self.calls.lock().unwrap().push(WriteCall {
                offset,
                len: buf.len(),
            });
            file.write_at(buf, offset)
        }
    }

    fn file_contents(file: &File) -> Vec<u8> {
        let len = usize::try_from(file.metadata().unwrap().len()).unwrap();
        let mut out = vec![0; len];
        file.read_exact_at(&mut out, 0).unwrap();
        out
    }

    /// What the file holds after appends of `logical`: those bytes, then zeros
    /// up to the next block.
    fn padded(logical: &[u8], block: usize) -> Vec<u8> {
        let mut expected = logical.to_vec();
        expected.resize(logical.len().div_ceil(block) * block, 0);
        expected
    }

    /// A writer on a new, empty temporary file that writes through `io`.
    fn empty_writer(align: DioAlign, io: Arc<dyn LogIo>) -> DirectWriter {
        DirectWriter::new(tempfile::tempfile().unwrap(), align, 0, Vec::new(), io).unwrap()
    }

    /// A writer on a new temporary file, and the I/O it writes through.
    fn writer(block: usize) -> (DirectWriter, Arc<RecordingIo>) {
        let align = DioAlign { mem: MEM, block };
        let io = RecordingIo::new(align);
        let writer = empty_writer(align, Arc::clone(&io) as Arc<dyn LogIo>);
        (writer, io)
    }

    #[test]
    fn appends_of_assorted_sizes_write_aligned_blocks_from_the_tail_start() {
        /// A block size, then the size of each append in turn.
        struct Case {
            block: usize,
            sizes: &'static [usize],
        }
        let cases = [
            Case {
                block: 512,
                sizes: &[1, 10, 100],
            },
            Case {
                block: 512,
                sizes: &[511, 1, 1],
            },
            Case {
                block: 512,
                sizes: &[512, 512, 1024],
            },
            Case {
                block: 512,
                sizes: &[300, 300, 300, 300],
            },
            Case {
                block: 512,
                sizes: &[1500, 7, 2048, 513],
            },
            Case {
                block: 4096,
                sizes: &[1, 4095, 4097, 100],
            },
            Case {
                block: 4096,
                sizes: &[10_000, 3, 8192],
            },
        ];
        for Case { block, sizes } in cases {
            let (mut writer, io) = writer(block);
            check!(writer.block() == block);
            let mut logical = Vec::new();
            for (seed, &size) in sizes.iter().enumerate() {
                let bytes = pattern(u8::try_from(seed).unwrap(), size);
                let tail_start = writer.end() - writer.end() % to_u64(block);
                writer.append(&[&bytes]).unwrap();
                writer.drain().unwrap();
                logical.extend_from_slice(&bytes);

                // The append writes the whole blocks it completes; the drain
                // then writes the partial last block alone.
                let new_end = to_u64(logical.len());
                let whole_end = new_end - new_end % to_u64(block);
                let mut expected_calls = Vec::new();
                if whole_end > tail_start {
                    expected_calls.push(WriteCall {
                        offset: tail_start,
                        len: usize::try_from(whole_end - tail_start).unwrap(),
                    });
                }
                if new_end > whole_end {
                    expected_calls.push(WriteCall {
                        offset: whole_end,
                        len: block,
                    });
                }
                check!(
                    io.take_calls() == expected_calls,
                    "block {block}, sizes {sizes:?}"
                );
                check!(writer.end() == new_end);
                check!(writer.written() == new_end);
                check!(writer.tail == logical[logical.len() - logical.len() % block..]);
                check!(
                    file_contents(writer.file()) == padded(&logical, block),
                    "block {block}, sizes {sizes:?}"
                );
            }
        }
    }

    #[test]
    fn several_parts_append_as_their_concatenation() {
        let (mut writer, _io) = writer(512);
        let (a, b, c) = (pattern(1, 100), pattern(2, 0), pattern(3, 700));
        writer.append(&[&a, &b, &c]).unwrap();
        writer.drain().unwrap();
        let logical = [a, c].concat();
        check!(writer.end() == 800);
        check!(file_contents(writer.file()) == padded(&logical, 512));
    }

    #[test]
    fn an_empty_append_writes_nothing() {
        let (mut writer, io) = writer(512);
        check!(writer.append(&[&[], &[]]).is_ok());
        writer.drain().unwrap();
        check!(io.take_calls().is_empty());
        check!(writer.end() == 0);
    }

    /// An append returns once its write is queued, so it does not wait for
    /// the disk; an append that would overfill the queue waits for the
    /// writes before it.
    #[test]
    fn an_append_waits_for_the_disk_only_when_the_queue_is_full() {
        let block = 512;
        let (mut writer, io) = writer(block);
        writer.queue_limit = block;
        io.hold(true);
        check!(writer.can_queue(block));
        check!(!writer.can_queue(2 * block));

        let first = pattern(1, 600);
        writer.append(&[&first]).unwrap();
        check!(writer.end() == 600);
        check!(writer.written() == 0);
        check!(!writer.can_queue(424));
        check!(!writer.can_queue(usize::MAX));

        let second = pattern(2, 600);
        let appended = AtomicBool::new(false);
        let writer = thread::scope(|scope| {
            let (second, appended) = (&second, &appended);
            let waiting = scope.spawn(move || {
                writer.append(&[second]).unwrap();
                appended.store(true, Ordering::SeqCst);
                writer
            });
            thread::sleep(Duration::from_millis(50));
            check!(!appended.load(Ordering::SeqCst));
            io.hold(false);
            waiting.join().unwrap()
        });
        writer.drain().unwrap();
        check!(writer.can_queue(block));
        check!(writer.written() == 1200);
        check!(file_contents(writer.file()) == padded(&[first, second].concat(), block));
    }

    #[test]
    fn inline_queue_admission_refuses_contention_and_write_failure() {
        let (writer, _io) = writer(512);
        let mut queue = writer.shared.queue.lock().unwrap();
        check!(!writer.can_queue(1));
        queue.failure = Some((ErrorKind::StorageFull, "disk full".into()));
        drop(queue);
        check!(!writer.can_queue(1));
    }

    #[test]
    fn an_oversized_queued_write_blocks_even_a_partial_block_append() {
        let (mut writer, io) = writer(512);
        writer.queue_limit = 512;
        io.hold(true);
        writer.append(&[&[7; 1024]]).unwrap();
        check!(!writer.can_queue(1));
        io.hold(false);
        writer.drain().unwrap();
        check!(writer.can_queue(1));
    }

    /// Writes finish out of order, and the written mark moves only over
    /// bytes with no gap below them.
    #[test]
    fn written_advances_only_over_a_gap_free_prefix() {
        let mut queue = Queue {
            jobs: VecDeque::new(),
            in_flight: 0,
            queued_end: 4096,
            end: 4096,
            tail: Vec::new(),
            tail_writing: false,
            written: 0,
            finished: BTreeMap::new(),
            failure: None,
            ready: false,
            spare: Vec::new(),
            ring: false,
        };
        queue.finish(2048, 3072);
        queue.finish(1024, 2048);
        check!(queue.written == 0);
        // Writes that finished behind the gap stay in memory, so they still
        // count against the queue limit.
        check!(queue.unwritten() == 4096);
        queue.finish(0, 1024);
        check!(queue.written == 3072);
        check!(queue.unwritten() == 1024);
        queue.finish(3072, 4096);
        check!(queue.written == 4096);
        check!(queue.finished.is_empty());
    }

    /// The spare buffers a segment keeps hold at most `QUEUE_LIMIT` bytes
    /// between them, whatever their count, so a segment that writes 1 MiB
    /// batches does not keep sixteen of them.
    #[test]
    fn spare_buffers_hold_at_most_the_queue_limit() {
        let mut queue = Queue {
            jobs: VecDeque::new(),
            in_flight: 0,
            queued_end: 0,
            end: 0,
            tail: Vec::new(),
            tail_writing: false,
            written: 0,
            finished: BTreeMap::new(),
            failure: None,
            ready: false,
            spare: Vec::new(),
            ring: true,
        };
        let one_mib = |fill: usize| {
            let mut buf = AlignedBuf::new(4096);
            buf.resize(fill);
            buf
        };
        for _ in 0..RING_WRITES_IN_FLIGHT {
            queue.give_back(one_mib(1 << 20));
        }
        let held: usize = queue.spare.iter().map(AlignedBuf::capacity).sum();
        check!(held <= QUEUE_LIMIT);
        // Each buffer is a little over 1 MiB, its alignment slack included.
        check!((7..=8).contains(&queue.spare.len()));
        // Small buffers still fill the count up to the in-flight bound.
        let mut small = Queue {
            spare: Vec::new(),
            ..queue
        };
        for _ in 0..RING_WRITES_IN_FLIGHT + 4 {
            small.give_back(one_mib(4096));
        }
        check!(small.spare.len() == RING_WRITES_IN_FLIGHT);
    }

    /// One segment's whole-block writes run side by side, up to
    /// `WRITES_IN_FLIGHT` of them, and the file still ends up as written.
    #[test]
    fn several_writes_of_one_segment_run_at_once() {
        #[derive(Debug)]
        struct Overlapping {
            running: AtomicUsize,
            most: AtomicUsize,
        }
        impl LogIo for Overlapping {
            fn write_direct(&self, file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
                use std::sync::atomic::Ordering::SeqCst;
                let now = self.running.fetch_add(1, SeqCst) + 1;
                self.most.fetch_max(now, SeqCst);
                thread::sleep(Duration::from_millis(20));
                self.running.fetch_sub(1, SeqCst);
                file.write_at(buf, offset)
            }
        }
        let align = DioAlign {
            mem: MEM,
            block: 512,
        };
        let io = Arc::new(Overlapping {
            running: 0.into(),
            most: 0.into(),
        });
        let mut writer = empty_writer(align, Arc::clone(&io) as Arc<dyn LogIo>);
        let mut logical = Vec::new();
        for seed in 0..8 {
            let bytes = pattern(seed, 1024);
            writer.append(&[&bytes]).unwrap();
            logical.extend_from_slice(&bytes);
        }
        writer.drain().unwrap();
        check!(io.most.load(Ordering::SeqCst) > 1);
        check!(io.most.load(Ordering::SeqCst) <= WRITES_IN_FLIGHT);
        check!(writer.written() == 8192);
        check!(file_contents(writer.file()) == logical);
    }

    /// A writer whose I/O changes writes the next appends through the new
    /// one: after a switch from a test's I/O to the real file I/O, the test's
    /// sees no more writes, and the file holds every append either way.
    #[test]
    fn a_new_io_makes_the_writes_after_it() {
        let (mut writer, io) = writer(512);
        let first = pattern(1, 700);
        writer.append(&[&first]).unwrap();
        writer.drain().unwrap();
        check!(!io.take_calls().is_empty());

        writer.set_io(Arc::new(FileIo));
        let mut logical = first;
        for seed in 2..40 {
            let bytes = pattern(seed, 300 * usize::from(seed));
            writer.append(&[&bytes]).unwrap();
            logical.extend_from_slice(&bytes);
        }
        writer.drain().unwrap();

        check!(io.take_calls().is_empty());
        check!(writer.written() == to_u64(logical.len()));
        check!(file_contents(writer.file()) == padded(&logical, 512));
    }

    /// The real file I/O hands its writes to the `io_uring` submitter where the
    /// kernel offers one, and to the write threads where it does not. Either
    /// way, appends of assorted sizes with the queue's worth of writes in
    /// flight through a real `O_DIRECT` handle leave exactly the bytes they
    /// appended, zero-padded to the block. A filesystem without `O_DIRECT`,
    /// such as tmpfs, has no handle to write through and is not tested.
    #[test]
    fn the_real_io_puts_every_append_on_a_direct_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("00000000000000000000.log");
        File::create(&path).unwrap();
        let io: Arc<dyn LogIo> = Arc::new(FileIo);
        let Ok(direct) = io.open_direct(&path) else {
            return;
        };
        let align = DioAlign {
            mem: direct.mem_align,
            block: direct.block,
        };
        let mut writer = DirectWriter::new(direct.file, align, 0, Vec::new(), io).unwrap();
        let mut logical = Vec::new();
        for seed in 0..=u8::MAX {
            let bytes = pattern(seed, 1 + usize::from(seed) * 997 % 70_000);
            writer.append(&[&bytes]).unwrap();
            logical.extend_from_slice(&bytes);
        }
        writer.drain().unwrap();
        check!(writer.written() == to_u64(logical.len()));
        check!(fs::read(&path).unwrap() == padded(&logical, align.block));
    }

    /// The bytes of a failed write were already acknowledged, so the writer
    /// fails every later append and drain, and a cut leaves the file at the
    /// end of the writes that did finish.
    #[test]
    fn a_failed_write_fails_the_writer_from_then_on() {
        let block = 512;
        let (mut writer, io) = writer(block);
        let first = pattern(1, 700);
        writer.append(&[&first]).unwrap();
        writer.drain().unwrap();

        io.fail.store(true, Ordering::SeqCst);
        writer.append(&[&pattern(2, 3000)]).unwrap();
        check!(writer.drain().unwrap_err().to_string() == "disk on fire");
        check!(writer.written() == 700);

        let end = writer.end();
        let tail = writer.tail.clone();
        check!(writer.append(&[&pattern(3, 10)]).unwrap_err().to_string() == "disk on fire");
        check!(writer.end() == end);
        check!(writer.tail == tail);

        writer.cut_padding().unwrap();
        check!(file_contents(writer.file()) == first);
    }

    /// A write that panics fails the writer as an error does, and the write
    /// threads go on serving other segments.
    #[test]
    fn a_panicking_write_fails_the_writer() {
        let (mut writer, _io) = writer(512);
        // A buffer that is not a block multiple long trips the recording
        // I/O's alignment check.
        writer.align.block = 128;
        writer.append(&[&pattern(1, 300)]).unwrap();
        check!(writer.drain().unwrap_err().to_string() == "O_DIRECT write panicked");

        let (mut other, _io) = self::writer(512);
        other.append(&[&pattern(2, 300)]).unwrap();
        check!(other.drain().is_ok());
    }

    #[test]
    fn an_interrupted_write_is_retried() {
        let file = tempfile::tempfile().unwrap();
        let interrupted = Cell::new(false);
        let bytes = padded(&pattern(1, 600), 512);
        write_aligned(&file, &bytes, 0, 512, |file, buf, offset| {
            if interrupted.replace(true) {
                file.write_at(buf, offset)
            } else {
                Err(Error::from(ErrorKind::Interrupted))
            }
        })
        .unwrap();
        check!(file_contents(&file) == bytes);
    }

    #[test]
    fn a_write_of_zero_bytes_fails_with_write_zero() {
        let file = tempfile::tempfile().unwrap();
        let result = write_aligned(&file, &[0; 512], 0, 512, |_, _, _| Ok(0));
        check!(result.unwrap_err().kind() == ErrorKind::WriteZero);
    }

    /// A short write that stops inside a block goes on from the start of that
    /// block, so every call stays aligned.
    #[test]
    fn a_short_unaligned_write_resumes_from_an_aligned_position() {
        let file = tempfile::tempfile().unwrap();
        let bytes = padded(&pattern(1, 1500), 512);
        let calls = RefCell::new(Vec::new());
        let first = Cell::new(true);
        write_aligned(&file, &bytes, 0, 512, |file, buf, offset| {
            calls.borrow_mut().push(WriteCall {
                offset,
                len: buf.len(),
            });
            let n = file.write_at(buf, offset)?;
            Ok(if first.replace(false) { n.min(700) } else { n })
        })
        .unwrap();
        check!(
            *calls.borrow()
                == vec![
                    WriteCall {
                        offset: 0,
                        len: 1536
                    },
                    WriteCall {
                        offset: 512,
                        len: 1024
                    }
                ]
        );
        check!(file_contents(&file) == bytes);
    }

    #[test]
    fn a_short_write_with_no_aligned_progress_fails_with_write_zero() {
        let file = tempfile::tempfile().unwrap();
        let result = write_aligned(&file, &[0; 1024], 0, 512, |_, _, _| Ok(100));
        check!(result.unwrap_err().kind() == ErrorKind::WriteZero);
    }

    #[test]
    fn reset_after_a_truncate_continues_from_the_new_end() {
        let block = 512;
        let (mut writer, io) = writer(block);
        let first = pattern(1, 1300);
        writer.append(&[&first]).unwrap();
        writer.drain().unwrap();
        io.take_calls();

        writer.file().set_len(900).unwrap();
        writer.reset(900, first[512..900].to_vec()).unwrap();
        check!(writer.end() == 900);
        check!(writer.written() == 900);

        let next = pattern(2, 200);
        writer.append(&[&next]).unwrap();
        writer.drain().unwrap();
        // The append completes the block the truncate ended in, and the
        // drain writes the new partial last block.
        check!(
            io.take_calls()
                == vec![
                    WriteCall {
                        offset: 512,
                        len: 512
                    },
                    WriteCall {
                        offset: 1024,
                        len: 512
                    }
                ]
        );
        check!(writer.end() == 1100);
        check!(file_contents(writer.file()) == padded(&[&first[..900], &next[..]].concat(), block));
    }

    #[test]
    fn new_and_reset_reject_a_tail_of_the_wrong_length() {
        let align = DioAlign {
            mem: MEM,
            block: 512,
        };
        // Each case: the end, the tail length, and whether they agree.
        let cases = [
            (0, 0, true),
            (0, 1, false),
            (512, 0, true),
            (512, 512, false),
            (700, 188, true),
            (700, 187, false),
            (700, 700, false),
        ];
        for (end, tail_len, valid) in cases {
            let created = DirectWriter::new(
                tempfile::tempfile().unwrap(),
                align,
                end,
                vec![0; tail_len],
                RecordingIo::new(align),
            );
            check!(created.is_ok() == valid, "new: end {end}, tail {tail_len}");
            if let Err(error) = created {
                check!(error.kind() == ErrorKind::InvalidInput);
            }

            let (mut writer, _io) = writer(512);
            let reset = writer.reset(end, vec![0; tail_len]);
            check!(reset.is_ok() == valid, "reset: end {end}, tail {tail_len}");
            if let Err(error) = reset {
                check!(error.kind() == ErrorKind::InvalidInput);
                check!(writer.end() == 0);
            }
        }
    }

    #[test]
    fn new_rejects_an_alignment_that_is_not_a_power_of_two() {
        let cases = [
            DioAlign { mem: 0, block: 512 },
            DioAlign { mem: 512, block: 0 },
            DioAlign {
                mem: 512,
                block: 1000,
            },
            DioAlign { mem: 3, block: 512 },
        ];
        for align in cases {
            let created = DirectWriter::new(
                tempfile::tempfile().unwrap(),
                align,
                0,
                Vec::new(),
                RecordingIo::new(DioAlign { mem: 1, block: 1 }),
            );
            check!(
                created.err().map(|error| error.kind()) == Some(ErrorKind::InvalidInput),
                "{align:?}"
            );
        }
    }

    #[test]
    fn aligned_buf_stays_aligned_and_keeps_its_contents_as_it_grows() {
        for align in [1, 2, 8, 64, 512, 4096] {
            let mut buf = AlignedBuf::new(align);
            check!(buf.as_slice().is_empty());
            let mut expected = Vec::new();
            for (seed, size) in [3, 100, 1000, 5000, 20_000].into_iter().enumerate() {
                let bytes = pattern(u8::try_from(seed).unwrap(), size);
                buf.extend_from_slice(&bytes);
                expected.extend_from_slice(&bytes);
                check!(buf.as_slice().as_ptr().addr() % align == 0, "align {align}");
                check!(buf.as_slice().len() == expected.len());
                check!(buf.as_slice() == &expected[..], "align {align}");
            }
        }
    }

    #[test]
    fn aligned_buf_resize_zero_fills_over_old_contents() {
        let mut buf = AlignedBuf::new(512);
        buf.extend_from_slice(&[7; 100]);
        buf.clear();
        check!(buf.as_slice().is_empty());
        buf.extend_from_slice(&[1; 10]);
        buf.resize(40);
        check!(buf.as_slice() == &[[1; 10].as_slice(), &[0; 30]].concat()[..]);
        buf.resize(5);
        check!(buf.as_slice() == &[1; 5]);
        buf.resize(3000);
        check!(buf.as_slice() == &[[1; 5].as_slice(), &[0; 2995]].concat()[..]);
        check!(buf.as_slice().as_ptr().addr() % 512 == 0);
    }

    /// A [`TailCache`] and the bytes pushed into it, in order.
    struct ThreeChunks {
        cache: TailCache,
        all: Vec<u8>,
    }

    /// A window that starts at 1000 and holds chunks of 10, 20 and 30 bytes,
    /// all of them on the disk.
    fn three_chunks(cap: usize) -> ThreeChunks {
        let mut cache = TailCache::new(cap, 1000);
        cache.set_written(1060);
        let mut all = Vec::new();
        for (seed, size) in [(1, 10), (2, 20), (3, 30)] {
            let bytes = pattern(seed, size);
            all.extend_from_slice(&bytes);
            cache.push(&[&bytes]);
        }
        ThreeChunks { cache, all }
    }

    fn read(cache: &TailCache, pos: u64, len: usize) -> Option<Vec<u8>> {
        let mut out = vec![0xAA; len];
        if cache.window().read(pos, &mut out) {
            Some(out)
        } else {
            check!(
                out == vec![0xAA; len],
                "a failed read leaves the buffer alone"
            );
            None
        }
    }

    #[test]
    fn tail_cache_reads_across_chunk_boundaries() {
        /// A position, a length, and the expected window slice.
        struct Case {
            pos: u64,
            len: usize,
            range: Option<Range<usize>>,
        }
        let ThreeChunks { cache, all } = three_chunks(1 << 20);
        check!((cache.start(), cache.end()) == (1000, 1060));
        let cases = [
            Case {
                pos: 1000,
                len: 60,
                range: Some(0..60),
            },
            Case {
                pos: 1000,
                len: 10,
                range: Some(0..10),
            },
            Case {
                pos: 1005,
                len: 10,
                range: Some(5..15),
            },
            Case {
                pos: 1009,
                len: 30,
                range: Some(9..39),
            },
            Case {
                pos: 1030,
                len: 30,
                range: Some(30..60),
            },
            Case {
                pos: 1059,
                len: 1,
                range: Some(59..60),
            },
            Case {
                pos: 1060,
                len: 0,
                range: Some(60..60),
            },
            Case {
                pos: 1000,
                len: 0,
                range: Some(0..0),
            },
            Case {
                pos: 999,
                len: 1,
                range: None,
            },
            Case {
                pos: 999,
                len: 0,
                range: None,
            },
            Case {
                pos: 1050,
                len: 11,
                range: None,
            },
            Case {
                pos: 1061,
                len: 0,
                range: None,
            },
            Case {
                pos: 2000,
                len: 5,
                range: None,
            },
            Case {
                pos: u64::MAX,
                len: 1,
                range: None,
            },
        ];
        for Case { pos, len, range } in cases {
            check!(
                read(&cache, pos, len) == range.map(|range| all[range].to_vec()),
                "pos {pos}, len {len}"
            );
        }
    }

    #[test]
    fn tail_cache_evicts_the_oldest_chunks_past_the_cap() {
        // Each case: the cap, and the start of the window after the three
        // pushes.
        let cases = [
            (1000, 1000),
            (60, 1000),
            (59, 1010),
            (50, 1010),
            (30, 1030),
            (29, 1030),
            (0, 1030),
        ];
        for (cap, start) in cases {
            let ThreeChunks { cache, all } = three_chunks(cap);
            check!((cache.start(), cache.end()) == (start, 1060), "cap {cap}");
            check!(
                read(&cache, start, 1060 - usize::try_from(start).unwrap())
                    == Some(all[usize::try_from(start - 1000).unwrap()..].to_vec())
            );
            check!(read(&cache, start - 1, 1).is_none());
        }
    }

    /// One chunk comes back as a slice of the cached bytes, with no copy;
    /// several are joined up to `max`, and the first comes whatever its size.
    #[test]
    fn tail_cache_bytes_from_slices_one_chunk_and_joins_several() {
        let ThreeChunks { cache, all } = three_chunks(1000);
        let one = cache.window().bytes_from(1010, 5).unwrap();
        check!(one[..] == all[10..30]);
        check!(one.as_ptr() == cache.window().chunks[1].as_ptr());
        check!(cache.window().bytes_from(1015, 0).unwrap()[..] == all[15..30]);
        check!(cache.window().bytes_from(1000, 30).unwrap()[..] == all[..30]);
        check!(cache.window().bytes_from(1000, 60).unwrap()[..] == all[..]);
        check!(cache.window().bytes_from(1000, 59).unwrap()[..] == all[..30]);
        check!(cache.window().bytes_from(999, 10).is_none());
        check!(cache.window().bytes_from(1060, 10).is_none());
    }

    /// A chunk that is not on the disk yet stays, whatever the cap, so a read
    /// below the window can always go to the file. Once it is written, the
    /// next push evicts it.
    #[test]
    fn tail_cache_keeps_the_bytes_that_are_not_on_the_disk() {
        let mut cache = TailCache::new(0, 1000);
        for (seed, size) in [(1, 10), (2, 20), (3, 30)] {
            cache.push(&[&pattern(seed, size)]);
        }
        check!((cache.start(), cache.end()) == (1000, 1060));

        cache.set_written(1015);
        cache.push(&[&pattern(4, 5)]);
        check!(cache.start() == 1010);

        cache.set_written(1065);
        cache.push(&[&pattern(5, 5)]);
        check!((cache.start(), cache.end()) == (1065, 1070));

        cache.truncate(1062);
        check!((cache.start(), cache.end()) == (1062, 1062));
        cache.push(&[&pattern(6, 5)]);
        cache.push(&[&pattern(7, 5)]);
        check!(cache.start() == 1062);
    }

    /// A coalesced write leaves the same window as pushing its batches one
    /// by one, and no batch left in the window shares the group's allocation,
    /// so the window still holds at most `cap` bytes (or the newest batch).
    #[test]
    fn tail_cache_group_push_matches_single_pushes_and_detaches_survivors() {
        for cap in [1000, 60, 59, 50, 30, 29, 0] {
            let ThreeChunks { cache: single, all } = three_chunks(cap);
            let group = Bytes::from(all.clone());
            let mut grouped = TailCache::new(cap, 1000);
            grouped.set_written(1060);
            grouped.push_group(&group, &[10, 30, 60]);
            check!(
                (grouped.start(), grouped.end()) == (single.start(), single.end()),
                "cap {cap}"
            );
            let start = single.start();
            let len = 1060 - usize::try_from(start).unwrap();
            check!(
                read(&grouped, start, len) == read(&single, start, len),
                "cap {cap}"
            );
            let group_range = group.as_ptr().addr()..group.as_ptr().addr() + group.len();
            for chunk in &grouped.window().chunks {
                check!(
                    !group_range.contains(&chunk.as_ptr().addr()),
                    "cap {cap}: a retained batch still points into the group"
                );
            }
        }
    }

    #[test]
    fn tail_cache_ignores_an_empty_push() {
        let mut cache = TailCache::new(0, 5);
        cache.push(&[b"abc"]);
        cache.push(&[&[]]);
        check!((cache.start(), cache.end()) == (5, 8));
        check!(read(&cache, 5, 3) == Some(b"abc".to_vec()));
    }

    #[test]
    fn tail_cache_truncate_forgets_bytes_past_the_new_end() {
        // Each case: the truncate point, then the window it leaves.
        let cases = [
            (2000, 1000, 1060),
            (1060, 1000, 1060),
            (1045, 1000, 1045),
            (1030, 1000, 1030),
            (1010, 1000, 1010),
            (1003, 1000, 1003),
            (1000, 1000, 1000),
            (900, 900, 900),
        ];
        for (end, start, new_end) in cases {
            let ThreeChunks { mut cache, all } = three_chunks(1 << 20);
            cache.truncate(end);
            check!(
                (cache.start(), cache.end()) == (start, new_end),
                "truncate to {end}"
            );
            let kept = usize::try_from(new_end - start).unwrap();
            let from = usize::try_from(start.saturating_sub(1000)).unwrap();
            check!(read(&cache, start, kept) == Some(all[from..from + kept].to_vec()));
            check!(read(&cache, new_end, 1).is_none());

            // The window grows again from the new end.
            cache.push(&[b"xyz"]);
            check!(cache.end() == new_end + 3);
            check!(read(&cache, new_end, 3) == Some(b"xyz".to_vec()));
        }
    }

    /// Push `count` written batches of `size` bytes into `cache`.
    fn fill(cache: &mut TailCache, count: usize, size: usize) {
        for seed in 0..count {
            let batch = pattern(u8::try_from(seed % 251).unwrap(), size);
            cache.push(&[&batch]);
            cache.set_written(cache.end());
        }
    }

    /// With no budget a window holds its floor; under a budget it grows to
    /// its maximum; at the budget it keeps the size it has.
    #[test]
    fn tail_cache_grows_past_its_floor_only_under_a_budget() {
        let batch = 1000;
        let unbudgeted = Arc::new(TailArena::new(None));
        let mut fixed = TailCache::in_arena(10 * batch, 100 * batch, 0, Arc::clone(&unbudgeted));
        fill(&mut fixed, 300, batch);
        check!(fixed.end() - fixed.start() == to_u64(10 * batch));

        let roomy = Arc::new(TailArena::new(Some(1 << 30)));
        let mut grown = TailCache::in_arena(10 * batch, 100 * batch, 0, Arc::clone(&roomy));
        fill(&mut grown, 300, batch);
        check!(grown.end() - grown.start() == to_u64(100 * batch));

        // A budget the caches already exceed: a new window stops at what it
        // holds once full, and stays at its floor.
        let tight = Arc::new(TailArena::new(Some(to_u64(FIRST_SLAB))));
        let mut held = TailCache::in_arena(10 * batch, 100 * batch, 0, Arc::clone(&tight));
        fill(&mut held, 300, batch);
        check!(held.end() - held.start() == to_u64(10 * batch));
    }

    /// Slabs the window no longer reaches go back to the arena, and dropping
    /// the cache gives back the rest, so the arena counts what the windows
    /// hold and nothing more.
    #[test]
    fn tail_cache_gives_its_slabs_back() {
        let arena = Arc::new(TailArena::new(None));
        let mut cache = TailCache::in_arena(64 * 1024, 64 * 1024, 0, Arc::clone(&arena));
        fill(&mut cache, 2000, 10 * 1024);
        check!(cache.end() == to_u64(2000 * 10 * 1024));
        check!(to_u64(cache.slab_bytes()) == arena.held());
        check!(
            cache.slab_bytes() <= 2 * SLAB + 64 * 1024,
            "a 64 KiB window keeps no more than the slabs it reaches: {}",
            cache.slab_bytes()
        );
        drop(cache);
        check!(arena.held() == 0);
    }

    /// A read view clones only the chunks from its position on.
    #[test]
    fn tail_window_suffix_keeps_the_chunks_from_a_position_on() {
        let ThreeChunks { cache, all } = three_chunks(1 << 20);
        let window = cache.window();
        for (pos, start, chunks) in [
            (0, 1000, 3),
            (1000, 1000, 3),
            (1009, 1000, 3),
            (1010, 1010, 2),
            (1045, 1030, 1),
            (1060, 1060, 0),
            (5000, 1060, 0),
        ] {
            let suffix = window.suffix(pos);
            check!(
                (suffix.start(), suffix.end(), suffix.chunks.len()) == (start, 1060, chunks),
                "suffix from {pos}"
            );
            let from = usize::try_from(start - 1000).unwrap();
            check!(
                suffix
                    .bytes_from(start, usize::MAX)
                    .map(|bytes| bytes.to_vec())
                    == (start < 1060).then(|| all[from..].to_vec())
            );
        }
    }

    /// A segment's cache takes the slabs the cache of the segment before it
    /// gave back when it rolled, up to the budget.
    #[test]
    fn a_new_segment_cache_takes_the_rolled_caches_slabs() {
        let arena = Arc::new(TailArena::new(Some(64 << 20)));
        for _segment in 0..4 {
            let mut cache = TailCache::in_arena(32 << 20, 32 << 20, 0, Arc::clone(&arena));
            fill(&mut cache, 40, 1 << 20);
        }
        check!(
            arena.fresh() <= 9,
            "{} slabs made new for four 40 MiB segments with 32 MiB windows",
            arena.fresh()
        );
        check!(arena.held() == 0);
    }

    /// A window that turns over many times reuses the same few slabs.
    #[test]
    fn tail_cache_reuses_its_slabs() {
        let arena = Arc::new(TailArena::new(None));
        let mut cache = TailCache::in_arena(16 << 20, 16 << 20, 0, Arc::clone(&arena));
        fill(&mut cache, 1000, 1 << 20);
        check!(
            arena.fresh() <= 16,
            "{} slabs made new for 1000 MiB through a 16 MiB window ({} pooled)",
            arena.fresh(),
            arena.pooled()
        );
    }

    /// A truncation gives back the slabs it leaves wholly past the cut, so a
    /// window cut back to its first batches does not keep its old size
    /// counted against the budget.
    #[test]
    fn tail_cache_truncate_gives_back_the_slabs_past_the_cut() {
        let arena = Arc::new(TailArena::new(Some(1 << 30)));
        let mut cache = TailCache::in_arena(1 << 20, 64 << 20, 0, Arc::clone(&arena));
        fill(&mut cache, 48, 1 << 20);
        check!(cache.end() - cache.start() == 48 << 20);
        let grown = arena.held();
        check!(grown >= 48 << 20);
        cache.truncate(2 << 20);
        check!((cache.start(), cache.end()) == (0, 2 << 20));
        check!(to_u64(cache.slab_bytes()) == arena.held());
        check!(
            arena.held() <= to_u64(2 * SLAB),
            "{} bytes held after cutting {grown} back to 2 MiB",
            arena.held()
        );
        cache.push(&[b"after the cut"]);
        check!(cache.end() == (2 << 20) + 13);
        check!(read(&cache, 2 << 20, 13) == Some(b"after the cut".to_vec()));
    }
}
