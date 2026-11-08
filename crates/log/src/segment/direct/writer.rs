//! The queue of a segment's `O_DIRECT` writes, and the threads and `io_uring`
//! submitter that put them on the disk.

use {
    super::{AlignedBuf, DioAlign, to_u64},
    crate::io::LogIo,
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

#[cfg(test)]
#[cfg(unix)]
mod tests;
