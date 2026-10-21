//! The `io_uring` submitter of the `O_DIRECT` writes.
//!
//! The write threads make one synchronous `pwrite` each, so a segment has at
//! most as many writes at the device as threads hold it. One thread here runs
//! a compio runtime on an `io_uring` and submits every segment's writes as
//! they are queued. Each write owns its buffer and a shared reference to the
//! segment's handle until it completes, which is what keeps the submission
//! free of `unsafe`. The queue bookkeeping is the write threads': a write is
//! taken with `Shared::take_work` and recorded with `Shared::complete`, so
//! the two paths put the same bytes at the same offsets.

use {
    super::{AlignedBuf, Shared, Work, aligned_progress, to_u64},
    compio::{
        buf::{IntoInner as _, IoBuf as _},
        driver::{ProactorBuilder, SharedFd, op::WriteAt},
        runtime::{Runtime, RuntimeBuilder},
    },
    std::{
        fs::File,
        io::{Error, ErrorKind},
        rc::Rc,
        sync::Arc,
    },
    tokio::sync::{
        Semaphore,
        mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    },
};

/// Submission queue entries of the ring, and the most writes the ring has
/// started at once, for every segment together. A burst across thousands of
/// partitions then waits in the segments' own queues, in file order, rather
/// than in the kernel's. A write past it waits before it is taken from its
/// segment's queue.
pub(super) const ENTRIES: u32 = 256;

/// The thread that owns the `io_uring`, and the channel segments with work to
/// start reach it by.
pub(super) struct Ring {
    segments: UnboundedSender<Arc<Shared>>,
}

impl Ring {
    /// Start the submitter thread on a ring of `entries`. None when the
    /// kernel refuses the `io_uring` or the thread does not start, and the
    /// write threads do every write.
    pub(super) fn start(entries: u32) -> Option<Self> {
        let (segments, receiver) = unbounded_channel();
        let (started, outcome) = std::sync::mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("log-direct-uring".to_owned())
            .spawn(move || match runtime(entries) {
                Ok(runtime) => {
                    let _ = started.send(Ok(()));
                    runtime.block_on(serve(receiver));
                }
                Err(error) => drop(started.send(Err(error))),
            });
        let outcome = spawned.and_then(|_| {
            outcome
                .recv()
                .unwrap_or_else(|_| Err(Error::other("the io_uring thread exited")))
        });
        match outcome {
            Ok(()) => Some(Self { segments }),
            Err(error) => {
                tracing::info!(%error, "no io_uring; direct writes use the write threads");
                None
            }
        }
    }

    /// Start the next write of `segment`.
    pub(super) fn push(&self, segment: Arc<Shared>) {
        // The receiver lives as long as the process: the thread never
        // returns once its runtime runs.
        let _ = self.segments.send(segment);
    }
}

/// A compio runtime on an `io_uring` of `entries`. The crate is built
/// without compio's `polling` driver, so a kernel that refuses the ring
/// fails here rather than falling back to readiness polling.
fn runtime(entries: u32) -> std::io::Result<Runtime> {
    RuntimeBuilder::new()
        .with_proactor(ProactorBuilder::new().capacity(entries).to_owned())
        .build()
}

/// Start one write task for every segment that arrives.
async fn serve(mut segments: UnboundedReceiver<Arc<Shared>>) {
    let started = Rc::new(Semaphore::new(ENTRIES as usize));
    while let Some(segment) = segments.recv().await {
        compio::runtime::spawn(write_one(segment, Rc::clone(&started))).detach();
    }
}

/// A write handed out by `Shared::take_work` that has not been recorded. A
/// task that panics drops it unrecorded, and the drop records a failure, so
/// the segment never waits for a write that will not finish.
struct Pending {
    segment: Arc<Shared>,
    work: Option<Work>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(work) = self.work.take() {
            self.segment
                .complete(work, Err(Error::other("O_DIRECT write panicked")));
        }
    }
}

/// Make one write of `segment`. Taking it puts the segment back on the
/// channel when another can run beside it, so a segment's writes overlap.
/// It first waits for one of the ring's `started` permits.
async fn write_one(segment: Arc<Shared>, started: Rc<Semaphore>) {
    // The semaphore is never closed, so the permit is always granted.
    let _permit = started.acquire().await;
    let Some(work) = segment.take_work() else {
        return;
    };
    let mut pending = Pending {
        segment: Arc::clone(&segment),
        work: Some(work),
    };
    let step = segment.align.step();
    let (buf, at) = pending.work.as_mut().expect("set just above").buf_mut();
    let result = write_owned(&segment.file, buf, at, step).await;
    if let Some(work) = pending.work.take() {
        segment.complete(work, result);
    }
}

/// Put all of `buf` on the disk at `offset`, as `write_aligned` does, through
/// the ring. `buf` lends its storage to each write and has it back after.
/// Each write holds a reference to `file`, so the handle stays open until
/// the kernel is done with it, and no write needs a descriptor of its own.
async fn write_owned(
    file: &Arc<File>,
    buf: &mut AlignedBuf,
    offset: u64,
    step: usize,
) -> std::io::Result<()> {
    let (mut storage, start, len) = buf.lend();
    let mut done = 0;
    let result = loop {
        if done == len {
            break Ok(());
        }
        let write = WriteAt::new(
            SharedFd::new(Arc::clone(file)),
            offset + to_u64(done),
            storage.slice(start + done..start + len),
        );
        let compio::BufResult(written, slice) = compio::runtime::submit(write).await.into_inner();
        storage = slice.into_inner();
        match written {
            Ok(written) => match aligned_progress(done, written, len, offset, step) {
                Ok(next) => done = next,
                Err(error) => break Err(error),
            },
            Err(error) if error.kind() == ErrorKind::Interrupted => {}
            Err(error) => break Err(error),
        }
    };
    buf.restore((storage, start, len));
    result
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            io::FileIo,
            segment::direct::{DioAlign, DirectWriter, Job},
        },
        assert2::check,
    };

    /// A ring the kernel refuses, here one of no entries, starts no
    /// submitter, and the write threads make every write.
    #[test]
    fn a_ring_the_kernel_refuses_starts_no_submitter() {
        check!(Ring::start(0).is_none());
    }

    /// A write the kernel refuses, here to a handle opened read-only, fails
    /// with the kernel's error, and the buffer has its contents back.
    #[test]
    fn a_refused_write_fails_and_gives_the_buffer_back() {
        let Ok(runtime) = runtime(ENTRIES) else {
            return;
        };
        let file = tempfile::NamedTempFile::new().unwrap();
        let read_only = Arc::new(File::open(file.path()).unwrap());
        let mut buf = AlignedBuf::new(512);
        buf.extend_from_slice(&[7; 1024]);

        let result = runtime.block_on(write_owned(&read_only, &mut buf, 0, 512));

        check!(result.is_err());
        check!(buf.as_slice() == [7; 1024]);
        check!(std::fs::read(file.path()).unwrap().is_empty());
    }

    /// A write task that ends without recording its write, as a panicking
    /// one does, records a failure: the segment fails from then on rather
    /// than waiting for the write forever.
    #[test]
    fn a_write_dropped_unrecorded_fails_the_segment() {
        let align = DioAlign {
            mem: 512,
            block: 512,
        };
        let writer = DirectWriter::new(
            tempfile::tempfile().unwrap(),
            align,
            0,
            Vec::new(),
            Arc::new(FileIo),
        )
        .unwrap();
        let shared = Arc::clone(&writer.shared);
        let mut buf = AlignedBuf::new(align.mem);
        buf.resize(512);
        {
            // Queued without a schedule, so only this test takes it.
            let mut queue = shared.queue.lock().unwrap();
            queue.jobs.push_back(Job { buf, at: 0 });
            queue.queued_end = 512;
            queue.end = 512;
        }
        let work = shared.take_work().unwrap();

        drop(Pending {
            segment: Arc::clone(&shared),
            work: Some(work),
        });

        let error = writer.drain().unwrap_err();
        check!(error.to_string() == "O_DIRECT write panicked");
        check!(writer.written() == 0);
    }
}
