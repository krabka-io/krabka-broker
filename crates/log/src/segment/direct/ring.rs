//! The `io_uring` submitter of the `O_DIRECT` writes.
//!
//! The write threads make one synchronous `pwrite` each, so a segment has at
//! most as many writes at the device as threads hold it. One thread here runs
//! an `io_uring` and submits every segment's writes as they are queued, each
//! from a buffer it owns until the write completes, which is what keeps the
//! submission free of `unsafe`. The queue bookkeeping is the write threads':
//! a write is taken with `Shared::take_work` and recorded with
//! `Shared::complete`, so the two paths put the same bytes at the same
//! offsets.

use {
    super::{AlignedBuf, Shared, Work, aligned_progress, to_u64},
    std::{
        fs::File,
        io::{Error, ErrorKind},
        sync::Arc,
    },
    tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
    tokio_uring::buf::BoundedBuf as _,
};

/// Submission queue entries of the ring. Every segment may have
/// `RING_WRITES_IN_FLIGHT` writes submitted; more wait in the ring's own
/// backlog, which costs nothing but latency.
const ENTRIES: u32 = 256;

/// The thread that owns the `io_uring`, and the channel segments with work to
/// start reach it by.
pub(super) struct Ring {
    segments: UnboundedSender<Arc<Shared>>,
}

impl Ring {
    /// Start the submitter thread. None when the kernel refuses an `io_uring`
    /// or the thread does not start, and the write threads do every write.
    pub(super) fn start() -> Option<Self> {
        let (segments, receiver) = unbounded_channel();
        let (started, outcome) = std::sync::mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("log-direct-uring".to_owned())
            .spawn(move || {
                let runtime = tokio_uring::Runtime::new(tokio_uring::builder().entries(ENTRIES));
                let runtime = match runtime {
                    Ok(runtime) => {
                        let _ = started.send(Ok(()));
                        runtime
                    }
                    Err(error) => {
                        let _ = started.send(Err(error));
                        return;
                    }
                };
                runtime.block_on(serve(receiver));
            });
        if let Err(error) = spawned {
            tracing::warn!(%error, "no io_uring submitter thread; direct writes use the write threads");
            return None;
        }
        match outcome.recv() {
            Ok(Ok(())) => Some(Self { segments }),
            Ok(Err(error)) => {
                tracing::info!(%error, "io_uring unavailable; direct writes use the write threads");
                None
            }
            Err(_) => None,
        }
    }

    /// Start the next write of `segment`.
    pub(super) fn push(&self, segment: Arc<Shared>) {
        // The receiver lives as long as the process: the thread never
        // returns once its runtime runs.
        let _ = self.segments.send(segment);
    }
}

/// Start one write task for every segment that arrives.
async fn serve(mut segments: UnboundedReceiver<Arc<Shared>>) {
    while let Some(segment) = segments.recv().await {
        tokio_uring::spawn(write_one(segment));
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
async fn write_one(segment: Arc<Shared>) {
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
async fn write_owned(
    file: &File,
    buf: &mut AlignedBuf,
    offset: u64,
    step: usize,
) -> std::io::Result<()> {
    // The ring owns the handle it writes through and closes it after, so it
    // gets its own descriptor on the segment's file.
    let file = tokio_uring::fs::File::from_std(file.try_clone()?);
    let (mut storage, start, len) = buf.lend();
    let mut done = 0;
    let result = loop {
        if done == len {
            break Ok(());
        }
        let (written, slice) = file
            .write_at(
                storage.slice(start + done..start + len),
                offset + to_u64(done),
            )
            .submit()
            .await;
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
    // A close error on a duplicate descriptor says nothing about the data,
    // which the write results already did.
    let _ = file.close().await;
    result
}
