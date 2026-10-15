//! Ordered rollover flushes that own file handles instead of the log mutex.

#[cfg(not(target_os = "wasi"))]
use crate::{
    io::{IoTarget, LogIo},
    producer_snapshot::PreparedSnapshot,
};
#[cfg(not(target_os = "wasi"))]
use std::{collections::VecDeque, fs::File};
use {
    krabka_ids::Offset,
    std::{
        io,
        sync::{Arc, Condvar, Mutex},
    },
};

#[cfg(not(target_os = "wasi"))]
mod executor;

/// A sealed segment and the producer state at its exclusive end.
#[cfg(not(target_os = "wasi"))]
#[derive(Debug)]
pub(super) struct Flush {
    pub files: [Arc<File>; 3],
    pub io: Arc<dyn LogIo>,
    pub snapshot: PreparedSnapshot,
    // Retain the reservation through the running job, including error unwinds.
    pub _permit: executor::Permit,
}

#[cfg(not(target_os = "wasi"))]
impl Flush {
    fn run(self) -> io::Result<()> {
        // Never publish a durable snapshot ahead of the records it describes.
        self.io.sync_data(&self.files[0])?;
        self.io.sync_file(IoTarget::OffsetIndex, &self.files[1])?;
        self.io.sync_file(IoTarget::TimeIndex, &self.files[2])?;
        self.snapshot.write(&*self.io)?;
        Ok(())
    }
}

#[derive(Debug, Default)]
struct State {
    #[cfg(not(target_os = "wasi"))]
    jobs: VecDeque<Flush>,
    /// The boundary-snapshot offset of the job a worker is running now.
    #[cfg(not(target_os = "wasi"))]
    in_progress: Option<Offset>,
    running: bool,
    error: Option<FlushError>,
}

/// The kind and message of the I/O error that failed a flush job.
#[derive(Debug)]
struct FlushError {
    kind: io::ErrorKind,
    message: String,
}

impl State {
    /// Whether a queued or running job will still write a boundary snapshot
    /// at or below `offset`.
    #[cfg(not(target_os = "wasi"))]
    fn writes_snapshot_through(&self, offset: Offset) -> bool {
        self.in_progress.is_some_and(|pending| pending <= offset)
            || self.jobs.iter().any(|job| job.snapshot.offset() <= offset)
    }

    #[cfg(target_os = "wasi")]
    fn writes_snapshot_through(&self, _offset: Offset) -> bool { false }

    fn result(&self) -> io::Result<()> {
        self.error.as_ref().map_or(Ok(()), |error| {
            Err(io::Error::new(error.kind, error.message.clone()))
        })
    }
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

/// Ordered per-log jobs on a bounded process-wide executor.
/// WASI flushes inline in the rollover caller and needs no executor.
#[derive(Debug, Default)]
pub(super) struct Flusher {
    shared: Arc<Shared>,
    #[cfg(not(target_os = "wasi"))]
    executor: Option<Arc<executor::Executor>>,
}

impl Flusher {
    pub fn check(&self) -> io::Result<()> { self.shared.state.lock().unwrap().result() }

    pub fn finish(&self) -> io::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        while state.running {
            state = self.shared.changed.wait(state).unwrap();
        }
        state.result()
    }

    /// Wait only for the jobs that still write a boundary snapshot at or below
    /// `offset`, then report a failed flush.
    ///
    /// Retention calls this before it deletes the segments up to `offset`, so
    /// that no job writes a snapshot at a deleted base afterwards. A job for a
    /// newer boundary does not touch what retention deletes, and waiting for
    /// its disk sync would hold the log mutex, and every append and read of
    /// the partition, for that long.
    pub fn finish_through(&self, offset: Offset) -> io::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        while state.writes_snapshot_through(offset) {
            state = self.shared.changed.wait(state).unwrap();
        }
        state.result()
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn reserve(&mut self, bytes: usize) -> io::Result<executor::Permit> {
        self.check()?;
        if self.executor.is_none() {
            self.executor = Some(executor::Executor::shared()?);
        }
        Ok(self.executor.as_ref().unwrap().reserve(bytes))
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn submit(&self, flush: Flush) -> io::Result<()> {
        let mut state = self.shared.state.lock().unwrap();
        state.result()?;
        state.jobs.push_back(flush);
        if !state.running {
            state.running = true;
            self.executor
                .as_ref()
                .unwrap()
                .submit(Arc::clone(&self.shared));
        }
        Ok(())
    }
}

#[cfg(not(target_os = "wasi"))]
fn run(shared: &Shared) -> bool {
    let job = {
        let mut state = shared.state.lock().unwrap();
        let job = state.jobs.pop_front().unwrap();
        state.in_progress = Some(job.snapshot.offset());
        job
    };
    // An injected I/O panic must wake waiters just like an I/O error.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.run()))
        .unwrap_or_else(|_| Err(io::Error::other("rollover flush panicked")));
    let mut state = shared.state.lock().unwrap();
    state.in_progress = None;
    if let Err(error) = result {
        tracing::error!(%error, "rollover flush failed");
        state.error = Some(FlushError {
            kind: error.kind(),
            message: error.to_string(),
        });
        state.jobs.clear();
    }
    // A `finish_through` waiter may be waiting for this job alone.
    shared.changed.notify_all();
    if state.jobs.is_empty() {
        state.running = false;
        false
    } else {
        true
    }
}

impl Drop for Flusher {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            tracing::warn!(%error, "log closed after a failed rollover flush");
        }
    }
}

#[cfg(all(test, not(target_os = "wasi")))]
mod tests;
