//! Ordered rollover flushes that own file handles instead of the log mutex.

#[cfg(not(target_os = "wasi"))]
use crate::{
    io::{IoTarget, LogIo},
    log::page_cache,
    producer_snapshot::PreparedSnapshot,
};
#[cfg(not(target_os = "wasi"))]
use std::{
    collections::VecDeque,
    fs::File,
    mem,
    panic::{self, AssertUnwindSafe},
};
use std::{
    io,
    sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError},
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

/// One job on a log's ordered queue.
#[cfg(not(target_os = "wasi"))]
#[derive(Debug)]
enum Job {
    /// A sealed segment's files and the producer snapshot at its end.
    Rollover(Flush),
    /// The active segment's `.log` file, synced so that the page cache holds
    /// little dirty data for it, and the whole pages the sync hands over,
    /// recorded against the page-cache budget afterwards. See
    /// `Log::pace_writeback` and [`page_cache::Ledger`].
    Writeback {
        file: Arc<File>,
        io: Arc<dyn LogIo>,
        /// The `len` bytes of the file from `offset` this job hands over.
        offset: u64,
        len: u64,
        /// The page-cache budget the range counts against, in bytes. `None`
        /// records nothing.
        budget: Option<u64>,
        ledger: Arc<page_cache::Ledger>,
    },
}

#[cfg(not(target_os = "wasi"))]
impl Job {
    fn run(self) -> io::Result<()> {
        match self {
            Self::Rollover(flush) => flush.run(),
            Self::Writeback {
                file,
                io,
                offset,
                len,
                budget,
                ledger,
            } => {
                io.sync_data(&file)?;
                if let Some(budget) = budget {
                    ledger.retain(file, io, offset, len, budget);
                }
                Ok(())
            }
        }
    }
}

#[derive(Debug, Default)]
struct State {
    #[cfg(not(target_os = "wasi"))]
    jobs: VecDeque<Job>,
    running: bool,
    error: Option<FlushError>,
    /// Jobs submitted so far.
    submitted: u64,
    /// Jobs run or dropped after a failure so far. Jobs run in submission
    /// order, so the first `ended` jobs are the ones that ended.
    ended: u64,
}

/// The kind and message of the I/O error that failed a flush job.
#[derive(Debug)]
struct FlushError {
    kind: io::ErrorKind,
    message: String,
}

impl State {
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

impl Shared {
    /// The state, whether or not a thread panicked while it held the lock.
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Wait for `done` to hold of the state, then report a failed flush.
    fn wait_until(&self, done: impl Fn(&State) -> bool) -> io::Result<()> {
        self.changed
            .wait_while(self.lock(), |state| !done(state))
            .unwrap_or_else(PoisonError::into_inner)
            .result()
    }

    fn finish_through(&self, ticket: FlushTicket) -> io::Result<()> {
        self.wait_until(|state| state.ended >= ticket.0)
    }
}

/// The jobs a [`Flusher`] had taken when [`Flusher::ticket`] was called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FlushTicket(u64);

/// Ordered per-log jobs on a bounded process-wide executor.
/// WASI flushes inline in the rollover caller and needs no executor.
#[derive(Debug, Default)]
pub(super) struct Flusher {
    shared: Arc<Shared>,
    #[cfg(not(target_os = "wasi"))]
    executor: Option<Arc<executor::Executor>>,
}

impl Flusher {
    pub fn check(&self) -> io::Result<()> { self.shared.lock().result() }

    pub fn finish(&self) -> io::Result<()> { self.shared.wait_until(|state| !state.running) }

    /// The jobs submitted so far, for [`Self::finish_through`] to wait for
    /// later without waiting for any submitted after.
    pub fn ticket(&self) -> FlushTicket { FlushTicket(self.shared.lock().submitted) }

    /// Wait until every job submitted before `ticket` was taken has ended,
    /// then report a failed flush as [`Self::check`] does.
    pub fn finish_through(&self, ticket: FlushTicket) -> io::Result<()> {
        self.shared.finish_through(ticket)
    }

    /// The executor this log's jobs run on, started on the first call.
    #[cfg(not(target_os = "wasi"))]
    fn executor(&mut self) -> io::Result<Arc<executor::Executor>> {
        if let Some(executor) = &self.executor {
            return Ok(Arc::clone(executor));
        }
        let executor = executor::Executor::shared()?;
        self.executor = Some(Arc::clone(&executor));
        Ok(executor)
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn reserve(&mut self, bytes: usize) -> io::Result<executor::Permit> {
        self.check()?;
        Ok(self.executor()?.reserve(bytes))
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn submit(&self, flush: Flush) -> io::Result<()> { self.enqueue(Job::Rollover(flush)) }

    /// Queue a sync of the active segment's `.log` file behind the jobs
    /// already queued, handing over its `len` whole pages from `offset` to
    /// the executor's page-cache ledger under `budget` once synced. Unlike a
    /// rollover flush it holds no snapshot memory, so it reserves nothing; it
    /// does start the executor when no rollover has.
    #[cfg(not(target_os = "wasi"))]
    pub fn submit_writeback(
        &mut self,
        file: Arc<File>,
        io: Arc<dyn LogIo>,
        offset: u64,
        len: u64,
        budget: Option<u64>,
    ) -> io::Result<()> {
        self.check()?;
        let ledger = Arc::clone(self.executor()?.page_cache());
        self.enqueue(Job::Writeback {
            file,
            io,
            offset,
            len,
            budget,
            ledger,
        })
    }

    #[cfg(not(target_os = "wasi"))]
    fn enqueue(&self, job: Job) -> io::Result<()> {
        // `reserve` or `submit_writeback` starts the executor before a job is
        // queued.
        let Some(executor) = &self.executor else {
            return Err(io::Error::other(
                "rollover flush queued before the executor started",
            ));
        };
        let start = {
            let mut state = self.shared.lock();
            state.result()?;
            state.jobs.push_back(job);
            state.submitted += 1;
            !mem::replace(&mut state.running, true)
        };
        if start {
            executor.submit(Arc::clone(&self.shared));
        }
        Ok(())
    }
}

#[cfg(not(target_os = "wasi"))]
fn run(shared: &Shared) -> bool {
    let job = {
        let mut state = shared.lock();
        let job = state.jobs.pop_front();
        // The executor runs a log's jobs only while some are queued.
        if job.is_none() {
            state.running = false;
        }
        job
    };
    let Some(job) = job else {
        return false;
    };
    // An injected I/O panic must wake waiters just like an I/O error.
    let result = panic::catch_unwind(AssertUnwindSafe(|| job.run()))
        .unwrap_or_else(|_| Err(io::Error::other("rollover flush panicked")));
    let mut state = shared.lock();
    state.ended += 1;
    if let Err(error) = result {
        tracing::error!(%error, "rollover flush failed");
        state.error = Some(FlushError {
            kind: error.kind(),
            message: error.to_string(),
        });
        state.jobs.clear();
        state.ended = state.submitted;
    }
    // A waiter on a ticket may be satisfied before the queue drains.
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

#[cfg(test)]
#[cfg(not(target_os = "wasi"))]
mod tests;
