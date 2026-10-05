//! Ordered rollover flushes that own file handles instead of the log mutex.

#[cfg(not(target_os = "wasi"))]
use std::collections::VecDeque;
use std::{
    fs::File,
    io,
    sync::{Arc, Condvar, Mutex},
};

use crate::{
    io::{IoTarget, LogIo},
    producer_snapshot::PreparedSnapshot,
};

/// A sealed segment and the producer state at its exclusive end.
#[derive(Debug)]
pub(super) struct Flush {
    pub files: [Arc<File>; 3],
    pub io: Arc<dyn LogIo>,
    pub snapshot: PreparedSnapshot,
}

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
    running: bool,
    error: Option<(io::ErrorKind, String)>,
}

impl State {
    fn result(&self) -> io::Result<()> {
        self.error.as_ref().map_or(Ok(()), |(kind, message)| {
            Err(io::Error::new(*kind, message.clone()))
        })
    }
}

#[derive(Debug, Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

/// One worker while this log has rollover work, with bounded backpressure.
/// No runtime dependency or idle thread is needed by a standalone `Log`.
#[derive(Debug, Default)]
pub(super) struct Flusher(Arc<Shared>);

impl Flusher {
    pub fn check(&self) -> io::Result<()> {
        self.0.state.lock().unwrap().result()
    }

    pub fn finish(&self) -> io::Result<()> {
        let mut state = self.0.state.lock().unwrap();
        while state.running {
            state = self.0.changed.wait(state).unwrap();
        }
        state.result()
    }

    #[cfg(not(target_os = "wasi"))]
    pub fn submit(&self, flush: Flush) -> io::Result<()> {
        let mut state = self.0.state.lock().unwrap();
        // Bound snapshot bytes and cloned descriptors when disk falls behind.
        // The running job owns one more frame outside this queue.
        while state.jobs.len() >= 16 && state.error.is_none() {
            state = self.0.changed.wait(state).unwrap();
        }
        state.result()?;
        state.jobs.push_back(flush);
        if !state.running {
            state.running = true;
            let shared = Arc::clone(&self.0);
            if let Err(error) = std::thread::Builder::new()
                .name("log-roll-flush".into())
                .spawn(move || run(&shared))
            {
                state.error = Some((error.kind(), error.to_string()));
                state.jobs.clear();
                state.running = false;
                self.0.changed.notify_all();
                return Err(error);
            }
        }
        Ok(())
    }

    // WASI cannot start native threads. Its durability operations stay inline.
    #[cfg(target_os = "wasi")]
    pub fn submit(&self, flush: Flush) -> io::Result<()> {
        self.check()?;
        let result = flush.run();
        if let Err(error) = &result {
            self.0.state.lock().unwrap().error = Some((error.kind(), error.to_string()));
        }
        result
    }
}

#[cfg(not(target_os = "wasi"))]
fn run(shared: &Shared) {
    loop {
        let job = {
            let mut state = shared.state.lock().unwrap();
            let Some(job) = state.jobs.pop_front() else {
                state.running = false;
                shared.changed.notify_all();
                return;
            };
            shared.changed.notify_all();
            job
        };
        // An injected I/O panic must wake waiters just like an I/O error.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.run()))
            .unwrap_or_else(|_| Err(io::Error::other("rollover flush panicked")));
        if let Err(error) = result {
            tracing::error!(%error, "rollover flush failed");
            let mut state = shared.state.lock().unwrap();
            state.error = Some((error.kind(), error.to_string()));
            state.jobs.clear();
            state.running = false;
            shared.changed.notify_all();
            return;
        }
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
