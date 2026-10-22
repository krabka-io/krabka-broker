//! A [`LogIo`] that holds a compaction build inside its first write, for
//! tests that act on a log while a pass is building.

use {
    crate::io::{IoTarget, LogIo},
    std::{
        fs::File,
        io::Write as _,
        sync::{
            Arc, Mutex, PoisonError,
            mpsc::{Receiver, Sender, channel},
        },
    },
};

/// Holds the first compaction write until its [`CompactionPauseControl`]
/// resumes it. Every other write goes straight to the file.
#[derive(Debug)]
pub struct CompactionPause {
    paused: Mutex<Option<Sender<()>>>,
    resume: Mutex<Receiver<()>>,
}

/// The test's side of a [`CompactionPause`].
#[derive(Debug)]
pub struct CompactionPauseControl {
    paused: Mutex<Receiver<()>>,
    resume: Sender<()>,
}

/// A pause to install with `Log::test_set_io`, and its control.
#[must_use]
pub fn compaction_pause() -> (Arc<CompactionPause>, CompactionPauseControl) {
    let (paused, wait) = channel();
    let (resume, held) = channel();
    (
        Arc::new(CompactionPause {
            paused: Mutex::new(Some(paused)),
            resume: Mutex::new(held),
        }),
        CompactionPauseControl {
            paused: Mutex::new(wait),
            resume,
        },
    )
}

impl CompactionPauseControl {
    /// Block until a build is held inside its first write.
    ///
    /// # Panics
    /// Panics if the pause was dropped without holding a build.
    pub fn wait_until_paused(&self) {
        self.paused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .recv()
            .expect("a build reaches its first compaction write");
    }

    /// Let the held build go on.
    ///
    /// # Panics
    /// Panics if the pause was dropped.
    pub fn resume(&self) { self.resume.send(()).expect("the pause is installed"); }
}

impl LogIo for CompactionPause {
    fn write_at(&self, target: IoTarget, file: &File, buf: &[u8]) -> std::io::Result<usize> {
        let first = if target == IoTarget::CompactionSwap {
            self.paused
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
        } else {
            None
        };
        if let Some(paused) = first {
            // A test that stopped waiting has nothing to hold the build for.
            if paused.send(()).is_ok() {
                let _ = self
                    .resume
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .recv();
            }
        }
        (&*file).write(buf)
    }
}
