//! The lock the unit tests share, so that two tests never mutate the process
//! environment at the same time.
//!
//! Every test that builds `Args` takes it, not only the ones that set
//! variables: clap reads `KRABKA_BROKER_*` through its `env` attribute, so a
//! parse outside the lock can see a variable another test is midway through
//! setting. Cargo hides this by running each test in its own process; the
//! Bazel `rust_test` binary runs all of them as threads in one.

use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub fn env_guard() -> MutexGuard<'static, ()> {
    // The lock guards nothing but ordering, so a test that panicked while
    // holding it leaves nothing to recover -- and poisoning it would turn one
    // failure into a failure of every test in the binary.
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}
