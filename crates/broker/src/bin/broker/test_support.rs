//! The lock the unit tests share, so that two tests never mutate the process
//! environment at the same time.

use std::sync::{Mutex, MutexGuard, OnceLock};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub fn env_guard() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .expect("environment lock")
}
