//! The message a step or accessor panics with.

use {
    rstest_bdd::panic_message,
    std::panic::{AssertUnwindSafe, catch_unwind},
};

/// The panic message of `run`, which must panic.
pub fn panic_of(run: impl FnOnce()) -> String {
    let payload = catch_unwind(AssertUnwindSafe(run)).expect_err("it panics");
    panic_message(payload.as_ref())
}
