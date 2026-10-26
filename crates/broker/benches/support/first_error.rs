//! The error every benchmark suite stops with, and the first one a measured
//! closure met.

use std::{cell::RefCell, error::Error};

/// Any failure while building or measuring a case; it stops the run.
pub type BenchError = Box<dyn Error + Send + Sync>;

/// The first error a measured closure met.
///
/// Criterion's closures must return a measurement or an input, not a
/// `Result`, so the error waits here until criterion returns and the bench
/// reports it.
#[derive(Default)]
pub struct FirstError(RefCell<Option<BenchError>>);

impl FirstError {
    /// The value, or `None` after keeping the error.
    pub fn keep<T, E: Into<BenchError>>(&self, result: Result<T, E>) -> Option<T> {
        match result {
            Ok(value) => Some(value),
            Err(error) => {
                self.0.borrow_mut().get_or_insert(error.into());
                None
            }
        }
    }

    /// The kept error, if any.
    pub fn into_result(self) -> Result<(), BenchError> { self.0.into_inner().map_or(Ok(()), Err) }
}
