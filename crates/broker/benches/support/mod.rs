//! Batch shapes and measurement controls shared by the leader and follower benchmarks.

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

pub struct BoundedResource<T> {
    value: T,
    written: usize,
    budget: usize,
    factory: fn() -> Result<T, BenchError>,
}

#[bon::builder]
pub fn bounded_resource<T>(
    budget: usize,
    factory: fn() -> Result<T, BenchError>,
) -> Result<BoundedResource<T>, BenchError> {
    Ok(BoundedResource {
        value: factory()?,
        written: 0,
        budget,
        factory,
    })
}

impl<T> BoundedResource<T> {
    /// Replace the resource when the next write would exceed its byte budget.
    pub fn rotate_if_full(&mut self, incoming: usize) -> Result<&mut T, BenchError> {
        if self.written + incoming > self.budget {
            self.value = (self.factory)()?;
            self.written = 0;
        }
        self.written += incoming;
        Ok(&mut self.value)
    }

    #[allow(dead_code)]
    pub fn get_mut(&mut self) -> &mut T { &mut self.value }
}

/// One batch shape a benchmark sweeps: its report label, record count and
/// per-record payload size.
pub struct BatchShape {
    pub label: &'static str,
    pub records: i32,
    pub payload: usize,
}

pub const SHAPES: [BatchShape; 3] = [
    BatchShape {
        label: "1rec_100KiB",
        records: 1,
        payload: 100 * 1024,
    },
    BatchShape {
        label: "100rec_1KiB",
        records: 100,
        payload: 1024,
    },
    BatchShape {
        label: "1000rec_100B",
        records: 1000,
        payload: 100,
    },
];

/// Reset the log outside the timed region after this many bytes.
pub const LOG_BUDGET: usize = 256 * 1024 * 1024;
/// Epoch carried by the benchmark's replicated batches.
pub const LEADER_EPOCH: i32 = 3;
/// Untimed iterations that fault in the fixture before measuring it.
pub const WARMUP: u64 = 8;
