//! The share-group dead-letter queue (KIP-1191).
//!
//! From `share.version` 2, a record that a share consumer rejects, or that
//! uses up the group's delivery count limit, can be written to a dead-letter
//! topic before the share partition archives it. The group opts in with
//! `errors.deadletterqueue.topic.name`. This module is what Kafka's
//! `ShareGroupDLQManager` and `ShareGroupDLQStateManager` do: it validates
//! the topic, creates it when the cluster allows, and produces one record for
//! each dead-lettered offset.
//!
//! The acquisition machine decides which records go ([`super::state`]), and
//! the leader manager runs the two phases around the write
//! ([`super::manager`]). This module owns the write:
//!
//! - `validate` checks the group's topic against the cluster's rules;
//! - `record` builds the dead-letter records;
//! - `source` reads the source records for a group that copies them;
//! - `coalesce` puts the rounds of every write for one destination into as few
//!   produce requests as `max.message.bytes` allows;
//! - `writer` sends the topic creation and the produce request, retries, and
//!   counts the `DeadLetterQueue*` meters.
//!
//! Whether a write succeeds does not decide the record's fate. Kafka archives
//! the record whatever [`DlqSink::write`] answers, and logs a failure.

use {
    crate::{metrics::BrokerMetrics, share_partition::state::DlqCause},
    async_trait::async_trait,
    krabka_log::Offset,
    krabka_metadata::NodeId,
    std::{
        fmt::{Debug, Formatter, Result as FmtResult},
        sync::Arc,
    },
};

mod coalesce;
mod record;
mod source;
mod validate;
mod writer;

pub use self::writer::DlqWriter;

/// One run of records to dead-letter: Kafka's `ShareGroupDLQRecordParameter`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DlqRequest {
    pub group: String,
    /// The id of the source topic.
    pub topic_id: uuid::Uuid,
    pub source_partition: i32,
    /// The first source offset of the run.
    pub first: Offset,
    /// The last source offset of the run.
    pub last: Offset,
    pub delivery_count: i16,
    pub cause: DlqCause,
}

/// Why a dead-letter write failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DlqError {
    /// The group's dead-letter topic is not usable as configured: Kafka's
    /// `ConfigException`.
    #[error("{0}")]
    Config(String),
    /// The topic could not be created, or the records could not be produced.
    #[error("{0}")]
    Write(String),
}

/// Where the leader manager sends a run of records: Kafka's
/// `ShareGroupDLQManager`.
#[async_trait]
pub trait DlqSink: Send + Sync {
    /// Writes the dead-letter records of `request`. It answers when the
    /// records are durable in the topic, or when the write has failed for
    /// good.
    async fn write(&self, request: DlqRequest) -> Result<(), DlqError>;

    /// Stop owned workers after the broker has cancelled and drained enqueue futures.
    ///
    /// Implementations must await termination of their own background IO here.
    /// Abrupt process death cannot run this callback; durable ARCHIVING ranges
    /// are replayed by the next leader and may duplicate a completed append.
    async fn stop(&self) {}
}

/// Per-broker factory for a native share DLQ manager.
///
/// Register with `BrokerConfig::share_dlq_manager_factory`. One manager is
/// constructed for each broker start, before coordinator background tasks.
/// The factory carries its own validated native settings; this is not Java
/// class loading or a dynamic-library ABI. Its manager receives the same
/// inclusive offset ranges as Kafka's `ShareGroupDLQManager.enqueue`.
/// Errors abort startup rather than silently falling back to the stock writer.
pub trait DlqManagerFactory: Debug + Send + Sync {
    /// Construct an independent manager for this broker's lifecycle.
    ///
    /// # Errors
    ///
    /// Return a configuration or resource-initialization error to abort startup.
    fn create(&self, context: DlqManagerContext) -> Result<Arc<dyn DlqSink>, DlqError>;
}

/// Broker identity and the fully configured stock writer available to a native factory.
///
/// A manager may wrap the stock writer to retain built-in source resolution,
/// validation, routing and metrics, or implement a distinct archival backend.
#[derive(Clone)]
pub struct DlqManagerContext {
    pub broker: NodeId,
    pub stock: Arc<dyn DlqSink>,
    /// Standard per-group counters; a stock delegate already records them.
    pub metrics: BrokerMetrics,
}

impl Debug for DlqManagerContext {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("DlqManagerContext")
            .field("broker", &self.broker)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
pub mod test_support {
    use {super::*, std::sync::Mutex};

    /// A sink that records what it was asked to write, and fails on demand.
    #[derive(Default)]
    pub struct RecordingDlq {
        requests: Mutex<Vec<DlqRequest>>,
        fail_with: Mutex<Option<DlqError>>,
    }

    impl RecordingDlq {
        pub fn failing(error: DlqError) -> Self {
            Self {
                requests: Mutex::default(),
                fail_with: Mutex::new(Some(error)),
            }
        }

        pub fn requests(&self) -> Vec<DlqRequest> {
            self.requests.lock().expect("requests lock").clone()
        }
    }

    #[async_trait]
    impl DlqSink for RecordingDlq {
        async fn write(&self, request: DlqRequest) -> Result<(), DlqError> {
            self.requests.lock().expect("requests lock").push(request);
            let failure = self.fail_with.lock().expect("failure lock").clone();
            failure.map_or(Ok(()), Err)
        }
    }
}
