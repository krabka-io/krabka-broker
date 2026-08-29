//! The recovery job, its outcome, and the handle that enqueues one.
//!
//! This is the manager's inbound API. The failover path and the `ElectLeaders`
//! handler hold an `UncleanRecoveryHandle` and post a `RecoveryJob` on it; the
//! admin path also keeps the reply channel that carries the `RecoveryOutcome`
//! back.

use krabka_raft::NodeId;
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

use crate::config_keys::RecoveryStrategy;

/// A request to run unclean recovery for one partition, if it is needed. The
/// failover path and the `ElectLeaders` handler enqueue it, and the URM
/// services it.
pub(crate) struct RecoveryJob {
    pub topic: String,
    pub partition: i32,
    pub strategy: RecoveryStrategy,
    /// Optional reply channel. The admin-triggered `ElectLeaders` path wants
    /// the outcome. The background failover path sends the job and does not
    /// wait for a reply.
    pub reply: Option<oneshot::Sender<RecoveryOutcome>>,
}

/// Result of attempting unclean recovery for a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecoveryOutcome {
    /// The URM elected a new leader and submitted the change. This variant
    /// carries the id.
    Elected(NodeId),
    /// No surviving replica could serve as a leader.
    NoEligibleReplica,
    /// Recovery was unnecessary. The leader is alive, or this node is not the
    /// controller leader, or the partition is gone.
    NotNeeded,
    /// A newer leader already exists, so this recovery is stale and the URM
    /// aborted it.
    Stale,
    /// Another recovery for the same `(topic, partition)` is already running.
    InProgress,
}

/// Cloneable handle that enqueues [`RecoveryJob`] values onto the URM task.
#[derive(Clone)]
pub(crate) struct UncleanRecoveryHandle {
    pub(super) tx: mpsc::Sender<RecoveryJob>,
}

impl UncleanRecoveryHandle {
    #[cfg(test)]
    pub(crate) fn for_tests(tx: mpsc::Sender<RecoveryJob>) -> Self {
        Self { tx }
    }

    /// Enqueues a recovery job. It logs a message, and does not panic, if the
    /// manager has shut down.
    pub(crate) async fn enqueue(&self, job: RecoveryJob) {
        if self.tx.send(job).await.is_err() {
            warn!("unclean recovery manager is gone; job dropped");
        }
    }
}
