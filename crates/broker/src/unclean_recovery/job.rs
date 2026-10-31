//! The recovery job, its outcome, and the handle that enqueues one.
//!
//! This is the manager's inbound API. The failover path and the `ElectLeaders`
//! handler hold an `UncleanRecoveryHandle` and post a `RecoveryJob` on it; the
//! admin path also keeps the reply channel that carries the `RecoveryOutcome`
//! back.

use {
    crate::config_keys::RecoveryStrategy,
    krabka_raft::NodeId,
    tokio::sync::{mpsc, oneshot},
    tracing::warn,
    uuid::Uuid,
};

/// A request to run unclean recovery for one partition, if it is needed. The
/// failover path and the `ElectLeaders` handler enqueue it, and the URM
/// services it.
pub struct RecoveryJob {
    pub topic: String,
    pub partition: i32,
    pub strategy: RecoveryStrategy,
    /// Optional reply channel. The admin-triggered `ElectLeaders` path wants
    /// the outcome. The background failover path sends the job and does not
    /// wait for a reply.
    pub reply: Option<oneshot::Sender<RecoveryOutcome>>,
    /// KFC-9: the break-glass proposal that authorized this recovery, and
    /// `None` when the controller started it on its own.
    ///
    /// The `ElectLeaders` handler spends the approval before it enqueues the
    /// job, so this id is evidence and not an authorization: it says that a
    /// person asked for this recovery, which is what takes the job out of
    /// [`BackgroundRecovery`](super::BackgroundRecovery)'s reach. A recovery
    /// the controller starts on its own has no caller to ask, so
    /// [`UncleanRecoveryHandle::enqueue_background`] sends `None`.
    pub proposal: Option<Uuid>,
}

/// Result of attempting unclean recovery for a single partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryOutcome {
    /// The URM elected a new leader and submitted the change. This variant
    /// carries the id.
    Elected(NodeId),
    /// No surviving replica could serve as a leader.
    NoEligibleReplica,
    /// Recovery was unnecessary. The leader is alive, or this node is not the
    /// controller leader, or the partition is gone.
    NotNeeded,
    /// The selection or controller admission changed before commit, so the
    /// URM aborted this attempt. A fresh job must select against current state.
    Stale,
    /// Another recovery for the same `(topic, partition)` is already running.
    InProgress,
    /// KFC-9: `break_glass.background_unclean_recovery` is `require`, and no
    /// proposal approved this recovery. The partition keeps no leader and
    /// stays visibly offline.
    BreakGlassRequired,
    /// Fail-closed audit storage could not durably record the election intent.
    AuditUnavailable,
}

/// Cloneable handle that enqueues [`RecoveryJob`] values onto the URM task.
#[derive(Clone)]
pub struct UncleanRecoveryHandle {
    pub(super) tx: mpsc::Sender<RecoveryJob>,
}

impl UncleanRecoveryHandle {
    #[cfg(test)]
    pub(crate) fn for_tests(tx: mpsc::Sender<RecoveryJob>) -> Self { Self { tx } }

    /// Enqueues a recovery job. It logs a message, and does not panic, if the
    /// manager has shut down.
    pub(crate) async fn enqueue(&self, job: RecoveryJob) {
        if self.tx.send(job).await.is_err() {
            warn!("unclean recovery manager is gone; job dropped");
        }
    }

    /// Enqueues one recovery job for each `(topic, partition, strategy)` that
    /// the controller started on its own, in order, and does not wait for an
    /// outcome.
    ///
    /// KIP-966: the dead-broker failover, an offline log dir that a heartbeat
    /// reports, and an unclean `BrokerRegistration` or re-registration hand
    /// partitions whose topic opted into an offset-aware recovery strategy to
    /// the URM this way. Each job carries no reply channel and no proposal
    /// (KFC-9). Nobody asked for the recovery, so there is no proposal to name
    /// and nobody to refuse: a heartbeat that reports an offline log dir is not
    /// a request for an unclean recovery, and the broker that sent it is not a
    /// person who can be asked for a second signature, and a dead leader at
    /// 03:00 has no caller waiting on an answer.
    /// `break_glass.background_unclean_recovery` decides whether the URM runs
    /// the job, audits it, or leaves the partition offline.
    pub(crate) async fn enqueue_background(
        &self,
        recoveries: impl IntoIterator<Item = (String, i32, RecoveryStrategy)>,
    ) {
        for (topic, partition, strategy) in recoveries {
            self.enqueue(RecoveryJob {
                topic,
                partition,
                strategy,
                reply: None,
                proposal: None,
            })
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn enqueue_sends_job_to_channel() {
        let (tx, mut rx) = mpsc::channel(1);
        let handle = UncleanRecoveryHandle::for_tests(tx);

        let job = RecoveryJob {
            topic: "test-topic".into(),
            partition: 0,
            strategy: RecoveryStrategy::Balanced,
            reply: None,
            proposal: None,
        };

        handle.enqueue(job).await;
        let received = rx.recv().await.expect("job received");
        assert2::check!(received.topic == "test-topic");
        assert2::check!(received.partition == 0);
    }

    /// What a [`RecoveryJob`] says, without its channel, which has no
    /// `PartialEq`.
    #[derive(Debug, PartialEq, Eq)]
    struct Received {
        topic: String,
        partition: i32,
        strategy: RecoveryStrategy,
        wants_reply: bool,
        proposal: Option<Uuid>,
    }

    impl From<RecoveryJob> for Received {
        fn from(job: RecoveryJob) -> Self {
            Self {
                topic: job.topic,
                partition: job.partition,
                strategy: job.strategy,
                wants_reply: job.reply.is_some(),
                proposal: job.proposal,
            }
        }
    }

    #[tokio::test]
    async fn enqueue_background_sends_unasked_jobs_in_order() {
        let (tx, mut rx) = mpsc::channel(2);
        let handle = UncleanRecoveryHandle::for_tests(tx);

        handle
            .enqueue_background([
                ("a".to_owned(), 0, RecoveryStrategy::Balanced),
                ("b".to_owned(), 3, RecoveryStrategy::Aggressive),
            ])
            .await;
        drop(handle);

        let mut received = Vec::new();
        while let Some(job) = rx.recv().await {
            received.push(Received::from(job));
        }
        assert2::assert!(
            received
                == [
                    Received {
                        topic: "a".into(),
                        partition: 0,
                        strategy: RecoveryStrategy::Balanced,
                        wants_reply: false,
                        proposal: None,
                    },
                    Received {
                        topic: "b".into(),
                        partition: 3,
                        strategy: RecoveryStrategy::Aggressive,
                        wants_reply: false,
                        proposal: None,
                    },
                ]
        );
    }
}
