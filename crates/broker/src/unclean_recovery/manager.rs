//! The controller-side Unclean Recovery Manager task.
//!
//! The manager owns the dispatch loop, the per-partition in-flight set, and
//! the decision to commit a new leader. It is the only part of the module that
//! talks to the metadata source, so the selection helpers and the replica
//! query stay free of controller state.

use std::{collections::HashSet, sync::Arc};

use futures_util::FutureExt as _;
use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use krabka_raft::NodeId;
use krabka_units::convert::TimeExt as _;
use tokio::sync::{Mutex, mpsc};
use tracing::warn;

use super::{
    RecoveryJob, RecoveryOutcome, RecoveryPolicy, ReplicaLogInfo, UncleanRecoveryHandle,
    has_newer_leader,
    query::{ReplicaQuery, gather_responses, query_replica},
    select_best_replica,
};
use crate::{
    heartbeat::controller_state::ControllerLivenessState, network::client::InterBrokerClient,
};

#[cfg(test)]
mod tests;

/// The controller-side Unclean Recovery Manager.
///
/// It receives [`RecoveryJob`] values, dedups the in-flight work for each
/// partition, queries surviving replicas for their log state, and elects the
/// replica with the most complete log through `submit_change`.
pub(crate) struct UncleanRecoveryManager {
    controller: Arc<dyn crate::metadata_source::MetadataSource>,
    liveness: Arc<ControllerLivenessState>,
    node_id: NodeId,
    inter_broker_client: Arc<InterBrokerClient>,
    listener_protocol: krabka_security::ListenerProtocol,
    metrics: crate::metrics::BrokerMetrics,
    policy: RecoveryPolicy,
    in_flight: Arc<Mutex<HashSet<(String, i32)>>>,
}

impl UncleanRecoveryManager {
    /// Spawns the URM dispatch loop and returns a cloneable handle that
    /// enqueues jobs. The loop exits when `shutdown` fires or when the last
    /// handle drops.
    pub(crate) fn spawn(
        controller: Arc<dyn crate::metadata_source::MetadataSource>,
        liveness: Arc<ControllerLivenessState>,
        node_id: NodeId,
        inter_broker_client: Arc<InterBrokerClient>,
        metrics: crate::metrics::BrokerMetrics,
        policy: RecoveryPolicy,
        shutdown: tokio_util::sync::CancellationToken,
    ) -> UncleanRecoveryHandle {
        let (tx, mut rx) = mpsc::channel::<RecoveryJob>(policy.queue_capacity);
        let mgr = Arc::new(Self {
            controller,
            liveness,
            node_id,
            inter_broker_client,
            listener_protocol: policy.listener_protocol,
            metrics,
            policy,
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        });
        tokio::spawn(async move {
            loop {
                let job = tokio::select! {
                    () = shutdown.cancelled() => return,
                    j = rx.recv() => match j { Some(j) => j, None => return },
                };
                let mgr = mgr.clone();
                tokio::spawn(async move {
                    mgr.recover_one(job).await;
                });
            }
        });
        UncleanRecoveryHandle { tx }
    }

    /// Per-job entry point. It dedups against the in-flight recoveries for
    /// the same partition, runs the recovery, then releases the in-flight slot
    /// and replies if the caller supplied a reply channel.
    async fn recover_one(self: Arc<Self>, job: RecoveryJob) {
        let key = (job.topic.clone(), job.partition);
        {
            let mut set = self.in_flight.lock().await;
            if !set.insert(key.clone()) {
                if let Some(r) = job.reply {
                    let _ = r.send(RecoveryOutcome::InProgress);
                }
                return;
            }
        }
        let outcome = self.run_recovery(&job).await;
        self.in_flight.lock().await.remove(&key);
        if let Some(r) = job.reply {
            let _ = r.send(outcome);
        }
    }

    /// Core recovery routine.
    ///
    /// It confirms that this node is the controller leader and that the
    /// partition still needs recovery, then queries the surviving replicas.
    /// If a winner emerges and no newer leader has appeared, it submits the
    /// leader change.
    async fn run_recovery(&self, job: &RecoveryJob) -> RecoveryOutcome {
        let is_leader = self
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == self.node_id);
        if !is_leader {
            return RecoveryOutcome::NotNeeded;
        }

        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        // If the current leader is alive, there's nothing to recover.
        if self.liveness.is_alive(pr.leader.0).await {
            return RecoveryOutcome::NotNeeded;
        }
        // KFC-9: the recovery is needed from here on, so this is where the
        // fail-closed rule bites. It runs before the replica poll, because a
        // recovery the broker will not commit has no reason to spend a round
        // trip to every surviving replica.
        if self.policy.background.refuses(job) {
            self.policy.background.audit_refusal(job, self.node_id);
            warn!(
                topic = %job.topic,
                partition = job.partition,
                "unclean recovery refused: break_glass.background_unclean_recovery is require \
                 and no proposal approved it; the partition stays offline"
            );
            return RecoveryOutcome::BreakGlassRequired;
        }
        let known_epoch = pr.leader_epoch;
        let topic_id = image
            .topic(&job.topic)
            .map_or(WireUuid::ZERO, |t| WireUuid(t.topic_id.into_bytes()));

        // Gather the surviving (alive) replicas to query.
        let mut alive: Vec<NodeId> = Vec::new();
        for &r in &pr.replicas {
            if self.liveness.is_alive(r.0).await {
                alive.push(r);
            }
        }
        if alive.is_empty() {
            return RecoveryOutcome::NoEligibleReplica;
        }

        let mut futs = Vec::with_capacity(alive.len());
        for r in alive {
            let Some(reg) = image.broker(r) else { continue };
            let (host, port) = (reg.host.clone(), reg.port);
            let client = self.inter_broker_client.clone();
            let proto = self.listener_protocol;
            let partition = job.partition;
            let server_name = self.policy.inter_broker_server_name.clone();
            let my_id = i32::try_from(self.node_id.0).unwrap_or(-1);
            futs.push(
                async move {
                    query_replica(
                        &client,
                        ReplicaQuery {
                            proto,
                            host,
                            port,
                            my_broker_id: my_id,
                            topic_id,
                            partition,
                            replica: r,
                            server_name,
                        },
                    )
                    .await
                }
                .boxed(),
            );
        }

        let deadline = self.policy.deadline(job.strategy);
        let collected: Vec<ReplicaLogInfo> = gather_responses(futs, deadline.to_std()).await;

        if has_newer_leader(&collected, known_epoch.0) {
            return RecoveryOutcome::Stale;
        }
        let Some(winner) = select_best_replica(&collected) else {
            return RecoveryOutcome::NoEligibleReplica;
        };

        // Re-read the image and re-check before committing: the leader may
        // have come back, or the partition may have been deleted, while we
        // were polling replicas.
        let image = self.controller.current_image();
        let Some(pr) = image.partition(&job.topic, job.partition) else {
            return RecoveryOutcome::NotNeeded;
        };
        if self.liveness.is_alive(pr.leader.0).await {
            return RecoveryOutcome::NotNeeded;
        }

        self.commit_elected_leader(job, pr, winner).await
    }

    /// Builds and submits the `PartitionRecord` that elects `winner` as the
    /// new leader. The record bumps the epoch and shrinks the ISR to the
    /// winner alone.
    async fn commit_elected_leader(
        &self,
        job: &RecoveryJob,
        pr: &PartitionRecord,
        winner: NodeId,
    ) -> RecoveryOutcome {
        let new_pr = PartitionRecord {
            topic: pr.topic.clone(),
            partition: pr.partition,
            leader: winner,
            replicas: pr.replicas.clone(),
            isr: vec![winner],
            leader_epoch: pr.leader_epoch.next(),
            adding_replicas: pr.adding_replicas.clone(),
            removing_replicas: pr.removing_replicas.clone(),
            directories: pr.directories.clone(),
            partition_epoch: pr.partition_epoch + 1,
        };
        if let Err(error) = self
            .policy
            .background
            .require_audit(job, self.node_id, winner)
            .await
        {
            warn!(%error, "unclean recovery refused by fail-closed audit policy");
            return RecoveryOutcome::AuditUnavailable;
        }
        warn!(
            topic = %job.topic,
            partition = job.partition,
            leader = winner.0,
            "unclean recovery: elected most-complete-log replica (possible data loss)"
        );
        if let Err(e) = self
            .controller
            .submit_change(vec![MetadataRecord::V1Partition(new_pr)])
            .await
        {
            warn!(error = %e, "unclean recovery submit_change failed");
            return RecoveryOutcome::NoEligibleReplica;
        }
        self.metrics.record_unclean_leader_election();
        // KFC-9: the data-losing election is durable now, so a bypass is a
        // fact rather than an intention. A job that carries a proposal took
        // the operator path, and its handler already audited the approval it
        // spent.
        self.policy
            .background
            .audit_bypass(job, self.node_id, winner, &self.metrics);
        RecoveryOutcome::Elected(winner)
    }
}
