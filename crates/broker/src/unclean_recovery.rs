//! KIP-966 offset-aware unclean recovery.
//!
//! This module holds the pure selection helpers and the controller-side
//! Unclean Recovery Manager (URM) task. The URM polls surviving replicas for
//! their log-end-offset and last-written leader epoch with `GetReplicaLogInfo`
//! (`api_key` 93), and elects the most complete log.
//!
//! # KFC-9: the path with no caller to refuse
//!
//! An unclean recovery loses committed data, and the break-glass two-person
//! rule gates every other transition that can. It cannot gate this one. Leader
//! election and the broker-heartbeat path start a recovery with no request and
//! no principal, so there is nobody to ask for a second signature and nobody to
//! send a refusal to. [`BackgroundRecovery`] holds the three-valued rule that
//! answers that, and [`RecoveryJob::proposal`] is what separates a recovery an
//! operator asked for from one the controller started on its own.

use krabka_raft::NodeId;
use krabka_units::{Time, convert::TimeExt as _};

/// One replica's reported log state, from a `GetReplicaLogInfo` response.
///
/// This type is separate from the generated wire type, so a unit test can
/// drive the selection logic without building protocol structs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReplicaLogInfo {
    pub broker_id: NodeId,
    pub last_written_leader_epoch: i32,
    pub log_end_offset: i64,
    pub current_leader_epoch: i32,
}

/// Picks the replica with the most complete log. It ranks by the highest
/// `last_written_leader_epoch`, then the highest `log_end_offset`, then the
/// lowest `broker_id` for determinism. Returns `None` for an empty input.
pub(crate) fn select_best_replica(responses: &[ReplicaLogInfo]) -> Option<NodeId> {
    responses
        .iter()
        .max_by(|a, b| {
            a.last_written_leader_epoch
                .cmp(&b.last_written_leader_epoch)
                .then(a.log_end_offset.cmp(&b.log_end_offset))
                .then(b.broker_id.cmp(&a.broker_id)) // lower broker_id wins ties
        })
        .map(|r| r.broker_id)
}

/// Returns true if any responder reports a `current_leader_epoch` strictly
/// greater than the controller's known `leader_epoch` for the partition. A
/// newer leader then already exists, and this recovery is stale.
pub(crate) fn has_newer_leader(responses: &[ReplicaLogInfo], known_leader_epoch: i32) -> bool {
    responses
        .iter()
        .any(|r| r.current_leader_epoch > known_leader_epoch)
}

// ---------------------------------------------------------------------------
// Unclean Recovery Manager (URM): the controller-side orchestrator.
// ---------------------------------------------------------------------------

use std::{collections::HashSet, sync::Arc, time::Duration};

use krabka_audit::{
    AuditEndpoint, AuditEvent, AuditLog, AuditOutcome, AuditPrincipal, PrivilegedPhase,
};
use krabka_metadata::{BreakGlassAction, MetadataRecord, PartitionRecord};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use futures_util::FutureExt as _;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::warn;
use uuid::Uuid;

use crate::{
    break_glass::{action_name, metrics as break_glass_metrics},
    config::{BackgroundUncleanRecovery, BreakGlassConfig},
    config_keys::RecoveryStrategy,
    heartbeat::controller_state::ControllerLivenessState,
    network::client::InterBrokerClient,
    operator_keys::approver_set_fingerprint,
    time_util::now_ms,
};

#[derive(Debug, Clone)]
pub(crate) struct RecoveryPolicy {
    pub aggressive_deadline: Time,
    pub balanced_deadline: Time,
    pub queue_capacity: usize,
    pub listener_protocol: krabka_security::ListenerProtocol,
    pub inter_broker_server_name: String,
    /// KFC-9: what the URM does for a job that no operator approved.
    pub background: BackgroundRecovery,
}

impl RecoveryPolicy {
    fn deadline(&self, strategy: RecoveryStrategy) -> Time {
        match strategy {
            RecoveryStrategy::Aggressive | RecoveryStrategy::None => self.aggressive_deadline,
            RecoveryStrategy::Balanced => self.balanced_deadline,
        }
    }
}

/// KFC-9: the break-glass rule for a recovery that nobody asked for.
///
/// # This path has no caller to refuse
///
/// Unclean recovery loses committed data exactly as an operator-typed unclean
/// election does, so the two-person rule looks as if it belongs on both. It
/// cannot be here. Leader election and the broker-heartbeat path start a
/// recovery with no request, no connection, and no principal, so a refusal has
/// no recipient and an approval has nobody to ask. An operator who types an
/// unclean election can be asked for a second signature, and a controller that
/// reacts to a dead broker at 03:00 cannot.
///
/// [`BackgroundUncleanRecovery`] is the three-valued answer to that, and
/// `audit-only` is the default for the reason the split states.
/// [`BackgroundUncleanRecovery::Require`] is the fail-closed option, and it
/// costs every partition whose leader dies at 03:00 its availability, and not
/// only the ones an incident touches.
///
/// A job that carries a proposal took the operator path, where the handler
/// already spent an approved proposal. None of this applies to it.
#[derive(Debug, Clone)]
pub(crate) struct BackgroundRecovery {
    /// Whether this broker runs the two-person rule at all.
    ///
    /// An empty `break_glass.approvers` turns the workflow off, and nobody can
    /// approve anything. Every recovery would then be unapproved, so `require`
    /// would make unclean recovery impossible and `audit-only` would count
    /// every failover as a bypass of a rule that does not exist. A cluster with
    /// no `[break_glass]` section behaves exactly as it does today, which is
    /// the rule the whole feature follows.
    enabled: bool,
    /// The configured `break_glass.background_unclean_recovery`.
    mode: BackgroundUncleanRecovery,
    /// Where the bypass and the refusal events go.
    audit_log: Arc<AuditLog>,
    /// A fingerprint of the sorted approver set, as every other break-glass
    /// event carries. Two brokers that disagree about `break_glass.approvers`
    /// are then visible after the fact.
    approver_set_fingerprint: String,
}

impl BackgroundRecovery {
    /// Read the rule out of `[break_glass]`.
    pub(crate) fn new(config: &BreakGlassConfig, audit_log: Arc<AuditLog>) -> Self {
        Self {
            enabled: crate::break_glass::gate::is_gated(config),
            mode: config.background_unclean_recovery,
            audit_log,
            approver_set_fingerprint: approver_set_fingerprint(&config.approvers),
        }
    }

    /// Whether this job must not run at all.
    ///
    /// Only [`BackgroundUncleanRecovery::Require`] refuses, and only a job that
    /// no operator approved on a broker that runs the two-person rule.
    fn refuses(&self, job: &RecoveryJob) -> bool {
        self.enabled && job.proposal.is_none() && self.mode == BackgroundUncleanRecovery::Require
    }

    /// Audit a recovery this rule refused. The partition stays leaderless and
    /// visibly offline, so the audit log is the only place that says why.
    fn audit_refusal(&self, job: &RecoveryJob, node_id: NodeId) {
        self.emit(
            PrivilegedPhase::Refused,
            AuditOutcome::Failure,
            job,
            node_id,
            format!(
                "break_glass.background_unclean_recovery is require, and no proposal approved \
                 this recovery; the partition stays offline (strategy {:?})",
                job.strategy
            ),
        );
    }

    /// Account one recovery that elected a leader with no approval behind it.
    ///
    /// `audit-only` is the default, so this is the ordinary path on a cluster
    /// that turned the two-person rule on. The counter is the series to alert
    /// on, and the event is the after-the-fact proof that a data-losing
    /// election happened that no second person agreed to.
    fn audit_bypass(
        &self,
        job: &RecoveryJob,
        node_id: NodeId,
        winner: NodeId,
        metrics: &crate::metrics::BrokerMetrics,
    ) {
        if !self.enabled
            || job.proposal.is_some()
            || self.mode != BackgroundUncleanRecovery::AuditOnly
        {
            return;
        }
        break_glass_metrics::record_bypass(metrics, BreakGlassAction::UncleanRecovery);
        self.emit(
            PrivilegedPhase::Bypassed,
            AuditOutcome::Success,
            job,
            node_id,
            format!(
                "unclean recovery elected broker {} with no break-glass approval (strategy {:?})",
                winner.0, job.strategy
            ),
        );
    }

    /// Emit one `PrivilegedAction` event for this path.
    ///
    /// The event names the controller that acted rather than a person, and its
    /// source endpoint is empty, because no connection carried this recovery.
    /// That absence is the whole reason the path cannot have a gate.
    fn emit(
        &self,
        phase: PrivilegedPhase,
        outcome: AuditOutcome,
        job: &RecoveryJob,
        node_id: NodeId,
        reason: String,
    ) {
        self.audit_log.emit(AuditEvent::PrivilegedAction {
            outcome,
            phase,
            action: action_name(BreakGlassAction::UncleanRecovery).to_owned(),
            target: format!("{}-{}", job.topic, job.partition),
            proposal_id: String::new(),
            principal: AuditPrincipal {
                name: format!("Controller:{}", node_id.0),
                auth_method: "Internal".to_owned(),
            },
            counterparties: Vec::new(),
            approver_set_fingerprint: self.approver_set_fingerprint.clone(),
            key_id: String::new(),
            signature: Vec::new(),
            signature_verified: false,
            source: AuditEndpoint {
                ip: String::new(),
                port: 0,
            },
            reason,
            time_ms: now_ms(),
        });
    }
}

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
    /// KFC-9: the break-glass proposal that authorized this recovery, and
    /// `None` when the controller started it on its own.
    ///
    /// The `ElectLeaders` handler spends the approval before it enqueues the
    /// job, so this id is evidence and not an authorization: it says that a
    /// person asked for this recovery, which is what takes the job out of
    /// [`BackgroundRecovery`]'s reach. The two background sites have no caller
    /// to ask, so they send `None`.
    pub proposal: Option<Uuid>,
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
    /// KFC-9: `break_glass.background_unclean_recovery` is `require`, and no
    /// proposal approved this recovery. The partition keeps no leader and
    /// stays visibly offline.
    BreakGlassRequired,
}

/// Cloneable handle that enqueues [`RecoveryJob`] values onto the URM task.
#[derive(Clone)]
pub(crate) struct UncleanRecoveryHandle {
    tx: mpsc::Sender<RecoveryJob>,
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

/// Queries one replica for its log-end-offset and leader-epoch state with
/// `GetReplicaLogInfo` (`api_key` 93). Returns `None` on any connect, send, or
/// decode error, and also if the replica reports an error for this
/// partition.
struct ReplicaQuery {
    proto: krabka_security::ListenerProtocol,
    host: String,
    port: u16,
    my_broker_id: i32,
    topic_id: WireUuid,
    partition: i32,
    replica: NodeId,
    server_name: String,
}

async fn query_replica(client: &InterBrokerClient, query: ReplicaQuery) -> Option<ReplicaLogInfo> {
    use krabka_protocol::owned::get_replica_log_info_request::{
        GetReplicaLogInfoRequest, TopicPartitions,
    };
    let opts = krabka_client_core::ConnectionOptions {
        client_id: "krabka-unclean-recovery".to_string(),
        ..krabka_client_core::ConnectionOptions::default()
    };
    let conn = client
        .connect_as_connection(
            &query.host,
            query.port,
            query.proto,
            &query.server_name,
            opts,
        )
        .await
        .ok()?;
    let req = GetReplicaLogInfoRequest {
        broker_id: query.my_broker_id,
        topic_partitions: vec![TopicPartitions {
            topic_id: query.topic_id,
            partitions: vec![query.partition],
            ..Default::default()
        }],
        ..Default::default()
    };
    let resp = conn.send(req).await.ok()?;
    for t in &resp.topic_partition_log_info_list {
        for pli in &t.partition_log_info {
            if pli.partition == query.partition && pli.error_code == 0 {
                return Some(ReplicaLogInfo {
                    broker_id: query.replica,
                    last_written_leader_epoch: pli.last_written_leader_epoch,
                    log_end_offset: pli.log_end_offset,
                    current_leader_epoch: pli.current_leader_epoch,
                });
            }
        }
    }
    None
}

/// Drives the per-replica query futures concurrently.
///
/// It returns when all futures resolve OR when `deadline` passes, whichever
/// comes first. On a timeout it returns the responses that arrived so far, and
/// never silently discards partial data.
async fn gather_responses<F>(futs: Vec<F>, deadline: Duration) -> Vec<ReplicaLogInfo>
where
    F: std::future::Future<Output = Option<ReplicaLogInfo>> + Send + 'static,
{
    use futures_util::stream::{FuturesUnordered, StreamExt};
    let total = futs.len();
    let mut stream: FuturesUnordered<_> = futs.into_iter().collect();
    let mut out: Vec<ReplicaLogInfo> = Vec::with_capacity(total);
    let sleep = tokio::time::sleep(deadline);
    tokio::pin!(sleep);
    loop {
        if out.len() == total {
            break;
        }
        tokio::select! {
            () = &mut sleep => break,
            item = stream.next() => match item {
                Some(Some(info)) => out.push(info),
                Some(None) => {}
                None => break,
            },
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::millis;

    use super::*;

    fn ri(broker_id: u64, epoch: i32, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(broker_id),
            last_written_leader_epoch: epoch,
            log_end_offset: leo,
            current_leader_epoch: epoch,
        }
    }

    #[test]
    fn picks_highest_epoch_then_offset() {
        // Broker 3 has a higher epoch even though broker 2 has a longer log.
        let r = [ri(2, 4, 100), ri(3, 5, 10)];
        assert!(select_best_replica(&r) == Some(NodeId(3)));
    }

    #[test]
    fn ties_on_epoch_break_by_offset() {
        let r = [ri(2, 5, 90), ri(3, 5, 120)];
        assert!(select_best_replica(&r) == Some(NodeId(3)));
    }

    #[test]
    fn ties_on_epoch_and_offset_break_by_lowest_broker_id() {
        let r = [ri(3, 5, 100), ri(1, 5, 100), ri(2, 5, 100)];
        assert!(select_best_replica(&r) == Some(NodeId(1)));
    }

    #[test]
    fn empty_input_returns_none() {
        assert!(select_best_replica(&[]) == None);
    }

    #[test]
    fn recovery_policy_selects_configured_deadlines() {
        let policy = RecoveryPolicy {
            aggressive_deadline: millis(7),
            balanced_deadline: millis(19),
            queue_capacity: 3,
            listener_protocol: krabka_security::ListenerProtocol::Ssl,
            inter_broker_server_name: "broker.internal".to_string(),
            background: BackgroundRecovery::new(&BreakGlassConfig::default(), AuditLog::disabled()),
        };

        assert!(policy.deadline(RecoveryStrategy::Aggressive) == millis(7));
        assert!(policy.deadline(RecoveryStrategy::Balanced) == millis(19));
        assert!(policy.queue_capacity == 3);
        assert!(policy.listener_protocol == krabka_security::ListenerProtocol::Ssl);
        assert!(policy.inter_broker_server_name == "broker.internal");
    }

    #[test]
    fn newer_leader_detected() {
        let r = [ReplicaLogInfo {
            broker_id: NodeId(2),
            last_written_leader_epoch: 5,
            log_end_offset: 10,
            current_leader_epoch: 7,
        }];
        assert!(has_newer_leader(&r, 6));
        assert!(!has_newer_leader(&r, 7));
    }
}

#[cfg(test)]
mod urm_tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;

    fn info(id: u64, leo: i64) -> ReplicaLogInfo {
        ReplicaLogInfo {
            broker_id: NodeId(id),
            last_written_leader_epoch: 1,
            log_end_offset: leo,
            current_leader_epoch: 1,
        }
    }

    #[tokio::test]
    async fn balanced_waits_for_all_then_picks_best() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_secs(5)).await;
        assert!(got.len() == 2);
        assert!(select_best_replica(&got) == Some(NodeId(2)));
    }

    #[tokio::test]
    async fn balanced_returns_partial_on_timeout() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_millis(50)).await;
        assert!(got.len() == 1, "must return what arrived before the cap");
        assert!(got[0].broker_id == krabka_audit::NodeId(1));
    }

    #[tokio::test]
    async fn aggressive_takes_early_responders() {
        let f1 = async { Some(info(1, 50)) };
        let f2 = async {
            tokio::time::sleep(Duration::from_secs(10)).await;
            Some(info(2, 90))
        };
        let got = gather_responses(vec![f1.boxed(), f2.boxed()], Duration::from_millis(50)).await;
        assert!(got == vec![info(1, 50)]);
    }
}

#[cfg(test)]
mod run_recovery_tests {
    use std::{collections::BTreeSet, net::SocketAddr};

    use assert2::{assert, check};
    use krabka_metadata::{
        BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use krabka_raft::{
        AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
        UpdateVoter,
    };
    use krabka_units::secs;
    use tokio::sync::watch;
    use uuid::Uuid;

    use super::*;
    use crate::{
        heartbeat::controller_state::ControllerLivenessState, metadata_source::MetadataSource,
    };

    /// Minimal `MetadataSource` that drives the control flow of
    /// `run_recovery`. These paths exercise only `watch_leader`,
    /// `current_image`, and `submit_change`, and never reach the rest.
    struct MockSource {
        leader_rx: watch::Receiver<Option<NodeId>>,
        _leader_tx: watch::Sender<Option<NodeId>>,
        image: Arc<MetadataImage>,
        /// Every batch that reached `submit_change`, in order. A test that
        /// asks what the URM appended reads this rather than a success flag.
        submitted: Arc<std::sync::Mutex<Vec<Vec<MetadataRecord>>>>,
    }

    impl MockSource {
        fn new(leader: Option<u64>, image: MetadataImage) -> Self {
            let (tx, rx) = watch::channel(leader.map(NodeId));
            Self {
                leader_rx: rx,
                _leader_tx: tx,
                image: Arc::new(image),
                submitted: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
    }

    #[async_trait::async_trait]
    impl MetadataSource for MockSource {
        fn current_image(&self) -> Arc<MetadataImage> {
            self.image.clone()
        }
        fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
            unimplemented!()
        }
        fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
            self.leader_rx.clone()
        }
        fn quorum_state(&self) -> QuorumState {
            unimplemented!()
        }
        async fn submit_change(
            &self,
            records: Vec<MetadataRecord>,
        ) -> Result<krabka_raft::SubmitChangeResult, RaftError> {
            self.submitted
                .lock()
                .expect("the submitted batches are not poisoned")
                .push(records);
            Ok(krabka_raft::SubmitChangeResult::default())
        }
        async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
            unimplemented!()
        }
        fn controller_bound_addr(&self) -> SocketAddr {
            unimplemented!()
        }
        fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
            unimplemented!()
        }
        async fn trigger_snapshot(&self) -> Result<(), RaftError> {
            unimplemented!()
        }
        async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
            unimplemented!()
        }
        async fn cancel(&self) {
            unimplemented!()
        }
    }

    const NODE: u64 = 10;

    fn image_with_partition(leader: u64, replicas: &[u64]) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "t".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).unwrap(),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "t".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: replicas.iter().copied().map(NodeId).collect(),
            leader_epoch: krabka_metadata::LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        img
    }

    fn register_broker(img: &mut MetadataImage, node_id: u64, host: &str, port: u16) {
        img.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::nil(),
                host: host.into(),
                port,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));
    }

    async fn liveness_with_alive(alive: &[u64]) -> Arc<ControllerLivenessState> {
        let l = ControllerLivenessState::new(krabka_units::secs(10));
        for &n in alive {
            l.record_heartbeat(n).await;
        }
        Arc::new(l)
    }

    fn manager(
        source: MockSource,
        liveness: Arc<ControllerLivenessState>,
    ) -> UncleanRecoveryManager {
        manager_with(
            source,
            liveness,
            gated(BackgroundUncleanRecovery::Off),
            AuditLog::disabled(),
        )
    }

    /// A broker that runs the two-person rule, with this background rule.
    fn gated(mode: BackgroundUncleanRecovery) -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            background_unclean_recovery: mode,
            ..BreakGlassConfig::default()
        }
    }

    /// A manager whose break-glass configuration and audit log the caller
    /// picks.
    fn manager_with(
        source: MockSource,
        liveness: Arc<ControllerLivenessState>,
        break_glass: BreakGlassConfig,
        audit_log: Arc<AuditLog>,
    ) -> UncleanRecoveryManager {
        UncleanRecoveryManager {
            controller: Arc::new(source),
            liveness,
            node_id: NodeId(NODE),
            inter_broker_client: Arc::new(InterBrokerClient::new(None, None)),
            listener_protocol: krabka_security::ListenerProtocol::Plaintext,
            metrics: crate::metrics::BrokerMetrics::new(),
            policy: RecoveryPolicy {
                aggressive_deadline: secs(2),
                balanced_deadline: secs(30),
                queue_capacity: 256,
                listener_protocol: krabka_security::ListenerProtocol::Plaintext,
                inter_broker_server_name: "localhost".to_string(),
                background: BackgroundRecovery::new(&break_glass, audit_log),
            },
            in_flight: Arc::new(Mutex::new(HashSet::new())),
        }
    }

    fn job() -> RecoveryJob {
        RecoveryJob {
            topic: "t".into(),
            partition: 0,
            strategy: RecoveryStrategy::None,
            reply: None,
            proposal: None,
        }
    }

    /// The job an operator's `ElectLeaders` request produces, which carries the
    /// proposal that the handler already spent.
    fn approved_job() -> RecoveryJob {
        RecoveryJob {
            proposal: Some(Uuid::from_u128(0x000B_ADC0_FFEE)),
            ..job()
        }
    }

    #[tokio::test]
    async fn not_controller_leader_is_not_needed() {
        let mgr = manager(
            MockSource::new(Some(99), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
    }

    #[tokio::test]
    async fn missing_partition_is_not_needed() {
        let mgr = manager(
            MockSource::new(Some(NODE), MetadataImage::new(Uuid::nil())),
            liveness_with_alive(&[]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
    }

    #[tokio::test]
    async fn live_leader_is_not_needed() {
        let mgr = manager(
            MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[1]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NotNeeded);
    }

    #[tokio::test]
    async fn dead_leader_no_alive_replicas_is_no_eligible() {
        // Leader 1 is dead and no replica is alive: nothing to query.
        let mgr = manager(
            MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NoEligibleReplica);
    }

    #[tokio::test]
    async fn dead_leader_all_queries_fail_is_no_eligible() {
        // Replica 2 is alive but its endpoint refuses connections, so the
        // query returns no log info and no winner can be selected.
        let mut img = image_with_partition(1, &[1, 2]);
        register_broker(&mut img, 2, "127.0.0.1", 1);
        let mgr = manager(
            MockSource::new(Some(NODE), img),
            liveness_with_alive(&[2]).await,
        );
        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::NoEligibleReplica);
    }

    /// A partition whose leader is dead and whose one surviving replica is
    /// registered at a port that refuses connections. The URM then reaches the
    /// replica poll, finds no log info, and answers `NoEligibleReplica`. That
    /// answer is the proof that the recovery ran, because a refusal never
    /// reaches the poll.
    fn dead_leader_with_a_survivor() -> MetadataImage {
        let mut img = image_with_partition(1, &[1, 2]);
        register_broker(&mut img, 2, "127.0.0.1", 1);
        img
    }

    async fn outcome_under(break_glass: BreakGlassConfig, job: &RecoveryJob) -> RecoveryOutcome {
        let mgr = manager_with(
            MockSource::new(Some(NODE), dead_leader_with_a_survivor()),
            liveness_with_alive(&[2]).await,
            break_glass,
            AuditLog::disabled(),
        );
        mgr.run_recovery(job).await
    }

    #[tokio::test]
    async fn a_broker_with_no_approver_set_runs_every_recovery() {
        // Nobody can approve on such a broker, so `require` would make unclean
        // recovery impossible rather than fail closed on the unapproved ones.
        let ungated = BreakGlassConfig {
            background_unclean_recovery: BackgroundUncleanRecovery::Require,
            ..BreakGlassConfig::default()
        };

        assert!(outcome_under(ungated, &job()).await == RecoveryOutcome::NoEligibleReplica);
    }

    #[tokio::test]
    async fn the_background_rule_decides_whether_an_unapproved_recovery_runs() {
        let cases = [
            (
                "off runs it",
                BackgroundUncleanRecovery::Off,
                RecoveryOutcome::NoEligibleReplica,
            ),
            (
                "audit-only runs it",
                BackgroundUncleanRecovery::AuditOnly,
                RecoveryOutcome::NoEligibleReplica,
            ),
            (
                "require refuses it",
                BackgroundUncleanRecovery::Require,
                RecoveryOutcome::BreakGlassRequired,
            ),
        ];
        for (label, mode, expected) in cases {
            assert!(
                outcome_under(gated(mode), &job()).await == expected,
                "case {label}"
            );
        }
    }

    #[tokio::test]
    async fn require_still_runs_a_recovery_that_a_proposal_approved() {
        // The `ElectLeaders` handler spent an approval before it enqueued this
        // job, so the rule for the path with no caller does not reach it.
        assert!(
            outcome_under(gated(BackgroundUncleanRecovery::Require), &approved_job()).await
                == RecoveryOutcome::NoEligibleReplica
        );
    }

    #[tokio::test]
    async fn a_refused_recovery_appends_nothing_and_audits_why() {
        let (audit_log, mut events) = AuditLog::new(8);
        let source = MockSource::new(Some(NODE), dead_leader_with_a_survivor());
        let submitted = Arc::clone(&source.submitted);
        let mgr = manager_with(
            source,
            liveness_with_alive(&[2]).await,
            gated(BackgroundUncleanRecovery::Require),
            audit_log,
        );

        assert!(mgr.run_recovery(&job()).await == RecoveryOutcome::BreakGlassRequired);

        assert!(
            submitted
                .lock()
                .expect("the submitted batches are not poisoned")
                .is_empty()
        );
        let event = events.try_recv().expect("a refusal reaches the audit log");
        assert!(let AuditEvent::PrivilegedAction { phase, action, target, outcome, .. } = &event);
        check!(*phase == PrivilegedPhase::Refused);
        check!(*outcome == AuditOutcome::Failure);
        check!(action == "unclean_recovery");
        check!(target == "t-0");
    }

    #[tokio::test]
    async fn audit_only_elects_and_records_the_bypass() {
        let (audit_log, mut events) = AuditLog::new(8);
        let source = MockSource::new(Some(NODE), image_with_partition(1, &[1, 2]));
        let image = source.current_image();
        let submitted = Arc::clone(&source.submitted);
        let mgr = manager_with(
            source,
            liveness_with_alive(&[2]).await,
            gated(BackgroundUncleanRecovery::AuditOnly),
            audit_log,
        );
        let pr = image
            .partition("t", 0)
            .expect("the partition is in the image");

        let outcome = mgr.commit_elected_leader(&job(), pr, NodeId(2)).await;

        assert!(outcome == RecoveryOutcome::Elected(NodeId(2)));
        let batches = submitted
            .lock()
            .expect("the submitted batches are not poisoned")
            .clone();
        let elected = PartitionRecord {
            leader: NodeId(2),
            isr: vec![NodeId(2)],
            leader_epoch: pr.leader_epoch.next(),
            partition_epoch: pr.partition_epoch + 1,
            ..pr.clone()
        };
        assert!(batches == vec![vec![MetadataRecord::V1Partition(elected)]]);
        check!(bypasses(&mgr.metrics) == 1);
        let event = events.try_recv().expect("a bypass reaches the audit log");
        assert!(let AuditEvent::PrivilegedAction { phase, target, .. } = &event);
        check!(*phase == PrivilegedPhase::Bypassed);
        check!(target == "t-0");
    }

    #[tokio::test]
    async fn a_recovery_that_nobody_bypassed_writes_no_bypass_event() {
        let cases = [
            ("off writes nothing", BackgroundUncleanRecovery::Off, job()),
            (
                "an approved job is not a bypass",
                BackgroundUncleanRecovery::AuditOnly,
                approved_job(),
            ),
        ];
        for (label, mode, job) in cases {
            let (audit_log, mut events) = AuditLog::new(8);
            let source = MockSource::new(Some(NODE), image_with_partition(1, &[1, 2]));
            let image = source.current_image();
            let mgr = manager_with(
                source,
                liveness_with_alive(&[2]).await,
                gated(mode),
                audit_log,
            );
            let pr = image
                .partition("t", 0)
                .expect("the partition is in the image");

            let outcome = mgr.commit_elected_leader(&job, pr, NodeId(2)).await;

            check!(
                outcome == RecoveryOutcome::Elected(NodeId(2)),
                "case {label}"
            );
            check!(bypasses(&mgr.metrics) == 0, "case {label}");
            check!(events.try_recv().is_err(), "case {label}");
        }
    }

    fn bypasses(metrics: &crate::metrics::BrokerMetrics) -> u64 {
        metrics
            .break_glass_bypassed
            .get_or_create(&crate::metrics::BreakGlassActionLabel {
                action: crate::metrics::BreakGlassAction(BreakGlassAction::UncleanRecovery),
            })
            .get()
    }

    #[tokio::test]
    async fn recover_one_dedups_in_flight_job() {
        let mgr = Arc::new(manager(
            MockSource::new(Some(NODE), image_with_partition(1, &[1, 2])),
            liveness_with_alive(&[]).await,
        ));
        // Pre-mark this partition as already recovering.
        mgr.in_flight.lock().await.insert(("t".to_string(), 0));
        let (tx, rx) = oneshot::channel();
        let j = RecoveryJob {
            reply: Some(tx),
            ..job()
        };
        mgr.clone().recover_one(j).await;
        assert!(rx.await.unwrap() == RecoveryOutcome::InProgress);
    }
}
