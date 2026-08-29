//! Behaviour tests for the manager's control flow: when recovery is not
//! needed, when no replica is eligible, and how a duplicate job for the same
//! partition is refused.

use std::{collections::BTreeSet, net::SocketAddr};

use assert2::{assert, check};
use krabka_audit::{AuditEvent, AuditLog, AuditOutcome, PrivilegedPhase};
use krabka_metadata::{
    BreakGlassAction, BrokerRegistrationRecord, MetadataImage, MetadataRecord, PartitionRecord,
    TopicRecord,
};
use krabka_raft::{
    AddVoter, Node, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    UpdateVoter,
};
use krabka_units::secs;
use tokio::sync::{oneshot, watch};
use uuid::Uuid;

use super::*;
use crate::{
    config::{BackgroundUncleanRecovery, BreakGlassConfig},
    config_keys::RecoveryStrategy,
    heartbeat::controller_state::ControllerLivenessState,
    metadata_source::MetadataSource,
    unclean_recovery::BackgroundRecovery,
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

fn manager(source: MockSource, liveness: Arc<ControllerLivenessState>) -> UncleanRecoveryManager {
    manager_with(
        source,
        liveness,
        &gated(BackgroundUncleanRecovery::Off),
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

/// A manager whose break-glass configuration and audit log the caller picks.
fn manager_with(
    source: MockSource,
    liveness: Arc<ControllerLivenessState>,
    break_glass: &BreakGlassConfig,
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
            background: BackgroundRecovery::new(break_glass, audit_log),
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
        &break_glass,
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
        &gated(BackgroundUncleanRecovery::Require),
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
        &gated(BackgroundUncleanRecovery::AuditOnly),
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
            &gated(mode),
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
