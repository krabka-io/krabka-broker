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
    config_keys::{ELIGIBLE_LEADER_REPLICAS, RecoveryStrategy},
    heartbeat::controller_state::ControllerLivenessState,
    metadata_source::MetadataSource,
    unclean_recovery::{BackgroundRecovery, selection::ElectionBasis},
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

/// Publish `krabka.elr` for partition 0 of topic `t`, in the grammar
/// `TopicElr::parse` reads: these node ids are eligible, none are last-known.
fn publish_elr(img: &mut MetadataImage, eligible: &[u64]) {
    let ids = eligible
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(",");
    img.apply(&MetadataRecord::V1TopicConfig(
        krabka_metadata::TopicConfigRecord {
            topic: "t".into(),
            overrides: [(ELIGIBLE_LEADER_REPLICAS.to_string(), format!("0:{ids}:"))]
                .into_iter()
                .collect(),
        },
    ));
}

/// The election the URM reaches when nothing but the log lengths decided it.
fn fallback_to(leader: u64) -> Election {
    Election {
        leader: NodeId(leader),
        basis: ElectionBasis::MostCompleteLog,
    }
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

/// The registration record for `node_id` under `incarnation`. A broker that
/// restarts registers under a new one, which is the only thing the controller
/// sees of a wiped disk or a truncated log.
fn broker_record(node_id: u64, incarnation: Uuid) -> BrokerRegistrationRecord {
    BrokerRegistrationRecord {
        node_id: NodeId(node_id),
        broker_epoch: 0,
        incarnation_id: incarnation,
        host: "127.0.0.1".into(),
        port: 1,
        rack: None,
        log_dirs: vec![],
        endpoints: vec![],
        features: std::collections::BTreeMap::new(),
    }
}

/// One replica's answer to the poll, with the fields the ranking reads.
fn info(broker_id: u64, log_end_offset: i64) -> ReplicaLogInfo {
    ReplicaLogInfo {
        broker_id: NodeId(broker_id),
        last_written_leader_epoch: 5,
        log_end_offset,
        current_leader_epoch: 5,
    }
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

    let selected_replicas: Vec<u64> = pr.replicas.iter().map(|node| node.0).collect();
    let outcome = mgr
        .commit_elected_leader(
            &job(),
            &image,
            pr,
            fallback_to(2),
            pr.partition_epoch,
            &selected_replicas,
            false,
        )
        .await;

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
    assert!(let AuditEvent::PrivilegedAction { phase, target, reason, .. } = &event);
    check!(*phase == PrivilegedPhase::Bypassed);
    check!(target == "t-0");
    check!(
        reason
            == "unclean recovery elected broker 2 as the most complete surviving log, so \
                committed records may be lost (strategy None), with no break-glass approval"
    );
    check!(mgr.metrics.unclean_leader_elections_total.get() == 1);
}

/// KIP-966: an eligible leader replica holds every committed record, so the
/// election that picks one is not the data-losing act the two-person rule
/// watches for and is not the election the unclean meter counts. The audit
/// record still says what happened, and names the rule that chose the leader.
#[tokio::test]
async fn an_elr_election_is_recorded_as_applied_and_meters_no_loss() {
    let (audit_log, mut events) = AuditLog::new(8);
    let source = MockSource::new(Some(NODE), image_with_partition(1, &[1, 2]));
    let image = source.current_image();
    let mgr = manager_with(
        source,
        liveness_with_alive(&[2]).await,
        &gated(BackgroundUncleanRecovery::AuditOnly),
        audit_log,
    );
    let pr = image
        .partition("t", 0)
        .expect("the partition is in the image");
    let election = Election {
        leader: NodeId(2),
        basis: ElectionBasis::EligibleLeaderReplica,
    };
    let selected_replicas: Vec<u64> = pr.replicas.iter().map(|node| node.0).collect();

    let outcome = mgr
        .commit_elected_leader(
            &job(),
            &image,
            pr,
            election,
            pr.partition_epoch,
            &selected_replicas,
            false,
        )
        .await;

    assert!(outcome == RecoveryOutcome::Elected(NodeId(2)));
    let event = events
        .try_recv()
        .expect("an election reaches the audit log");
    assert!(let AuditEvent::PrivilegedAction { phase, target, reason, .. } = &event);
    check!(*phase == PrivilegedPhase::Applied);
    check!(target == "t-0");
    check!(
        reason
            == "unclean recovery elected broker 2 from the eligible leader replicas, so no \
                committed record is lost (strategy None)"
    );
    check!(bypasses(&mgr.metrics) == 0);
    check!(mgr.metrics.unclean_leader_elections_total.get() == 0);
}

#[tokio::test]
async fn commit_rejects_exhausted_metadata_epochs() {
    for (partition_epoch, leader_epoch) in [(i32::MAX, 5), (0, i32::MAX)] {
        let mut image = image_with_partition(1, &[1, 2]);
        let mut record = image.partition("t", 0).expect("seeded partition").clone();
        record.partition_epoch = partition_epoch;
        record.leader_epoch = krabka_metadata::LeaderEpoch(leader_epoch);
        image.apply(&MetadataRecord::V1Partition(record));
        let source = MockSource::new(Some(NODE), image);
        let current = source.current_image();
        let submitted = Arc::clone(&source.submitted);
        let manager = manager_with(
            source,
            liveness_with_alive(&[2]).await,
            &gated(BackgroundUncleanRecovery::AuditOnly),
            AuditLog::disabled(),
        );
        let partition = current.partition("t", 0).expect("seeded partition");
        let replicas: Vec<u64> = partition.replicas.iter().map(|node| node.0).collect();

        let outcome = manager
            .commit_elected_leader(
                &job(),
                &current,
                partition,
                fallback_to(2),
                partition.partition_epoch,
                &replicas,
                false,
            )
            .await;

        assert!(outcome == RecoveryOutcome::Stale);
        assert!(
            submitted
                .lock()
                .expect("submitted batches are not poisoned")
                .is_empty()
        );
    }
}

#[tokio::test]
async fn commit_fences_every_change_since_replica_selection() {
    for (label, selected_epoch, selected_replicas, winner, leader_alive, expected) in [
        (
            "partition epoch changed",
            -1,
            vec![1, 2],
            2,
            false,
            RecoveryOutcome::Stale,
        ),
        (
            "assignment changed",
            0,
            vec![2, 1],
            2,
            false,
            RecoveryOutcome::Stale,
        ),
        (
            "winner removed",
            0,
            vec![1, 2, 3],
            3,
            false,
            RecoveryOutcome::Stale,
        ),
        (
            "leader recovered",
            0,
            vec![1, 2],
            2,
            true,
            RecoveryOutcome::NotNeeded,
        ),
        (
            "selection unchanged",
            0,
            vec![1, 2],
            2,
            false,
            RecoveryOutcome::Elected(NodeId(2)),
        ),
    ] {
        let source = MockSource::new(Some(NODE), image_with_partition(1, &[1, 2]));
        let image = source.current_image();
        let submitted = Arc::clone(&source.submitted);
        let mgr = manager(source, liveness_with_alive(&[]).await);
        let pr = image
            .partition("t", 0)
            .expect("the partition is in the image");

        let outcome = mgr
            .commit_elected_leader(
                &job(),
                &image,
                pr,
                fallback_to(winner),
                selected_epoch,
                &selected_replicas,
                leader_alive,
            )
            .await;

        assert!(outcome == expected, "case {label}");
        assert!(
            submitted
                .lock()
                .expect("the submitted batches are not poisoned")
                .len()
                == usize::from(matches!(expected, RecoveryOutcome::Elected(_))),
            "case {label}"
        );
    }
}

/// KFC-9 gives the three settings three distinct meanings, and `off` means the
/// behaviour the broker had before the rule existed: no audit event and no
/// counter. A broker that does run the rule records the election it committed
/// with the applied phase whenever nobody bypassed anything.
#[tokio::test]
async fn a_recovery_that_nobody_bypassed_is_applied_rather_than_bypassed() {
    let cases = [
        (
            "off writes nothing at all",
            BackgroundUncleanRecovery::Off,
            job(),
            None,
        ),
        (
            "an approved job is not a bypass",
            BackgroundUncleanRecovery::AuditOnly,
            approved_job(),
            Some(PrivilegedPhase::Applied),
        ),
        (
            "an approved job under require is not a bypass either",
            BackgroundUncleanRecovery::Require,
            approved_job(),
            Some(PrivilegedPhase::Applied),
        ),
    ];
    for (label, mode, job, expected_phase) in cases {
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

        let selected_replicas: Vec<u64> = pr.replicas.iter().map(|node| node.0).collect();
        let outcome = mgr
            .commit_elected_leader(
                &job,
                &image,
                pr,
                fallback_to(2),
                pr.partition_epoch,
                &selected_replicas,
                false,
            )
            .await;

        check!(
            outcome == RecoveryOutcome::Elected(NodeId(2)),
            "case {label}"
        );
        check!(bypasses(&mgr.metrics) == 0, "case {label}");
        if let Some(expected) = expected_phase {
            let event = events
                .try_recv()
                .expect("a broker that runs the rule records the election");
            assert!(let AuditEvent::PrivilegedAction { phase, .. } = &event, "case {label}");
            check!(*phase == expected, "case {label}");
        }
        check!(events.try_recv().is_err(), "case {label}");
    }
}

/// KFC-9 meets KIP-966 at the point the poll settles: `require` refuses the
/// election that can lose a committed record and lets through the one that
/// cannot, whatever the partition's published ELR read before the poll ran.
#[test]
fn require_refuses_the_fallback_election_and_not_the_elr_one() {
    let elr = Election {
        leader: NodeId(2),
        basis: ElectionBasis::EligibleLeaderReplica,
    };
    let cases = [
        (
            "require refuses the fallback nobody approved",
            BackgroundUncleanRecovery::Require,
            job(),
            fallback_to(2),
            true,
        ),
        (
            "require lets an ELR election through",
            BackgroundUncleanRecovery::Require,
            job(),
            elr,
            false,
        ),
        (
            "an approved fallback is not refused",
            BackgroundUncleanRecovery::Require,
            approved_job(),
            fallback_to(2),
            false,
        ),
        (
            "audit-only refuses nothing",
            BackgroundUncleanRecovery::AuditOnly,
            job(),
            fallback_to(2),
            false,
        ),
        (
            "off refuses nothing",
            BackgroundUncleanRecovery::Off,
            job(),
            fallback_to(2),
            false,
        ),
    ];
    for (label, mode, job, election, expected) in cases {
        let rule = BackgroundRecovery::new(&gated(mode), AuditLog::disabled());
        check!(rule.refuses_election(&job, election) == expected, "{label}");
    }
}

/// KFC-9 meets KIP-966: the fail-closed rule refuses a recovery that can only
/// lose data, and a partition with a surviving eligible leader replica is not
/// one of those. The ELR case reaches the replica poll -- which the refused
/// case never does -- and answers `NoEligibleReplica` when nothing replies.
#[tokio::test]
async fn require_refuses_only_a_recovery_that_no_elr_could_save() {
    let cases = [
        (
            "no ELR is refused before the poll",
            &[][..],
            RecoveryOutcome::BreakGlassRequired,
        ),
        (
            "a surviving ELR member is polled",
            &[2][..],
            RecoveryOutcome::NoEligibleReplica,
        ),
    ];
    for (label, eligible, expected) in cases {
        let mut image = dead_leader_with_a_survivor();
        if !eligible.is_empty() {
            publish_elr(&mut image, eligible);
        }
        let mgr = manager_with(
            MockSource::new(Some(NODE), image),
            liveness_with_alive(&[2]).await,
            &gated(BackgroundUncleanRecovery::Require),
            AuditLog::disabled(),
        );

        check!(mgr.run_recovery(&job()).await == expected, "case {label}");
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

/// KIP-966 meets the witness role: `broker.witness` says the node serves no
/// client, so a partition it leads is as unusable as the offline one, and ELR
/// membership does not change that. The two cases are the same partition and
/// the same poll, and differ only in whether broker 3 -- the partition's one
/// eligible leader replica, and the shortest log of the two that answered --
/// carries the role.
#[tokio::test]
async fn a_witness_never_wins_the_election_however_the_elr_reads() {
    let cases = [
        (
            "a data ELR member wins on its membership",
            None,
            Election {
                leader: NodeId(3),
                basis: ElectionBasis::EligibleLeaderReplica,
            },
        ),
        (
            "an ELR witness leaves the fallback to decide",
            Some(3),
            fallback_to(2),
        ),
    ];
    for (label, witness, expected) in cases {
        let mut img = image_with_partition(1, &[1, 2, 3]);
        publish_elr(&mut img, &[3]);
        if let Some(id) = witness {
            crate::leader_election::test_support::mark_witnesses_in_image(&mut img, &[id]);
        }
        let mgr = manager(
            MockSource::new(Some(NODE), img),
            liveness_with_alive(&[2, 3]).await,
        );
        let image = mgr.controller.current_image();

        let election =
            UncleanRecoveryManager::elect_from(&image, &[3], &[info(2, 400), info(3, 20)]);

        check!(election == Some(expected), "case {label}");
    }
}

/// KIP-966 meets a broker that came back as a new process: membership said
/// that the log broker 3 held when it left the ISR held every committed
/// record, and the registration that replaces broker 3's withdraws the
/// statement, because the log the returning process holds is not that one.
///
/// Both halves are the same partition and the same poll. Broker 3 wins on its
/// membership while the membership stands, and broker 2's longer log wins once
/// it does not -- as the most complete surviving log, which is the answer that
/// meters, audits and, under `require`, refuses.
#[tokio::test]
async fn a_returning_incarnation_loses_its_eligible_leader_priority() {
    let mut img = image_with_partition(1, &[1, 2, 3]);
    img.apply(&MetadataRecord::V1BrokerRegistration(broker_record(
        3,
        Uuid::from_u128(1),
    )));
    publish_elr(&mut img, &[3]);
    let poll = [info(2, 400), info(3, 20)];

    check!(
        elected_from(img.clone(), &poll).await
            == Some(Election {
                leader: NodeId(3),
                basis: ElectionBasis::EligibleLeaderReplica,
            })
    );

    let liveness = liveness_with_alive(&[2, 3]).await;
    let mut batch = crate::leader_election::compute_unclean_restart_changes(
        &img,
        NodeId(3),
        &liveness,
        &crate::metrics::BrokerMetrics::new(),
    )
    .await
    .changes;
    batch.push(MetadataRecord::V1BrokerRegistration(broker_record(
        3,
        Uuid::from_u128(2),
    )));
    for record in batch {
        img.apply(&record);
    }

    check!(elected_from(img, &poll).await == Some(fallback_to(2)));
}

/// The election one recovery of `t-0` reaches against `image`, from the
/// responses `poll` carries: the partition's published ELR and the cluster's
/// witness set both come out of the image, which is what makes the two halves
/// of the test above differ.
async fn elected_from(image: MetadataImage, poll: &[ReplicaLogInfo]) -> Option<Election> {
    let eligible = crate::elr::TopicElr::of_topic(&image, "t")
        .partition(0)
        .eligible_leader_replicas;
    let mgr = manager(
        MockSource::new(Some(NODE), image),
        liveness_with_alive(&[2, 3]).await,
    );
    let image = mgr.controller.current_image();
    UncleanRecoveryManager::elect_from(&image, &eligible, poll)
}

/// KFC-9: the applied event is what an auditor joins to the approval that
/// authorized an election, so it has to name the proposal. A background
/// recovery has none to name, and says so with an empty id.
#[tokio::test]
async fn an_applied_election_names_the_proposal_that_authorized_it() {
    let approved = approved_job();
    let approval = approved
        .proposal
        .expect("the operator path carries its proposal")
        .to_string();
    let cases = [
        (
            "an approved recovery names its approval",
            approved,
            approval,
        ),
        ("a background recovery names none", job(), String::new()),
    ];
    for (label, job, expected) in cases {
        let (audit_log, mut events) = AuditLog::new(8);
        let source = MockSource::new(Some(NODE), image_with_partition(1, &[1, 2]));
        let image = source.current_image();
        let mgr = manager_with(
            source,
            liveness_with_alive(&[2]).await,
            &gated(BackgroundUncleanRecovery::AuditOnly),
            audit_log,
        );
        let pr = image
            .partition("t", 0)
            .expect("the partition is in the image");
        let election = Election {
            leader: NodeId(2),
            basis: ElectionBasis::EligibleLeaderReplica,
        };
        let selected_replicas: Vec<u64> = pr.replicas.iter().map(|node| node.0).collect();

        let outcome = mgr
            .commit_elected_leader(
                &job,
                &image,
                pr,
                election,
                pr.partition_epoch,
                &selected_replicas,
                false,
            )
            .await;

        check!(
            outcome == RecoveryOutcome::Elected(NodeId(2)),
            "case {label}"
        );
        let event = events
            .try_recv()
            .expect("an election reaches the audit log");
        assert!(let AuditEvent::PrivilegedAction { phase, proposal_id, .. } = &event, "case {label}");
        check!(*phase == PrivilegedPhase::Applied, "case {label}");
        check!(*proposal_id == expected, "case {label}");
    }
}
