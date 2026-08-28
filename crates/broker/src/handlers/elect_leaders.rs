//! `ElectLeaders` (`api_key` 43, KIP-460).
//!
//! Operator-triggered leader election. PREFERRED type moves leadership
//! back to `replicas[0]` after operator intervention. UNCLEAN type
//! elects outside the ISR when every ISR member is dead.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny the
//! whole request returns `CLUSTER_AUTHORIZATION_FAILED (31)` on every
//! per-partition row.
//!
//! # KFC-9: an unclean election needs two people
//!
//! An unclean election elects a replica that does not hold every committed
//! record, so it is one of the transitions the break-glass two-person rule
//! gates. The request gains no field for it. KIP-460 defines the shape that
//! `kafka-leader-election.sh` sends and there is nowhere in it to name a
//! proposal, so an operator gets the approval out of band through
//! `krabka-guard` and the broker looks it up in its own metadata image.
//!
//! **Preferred election is not gated.** It elects a replica that is already in
//! the ISR, it loses nothing, and gating it would stop routine operation on
//! every cluster that turns the rule on.
//!
//! A refused partition answers `POLICY_VIOLATION` (44) on its own row, with the
//! refusal text in `error_message`. The gate is active only when
//! `[break_glass]` names an approver set, so a stock cluster elects exactly as
//! it does today.

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord, NodeId};
use krabka_protocol::{
    Encode,
    owned::{
        elect_leaders_request::ElectLeadersRequest,
        elect_leaders_response::{ElectLeadersResponse, PartitionResult, ReplicaElectionResult},
    },
};
use krabka_units::convert::TimeExt as _;
use tokio::sync::oneshot;
use uuid::Uuid;

use crate::{
    break_glass::{
        action_name,
        gate::{self, BreakGlassDenial},
        handlers::{PrivilegedAudit, audit_privileged},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes,
    config::BreakGlassConfig,
    config_keys::{RecoveryStrategy, resolve_recovery_strategy},
    handlers::{RequestContext, cluster_alter_denied},
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::{ElectError, ElectionType, select_new_leader_for_partition},
    operator_keys::approver_set_fingerprint,
    time_util::now_ms,
    unclean_recovery::{RecoveryJob, RecoveryOutcome},
};

const WIRE_ELECTION_PREFERRED: i8 = 0;
const WIRE_ELECTION_UNCLEAN: i8 = 1;

#[tracing::instrument(
    name = "handle_elect_leaders",
    level = "info",
    skip_all,
    fields(api = "ElectLeaders"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: ElectLeadersRequest,
    ctx: &RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Authorize Cluster Alter — whole-request gate.
    let image = broker.controller.current_image();
    if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_whole_request_error(
            &req,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "elect-leaders denied",
            api_version,
        );
    }

    // Decode election_type discriminant.
    let election = match req.election_type {
        WIRE_ELECTION_PREFERRED => ElectionType::Preferred,
        WIRE_ELECTION_UNCLEAN => ElectionType::Unclean,
        _ => {
            return encode_whole_request_error(
                &req,
                codes::INVALID_REQUEST,
                "unknown election_type",
                api_version,
            );
        }
    };

    // Resolve target partition set:
    //   topic_partitions = None      → every partition in the image
    //   Some([{topic, []}])          → every partition of that topic
    //   Some([{topic, [p, q, ...]}]) → exact set
    let targets = resolve_targets(&image, &req);

    // Run the algorithm per target; accumulate new records to submit
    // and per-partition results to ship back.
    let env = ElectionEnv {
        broker,
        image: &image,
        ctx,
        liveness: &broker.liveness,
        // Witness nodes never lead a partition. Build the set once for the
        // whole request, not once per target partition.
        witnesses: &crate::config_keys::witness_node_ids(&image),
        election,
    };
    let mut by_topic: HashMap<String, Vec<PartitionResult>> = HashMap::new();
    let mut batch = ElectionBatch::default();
    for (topic, partitions) in &targets {
        let mut rows = Vec::with_capacity(partitions.len());
        for &p in partitions {
            rows.push(elect_one(&env, &mut batch, topic, p).await);
        }
        by_topic.insert(topic.clone(), rows);
    }

    // Submit accumulated records. On failure, mark every queued OK row
    // with COORDINATOR_NOT_AVAILABLE.
    let mut submit_failure = None;
    if !batch.records.is_empty()
        && let Err(e) = broker
            .controller
            .submit_change(std::mem::take(&mut batch.records))
            .await
    {
        tracing::warn!(error = %e, "elect-leaders submit failed");
        submit_failure = Some(format!("submit failed: {e}"));
        for rows in by_topic.values_mut() {
            for r in rows.iter_mut() {
                if r.error_code == 0 {
                    r.error_code = codes::COORDINATOR_NOT_AVAILABLE;
                    r.error_message = submit_failure.clone();
                }
            }
        }
    }
    // KFC-9: audit the approvals this append spent, now that its outcome is
    // known. An `applied` event for a transition that never committed would be
    // a false record of a data-losing election.
    batch.audit_applied(broker, ctx, submit_failure.as_deref());

    // Build response.
    let replica_election_results: Vec<ReplicaElectionResult> = by_topic
        .into_iter()
        .map(|(topic, partition_result)| ReplicaElectionResult {
            topic,
            partition_result,
            ..Default::default()
        })
        .collect();

    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

/// Everything one partition's election reads, and nothing it writes.
///
/// The whole request resolves these once. A partition then costs one image
/// lookup and, on an unclean election, one walk of the proposal registry.
struct ElectionEnv<'a> {
    broker: &'a Broker,
    image: &'a MetadataImage,
    ctx: &'a RequestContext<'a>,
    liveness: &'a ControllerLivenessState,
    witnesses: &'a HashSet<NodeId>,
    election: ElectionType,
}

/// What one `ElectLeaders` request accumulates across its partitions.
///
/// `records` is the single raft append that carries every consumed proposal
/// beside every leader change the request makes. That one append is why a
/// proposal lives in the metadata log at all: the approval and the transition
/// it authorizes commit together, so a crash between them cannot spend one
/// approval twice.
#[derive(Default)]
struct ElectionBatch {
    /// The consumed proposals first, then the partition records.
    records: Vec<MetadataRecord>,
    /// The proposals this request already spent. One approved proposal on a
    /// bare topic name covers every partition of that topic, so a request that
    /// elects ten of them reads one proposal ten times and spends it once.
    spent: HashSet<Uuid>,
    /// The transitions waiting on the append, to audit once it commits.
    applied: Vec<(String, Option<Uuid>)>,
}

impl ElectionBatch {
    /// Take a consumed proposal into the append, and answer the proposal it
    /// names.
    ///
    /// The record goes in ahead of every partition record, and only the first
    /// time this request sees the proposal.
    fn spend(&mut self, consumed: Option<MetadataRecord>) -> Option<Uuid> {
        let consumed = consumed?;
        let proposal_id = consumed_proposal_id(&consumed)?;
        if self.spent.insert(proposal_id) {
            self.records.insert(0, consumed);
        }
        Some(proposal_id)
    }

    /// Audit every transition this append carried.
    ///
    /// `failure` is the submit error when the append did not commit, and the
    /// event then records a refusal with that text rather than an application
    /// that never happened.
    fn audit_applied(&self, broker: &Broker, ctx: &RequestContext<'_>, failure: Option<&str>) {
        for (target, proposal_id) in &self.applied {
            match failure {
                None => audit_unclean(
                    broker,
                    ctx,
                    target,
                    PrivilegedPhase::Applied,
                    *proposal_id,
                    "unclean leader election committed",
                ),
                Some(error) => {
                    audit_unclean(
                        broker,
                        ctx,
                        target,
                        PrivilegedPhase::Refused,
                        *proposal_id,
                        error,
                    );
                }
            }
        }
    }
}

/// Elect one partition, and answer the row the response carries for it.
async fn elect_one(
    env: &ElectionEnv<'_>,
    batch: &mut ElectionBatch,
    topic: &str,
    partition: i32,
) -> PartitionResult {
    // KFC-9: the break-glass gate runs before any election work, because it is
    // an authority gate and not a content gate. It never sees a preferred
    // election.
    let consumed = if matches!(env.election, ElectionType::Unclean) {
        match authorize_unclean(env.image, &env.broker.config.break_glass, topic, partition) {
            Ok(consumed) => consumed,
            Err(denial) => return refuse_unclean(env, topic, partition, &denial),
        }
    } else {
        None
    };

    // KIP-966: an UNCLEAN election on a topic that opted into an offset-aware
    // recovery strategy is routed through the Unclean Recovery Manager, which
    // polls surviving replicas for their log state before electing. The URM
    // owns `submit_change` for these, so we must NOT push a record into the
    // batch here — we just await the outcome and translate it to a
    // per-partition row.
    let strategy = resolve_recovery_strategy(env.image, topic);
    let use_offset_aware = matches!(env.election, ElectionType::Unclean)
        && !matches!(strategy, RecoveryStrategy::None);
    if use_offset_aware {
        return run_offset_aware_recovery(env, batch, topic, partition, strategy, consumed).await;
    }

    let result = select_new_leader_for_partition(
        env.image,
        env.liveness,
        env.witnesses,
        topic,
        partition,
        env.election,
    )
    .await;
    match result {
        Ok(new_pr) => {
            let proposal_id = batch.spend(consumed);
            batch.records.push(MetadataRecord::V1Partition(new_pr));
            if matches!(env.election, ElectionType::Unclean) {
                batch
                    .applied
                    .push((unclean_target(topic, partition), proposal_id));
            }
            PartitionResult {
                partition_id: partition,
                error_code: 0,
                error_message: None,
                ..Default::default()
            }
        }
        Err(err) => {
            let (code, msg) = elect_error_to_wire(err);
            PartitionResult {
                partition_id: partition,
                error_code: code,
                error_message: Some(msg.into()),
                ..Default::default()
            }
        }
    }
}

/// KFC-9: find the approved proposal that authorizes an unclean election of one
/// partition, and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. Every transition then behaves as it does on a cluster with no such
/// section, which is what keeps a stock cluster working.
fn authorize_unclean(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    topic: &str,
    partition: i32,
) -> Result<Option<MetadataRecord>, BreakGlassDenial> {
    if !gate::is_gated(config) {
        return Ok(None);
    }
    gate::authorize(
        image,
        config,
        BreakGlassAction::UncleanElectLeaders,
        &unclean_target(topic, partition),
        now_ms(),
    )
    .map(Some)
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
fn unclean_target(topic: &str, partition: i32) -> String {
    format!("{topic}-{partition}")
}

/// The proposal that a consumed record names.
///
/// [`gate::authorize`] only ever answers with a proposal record, so the `None`
/// arm costs one match rather than a panic.
fn consumed_proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}

/// Refuse one partition: count it, audit it, and build its error row.
fn refuse_unclean(
    env: &ElectionEnv<'_>,
    topic: &str,
    partition: i32,
    denial: &BreakGlassDenial,
) -> PartitionResult {
    let message = denial.to_string();
    break_glass_metrics::record_refusal(&env.broker.metrics, denial.action);
    audit_unclean(
        env.broker,
        env.ctx,
        &unclean_target(topic, partition),
        PrivilegedPhase::Refused,
        denial.proposal_id(),
        &message,
    );
    PartitionResult {
        partition_id: partition,
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(message),
        ..Default::default()
    }
}

/// Emit one `PrivilegedAction` event for an unclean election.
///
/// `counterparties` stays empty for the reason the freeze events give: the
/// approvers are named on the proposal's own approve events, and the proposal
/// id joins those rows to this one.
fn audit_unclean(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    target: &str,
    phase: PrivilegedPhase,
    proposal_id: Option<Uuid>,
    reason: &str,
) {
    audit_privileged(
        &broker.audit_log,
        ctx,
        approver_set_fingerprint(&broker.config.break_glass.approvers),
        &PrivilegedAudit {
            outcome: if matches!(phase, PrivilegedPhase::Refused) {
                AuditOutcome::Failure
            } else {
                AuditOutcome::Success
            },
            phase,
            action: action_name(BreakGlassAction::UncleanElectLeaders),
            target,
            proposal_id,
            counterparties: &[],
            key_id: "",
            signature: &[],
            signature_verified: false,
            reason,
        },
    );
}

fn resolve_targets(
    image: &krabka_metadata::MetadataImage,
    request: &ElectLeadersRequest,
) -> Vec<(String, Vec<i32>)> {
    request.topic_partitions.as_ref().map_or_else(
        || {
            image
                .topics()
                .map(|topic| {
                    let partitions = image
                        .partitions_of(&topic.name)
                        .map(|partition| partition.partition)
                        .collect();
                    (topic.name.clone(), partitions)
                })
                .collect()
        },
        |topics| {
            topics
                .iter()
                .map(|topic| {
                    let partitions = if topic.partitions.is_empty() {
                        image
                            .partitions_of(&topic.topic)
                            .map(|partition| partition.partition)
                            .collect()
                    } else {
                        topic.partitions.clone()
                    };
                    (topic.topic.clone(), partitions)
                })
                .collect()
        },
    )
}

/// Hand one partition to the Unclean Recovery Manager and wait for its outcome.
///
/// # KFC-9: the approval is spent before the recovery starts
///
/// The URM owns the `submit_change` that elects the leader, and it runs after
/// this handler answers, so there is no batch to carry the consume in. The
/// broker appends the consume on its own first instead. Consume-then-transition
/// is the safe order of the two: a crash between them loses the approval, where
/// the reverse order would leave an unconsumed proposal that a second unclean
/// election could spend again.
///
/// The job carries the proposal id, which is what takes the recovery out of the
/// background rule in [`crate::unclean_recovery::BackgroundRecovery`]. A person
/// asked for this one.
async fn run_offset_aware_recovery(
    env: &ElectionEnv<'_>,
    batch: &mut ElectionBatch,
    topic: &str,
    partition: i32,
    strategy: RecoveryStrategy,
    consumed: Option<MetadataRecord>,
) -> PartitionResult {
    let broker = env.broker;
    let proposal = match spend_before_recovery(broker, batch, consumed).await {
        Ok(proposal) => proposal,
        Err(message) => {
            return PartitionResult {
                partition_id: partition,
                error_code: codes::COORDINATOR_NOT_AVAILABLE,
                error_message: Some(message),
                ..Default::default()
            };
        }
    };
    if let Some(proposal_id) = proposal {
        audit_unclean(
            broker,
            env.ctx,
            &unclean_target(topic, partition),
            PrivilegedPhase::Consumed,
            Some(proposal_id),
            "approval spent on an offset-aware unclean recovery",
        );
    }
    let (tx, rx) = oneshot::channel();
    broker
        .unclean_recovery
        .enqueue(RecoveryJob {
            topic: topic.to_string(),
            partition,
            strategy,
            reply: Some(tx),
            proposal,
        })
        .await;
    let (error_code, error_message) =
        match tokio::time::timeout(broker.config.operator_recovery_deadline.to_std(), rx).await {
            Ok(Ok(RecoveryOutcome::Elected(_))) => (codes::NONE, None),
            Ok(Ok(RecoveryOutcome::NoEligibleReplica)) => (
                codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                Some("no eligible replica responded".into()),
            ),
            Ok(Ok(RecoveryOutcome::NotNeeded)) => (
                codes::ELECTION_NOT_NEEDED,
                Some("partition already has a leader".into()),
            ),
            Ok(Ok(RecoveryOutcome::BreakGlassRequired)) => (
                codes::POLICY_VIOLATION,
                Some("break_glass.background_unclean_recovery is require".into()),
            ),
            _ => (
                codes::ELIGIBLE_LEADERS_NOT_AVAILABLE,
                Some("unclean recovery in progress".into()),
            ),
        };
    PartitionResult {
        partition_id: partition,
        error_code,
        error_message,
        ..Default::default()
    }
}

/// Append the consumed proposal for a partition the URM will elect, and answer
/// the proposal it names.
///
/// # Errors
///
/// Returns the submit failure text when the quorum did not take the consume. No
/// recovery starts in that case, so the approval stays unspent and the operator
/// can retry.
async fn spend_before_recovery(
    broker: &Broker,
    batch: &mut ElectionBatch,
    consumed: Option<MetadataRecord>,
) -> Result<Option<Uuid>, String> {
    let Some(consumed) = consumed else {
        return Ok(None);
    };
    let Some(proposal_id) = consumed_proposal_id(&consumed) else {
        return Ok(None);
    };
    if !batch.spent.insert(proposal_id) {
        return Ok(Some(proposal_id));
    }
    match broker.controller.submit_change(vec![consumed]).await {
        Ok(_) => Ok(Some(proposal_id)),
        Err(error) => {
            tracing::warn!(%error, "elect-leaders could not spend the break-glass approval");
            Err(format!("submit failed: {error}"))
        }
    }
}

fn elect_error_to_wire(err: ElectError) -> (i16, &'static str) {
    match err {
        ElectError::UnknownTopicOrPartition => (
            codes::UNKNOWN_TOPIC_OR_PARTITION,
            "unknown topic or partition",
        ),
        ElectError::PreferredAlreadyLeader => (
            codes::ELECTION_NOT_NEEDED,
            "preferred replica is already leader",
        ),
        ElectError::ElectionNotNeeded => (
            codes::ELECTION_NOT_NEEDED,
            "isr still has a live member; unclean election not needed",
        ),
        ElectError::PreferredNotInIsr => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica not in ISR",
        ),
        ElectError::PreferredNotAlive => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica not alive",
        ),
        ElectError::PreferredIsWitness => (
            codes::PREFERRED_LEADER_NOT_AVAILABLE,
            "preferred replica is a witness and cannot lead",
        ),
        ElectError::NoEligibleReplica => {
            (codes::ELIGIBLE_LEADERS_NOT_AVAILABLE, "no alive replica")
        }
    }
}

fn encode_whole_request_error(
    req: &ElectLeadersRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    // Build a response where every requested (topic, partition) row
    // carries the whole-request error code. Top-level error_code = 0
    // since the per-row codes carry the failure (matches Kafka).
    let results: Vec<ReplicaElectionResult> = match &req.topic_partitions {
        None => vec![],
        Some(list) => list
            .iter()
            .map(|tp| ReplicaElectionResult {
                topic: tp.topic.clone(),
                partition_result: tp
                    .partitions
                    .iter()
                    .map(|&p| PartitionResult {
                        partition_id: p,
                        error_code: code,
                        error_message: Some(msg.into()),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect(),
    };
    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: 0,
        replica_election_results: results,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(resp, api_version, "encode ElectLeaders")
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{
        BreakGlassProposalRecord, LeaderEpoch, PartitionRecord, TopicRecord,
    };
    use krabka_protocol::owned::{
        elect_leaders_request::TopicPartitions, elect_leaders_response,
    };

    use super::*;
    use crate::{
        break_glass::gate::tests::approval,
        broker::BrokerHandle,
        test_support::{peer, principal, start_broker_with},
    };

    const TOPIC: &str = "orders";
    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    const VERSION: i16 = elect_leaders_response::MAX_VERSION;

    crate::test_support::response_helpers!(
        ElectLeadersResponse,
        version = VERSION,
        client_id = "kafka-leader-election"
    );

    fn gated_config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            ..BreakGlassConfig::default()
        }
    }

    /// A proposal that two people approved, and that has not expired against
    /// the wall clock the gate reads.
    fn approved_proposal(target: &str) -> BreakGlassProposalRecord {
        let now = now_ms();
        BreakGlassProposalRecord {
            proposal_id: PROPOSAL,
            action: BreakGlassAction::UncleanElectLeaders,
            target: target.to_owned(),
            proposer: "User:carol".to_owned(),
            reason: "incident 42".to_owned(),
            created_at_ms: now - 1_000,
            expires_at_ms: now + 600_000,
            approvals: vec![approval("User:alice"), approval("User:bob")],
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    /// One two-replica partition of [`TOPIC`], led by broker 1, beside the
    /// proposals the registry holds.
    fn image_with(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.to_owned(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: 2,
        }));
        image.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: TOPIC.to_owned(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }));
        for proposal in proposals {
            image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
        }
        image
    }

    /// The leader change that an unclean election of `orders-0` makes: broker 1
    /// is in the ISR and dead, broker 2 is alive and out of it.
    fn elected() -> PartitionRecord {
        PartitionRecord {
            topic: TOPIC.to_owned(),
            partition: 0,
            leader: NodeId(2),
            replicas: vec![NodeId(1), NodeId(2)],
            isr: vec![NodeId(2)],
            leader_epoch: LeaderEpoch(6),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 1,
        }
    }

    /// Broker 2 alive, broker 1 dead, so an unclean election has an out-of-ISR
    /// replica to elect.
    async fn liveness() -> ControllerLivenessState {
        let liveness = ControllerLivenessState::new(krabka_units::secs(30));
        liveness.record_heartbeat(2).await;
        liveness
    }

    async fn broker_with(config: BreakGlassConfig) -> (BrokerHandle, tempfile::TempDir) {
        start_broker_with(move |cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = std::sync::Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.break_glass = config;
        })
        .await
    }

    /// Drive one partition through the election, and answer its row beside the
    /// records the request would append.
    async fn elect(
        broker: &Broker,
        image: &MetadataImage,
        election: ElectionType,
    ) -> (PartitionResult, Vec<MetadataRecord>) {
        let liveness = liveness().await;
        let witnesses = HashSet::new();
        let principal = principal("admin");
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let env = ElectionEnv {
            broker,
            image,
            ctx: &ctx,
            liveness: &liveness,
            witnesses: &witnesses,
            election,
        };
        let mut batch = ElectionBatch::default();
        let row = elect_one(&env, &mut batch, TOPIC, 0).await;
        (row, batch.records)
    }

    #[tokio::test]
    async fn an_unclean_election_with_no_proposal_is_refused_and_appends_nothing() {
        let (handle, _dir) = broker_with(gated_config()).await;
        let broker = handle.broker_arc_for_test();

        let (row, records) = elect(&broker, &image_with(&[]), ElectionType::Unclean).await;

        check!(row.error_code == codes::POLICY_VIOLATION);
        check!(
            row.error_message
                == Some("break-glass refused unclean_elect_leaders on orders-0: no approved proposal covers the request".to_owned())
        );
        assert!(records == vec![], "a refused election appends nothing");
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn an_approved_unclean_election_appends_the_consume_beside_the_leader_change() {
        let (handle, _dir) = broker_with(gated_config()).await;
        let broker = handle.broker_arc_for_test();
        let proposal = approved_proposal("orders-0");
        let image = image_with(std::slice::from_ref(&proposal));

        let (row, records) = elect(&broker, &image, ElectionType::Unclean).await;

        check!(row.error_code == codes::NONE);
        // The consume and the transition it authorized are one raft append.
        assert!(records.len() == 2, "{records:?}");
        assert!(let MetadataRecord::V1BreakGlassProposal(consumed) = &records[0]);
        check!(consumed.proposal_id == PROPOSAL);
        check!(consumed.consumed_at_ms != 0, "the approval is spent");
        check!(
            *consumed
                == BreakGlassProposalRecord {
                    consumed_at_ms: consumed.consumed_at_ms,
                    ..proposal
                }
        );
        check!(records[1] == MetadataRecord::V1Partition(elected()));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn a_topic_wide_proposal_is_spent_once_for_every_partition_it_covers() {
        let (handle, _dir) = broker_with(gated_config()).await;
        let broker = handle.broker_arc_for_test();
        let image = image_with(&[approved_proposal(TOPIC)]);
        let liveness = liveness().await;
        let witnesses = HashSet::new();
        let principal = principal("admin");
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let env = ElectionEnv {
            broker: &broker,
            image: &image,
            ctx: &ctx,
            liveness: &liveness,
            witnesses: &witnesses,
            election: ElectionType::Unclean,
        };
        let mut batch = ElectionBatch::default();

        let first = elect_one(&env, &mut batch, TOPIC, 0).await;
        let second = elect_one(&env, &mut batch, TOPIC, 0).await;

        check!(first.error_code == codes::NONE);
        check!(second.error_code == codes::NONE);
        let consumes = batch
            .records
            .iter()
            .filter(|record| matches!(record, MetadataRecord::V1BreakGlassProposal(_)))
            .count();
        check!(consumes == 1, "one approval is spent once: {:?}", batch.records);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn a_preferred_election_is_never_gated() {
        let (handle, _dir) = broker_with(gated_config()).await;
        let broker = handle.broker_arc_for_test();

        let (row, records) = elect(&broker, &image_with(&[]), ElectionType::Preferred).await;

        // Broker 1 is already the preferred leader, so the election is not
        // needed. It is never `POLICY_VIOLATION`, which is the point.
        check!(row.error_code == codes::ELECTION_NOT_NEEDED);
        assert!(records == vec![]);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn a_broker_with_no_approver_set_gates_nothing() {
        let (handle, _dir) = broker_with(BreakGlassConfig::default()).await;
        let broker = handle.broker_arc_for_test();

        let (row, records) = elect(&broker, &image_with(&[]), ElectionType::Unclean).await;

        check!(row.error_code == codes::NONE);
        assert!(records == vec![MetadataRecord::V1Partition(elected())]);
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn the_wire_handler_refuses_an_unclean_election_that_no_proposal_covers() {
        let (handle, _dir) = broker_with(gated_config()).await;
        let broker = handle.broker_arc_for_test();
        let principal = principal("admin");
        let peer = peer();
        let ctx = test_context(&principal, &peer);
        let request = |election_type| ElectLeadersRequest {
            election_type,
            topic_partitions: Some(vec![TopicPartitions {
                topic: TOPIC.to_owned(),
                partitions: vec![0],
                ..Default::default()
            }]),
            timeout_ms: 5_000,
            ..Default::default()
        };

        let unclean = handle_request(&broker, request(WIRE_ELECTION_UNCLEAN), &ctx).await;
        let preferred = handle_request(&broker, request(WIRE_ELECTION_PREFERRED), &ctx).await;

        // The gate is an authority gate, so it answers before the broker looks
        // the partition up. A preferred election never reaches it and reports
        // the missing partition instead.
        check!(unclean == codes::POLICY_VIOLATION);
        check!(preferred == codes::UNKNOWN_TOPIC_OR_PARTITION);
        handle.shutdown().await;
    }

    /// Run the wire handler and answer the one partition row's error code.
    async fn handle_request(
        broker: &Broker,
        req: ElectLeadersRequest,
        ctx: &RequestContext<'_>,
    ) -> i16 {
        let bytes = handle(broker, req, ctx, VERSION).await.expect("handle");
        let response = decode_response(&bytes);
        response.replica_election_results[0].partition_result[0].error_code
    }
}
