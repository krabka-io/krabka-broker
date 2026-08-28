//! `AlterPartitionReassignments` (`api_key` 45, KIP-455).
//!
//! The wire handler lives here too. The pure-logic `process_one_partition`
//! helper turns one alter row into a `PartitionRecord` that is ready to
//! submit, or into a wire error code.
//!
//! # KFC-9: a cancel needs two people, and a start does not
//!
//! A cancel reverts a reassignment that is already under way, drops every
//! adding replica, and can move leadership off one. It is one of the
//! transitions the break-glass two-person rule gates. A start is not: it adds
//! replicas and removes none until the new ones catch up. **The completion path
//! in [`crate::reassignment`] is not a cancel either, and it is not gated.**
//!
//! KIP-455 defines the request that `kafka-reassign-partitions` sends and it
//! gains no field for this. An operator gets the approval out of band through
//! `krabka-guard`, targeted at `"<topic>-<partition>"` or at the bare topic
//! name, and the broker looks it up in its own metadata image. A refused row
//! answers `POLICY_VIOLATION (44)` with the refusal text, and the gate is
//! active only when `[break_glass]` names an approver set.

use krabka_metadata::{MetadataImage, PartitionRecord};
use krabka_raft::NodeId;

use crate::codes::{
    ELIGIBLE_LEADERS_NOT_AVAILABLE, INVALID_REPLICA_ASSIGNMENT, NO_REASSIGNMENT_IN_PROGRESS,
    POLICY_VIOLATION, UNKNOWN_TOPIC_OR_PARTITION,
};

/// Per-row rejection: a Kafka wire error code and a readable message.
type RowError = (i16, String);

/// Process one (topic, partition, `target_opt`) row from an
/// `AlterPartitionReassignments` request.
///
/// `cancel_approved` is KFC-9's answer for this row: whether an approved
/// break-glass proposal covers a cancel of this partition. The caller resolves
/// it against the metadata image, so the per-row decision stays in this pure
/// function. It is `true` on a broker that gates nothing, and it is read only
/// on the cancel path.
///
/// The return values are:
///   - `Ok(Some(PartitionRecord))`: submit this intermediate record
///   - `Ok(None)`: do nothing, because the row is already at target or the
///     alter is empty
///   - `Err((wire_code, message))`: reject this row
pub(crate) fn process_one_partition(
    image: &MetadataImage,
    topic: &str,
    partition: i32,
    target: Option<&[i32]>,
    allow_rf_change: bool,
    cancel_approved: bool,
) -> Result<Option<PartitionRecord>, RowError> {
    let pr = image
        .partition(topic, partition)
        .ok_or((UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()))?;

    match target {
        None => cancel_path(pr, cancel_approved),
        Some(target_slice) => {
            validate_target(target_slice, image, allow_rf_change, pr)?;
            Ok(start_path(pr, target_slice))
        }
    }
}

/// The row message that an unapproved cancel carries when the caller has no
/// refusal text of its own to put there.
///
/// The handler replaces it with the gate's own text, which names the proposal
/// that nearly authorized the cancel. This constant is what a caller that
/// resolved the gate elsewhere still gets.
const CANCEL_NEEDS_APPROVAL: &str = "a reassignment cancel needs an approved break-glass proposal";

fn validate_target(
    target: &[i32],
    image: &MetadataImage,
    allow_rf_change: bool,
    pr: &PartitionRecord,
) -> Result<(), RowError> {
    if target.is_empty() {
        return Err((INVALID_REPLICA_ASSIGNMENT, "empty target".into()));
    }
    // Duplicates.
    let mut seen: std::collections::HashSet<i32> = std::collections::HashSet::new();
    for &n in target {
        if !seen.insert(n) {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("duplicate replica {n}")));
        }
    }
    // Every node id must be a registered broker.
    for &n in target {
        let Ok(node_id) = u64::try_from(n) else {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("negative broker {n}")));
        };
        if image.broker(NodeId(node_id)).is_none() {
            return Err((INVALID_REPLICA_ASSIGNMENT, format!("unknown broker {n}")));
        }
    }
    // RF-change check.
    if !allow_rf_change {
        let current_target_len = pr
            .replicas
            .iter()
            .filter(|n| !pr.removing_replicas.contains(n))
            .count();
        if target.len() != current_target_len {
            return Err((
                INVALID_REPLICA_ASSIGNMENT,
                format!(
                    "rf change disallowed: target len {} != current target len {}",
                    target.len(),
                    current_target_len,
                ),
            ));
        }
    }
    Ok(())
}

fn cancel_path(pr: &PartitionRecord, approved: bool) -> Result<Option<PartitionRecord>, RowError> {
    // KFC-9: the two-person rule is an authority gate, so it answers before any
    // question about the partition's own state. Reading the reassignment first
    // would make "does this need an approval" depend on state that a
    // concurrent reassignment can change between the check and the append.
    if !approved {
        return Err((POLICY_VIOLATION, CANCEL_NEEDS_APPROVAL.into()));
    }
    if pr.adding_replicas.is_empty() && pr.removing_replicas.is_empty() {
        return Err((NO_REASSIGNMENT_IN_PROGRESS, "nothing to cancel".into()));
    }
    let reverted_replicas: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let reverted_isr: Vec<NodeId> = pr
        .isr
        .iter()
        .filter(|n| !pr.adding_replicas.contains(n))
        .copied()
        .collect();
    let (leader, epoch_bump) = if pr.adding_replicas.contains(&pr.leader) {
        // Leader was an adding replica; revert leadership.
        match reverted_replicas.iter().find(|n| reverted_isr.contains(n)) {
            Some(&n) => (n, 1),
            None => {
                return Err((
                    ELIGIBLE_LEADERS_NOT_AVAILABLE,
                    "no eligible leader after cancel".into(),
                ));
            }
        }
    } else {
        (pr.leader, 0)
    };
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &reverted_replicas);
    Ok(Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader,
        replicas: reverted_replicas,
        isr: reverted_isr,
        leader_epoch: krabka_metadata::LeaderEpoch(pr.leader_epoch.0 + epoch_bump),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    }))
}

fn start_path(pr: &PartitionRecord, target: &[i32]) -> Option<PartitionRecord> {
    let target_set: Vec<NodeId> = target
        .iter()
        .map(|&id| NodeId(u64::try_from(id).expect("target validated as non-negative")))
        .collect();
    let current_target: Vec<NodeId> = pr
        .replicas
        .iter()
        .filter(|n| !pr.removing_replicas.contains(n))
        .copied()
        .collect();
    let old: Vec<NodeId> = current_target
        .iter()
        .filter(|n| !target_set.contains(n))
        .copied()
        .collect();
    let new: Vec<NodeId> = target_set
        .iter()
        .filter(|n| !current_target.contains(n))
        .copied()
        .collect();
    if old.is_empty() && new.is_empty() {
        return None; // already at target — no-op
    }
    // replicas = current_target ∪ target (current_target first, then new).
    let mut new_replicas = current_target;
    for n in &new {
        new_replicas.push(*n);
    }
    let new_directories =
        crate::reassignment::remap_directories(&pr.replicas, &pr.directories, &new_replicas);
    Some(PartitionRecord {
        topic: pr.topic.clone(),
        partition: pr.partition,
        leader: pr.leader,
        replicas: new_replicas,
        isr: pr.isr.clone(),
        leader_epoch: pr.leader_epoch,
        adding_replicas: new,
        removing_replicas: old,
        directories: new_directories,
        partition_epoch: pr.partition_epoch + 1,
    })
}

use std::collections::{HashMap, HashSet};

use bytes::Bytes;
use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{BreakGlassAction, MetadataRecord, ResourceType};
use krabka_protocol::{
    Encode,
    owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition,
        },
        alter_partition_reassignments_response::{
            AlterPartitionReassignmentsResponse, ReassignablePartitionResponse,
            ReassignableTopicResponse,
        },
    },
};
use uuid::Uuid;

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult},
    break_glass::{
        action_name,
        gate::{self, BreakGlassDenial},
        handlers::{PrivilegedAudit, audit_privileged},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes::{CLUSTER_AUTHORIZATION_FAILED, COORDINATOR_NOT_AVAILABLE},
    config::BreakGlassConfig,
    handlers::RequestContext,
    operator_keys::approver_set_fingerprint,
    time_util::now_ms,
};

#[tracing::instrument(
    name = "handle_alter_partition_reassignments",
    level = "info",
    skip_all,
    fields(api = "AlterPartitionReassignments"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: AlterPartitionReassignmentsRequest,
    ctx: &RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    // Whole-request Cluster Alter authorize.
    let allow = broker.config.authorizer.authorize(
        &*image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            operation: krabka_metadata::AclOperation::Alter,
        },
    );
    if matches!(allow, AuthorizationResult::Deny) {
        return encode_whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "alter-reassignment denied",
            api_version,
        );
    }

    let env = ReassignEnv {
        broker,
        image: &image,
        ctx,
        allow_rf_change: req.allow_replication_factor_change,
    };
    let mut by_topic: HashMap<String, Vec<ReassignablePartitionResponse>> = HashMap::new();
    let mut batch = ReassignBatch::default();
    for topic in &req.topics {
        let mut rows = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            rows.push(alter_one(&env, &mut batch, &topic.name, p));
        }
        by_topic.insert(topic.name.clone(), rows);
    }

    let mut submit_failure = None;
    if !batch.records.is_empty()
        && let Err(e) = broker
            .controller
            .submit_change(std::mem::take(&mut batch.records))
            .await
    {
        tracing::warn!(error = %e, "alter-reassignment submit failed");
        submit_failure = Some(format!("submit failed: {e}"));
        mark_submit_failed(&mut by_topic, &format!("submit failed: {e}"));
    }
    // KFC-9: audit the approvals this append spent, now that its outcome is
    // known.
    batch.audit_applied(broker, ctx, submit_failure.as_deref());

    let responses: Vec<ReassignableTopicResponse> = by_topic
        .into_iter()
        .map(|(name, partitions)| ReassignableTopicResponse {
            name,
            partitions,
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: req.allow_replication_factor_change,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

/// Everything one alter row reads, and nothing it writes.
struct ReassignEnv<'a> {
    broker: &'a Broker,
    image: &'a MetadataImage,
    ctx: &'a RequestContext<'a>,
    allow_rf_change: bool,
}

/// What one `AlterPartitionReassignments` request accumulates across its rows.
///
/// `records` is the single raft append that carries every consumed proposal
/// beside every partition record the request makes, so an approval and the
/// cancel it authorized commit together.
#[derive(Default)]
struct ReassignBatch {
    /// The consumed proposals first, then the partition records.
    records: Vec<MetadataRecord>,
    /// The proposals this request already spent. One proposal on a bare topic
    /// name covers every partition of it, and it is spent once.
    spent: HashSet<Uuid>,
    /// The cancels waiting on the append, to audit once it commits.
    applied: Vec<(String, Option<Uuid>)>,
}

impl ReassignBatch {
    /// Take a consumed proposal into the append, and answer the proposal it
    /// names.
    fn spend(&mut self, consumed: Option<MetadataRecord>) -> Option<Uuid> {
        let consumed = consumed?;
        let proposal_id = consumed_proposal_id(&consumed)?;
        if self.spent.insert(proposal_id) {
            self.records.insert(0, consumed);
        }
        Some(proposal_id)
    }

    /// Audit every cancel this append carried.
    ///
    /// `failure` is the submit error when the append did not commit, and the
    /// event then records a refusal with that text rather than a cancel that
    /// never happened.
    fn audit_applied(&self, broker: &Broker, ctx: &RequestContext<'_>, failure: Option<&str>) {
        for (target, proposal_id) in &self.applied {
            let (phase, reason) = match failure {
                None => (PrivilegedPhase::Applied, "reassignment cancel committed"),
                Some(error) => (PrivilegedPhase::Refused, error),
            };
            audit_cancel(broker, ctx, target, phase, *proposal_id, reason);
        }
    }
}

/// Process one alter row, and answer the response row it becomes.
fn alter_one(
    env: &ReassignEnv<'_>,
    batch: &mut ReassignBatch,
    topic: &str,
    partition: &ReassignablePartition,
) -> ReassignablePartitionResponse {
    let index = partition.partition_index;
    let target: Option<&[i32]> = partition.replicas.as_deref();
    // KFC-9: only a cancel is gated. A start adds replicas and removes none,
    // and a completion is not a cancel at all.
    let mut consumed = None;
    let mut denial = None;
    if target.is_none() {
        match authorize_cancel(env.image, &env.broker.config.break_glass, topic, index) {
            Ok(record) => consumed = record,
            Err(refusal) => denial = Some(refusal),
        }
    }

    match process_one_partition(
        env.image,
        topic,
        index,
        target,
        env.allow_rf_change,
        denial.is_none(),
    ) {
        Ok(Some(record)) => {
            let proposal_id = batch.spend(consumed);
            batch.records.push(MetadataRecord::V1Partition(record));
            if target.is_none() {
                batch
                    .applied
                    .push((cancel_target(topic, index), proposal_id));
            }
            ok_row(index)
        }
        Ok(None) => ok_row(index),
        Err((code, message)) => {
            // The pure function knows only that the cancel is unapproved. The
            // gate's own text names the proposal that nearly authorized it, so
            // that is what the row and the audit event carry.
            let Some(denial) = denial.filter(|_| code == POLICY_VIOLATION) else {
                return err_row(index, code, message);
            };
            let message = denial.to_string();
            break_glass_metrics::record_refusal(&env.broker.metrics, denial.action);
            audit_cancel(
                env.broker,
                env.ctx,
                &cancel_target(topic, index),
                PrivilegedPhase::Refused,
                denial.proposal_id(),
                &message,
            );
            err_row(index, code, message)
        }
    }
}

/// KFC-9: find the approved proposal that authorizes a cancel of one partition,
/// and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. A cancel then behaves as it does on a cluster with no such
/// section.
fn authorize_cancel(
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
        BreakGlassAction::CancelReassignment,
        &cancel_target(topic, partition),
        now_ms(),
    )
    .map(Some)
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
fn cancel_target(topic: &str, partition: i32) -> String {
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

/// Emit one `PrivilegedAction` event for a reassignment cancel.
///
/// `counterparties` stays empty for the reason the freeze events give: the
/// approvers are named on the proposal's own approve events, and the proposal
/// id joins those rows to this one.
fn audit_cancel(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    target: &str,
    phase: PrivilegedPhase,
    proposal_id: Option<Uuid>,
    reason: &str,
) {
    // A broker that gates nothing has no two-person evidence to record, and
    // this event exists to carry that evidence. The ordinary administrative
    // event already reports the transition itself, so a stock cluster's audit
    // stream is unchanged.
    if !gate::is_gated(&broker.config.break_glass) {
        return;
    }
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
            action: action_name(BreakGlassAction::CancelReassignment),
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

fn ok_row(partition_index: i32) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        ..Default::default()
    }
}

fn err_row(partition_index: i32, code: i16, msg: String) -> ReassignablePartitionResponse {
    ReassignablePartitionResponse {
        partition_index,
        error_code: code,
        error_message: Some(msg),
        ..Default::default()
    }
}

fn mark_submit_failed(
    by_topic: &mut HashMap<String, Vec<ReassignablePartitionResponse>>,
    msg: &str,
) {
    for rows in by_topic.values_mut() {
        for r in rows.iter_mut() {
            if r.error_code == 0 {
                r.error_code = COORDINATOR_NOT_AVAILABLE;
                r.error_message = Some(msg.to_string());
            }
        }
    }
}

fn encode_whole_request_error(
    req: &AlterPartitionReassignmentsRequest,
    code: i16,
    msg: &str,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let responses: Vec<ReassignableTopicResponse> = req
        .topics
        .iter()
        .map(|t| ReassignableTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| err_row(p.partition_index, code, msg.into()))
                .collect(),
            ..Default::default()
        })
        .collect();
    let resp = AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: req.allow_replication_factor_change,
        responses,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

fn encode_response<R: Encode>(
    resp: &R,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    crate::handlers::encode_response_with_context(
        resp,
        api_version,
        "encode AlterPartitionReassignments",
    )
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc, time::Duration};

    use assert2::{assert, check};
    use krabka_metadata::{
        BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicRecord,
    };
    use krabka_protocol::{
        UnknownTaggedFields,
        owned::alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
    };
    use krabka_security::{AuthMethod, Principal};
    use uuid::Uuid;

    use super::*;
    use crate::test_support::DenyAll;

    fn img_with(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
    ) -> MetadataImage {
        img_with_epoch(replicas, isr, adding, removing, leader, 0)
    }

    fn img_with_epoch(
        replicas: &[u64],
        isr: &[u64],
        adding: &[u64],
        removing: &[u64],
        leader: u64,
        partition_epoch: i32,
    ) -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        // Register brokers 1..=6 so validate_target accepts target lists.
        for n in 1u64..=6 {
            img.apply(&MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    node_id: NodeId(n),
                    broker_epoch: 0,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9092,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                },
            ));
        }
        img.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "foo".into(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(replicas.len()).expect("replication factor fits i16"),
        }));
        img.apply(&MetadataRecord::V1Partition(PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(leader),
            replicas: replicas.iter().copied().map(NodeId).collect(),
            isr: isr.iter().copied().map(NodeId).collect(),
            leader_epoch: krabka_metadata::LeaderEpoch(5),
            adding_replicas: adding.iter().copied().map(NodeId).collect(),
            removing_replicas: removing.iter().copied().map(NodeId).collect(),
            directories: vec![],
            partition_epoch,
        }));
        img
    }

    fn request(
        allow_replication_factor_change: bool,
        topic: &str,
        partition_index: i32,
        replicas: Option<Vec<i32>>,
    ) -> AlterPartitionReassignmentsRequest {
        AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change,
            topics: vec![ReassignableTopic {
                name: topic.into(),
                partitions: vec![ReassignablePartition {
                    partition_index,
                    replicas,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[test]
    fn validate_target_rejects_negative_broker_id() {
        let image = img_with(&[1], &[1], &[], &[], 1);
        let partition = image.partition("foo", 0).expect("seeded partition");
        let error = validate_target(&[-1], &image, true, partition).expect_err("negative broker");
        assert!(error.0 == INVALID_REPLICA_ASSIGNMENT);
        assert!(error.1.contains("negative broker"));
    }

    crate::test_support::response_helpers!(
        AlterPartitionReassignmentsResponse,
        client_id = "admin-client"
    );

    use crate::test_support::start_broker_with_authorizer as start_broker;

    async fn wait_for_leader(broker: &Broker) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if broker
                .controller
                .watch_leader()
                .borrow()
                .is_some_and(|n| n == broker.config.node_id)
            {
                return;
            }
            assert!(
                std::time::Instant::now() <= deadline,
                "broker did not become controller leader"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    async fn seed_reassignable_partition(broker: &Broker) {
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                    node_id: NodeId(1),
                    broker_epoch: 1,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9092,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                }),
                MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                    node_id: NodeId(2),
                    broker_epoch: 1,
                    incarnation_id: uuid::Uuid::nil(),
                    host: "localhost".into(),
                    port: 9093,
                    rack: None,
                    log_dirs: vec![],
                    endpoints: vec![],
                    features: std::collections::BTreeMap::new(),
                }),
                MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id: Uuid::nil(),
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders".into(),
                    partition: 7,
                    leader: NodeId(1),
                    replicas: vec![NodeId(1)],
                    isr: vec![NodeId(1)],
                    leader_epoch: LeaderEpoch(3),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 11,
                }),
            ])
            .await
            .expect("seed reassignment metadata");
    }

    #[test]
    fn noop_when_already_at_target() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 3]), true, true).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn start_writes_union_replicas() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[], 1, 11);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 4]), true, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5), // unchanged on start
            adding_replicas: vec![NodeId(4)],
            removing_replicas: vec![NodeId(2), NodeId(3)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn row_builders_preserve_non_default_fields() {
        let ok = ok_row(7);
        let expected_ok = ReassignablePartitionResponse {
            partition_index: 7,
            error_code: 0,
            error_message: None,
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(ok == expected_ok);

        let err = err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "missing partition".into());
        let expected_err = ReassignablePartitionResponse {
            partition_index: 8,
            error_code: UNKNOWN_TOPIC_OR_PARTITION,
            error_message: Some("missing partition".into()),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(err == expected_err);
    }

    #[test]
    fn encode_whole_request_error_preserves_request_shape() {
        let version = 1;
        let req = request(false, "payments", 8, Some(vec![1, 2]));

        let bytes =
            encode_whole_request_error(&req, CLUSTER_AUTHORIZATION_FAILED, "denied", version)
                .expect("encode whole request error");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("denied".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }

    #[test]
    fn mark_submit_failed_only_rewrites_successful_rows() {
        let mut by_topic = std::collections::HashMap::from([(
            "orders".to_string(),
            vec![
                ok_row(7),
                err_row(8, UNKNOWN_TOPIC_OR_PARTITION, "unknown partition".into()),
            ],
        )]);

        mark_submit_failed(&mut by_topic, "submit failed: not controller");
        let rows = by_topic.get("orders").expect("topic rows");

        let expected = vec![
            ReassignablePartitionResponse {
                partition_index: 7,
                error_code: COORDINATOR_NOT_AVAILABLE,
                error_message: Some("submit failed: not controller".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
            ReassignablePartitionResponse {
                partition_index: 8,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("unknown partition".into()),
                unknown_tagged_fields: UnknownTaggedFields::default(),
            },
        ];
        assert!(*rows == expected);
    }

    #[tokio::test]
    async fn handle_preserves_unknown_partition_response_shape() {
        let version = 1;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let bytes = handle(
            &broker,
            request(false, "payments", 8, Some(vec![1, 2])),
            &ctx,
            version,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: UNKNOWN_TOPIC_OR_PARTITION,
                    error_message: Some("unknown partition".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_denies_cluster_alter_for_each_requested_partition() {
        let version = 1;
        let (broker_handle, _dir) = start_broker(Arc::new(DenyAll)).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let bytes = handle(
            &broker,
            request(false, "payments", 8, Some(vec![1, 2])),
            &ctx,
            version,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: false,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "payments".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 8,
                    error_code: CLUSTER_AUTHORIZATION_FAILED,
                    error_message: Some("alter-reassignment denied".into()),
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_submits_successful_reassignment_records() {
        let version = 1;
        let (broker_handle, _dir) =
            start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
        let broker = broker_handle.broker_arc_for_test();
        wait_for_leader(&broker).await;
        seed_reassignable_partition(&broker).await;
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::Anonymous,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = test_context(&principal, &peer);

        let bytes = handle(
            &broker,
            request(true, "orders", 7, Some(vec![1, 2])),
            &ctx,
            version,
        )
        .await
        .expect("handle");
        let resp = decode_response(&bytes, version);

        let expected = AlterPartitionReassignmentsResponse {
            throttle_time_ms: 0,
            allow_replication_factor_change: true,
            error_code: 0,
            error_message: None,
            responses: vec![ReassignableTopicResponse {
                name: "orders".into(),
                partitions: vec![ReassignablePartitionResponse {
                    partition_index: 7,
                    error_code: 0,
                    error_message: None,
                    unknown_tagged_fields: UnknownTaggedFields::default(),
                }],
                unknown_tagged_fields: UnknownTaggedFields::default(),
            }],
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);

        let image = broker.controller.current_image();
        let partition = image.partition("orders", 7).expect("partition committed");
        assert!(partition.adding_replicas == vec![NodeId(2)]);
        assert!(partition.partition_epoch == 12);
        broker_handle.shutdown().await;
    }

    #[test]
    fn replaces_existing_in_flight_reassignment() {
        // Currently in flight: replicas=[1,2,3,4], adding=[4], removing=[2,3].
        // current_target = [1,4]. New alter target = [5,6].
        // Expected: replicas=[1,4,5,6], adding=[5,6], removing=[1,4].
        let img = img_with(&[1, 2, 3, 4], &[1, 2, 3], &[4], &[2, 3], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[5, 6]), true, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(4), NodeId(5), NodeId(6)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![NodeId(5), NodeId(6)],
            removing_replicas: vec![NodeId(1), NodeId(4)],
            directories: vec![Uuid::nil(); 4],
            partition_epoch: 1,
        };
        assert!(res == expected);
    }

    #[test]
    fn rf_change_rejected_when_disabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[1, 2]), false, true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }

    #[test]
    fn rf_change_allowed_when_enabled() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2]), true, true)
            .expect("ok")
            .expect("Some");
        assert!(res.removing_replicas == vec![NodeId(3)]);
    }

    #[test]
    fn rf_check_counts_current_target_without_removing_replicas() {
        let img = img_with(&[1, 2, 3, 4], &[1, 3, 4], &[4], &[2], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 3, 4]), false, true).expect("ok");

        assert!(res.is_none());
    }

    #[test]
    fn cancel_with_leader_in_adding_reverts_leader() {
        // After a successful leader handoff during reassignment, leader=4 (an adding replica).
        // Cancel: leader should revert to whoever in reverted replicas ∩ isr.
        // replicas=[1,2,3,4], adding=[4], removing=[2,3], leader=4, isr=[1,4].
        let img = img_with_epoch(&[1, 2, 3, 4], &[1, 4], &[4], &[2, 3], 4, 11);
        let res = process_one_partition(&img, "foo", 0, None, true, true)
            .expect("ok")
            .expect("Some");
        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1), // reverted replicas ∩ isr = [1]
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1)],
            leader_epoch: LeaderEpoch(6), // bumped
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn cancel_with_only_removing_replicas_is_valid() {
        let img = img_with_epoch(&[1, 2, 3], &[1, 2, 3], &[], &[3], 1, 11);
        let res = process_one_partition(&img, "foo", 0, None, true, true)
            .expect("ok")
            .expect("Some");

        let expected = PartitionRecord {
            topic: "foo".into(),
            partition: 0,
            leader: NodeId(1),
            replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
            isr: vec![NodeId(1), NodeId(2), NodeId(3)],
            leader_epoch: LeaderEpoch(5),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![Uuid::nil(); 3],
            partition_epoch: 12,
        };
        assert!(res == expected);
    }

    #[test]
    fn empty_target_rejected() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let err = process_one_partition(&img, "foo", 0, Some(&[]), true, true).unwrap_err();
        assert!(err.0 == INVALID_REPLICA_ASSIGNMENT);
    }

    // ── KFC-9: the break-glass gate over a cancel ───────────────────────

    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    const NOW_MS: i64 = 60_000;

    fn gated_config() -> crate::config::BreakGlassConfig {
        crate::config::BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            ..crate::config::BreakGlassConfig::default()
        }
    }

    /// A proposal that two people approved, and that has not expired against
    /// the wall clock the gate reads.
    fn approved_proposal(target: &str) -> krabka_metadata::BreakGlassProposalRecord {
        let now = now_ms();
        krabka_metadata::BreakGlassProposalRecord {
            proposal_id: PROPOSAL,
            action: BreakGlassAction::CancelReassignment,
            target: target.to_owned(),
            proposer: "User:carol".to_owned(),
            reason: "the reassignment is making things worse".to_owned(),
            created_at_ms: now - 1_000,
            expires_at_ms: now + 600_000,
            approvals: vec![
                crate::break_glass::gate::tests::approval("User:alice"),
                crate::break_glass::gate::tests::approval("User:bob"),
            ],
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    /// A partition mid-reassignment, beside the proposals the registry holds.
    fn img_reassigning(proposals: &[krabka_metadata::BreakGlassProposalRecord]) -> MetadataImage {
        let mut img = img_with(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1);
        for proposal in proposals {
            img.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
        }
        img
    }

    #[test]
    fn an_unapproved_cancel_is_refused_before_any_question_about_the_partition() {
        let cases = [
            (
                "a reassignment is in progress",
                img_with(&[1, 2, 3], &[1, 2, 3], &[3], &[2], 1),
            ),
            (
                "nothing to cancel",
                img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1),
            ),
        ];
        for (label, img) in cases {
            let err = process_one_partition(&img, "foo", 0, None, true, false).unwrap_err();
            check!(err.0 == POLICY_VIOLATION, "case {label}");
            check!(err.1 == CANCEL_NEEDS_APPROVAL, "case {label}");
        }
    }

    #[test]
    fn an_unknown_partition_still_answers_that_it_is_unknown() {
        // The gate never masks a request that names nothing.
        let img = MetadataImage::new(Uuid::nil());
        let err = process_one_partition(&img, "foo", 0, None, true, false).unwrap_err();
        check!(err.0 == UNKNOWN_TOPIC_OR_PARTITION);
    }

    #[test]
    fn a_start_is_never_gated() {
        let img = img_with(&[1, 2, 3], &[1, 2, 3], &[], &[], 1);
        let res = process_one_partition(&img, "foo", 0, Some(&[1, 2, 4]), true, false)
            .expect("a start needs no approval")
            .expect("Some");
        check!(res.adding_replicas == vec![NodeId(4)]);
    }

    #[test]
    fn the_cancel_gate_answers_from_the_proposal_registry() {
        let approved = approved_proposal("foo-0");
        let cases: [(
            &'static str,
            MetadataImage,
            crate::config::BreakGlassConfig,
            bool,
        ); 4] = [
            (
                "an approved proposal on the partition",
                img_reassigning(std::slice::from_ref(&approved)),
                gated_config(),
                true,
            ),
            (
                "an approved proposal on the whole topic",
                img_reassigning(&[approved_proposal("foo")]),
                gated_config(),
                true,
            ),
            (
                "no proposal at all",
                img_reassigning(&[]),
                gated_config(),
                false,
            ),
            (
                "no approver set, so nothing is gated",
                img_reassigning(&[]),
                crate::config::BreakGlassConfig::default(),
                true,
            ),
        ];
        for (label, img, config, expected) in cases {
            let authorized = authorize_cancel(&img, &config, "foo", 0).is_ok();
            check!(authorized == expected, "case {label}");
        }
    }

    #[tokio::test]
    async fn an_approved_cancel_appends_the_consume_beside_the_partition_record() {
        let (handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.break_glass = gated_config();
        })
        .await;
        let broker = handle.broker_arc_for_test();
        let proposal = approved_proposal("foo-0");
        let image = img_reassigning(std::slice::from_ref(&proposal));
        let principal = crate::test_support::principal("admin");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "reassign-client");
        let env = ReassignEnv {
            broker: &broker,
            image: &image,
            ctx: &ctx,
            allow_rf_change: true,
        };
        let mut batch = ReassignBatch::default();

        let row = alter_one(
            &env,
            &mut batch,
            "foo",
            &ReassignablePartition {
                partition_index: 0,
                replicas: None,
                ..Default::default()
            },
        );

        check!(row.error_code == 0);
        // The consume and the cancel it authorized are one raft append.
        assert!(batch.records.len() == 2, "{:?}", batch.records);
        assert!(let MetadataRecord::V1BreakGlassProposal(consumed) = &batch.records[0]);
        check!(consumed.proposal_id == PROPOSAL);
        check!(consumed.consumed_at_ms != 0, "the approval is spent");
        let reverted = process_one_partition(&image, "foo", 0, None, true, true)
            .expect("ok")
            .expect("Some");
        check!(batch.records[1] == MetadataRecord::V1Partition(reverted));
        handle.shutdown().await;
    }

    #[tokio::test]
    async fn an_unapproved_cancel_appends_nothing_and_carries_the_gate_text() {
        let (handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.audit_enabled = false;
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.break_glass = gated_config();
        })
        .await;
        let broker = handle.broker_arc_for_test();
        let image = img_reassigning(&[]);
        let principal = crate::test_support::principal("admin");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&principal, &peer, "reassign-client");
        let env = ReassignEnv {
            broker: &broker,
            image: &image,
            ctx: &ctx,
            allow_rf_change: true,
        };
        let mut batch = ReassignBatch::default();

        let row = alter_one(
            &env,
            &mut batch,
            "foo",
            &ReassignablePartition {
                partition_index: 0,
                replicas: None,
                ..Default::default()
            },
        );

        check!(row.error_code == POLICY_VIOLATION);
        check!(
            row.error_message
                == Some(
                    "break-glass refused cancel_reassignment on foo-0: no approved proposal covers the request"
                        .to_owned()
                )
        );
        assert!(batch.records == vec![], "a refused cancel appends nothing");
        handle.shutdown().await;
    }

    #[test]
    fn a_topic_wide_proposal_is_spent_once_for_every_partition_it_covers() {
        let mut batch = ReassignBatch::default();
        let consumed =
            MetadataRecord::V1BreakGlassProposal(krabka_metadata::BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS,
                ..approved_proposal("foo")
            });

        let first = batch.spend(Some(consumed.clone()));
        let second = batch.spend(Some(consumed.clone()));

        check!(first == Some(PROPOSAL));
        check!(second == Some(PROPOSAL));
        assert!(batch.records == vec![consumed]);
    }
}
