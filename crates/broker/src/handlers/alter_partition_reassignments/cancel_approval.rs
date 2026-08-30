//! KFC-9: the break-glass approval a reassignment cancel needs, the per-row
//! step that spends it, and the batch one request accumulates.
//!
//! [`alter_one`] is the whole of one requested row: it resolves the gate for a
//! cancel, hands the row to the pure planner in [`plan`](super::plan), and
//! answers with the response row that row becomes. A start never reaches the
//! gate, because only a cancel is gated.
//!
//! [`ReassignBatch`] is what carries a consumed approval into the same raft
//! append as the partition record it authorized, so the two commit together,
//! and it holds back the `Applied` audit events until that append's outcome is
//! known.

use std::collections::HashSet;

use krabka_audit::{AuditError, PrivilegedPhase};
use krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    alter_partition_reassignments_request::ReassignablePartition,
    alter_partition_reassignments_response::ReassignablePartitionResponse,
};
use uuid::Uuid;

use super::{
    process_one_partition,
    response::{err_row, ok_row},
};
use crate::{
    break_glass::{
        gate::{self, BreakGlassDenial},
        handlers::audit::{GatedTransition, audit_transition, require_transition},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes::POLICY_VIOLATION,
    config::BreakGlassConfig,
    handlers::RequestContext,
    time_util::now_ms,
};

/// Everything one alter row reads, and nothing it writes.
pub(super) struct ReassignEnv<'a> {
    pub(super) broker: &'a Broker,
    pub(super) image: &'a MetadataImage,
    pub(super) ctx: &'a RequestContext<'a>,
    pub(super) allow_rf_change: bool,
}

/// What one `AlterPartitionReassignments` request accumulates across its rows.
///
/// `records` is the single raft append that carries every consumed proposal
/// beside every partition record the request makes, so an approval and the
/// cancel it authorized commit together.
#[derive(Default)]
pub(super) struct ReassignBatch {
    /// The consumed proposals first, then the partition records.
    pub(super) records: Vec<MetadataRecord>,
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

    /// Durably admit every queued cancel before the raft append.
    pub(super) async fn require_audit(
        &self,
        broker: &Broker,
        ctx: &RequestContext<'_>,
    ) -> Result<(), AuditError> {
        for (target, proposal_id) in &self.applied {
            require_transition(
                &broker.audit_log,
                &broker.config.break_glass,
                ctx,
                &GatedTransition {
                    action: BreakGlassAction::CancelReassignment,
                    target,
                    phase: PrivilegedPhase::Applied,
                    proposal_id: *proposal_id,
                    reason: "reassignment cancel admitted",
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Audit every cancel this append carried.
    ///
    /// `failure` is the submit error when the append did not commit, and the
    /// event then records a refusal with that text rather than a cancel that
    /// never happened.
    pub(super) fn audit_applied(
        &self,
        broker: &Broker,
        ctx: &RequestContext<'_>,
        failure: Option<&str>,
    ) {
        for (target, proposal_id) in &self.applied {
            let (phase, reason) = match failure {
                None => (PrivilegedPhase::Applied, "reassignment cancel committed"),
                Some(error) => (PrivilegedPhase::Refused, error),
            };
            audit_transition(
                &broker.audit_log,
                &broker.config.break_glass,
                ctx,
                &GatedTransition {
                    action: BreakGlassAction::CancelReassignment,
                    target,
                    phase,
                    proposal_id: *proposal_id,
                    reason,
                },
            );
        }
    }
}

/// Process one alter row, and answer the response row it becomes.
pub(super) fn alter_one(
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
            audit_transition(
                &env.broker.audit_log,
                &env.broker.config.break_glass,
                env.ctx,
                &GatedTransition {
                    action: BreakGlassAction::CancelReassignment,
                    target: &cancel_target(topic, index),
                    phase: PrivilegedPhase::Refused,
                    proposal_id: denial.proposal_id(),
                    reason: &message,
                },
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::{assert, check};

    use super::*;
    use crate::handlers::alter_partition_reassignments::test_support::img_with;

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
