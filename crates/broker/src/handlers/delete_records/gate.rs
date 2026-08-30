//! KFC-9: the break-glass two-person rule over a `DeleteRecords` trim.
//!
//! A trim destroys committed records, so it is gated. This module finds the
//! approved proposal that authorizes one partition's trim, spends it, and
//! builds the refusal row for a trim no proposal covers. The trim itself, and
//! the freeze check that answers ahead of this gate, stay in the module root.

use std::collections::HashSet;

use krabka_audit::PrivilegedPhase;
use krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord};
use krabka_protocol::owned::delete_records_response::DeleteRecordsPartitionResult;
use uuid::Uuid;

use super::{TrimEnv, response::error_partition_result};
use crate::{
    break_glass::{
        gate::{self, BreakGlassDenial},
        handlers::audit::{GatedTransition, audit_transition},
        metrics as break_glass_metrics,
    },
    broker::Broker,
    codes,
    config::BreakGlassConfig,
    time_util::now_ms,
};

/// KFC-9: find the approved proposal that authorizes a trim of one partition,
/// and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. A trim then behaves as it does on a cluster with no such section.
pub(super) fn authorize_trim(
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
        BreakGlassAction::DeleteRecords,
        &trim_target(topic, partition),
        now_ms(),
    )
    .map(Some)
}

/// The records that spending this approval appends, and the proposal they name.
///
/// A trim writes no metadata record, so the append carries the consume alone.
/// The record list is empty when there is nothing to append: the broker gates
/// nothing, or this request already spent the proposal on another partition
/// that one topic-wide approval covers.
fn consume_records(
    spent: &mut HashSet<Uuid>,
    consumed: Option<MetadataRecord>,
) -> (Option<Uuid>, Vec<MetadataRecord>) {
    let Some(consumed) = consumed else {
        return (None, Vec::new());
    };
    let Some(proposal_id) = consumed_proposal_id(&consumed) else {
        return (None, Vec::new());
    };
    if spent.insert(proposal_id) {
        (Some(proposal_id), vec![consumed])
    } else {
        (Some(proposal_id), Vec::new())
    }
}

/// Append the consumed proposal, and answer the proposal it names.
///
/// # Errors
///
/// Returns the submit failure when the quorum did not take the consume. No trim
/// runs in that case, so the approval stays unspent.
pub(super) async fn spend_approval(
    broker: &Broker,
    spent: &mut HashSet<Uuid>,
    consumed: Option<MetadataRecord>,
) -> Result<Option<Uuid>, krabka_raft::RaftError> {
    let (proposal_id, records) = consume_records(spent, consumed);
    if !records.is_empty() {
        broker.controller.submit_change(records).await?;
    }
    Ok(proposal_id)
}

/// Refuse one partition: count it, audit it, and build its error row.
///
/// The row carries the code alone, because no `DeleteRecords` response version
/// has an `error_message` field to put the reason in.
pub(super) fn refuse_trim(
    env: &TrimEnv<'_>,
    topic: &str,
    partition: i32,
    denial: &BreakGlassDenial,
) -> DeleteRecordsPartitionResult {
    let message = denial.to_string();
    tracing::warn!(%topic, partition, refusal = %message, "DeleteRecords refused");
    break_glass_metrics::record_refusal(&env.broker.metrics, denial.action);
    audit_transition(
        &env.broker.audit_log,
        &env.broker.config.break_glass,
        env.ctx,
        &GatedTransition {
            action: BreakGlassAction::DeleteRecords,
            target: &trim_target(topic, partition),
            phase: PrivilegedPhase::Refused,
            proposal_id: denial.proposal_id(),
            reason: &message,
        },
    );
    error_partition_result(partition, codes::POLICY_VIOLATION)
}

/// The break-glass target of one partition.
///
/// A proposal on the bare topic name covers every partition of it, which
/// `gate::authorize` resolves from this spelling.
pub(super) fn trim_target(topic: &str, partition: i32) -> String {
    format!("{topic}-{partition}")
}

/// The proposal that a consumed record names.
///
/// [`gate::authorize`] only ever answers with a proposal record, so the `None`
/// arm costs one match rather than a panic.
pub(super) fn consumed_proposal_id(record: &MetadataRecord) -> Option<Uuid> {
    match record {
        MetadataRecord::V1BreakGlassProposal(proposal) => Some(proposal.proposal_id),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::handlers::delete_records::test_support::{
        NOW_MS, PROPOSAL, approved_proposal, gated_config, image_of,
    };

    #[test]
    fn the_trim_gate_answers_from_the_proposal_registry() {
        let cases: [(&'static str, MetadataImage, BreakGlassConfig, bool); 5] = [
            (
                "an approved proposal on the partition",
                image_of(&[approved_proposal("orders-3")]),
                gated_config(),
                true,
            ),
            (
                "an approved proposal on the whole topic",
                image_of(&[approved_proposal("orders")]),
                gated_config(),
                true,
            ),
            (
                "a proposal for another partition",
                image_of(&[approved_proposal("orders-4")]),
                gated_config(),
                false,
            ),
            ("no proposal at all", image_of(&[]), gated_config(), false),
            (
                "no approver set, so nothing is gated",
                image_of(&[]),
                BreakGlassConfig::default(),
                true,
            ),
        ];
        for (label, image, config, expected) in cases {
            let authorized = authorize_trim(&image, &config, "orders", 3).is_ok();
            check!(authorized == expected, "case {label}");
        }
    }

    #[test]
    fn a_refused_trim_names_the_partition_it_refused() {
        let denial = authorize_trim(&image_of(&[]), &gated_config(), "orders", 3)
            .expect_err("no proposal covers orders-3");

        check!(denial.action == BreakGlassAction::DeleteRecords);
        check!(denial.target == "orders-3");
    }

    #[test]
    fn the_append_carries_the_consume_alone_and_spends_it_once() {
        let consumed =
            MetadataRecord::V1BreakGlassProposal(krabka_metadata::BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS,
                ..approved_proposal("orders")
            });
        let mut spent = HashSet::new();

        let first = consume_records(&mut spent, Some(consumed.clone()));
        let second = consume_records(&mut spent, Some(consumed.clone()));
        let ungated = consume_records(&mut spent, None);

        // A trim writes no metadata record, so the consume is the whole append.
        assert!(first == (Some(PROPOSAL), vec![consumed]));
        assert!(second == (Some(PROPOSAL), vec![]));
        assert!(ungated == (None, vec![]));
    }
}
