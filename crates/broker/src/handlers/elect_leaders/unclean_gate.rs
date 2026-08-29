//! The KFC-9 break-glass gate that an unclean election passes, and the refusal
//! row a partition gets when it does not.
//!
//! An unclean election elects a replica that is missing committed records, so
//! the broker looks up the approved proposal that authorizes it in its own
//! metadata image. A cluster whose `[break_glass]` section names no approver
//! gates nothing, and a preferred election never reaches this module at all.

use krabka_audit::PrivilegedPhase;
use krabka_metadata::{BreakGlassAction, MetadataImage, MetadataRecord};
use krabka_protocol::owned::elect_leaders_response::PartitionResult;
use uuid::Uuid;

use super::env::ElectionEnv;
use crate::{
    break_glass::{
        gate::{self, BreakGlassDenial},
        handlers::audit::{GatedTransition, audit_transition},
        metrics as break_glass_metrics,
    },
    codes,
    config::BreakGlassConfig,
    time_util::now_ms,
};

/// KFC-9: find the approved proposal that authorizes an unclean election of one
/// partition, and stamp it consumed.
///
/// `Ok(None)` is a broker that gates nothing, where `[break_glass]` names no
/// approver. Every transition then behaves as it does on a cluster with no such
/// section, which is what keeps a stock cluster working.
pub(super) fn authorize_unclean(
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
pub(super) fn unclean_target(topic: &str, partition: i32) -> String {
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

/// Refuse one partition: count it, audit it, and build its error row.
pub(super) fn refuse_unclean(
    env: &ElectionEnv<'_>,
    topic: &str,
    partition: i32,
    denial: &BreakGlassDenial,
) -> PartitionResult {
    let message = denial.to_string();
    break_glass_metrics::record_refusal(&env.broker.metrics, denial.action);
    audit_transition(
        &env.broker.audit_log,
        &env.broker.config.break_glass,
        env.ctx,
        &GatedTransition {
            action: BreakGlassAction::UncleanElectLeaders,
            target: &unclean_target(topic, partition),
            phase: PrivilegedPhase::Refused,
            proposal_id: denial.proposal_id(),
            reason: &message,
        },
    );
    PartitionResult {
        partition_id: partition,
        error_code: codes::POLICY_VIOLATION,
        error_message: Some(message),
        ..Default::default()
    }
}
