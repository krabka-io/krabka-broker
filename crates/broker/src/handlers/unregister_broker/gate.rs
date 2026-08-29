//! KFC-9: the break-glass two-person rule over a broker unregistration.
//!
//! Dropping a broker takes its endpoints out of every `Metadata` response, so
//! it is gated. This module builds the record list one unregistration appends
//! -- the consumed proposal ahead of the unregister record, so a single raft
//! append carries both -- spells the target that a proposal must name, and
//! reads back the proposal that list spends.

use krabka_metadata::{
    BreakGlassAction, MetadataImage, MetadataRecord, NodeId, UnregisterBrokerRecord,
};
use uuid::Uuid;

use crate::{
    break_glass::gate::{self, BreakGlassDenial},
    config::BreakGlassConfig,
};

/// The records one unregistration appends.
///
/// The consumed break-glass proposal goes first, and the unregister record
/// follows it, so one raft append carries both. That single append is what
/// stops an approval from being spent twice across a crash: a broker that
/// committed the transition has committed the consume with it.
///
/// A broker whose `[break_glass]` names no approver gates nothing, and the
/// answer is then the unregister record alone.
///
/// # Errors
///
/// Returns the [`BreakGlassDenial`] when no approved proposal covers this
/// broker id. The caller answers `POLICY_VIOLATION (44)` with its text.
pub(super) fn unregister_records(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    node_id: NodeId,
    now_ms: i64,
) -> Result<Vec<MetadataRecord>, BreakGlassDenial> {
    let record = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord { node_id });
    if !gate::is_gated(config) {
        return Ok(vec![record]);
    }
    let consumed = gate::authorize(
        image,
        config,
        BreakGlassAction::UnregisterBroker,
        &broker_target(node_id),
        now_ms,
    )?;
    Ok(vec![consumed, record])
}

/// The break-glass target of one broker: its id in decimal, as an operator
/// spells it on `krabka-guard break-glass propose --target`.
///
/// `UnregisterBroker` names no partition, so the gate takes this target
/// exactly and no wider proposal covers it.
pub(super) fn broker_target(node_id: NodeId) -> String {
    node_id.0.to_string()
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
