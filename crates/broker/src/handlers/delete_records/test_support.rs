//! KFC-9 fixtures shared by the `DeleteRecords` tests: the gated broker
//! configuration, an approved proposal, and the metadata image that holds it.
//!
//! The gate's unit tests and the end-to-end refusal test both build a broker
//! that names an approver set, so the fixtures live beside neither and are
//! reachable from both.

use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataImage, MetadataRecord};
use uuid::Uuid;

use crate::{config::BreakGlassConfig, time_util::now_ms};

pub(super) const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

pub(super) fn gated_config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
        ..BreakGlassConfig::default()
    }
}

/// A proposal that two people approved, and that has not expired against
/// the wall clock the gate reads.
pub(super) fn approved_proposal(target: &str) -> BreakGlassProposalRecord {
    let now = now_ms();
    BreakGlassProposalRecord {
        proposal_id: PROPOSAL,
        action: BreakGlassAction::DeleteRecords,
        target: target.to_owned(),
        proposer: "User:carol".to_owned(),
        reason: "the tail is poison".to_owned(),
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

pub(super) fn image_of(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    for proposal in proposals {
        image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    }
    image
}
