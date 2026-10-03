use super::*;

/// The stored record that one model state stands for.
pub(super) fn record(state: &ProposalState) -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        proposal_id: Uuid::from_u128(1),
        action: ACTION,
        target: TARGET.to_owned(),
        proposer: PROPOSER.to_owned(),
        reason: "incident 42".to_owned(),
        created_at_ms: 0,
        expires_at_ms: EXPIRES_AT,
        approvals: state
            .approvals
            .iter()
            .map(|principal| BreakGlassApproval {
                principal: (*principal).to_owned(),
                approved_at_ms: 0,
                key_id: String::new(),
                signature: Vec::new(),
            })
            .collect(),
        consumed_at_ms: i64::from(state.consumed),
        withdrawn: state.withdrawn,
    }
}

/// The image that a gated handler reads for one model state.
pub(super) fn image_of(state: &ProposalState) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1BreakGlassProposal(record(state)));
    image
}

/// How many different principals appear in `approvals`.
pub(super) fn distinct(approvals: &[&'static str]) -> usize {
    let mut seen: Vec<&str> = Vec::with_capacity(approvals.len());
    for principal in approvals {
        if !seen.contains(principal) {
            seen.push(principal);
        }
    }
    seen.len()
}

pub(super) fn config(approvers: &[&str], required_approvals: usize) -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: approvers.iter().map(|name| (*name).to_owned()).collect(),
        required_approvals,
        proposal_ttl: millis(u32::try_from(EXPIRES_AT).expect("a small logical expiry")),
        signed_actions: Vec::new(),
        ..BreakGlassConfig::default()
    }
}
