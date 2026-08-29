//! Whether one covering proposal can still authorize a transition.
//!
//! The checks run in a fixed order -- withdrawn, consumed, expired, short of
//! approvals, unsigned -- and the first one that holds is the reason the gate
//! reports for that proposal. The approval count is over distinct principals,
//! because the rule the feature promises is a rule about people.

use krabka_metadata::BreakGlassProposalRecord;

use super::DenialReason;
use crate::break_glass::config::BreakGlassPolicy;

/// Why `proposal` cannot authorize a transition now, or `None` when it can.
pub(super) fn unusable_because(
    policy: BreakGlassPolicy<'_>,
    proposal: &BreakGlassProposalRecord,
    now_ms: i64,
) -> Option<DenialReason> {
    let proposal_id = proposal.proposal_id;
    if proposal.withdrawn {
        return Some(DenialReason::Withdrawn { proposal_id });
    }
    if proposal.consumed_at_ms != 0 {
        return Some(DenialReason::Consumed {
            proposal_id,
            consumed_at_ms: proposal.consumed_at_ms,
        });
    }
    if now_ms >= proposal.expires_at_ms {
        return Some(DenialReason::Expired {
            proposal_id,
            expires_at_ms: proposal.expires_at_ms,
        });
    }
    let held = distinct_approvers(proposal);
    let required = policy.required_approvals();
    if held < required {
        return Some(DenialReason::NotEnoughApprovals {
            proposal_id,
            held,
            required,
        });
    }
    if policy.needs_signature(proposal.action) && !every_approval_is_signed(proposal) {
        return Some(DenialReason::Unsigned { proposal_id });
    }
    None
}

/// How many distinct principals approved `proposal`.
///
/// The approve handler already refuses a principal that appears in the list, so
/// this count and the list length agree on every record the handler wrote. The
/// count is over distinct principals anyway, because the rule the feature
/// promises is a rule about people and not about rows.
pub(crate) fn distinct_approvers(proposal: &BreakGlassProposalRecord) -> usize {
    let mut seen: Vec<&str> = Vec::with_capacity(proposal.approvals.len());
    for approval in &proposal.approvals {
        if !seen.contains(&approval.principal.as_str()) {
            seen.push(&approval.principal);
        }
    }
    seen.len()
}

/// Whether every approval on `proposal` carries a key id and a signature.
fn every_approval_is_signed(proposal: &BreakGlassProposalRecord) -> bool {
    !proposal.approvals.is_empty()
        && proposal
            .approvals
            .iter()
            .all(|approval| !approval.key_id.is_empty() && !approval.signature.is_empty())
}
