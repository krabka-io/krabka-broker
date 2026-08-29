//! Which stored proposals reach a request, and which one of them the gate
//! spends.
//!
//! Target matching decides the set a request can draw on: an exact target
//! always, and the topic of a partition target for the actions that name a
//! partition. When more than one usable proposal remains, the choice is a
//! function of the records alone, so two brokers reading one image spend the
//! same proposal.

use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord};

use crate::break_glass::action_targets_partition;

/// The proposal to spend when more than one covers the request.
///
/// The one that expires first goes first, so the approval that would be lost
/// soonest is the one that gets used. The proposal id breaks a tie, because
/// [`MetadataImage::break_glass_proposals`] does not define an order and two
/// brokers must reach the same answer from one image.
///
/// [`MetadataImage::break_glass_proposals`]: krabka_metadata::MetadataImage::break_glass_proposals
pub(super) fn better_candidate<'a>(
    current: Option<&'a BreakGlassProposalRecord>,
    candidate: &'a BreakGlassProposalRecord,
) -> &'a BreakGlassProposalRecord {
    match current {
        None => candidate,
        Some(current) => {
            let current_key = (current.expires_at_ms, current.proposal_id);
            let candidate_key = (candidate.expires_at_ms, candidate.proposal_id);
            if candidate_key < current_key {
                candidate
            } else {
                current
            }
        }
    }
}

/// Whether a proposal on `proposal_target` covers a request for
/// `request_target`.
pub(super) fn covers(
    proposal_target: &str,
    request_target: &str,
    action: BreakGlassAction,
) -> bool {
    if proposal_target == request_target {
        return true;
    }
    if !action_targets_partition(action) {
        return false;
    }
    topic_of_partition_target(request_target).is_some_and(|topic| proposal_target == topic)
}

/// The topic name in a `"<topic>-<partition>"` target, or `None` when the
/// target does not take that form.
fn topic_of_partition_target(target: &str) -> Option<&str> {
    let (topic, partition) = target.rsplit_once('-')?;
    if topic.is_empty() || partition.is_empty() {
        return None;
    }
    if !partition.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some(topic)
}
