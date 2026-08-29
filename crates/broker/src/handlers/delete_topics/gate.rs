//! KFC-9: the break-glass two-person rule over a topic deletion.
//!
//! A deletion destroys every record the topic holds, so it is gated. This
//! module builds the record list one deletion appends -- the consumed proposal
//! ahead of the delete record, so a single raft append carries both -- and
//! reads back the proposal that list spends. The freeze check that answers
//! ahead of this gate stays in the module root.

use krabka_metadata::{BreakGlassAction, DeleteTopicRecord, MetadataImage, MetadataRecord};
use uuid::Uuid;

use crate::{
    break_glass::gate::{self, BreakGlassDenial},
    config::BreakGlassConfig,
};

/// The records one topic deletion appends.
///
/// The consumed break-glass proposal goes first, and the delete record follows
/// it, so one raft append carries both. That single append is what stops an
/// approval from being spent twice across a crash: a broker that committed the
/// deletion has committed the consume with it.
///
/// A broker whose `[break_glass]` names no approver gates nothing, and the
/// answer is then the delete record alone.
///
/// # Errors
///
/// Returns the [`BreakGlassDenial`] when no approved proposal covers this
/// topic. The caller answers `POLICY_VIOLATION (44)` on that topic's row.
pub(super) fn delete_topic_records(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    name: &str,
    now_ms: i64,
) -> Result<Vec<MetadataRecord>, BreakGlassDenial> {
    let record = MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
        name: name.to_owned(),
    });
    if !gate::is_gated(config) {
        return Ok(vec![record]);
    }
    let consumed = gate::authorize(image, config, BreakGlassAction::DeleteTopic, name, now_ms)?;
    Ok(vec![consumed, record])
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
    use crate::handlers::delete_topics::test_support::{DOOMED, gated_config};

    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    const NOW_MS: i64 = 60_000;

    /// A proposal that two people approved, and that has not expired.
    fn approved_proposal(target: &str) -> krabka_metadata::BreakGlassProposalRecord {
        krabka_metadata::BreakGlassProposalRecord {
            proposal_id: PROPOSAL,
            action: BreakGlassAction::DeleteTopic,
            target: target.to_owned(),
            proposer: "User:carol".to_owned(),
            reason: "the tenant offboarded".to_owned(),
            created_at_ms: 1_000,
            expires_at_ms: 600_000,
            approvals: vec![
                crate::break_glass::gate::tests::approval("User:alice"),
                crate::break_glass::gate::tests::approval("User:bob"),
            ],
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    fn image_of(proposals: &[krabka_metadata::BreakGlassProposalRecord]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for proposal in proposals {
            image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
        }
        image
    }

    fn deleted() -> MetadataRecord {
        MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
            name: DOOMED.to_owned(),
        })
    }

    #[test]
    fn a_deletion_with_no_proposal_appends_nothing() {
        let denial = delete_topic_records(&image_of(&[]), &gated_config(), DOOMED, NOW_MS)
            .expect_err("no proposal covers the topic");

        check!(denial.action == BreakGlassAction::DeleteTopic);
        check!(
            denial.to_string()
                == "break-glass refused delete_topic on doomed: no approved proposal covers the request"
        );
    }

    #[test]
    fn an_approved_deletion_appends_the_consume_beside_the_delete() {
        let proposal = approved_proposal(DOOMED);
        let image = image_of(std::slice::from_ref(&proposal));

        let records = delete_topic_records(&image, &gated_config(), DOOMED, NOW_MS)
            .expect("the proposal authorizes the deletion");

        let expected = vec![
            MetadataRecord::V1BreakGlassProposal(krabka_metadata::BreakGlassProposalRecord {
                consumed_at_ms: NOW_MS,
                ..proposal
            }),
            deleted(),
        ];
        assert!(records == expected);
    }

    #[test]
    fn a_topic_scoped_proposal_covers_no_other_topic() {
        // `delete_topic` names no partition, so `doomed` never covers
        // `doomed-2024`, which reads as partition 2024 of topic `doomed`.
        let image = image_of(&[approved_proposal(DOOMED)]);

        let denial = delete_topic_records(&image, &gated_config(), "doomed-2024", NOW_MS)
            .expect_err("a proposal for one topic authorizes nothing about another");

        check!(denial.proposal_id() == None);
    }

    #[test]
    fn a_broker_with_no_approver_set_gates_nothing() {
        let records =
            delete_topic_records(&image_of(&[]), &BreakGlassConfig::default(), DOOMED, NOW_MS)
                .expect("an ungated broker deletes with no proposal");

        assert!(records == vec![deleted()]);
    }
}
