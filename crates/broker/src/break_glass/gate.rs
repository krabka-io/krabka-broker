//! The gate that every break-glass transition calls.
//!
//! [`authorize`] finds an approved proposal that covers a request and returns
//! the metadata record that spends it. Every gated handler goes through this
//! one function, so one set of rules decides what an approval authorizes.

use krabka_metadata::{BreakGlassAction, BreakGlassProposalRecord, MetadataImage, MetadataRecord};
use uuid::Uuid;

use crate::{
    break_glass::{action_name, action_targets_partition, config::BreakGlassPolicy},
    config::BreakGlassConfig,
};

/// Whether this broker gates the privileged transitions at all.
///
/// A caller asks this first. An empty `break_glass.approvers` turns the
/// workflow off, and every gated transition then behaves as it does on a
/// cluster with no `[break_glass]` section. [`authorize`] on such a broker
/// denies with [`DenialReason::NoProposal`], because there is no proposal for
/// it to return, so a caller that skips this test refuses every transition.
pub(crate) fn is_gated(config: &BreakGlassConfig) -> bool {
    BreakGlassPolicy::new(config).is_enabled()
}

/// Find the approved proposal that authorizes `action` on `target`, and return
/// the record that spends it.
///
/// # The caller must append the record in the same `submit_change` call
///
/// The returned record is the stored proposal with `consumed_at_ms` stamped.
/// **The caller prepends it to its own records and submits one raft append.**
/// That single append is the whole reason a proposal lives in the metadata log:
/// the consume of the approval and the transition it authorizes commit
/// together. A caller that submits the two separately spends the approval twice
/// after a crash between them, or loses it. Nothing in the type system enforces
/// this, so a caller that ignores the rule breaks the guarantee silently.
///
/// # What makes a proposal usable
///
/// The proposal must name `action`, must cover `target`, must not be withdrawn,
/// must not be consumed, must not have expired, and must hold approvals from at
/// least `break_glass.required_approvals` distinct principals. Every approval
/// must also carry a signature when `break_glass.signed_actions` names the
/// action.
///
/// Two concurrent approvals cannot overwrite each other, because
/// [`MetadataImage::validate`] refuses a record whose approval list is not a
/// strict extension of the stored list, and refuses any change to a consumed or
/// a withdrawn proposal. That is the concurrency guard for the approval list,
/// and this function relies on it rather than repeating it.
///
/// # Target matching
///
/// A proposal covers a request when the two targets are equal. A proposal on a
/// bare topic name also covers `"<topic>-<partition>"`, so one proposal can
/// authorize the same action on every partition of a topic. The wider rule
/// applies only to the actions that name a partition, which
/// [`action_targets_partition`] lists. Without that limit a proposal to delete
/// the topic `logs` would also cover the topic `logs-2024`, whose name reads as
/// partition 2024 of topic `logs`.
///
/// # The approver set is not read here
///
/// The broker checks `break_glass.approvers` when a person approves, and never
/// when it spends the approval. A second check here would make the consume
/// depend on a per-node file value, and two brokers can legitimately disagree
/// about that value during a rolling configuration change. The operator-facing
/// consequence is the right one as well: removing a person stops that person
/// from making new approvals, and it does not silently invalidate an incident
/// response that is already under way. The time to live is the safety bound.
///
/// `break_glass.signed_actions` is read here, because it answers a different
/// question. The approver set answers "may this person approve", which is
/// settled when they approve. `signed_actions` answers "does this action need a
/// signature", which is a property of the transition the broker is about to do,
/// so the broker answers it when it acts.
///
/// # Errors
///
/// Returns [`BreakGlassDenial`] when no proposal covers the request, or when
/// the covering proposal is withdrawn, consumed, expired, short of approvals,
/// or unsigned for an action that needs a signature. The caller picks the wire
/// code: `POLICY_VIOLATION` (44) on a Kafka API, and
/// `BREAK_GLASS_APPROVAL_REQUIRED` (1006) on the private thaw path.
pub(crate) fn authorize(
    image: &MetadataImage,
    config: &BreakGlassConfig,
    action: BreakGlassAction,
    target: &str,
    now_ms: i64,
) -> Result<MetadataRecord, BreakGlassDenial> {
    let policy = BreakGlassPolicy::new(config);
    let mut usable: Option<&BreakGlassProposalRecord> = None;
    let mut denial: Option<DenialReason> = None;

    for proposal in image.break_glass_proposals() {
        if proposal.action != action || !covers(&proposal.target, target, action) {
            continue;
        }
        match unusable_because(policy, proposal, now_ms) {
            None => usable = Some(better_candidate(usable, proposal)),
            Some(reason) => denial = Some(nearer_reason(denial, reason)),
        }
    }

    match usable {
        Some(proposal) => Ok(MetadataRecord::V1BreakGlassProposal(
            BreakGlassProposalRecord {
                // `0` is the unconsumed sentinel, so a clock that reads zero
                // still has to stamp a consumed record.
                consumed_at_ms: now_ms.max(1),
                ..proposal.clone()
            },
        )),
        None => Err(BreakGlassDenial {
            action,
            target: target.to_owned(),
            reason: denial.unwrap_or(DenialReason::NoProposal),
        }),
    }
}

/// A refused privileged transition, and the evidence for the refusal.
///
/// The caller picks the wire code from its own context, because the same
/// refusal reaches a Kafka client as `POLICY_VIOLATION` (44) and reaches the
/// private thaw path as `BREAK_GLASS_APPROVAL_REQUIRED` (1006). The `Display`
/// text is what a response carries as its `error_message`, and what the audit
/// event carries as its reason.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("break-glass refused {} on {}: {}", action_name(*.action), .target, .reason)]
pub(crate) struct BreakGlassDenial {
    /// The transition the caller asked for.
    pub action: BreakGlassAction,
    /// The target the caller asked for, in the form the caller uses.
    pub target: String,
    /// Why no approval authorized the transition.
    pub reason: DenialReason,
}

impl BreakGlassDenial {
    /// The proposal the refusal names, or `None` when no proposal covers the
    /// request.
    ///
    /// An audit event carries this id, so an operator can join the refusal to
    /// the proposal that nearly authorized it.
    pub(crate) fn proposal_id(&self) -> Option<Uuid> {
        match self.reason {
            DenialReason::NoProposal => None,
            DenialReason::NotEnoughApprovals { proposal_id, .. }
            | DenialReason::Unsigned { proposal_id }
            | DenialReason::Expired { proposal_id, .. }
            | DenialReason::Withdrawn { proposal_id }
            | DenialReason::Consumed { proposal_id, .. } => Some(proposal_id),
        }
    }
}

/// Why the gate refused a transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum DenialReason {
    /// No proposal names the action and covers the target.
    #[error("no approved proposal covers the request")]
    NoProposal,
    /// A proposal covers the request, and too few distinct principals approved
    /// it.
    #[error("proposal {proposal_id} holds {held} of {required} approvals")]
    NotEnoughApprovals {
        /// The proposal that is short of approvals.
        proposal_id: Uuid,
        /// Distinct principals that approved it.
        held: usize,
        /// Distinct principals it needs.
        required: usize,
    },
    /// The action needs a signed approval, and an approval carries none.
    #[error("proposal {proposal_id} carries an unsigned approval and the action needs a signature")]
    Unsigned {
        /// The proposal with the unsigned approval.
        proposal_id: Uuid,
    },
    /// The proposal passed its expiry time.
    #[error("proposal {proposal_id} expired at {expires_at_ms}")]
    Expired {
        /// The proposal that expired.
        proposal_id: Uuid,
        /// Epoch milliseconds at which it expired.
        expires_at_ms: i64,
    },
    /// An operator withdrew the proposal.
    #[error("proposal {proposal_id} is withdrawn")]
    Withdrawn {
        /// The withdrawn proposal.
        proposal_id: Uuid,
    },
    /// Another transition already spent the proposal.
    #[error("proposal {proposal_id} was consumed at {consumed_at_ms}")]
    Consumed {
        /// The spent proposal.
        proposal_id: Uuid,
        /// Epoch milliseconds at which it was spent.
        consumed_at_ms: i64,
    },
}

impl DenialReason {
    /// How near this proposal came to authorizing the transition. A lower rank
    /// is nearer.
    ///
    /// The gate reports the nearest refusal, because that is the one an
    /// operator can act on. A proposal one approval short is a better answer
    /// than a proposal somebody withdrew last week.
    fn rank(self) -> u8 {
        match self {
            DenialReason::NotEnoughApprovals { .. } => 0,
            DenialReason::Unsigned { .. } => 1,
            DenialReason::Expired { .. } => 2,
            DenialReason::Withdrawn { .. } => 3,
            DenialReason::Consumed { .. } => 4,
            DenialReason::NoProposal => 5,
        }
    }

    /// The proposal this reason names, or the nil id for
    /// [`DenialReason::NoProposal`]. It breaks a tie between two reasons of one
    /// rank, so the answer does not depend on the image iteration order.
    fn tie_break(self) -> Uuid {
        match self {
            DenialReason::NoProposal => Uuid::nil(),
            DenialReason::NotEnoughApprovals { proposal_id, .. }
            | DenialReason::Unsigned { proposal_id }
            | DenialReason::Expired { proposal_id, .. }
            | DenialReason::Withdrawn { proposal_id }
            | DenialReason::Consumed { proposal_id, .. } => proposal_id,
        }
    }
}

/// Why `proposal` cannot authorize a transition now, or `None` when it can.
fn unusable_because(
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

/// The proposal to spend when more than one covers the request.
///
/// The one that expires first goes first, so the approval that would be lost
/// soonest is the one that gets used. The proposal id breaks a tie, because
/// [`MetadataImage::break_glass_proposals`] does not define an order and two
/// brokers must reach the same answer from one image.
fn better_candidate<'a>(
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

/// The refusal to report when more than one covering proposal is unusable.
fn nearer_reason(current: Option<DenialReason>, candidate: DenialReason) -> DenialReason {
    match current {
        None => candidate,
        Some(current) => {
            if (candidate.rank(), candidate.tie_break()) < (current.rank(), current.tie_break()) {
                candidate
            } else {
                current
            }
        }
    }
}

/// Whether a proposal on `proposal_target` covers a request for
/// `request_target`.
fn covers(proposal_target: &str, request_target: &str, action: BreakGlassAction) -> bool {
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

#[cfg(test)]
pub(crate) mod tests {
    use assert2::{assert, check};
    use krabka_metadata::BreakGlassApproval;
    use krabka_units::minutes;

    use super::*;
    use crate::break_glass::ALL_ACTIONS;

    pub(crate) const CREATED_MS: i64 = 1_770_000_000_000;
    pub(crate) const EXPIRES_MS: i64 = 1_770_000_180_000;
    pub(crate) const NOW_MS: i64 = 1_770_000_060_000;

    pub(crate) fn config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob", "User:carol"]
                .map(str::to_owned)
                .to_vec(),
            required_approvals: 2,
            proposal_ttl: minutes(3),
            signed_actions: Vec::new(),
            ..BreakGlassConfig::default()
        }
    }

    pub(crate) fn approval(principal: &str) -> BreakGlassApproval {
        BreakGlassApproval {
            principal: principal.to_owned(),
            approved_at_ms: CREATED_MS + 1_000,
            key_id: String::new(),
            signature: Vec::new(),
        }
    }

    pub(crate) fn signed_approval(principal: &str, key_id: &str) -> BreakGlassApproval {
        BreakGlassApproval {
            key_id: key_id.to_owned(),
            signature: vec![7; 64],
            ..approval(principal)
        }
    }

    pub(crate) fn proposal(
        id: u128,
        action: BreakGlassAction,
        target: &str,
    ) -> BreakGlassProposalRecord {
        BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(id),
            action,
            target: target.to_owned(),
            proposer: "User:alice".to_owned(),
            reason: "incident 42".to_owned(),
            created_at_ms: CREATED_MS,
            expires_at_ms: EXPIRES_MS,
            approvals: vec![approval("User:bob"), approval("User:carol")],
            consumed_at_ms: 0,
            withdrawn: false,
        }
    }

    pub(crate) fn image_of(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        for proposal in proposals {
            image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
        }
        image
    }

    fn consumed_record(record: &MetadataRecord) -> &BreakGlassProposalRecord {
        assert!(let MetadataRecord::V1BreakGlassProposal(record) = record);
        record
    }

    #[test]
    fn an_approved_proposal_comes_back_stamped_consumed() {
        let approved = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
        let image = image_of(std::slice::from_ref(&approved));

        let record = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        )
        .expect("the proposal authorizes the deletion");

        let expected = BreakGlassProposalRecord {
            consumed_at_ms: NOW_MS,
            ..approved
        };
        check!(consumed_record(&record) == &expected);
    }

    #[test]
    fn the_gate_refuses_a_proposal_that_is_not_usable() {
        let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
        let id = base.proposal_id;
        let cases = [
            (
                "one approval short of the rule",
                BreakGlassProposalRecord {
                    approvals: vec![approval("User:bob")],
                    ..base.clone()
                },
                DenialReason::NotEnoughApprovals {
                    proposal_id: id,
                    held: 1,
                    required: 2,
                },
            ),
            (
                "two approvals from one principal",
                BreakGlassProposalRecord {
                    approvals: vec![approval("User:bob"), approval("User:bob")],
                    ..base.clone()
                },
                DenialReason::NotEnoughApprovals {
                    proposal_id: id,
                    held: 1,
                    required: 2,
                },
            ),
            (
                "no approval at all",
                BreakGlassProposalRecord {
                    approvals: Vec::new(),
                    ..base.clone()
                },
                DenialReason::NotEnoughApprovals {
                    proposal_id: id,
                    held: 0,
                    required: 2,
                },
            ),
            (
                "an expired proposal",
                BreakGlassProposalRecord {
                    expires_at_ms: NOW_MS,
                    ..base.clone()
                },
                DenialReason::Expired {
                    proposal_id: id,
                    expires_at_ms: NOW_MS,
                },
            ),
            (
                "a withdrawn proposal",
                BreakGlassProposalRecord {
                    withdrawn: true,
                    ..base.clone()
                },
                DenialReason::Withdrawn { proposal_id: id },
            ),
            (
                "an already consumed proposal",
                BreakGlassProposalRecord {
                    consumed_at_ms: NOW_MS - 1,
                    ..base.clone()
                },
                DenialReason::Consumed {
                    proposal_id: id,
                    consumed_at_ms: NOW_MS - 1,
                },
            ),
        ];
        for (label, stored, reason) in cases {
            let image = image_of(&[stored]);
            let expected = BreakGlassDenial {
                action: BreakGlassAction::DeleteTopic,
                target: "doomed".to_owned(),
                reason,
            };

            let outcome = authorize(
                &image,
                &config(),
                BreakGlassAction::DeleteTopic,
                "doomed",
                NOW_MS,
            );

            check!(outcome == Err(expected), "case {label}");
        }
    }

    #[test]
    fn an_action_that_needs_a_signature_refuses_an_unsigned_approval() {
        let config = BreakGlassConfig {
            signed_actions: vec!["delete_topic".to_owned()],
            ..config()
        };
        let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
        let id = base.proposal_id;
        let cases = [
            (
                "no approval carries a signature",
                vec![approval("User:bob"), approval("User:carol")],
                Err(DenialReason::Unsigned { proposal_id: id }),
            ),
            (
                "one of two approvals is unsigned",
                vec![
                    signed_approval("User:bob", "bob-yubi"),
                    approval("User:carol"),
                ],
                Err(DenialReason::Unsigned { proposal_id: id }),
            ),
            (
                "an approval carries a key id and no signature",
                vec![
                    signed_approval("User:bob", "bob-yubi"),
                    BreakGlassApproval {
                        signature: Vec::new(),
                        ..signed_approval("User:carol", "carol-yubi")
                    },
                ],
                Err(DenialReason::Unsigned { proposal_id: id }),
            ),
            (
                "every approval is signed",
                vec![
                    signed_approval("User:bob", "bob-yubi"),
                    signed_approval("User:carol", "carol-yubi"),
                ],
                Ok(()),
            ),
        ];
        for (label, approvals, expected) in cases {
            let image = image_of(&[BreakGlassProposalRecord {
                approvals,
                ..base.clone()
            }]);

            let outcome = authorize(
                &image,
                &config,
                BreakGlassAction::DeleteTopic,
                "doomed",
                NOW_MS,
            );

            check!(outcome.is_ok() == expected.is_ok(), "case {label}");
            if let Err(reason) = expected {
                assert!(let Err(denial) = outcome, "case {label}");
                check!(denial.reason == reason, "case {label}");
            }
        }
    }

    #[test]
    fn an_unsigned_approval_stays_usable_when_the_action_needs_no_signature() {
        let image = image_of(&[proposal(1, BreakGlassAction::DeleteTopic, "doomed")]);

        let outcome = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        );

        check!(outcome.is_ok());
    }

    #[test]
    fn a_proposal_covers_a_target_by_the_documented_rule() {
        let cases = [
            (
                "the same partition",
                BreakGlassAction::DeleteRecords,
                "orders-3",
                "orders-3",
                true,
            ),
            (
                "the topic of the partition",
                BreakGlassAction::DeleteRecords,
                "orders",
                "orders-3",
                true,
            ),
            (
                "a topic name that itself holds a dash",
                BreakGlassAction::DeleteRecords,
                "my-orders",
                "my-orders-11",
                true,
            ),
            (
                "another partition of the same topic",
                BreakGlassAction::DeleteRecords,
                "orders-4",
                "orders-3",
                false,
            ),
            (
                "another topic",
                BreakGlassAction::DeleteRecords,
                "payments",
                "orders-3",
                false,
            ),
            (
                "a partition proposal does not cover the whole topic",
                BreakGlassAction::DeleteRecords,
                "orders-3",
                "orders",
                false,
            ),
            (
                "a topic-scoped action takes the exact target only",
                BreakGlassAction::DeleteTopic,
                "logs",
                "logs-2024",
                false,
            ),
            (
                "a topic-scoped action on its own topic",
                BreakGlassAction::DeleteTopic,
                "logs-2024",
                "logs-2024",
                true,
            ),
            (
                "a non-numeric suffix is part of the topic name",
                BreakGlassAction::DeleteRecords,
                "orders",
                "orders-east",
                false,
            ),
            (
                "an empty partition suffix",
                BreakGlassAction::DeleteRecords,
                "orders",
                "orders-",
                false,
            ),
            (
                "an empty topic before the suffix",
                BreakGlassAction::DeleteRecords,
                "",
                "-3",
                false,
            ),
            (
                "a broker id target",
                BreakGlassAction::UnregisterBroker,
                "7",
                "7",
                true,
            ),
            (
                "a freeze scope target",
                BreakGlassAction::ThawTopicFreeze,
                "literal:orders",
                "literal:orders",
                true,
            ),
        ];
        for (label, action, proposal_target, request_target, expected) in cases {
            let image = image_of(&[proposal(1, action, proposal_target)]);

            let outcome = authorize(&image, &config(), action, request_target, NOW_MS);

            check!(outcome.is_ok() == expected, "case {label}");
        }
    }

    #[test]
    fn a_proposal_for_another_action_never_covers_the_request() {
        for stored in ALL_ACTIONS {
            let image = image_of(&[proposal(1, stored, "orders")]);
            for asked in ALL_ACTIONS {
                let outcome = authorize(&image, &config(), asked, "orders", NOW_MS);
                check!(
                    outcome.is_ok() == (stored == asked),
                    "{} stored, {} asked",
                    action_name(stored),
                    action_name(asked)
                );
            }
        }
    }

    #[test]
    fn an_empty_registry_refuses_with_no_proposal() {
        let image = MetadataImage::new(Uuid::nil());

        let outcome = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        );

        check!(
            outcome
                == Err(BreakGlassDenial {
                    action: BreakGlassAction::DeleteTopic,
                    target: "doomed".to_owned(),
                    reason: DenialReason::NoProposal,
                })
        );
    }

    #[test]
    fn the_gate_reports_the_refusal_that_came_nearest() {
        let base = proposal(1, BreakGlassAction::DeleteTopic, "doomed");
        let short = BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(2),
            approvals: vec![approval("User:bob")],
            ..base.clone()
        };
        let withdrawn = BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(3),
            withdrawn: true,
            ..base.clone()
        };
        let image = image_of(&[withdrawn, short]);

        let outcome = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        );

        assert!(let Err(denial) = outcome);
        check!(
            denial.reason
                == DenialReason::NotEnoughApprovals {
                    proposal_id: Uuid::from_u128(2),
                    held: 1,
                    required: 2,
                }
        );
        check!(denial.proposal_id() == Some(Uuid::from_u128(2)));
    }

    #[test]
    fn the_gate_spends_the_proposal_that_expires_first() {
        let early = BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(9),
            expires_at_ms: NOW_MS + 1_000,
            ..proposal(9, BreakGlassAction::DeleteTopic, "doomed")
        };
        let late = BreakGlassProposalRecord {
            proposal_id: Uuid::from_u128(2),
            expires_at_ms: NOW_MS + 60_000,
            ..proposal(2, BreakGlassAction::DeleteTopic, "doomed")
        };
        let image = image_of(&[late, early]);

        let record = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            NOW_MS,
        )
        .expect("one of the two proposals authorizes the deletion");

        check!(consumed_record(&record).proposal_id == Uuid::from_u128(9));
    }

    #[test]
    fn a_denial_names_no_proposal_when_none_covers_the_request() {
        let denial = BreakGlassDenial {
            action: BreakGlassAction::DeleteTopic,
            target: "doomed".to_owned(),
            reason: DenialReason::NoProposal,
        };

        check!(denial.proposal_id() == None);
        check!(denial.to_string().contains("delete_topic"));
        check!(denial.to_string().contains("doomed"));
    }

    #[test]
    fn a_broker_with_no_approver_set_gates_nothing() {
        let off = BreakGlassConfig::default();

        check!(!is_gated(&off));
        check!(is_gated(&config()));
    }

    #[test]
    fn a_clock_that_reads_zero_still_stamps_a_consumed_record() {
        let image = image_of(&[BreakGlassProposalRecord {
            created_at_ms: -1,
            expires_at_ms: 1,
            ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
        }]);

        let record = authorize(
            &image,
            &config(),
            BreakGlassAction::DeleteTopic,
            "doomed",
            0,
        )
        .expect("the proposal has not expired at zero");

        check!(consumed_record(&record).consumed_at_ms == 1);
    }
}
