//! The `PrivilegedAction` event that a gated transition emits.
//!
//! Five handlers gate a privileged transition — an unclean election, an
//! unregistration, a reassignment cancel, a topic deletion, and a record trim
//! — and every one of them records the same event in the same shape. One
//! helper is what keeps a new phase, a new field, or a corrected guard from
//! reaching four of the five.

use crabka_audit::{AuditLog, AuditOutcome, PrivilegedPhase};
use crabka_metadata::BreakGlassAction;
use uuid::Uuid;

use crate::{
    break_glass::{
        action_name, gate,
        handlers::{PrivilegedAudit, audit_privileged},
    },
    config::BreakGlassConfig,
    handlers::RequestContext,
    operator_keys::approver_set_fingerprint,
};

/// One gated transition, as its audit event records it.
///
/// The struct exists so that each call site names the field it fills, which is
/// the reason [`PrivilegedAudit`] is a struct too.
pub(crate) struct GatedTransition<'a> {
    /// The gated transition this event is about.
    pub action: BreakGlassAction,
    /// What the transition applies to, in the spelling the gate resolves.
    pub target: &'a str,
    /// Which step of the workflow this event records.
    pub phase: PrivilegedPhase,
    /// The proposal that authorized the transition, or `None` when none did.
    pub proposal_id: Option<Uuid>,
    /// Free text that says what happened, or why it did not.
    pub reason: &'a str,
}

/// Emit one `PrivilegedAction` event for a gated transition.
///
/// A broker whose `[break_glass]` names no approver emits nothing: it gates
/// nothing, so it has no two-person evidence to record, and this event exists
/// to carry that evidence. The ordinary administrative event already reports
/// the transition itself, so a stock cluster's audit stream is unchanged by
/// the feature.
///
/// `counterparties` stays empty for the reason the freeze events give: the
/// approvers are named on the proposal's own approve events, and the proposal
/// id joins those rows to this one.
///
/// An `Applied` event belongs after the batch append commits. Emitting one
/// before the commit would record a transition that the quorum can still
/// refuse.
pub(crate) fn audit_transition(
    audit_log: &AuditLog,
    config: &BreakGlassConfig,
    ctx: &RequestContext<'_>,
    transition: &GatedTransition<'_>,
) {
    if !gate::is_gated(config) {
        return;
    }
    audit_privileged(
        audit_log,
        ctx,
        approver_set_fingerprint(&config.approvers),
        &PrivilegedAudit {
            outcome: if matches!(transition.phase, PrivilegedPhase::Refused) {
                AuditOutcome::Failure
            } else {
                AuditOutcome::Success
            },
            phase: transition.phase,
            action: action_name(transition.action),
            target: transition.target,
            proposal_id: transition.proposal_id,
            counterparties: &[],
            key_id: "",
            signature: &[],
            signature_verified: false,
            reason: transition.reason,
        },
    );
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use crabka_audit::{AuditEndpoint, AuditEvent, AuditPrincipal};

    use super::*;
    use crate::break_glass::handlers::tests::{audit_channel, context, peer, principal};

    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

    fn gated_config() -> BreakGlassConfig {
        BreakGlassConfig {
            approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
            ..BreakGlassConfig::default()
        }
    }

    fn trim() -> GatedTransition<'static> {
        GatedTransition {
            action: BreakGlassAction::DeleteRecords,
            target: "orders-3",
            phase: PrivilegedPhase::Applied,
            proposal_id: Some(PROPOSAL),
            reason: "records deleted below the trim point",
        }
    }

    #[test]
    fn a_broker_that_names_no_approver_emits_no_privileged_event() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();

        // Every phase is silent, not just the refusals: with no rule there is
        // no two-person evidence for any of them to carry.
        for phase in [
            PrivilegedPhase::Applied,
            PrivilegedPhase::Refused,
            PrivilegedPhase::Consumed,
        ] {
            audit_transition(
                &audit_log,
                &BreakGlassConfig::default(),
                &context(&principal, &peer),
                &GatedTransition { phase, ..trim() },
            );
        }

        check!(events.try_recv().is_err());
    }

    #[test]
    fn a_gated_transition_emits_the_event_the_two_person_rule_needs() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();
        let config = gated_config();

        audit_transition(&audit_log, &config, &context(&principal, &peer), &trim());

        assert!(let Ok(event) = events.try_recv());
        let AuditEvent::PrivilegedAction { time_ms, .. } = &event else {
            panic!("the helper emits a privileged-action event")
        };
        let expected = AuditEvent::PrivilegedAction {
            outcome: AuditOutcome::Success,
            phase: PrivilegedPhase::Applied,
            action: "delete_records".to_owned(),
            target: "orders-3".to_owned(),
            proposal_id: PROPOSAL.to_string(),
            principal: AuditPrincipal {
                name: "User:alice".to_owned(),
                auth_method: "SaslPlain".to_owned(),
            },
            counterparties: Vec::new(),
            approver_set_fingerprint: approver_set_fingerprint(&config.approvers),
            key_id: String::new(),
            signature: Vec::new(),
            signature_verified: false,
            source: AuditEndpoint {
                ip: "10.0.0.7".to_owned(),
                port: 51120,
            },
            reason: "records deleted below the trim point".to_owned(),
            time_ms: *time_ms,
        };
        check!(event == expected);
    }

    #[test]
    fn a_refusal_is_the_one_phase_that_records_a_failure() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();

        for (label, phase, expected) in [
            ("a refusal", PrivilegedPhase::Refused, AuditOutcome::Failure),
            (
                "a consume",
                PrivilegedPhase::Consumed,
                AuditOutcome::Success,
            ),
            (
                "an application",
                PrivilegedPhase::Applied,
                AuditOutcome::Success,
            ),
        ] {
            audit_transition(
                &audit_log,
                &gated_config(),
                &context(&principal, &peer),
                &GatedTransition { phase, ..trim() },
            );

            assert!(let Ok(event) = events.try_recv(), "case {label}");
            let AuditEvent::PrivilegedAction { outcome, .. } = &event else {
                panic!("case {label}: the helper emits a privileged-action event")
            };
            check!(*outcome == expected, "case {label}");
        }
    }

    #[test]
    fn every_action_reaches_the_event_under_its_one_name() {
        let (audit_log, mut events) = audit_channel();
        let principal = principal("alice");
        let peer = peer();

        for action in crate::break_glass::ALL_ACTIONS {
            audit_transition(
                &audit_log,
                &gated_config(),
                &context(&principal, &peer),
                &GatedTransition { action, ..trim() },
            );

            assert!(let Ok(event) = events.try_recv(), "{action:?}");
            let AuditEvent::PrivilegedAction { action: named, .. } = &event else {
                panic!("the helper emits a privileged-action event")
            };
            check!(named.as_str() == action_name(action));
        }
    }
}
