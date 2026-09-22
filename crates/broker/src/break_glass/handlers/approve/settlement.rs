//! The step that runs one request against the stored proposal and writes the
//! result to the metadata log.
//!
//! The decision itself is pure. This is where the record it produces reaches
//! the controller, and where a rejected append becomes a wire error code that
//! an operator can act on.

use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{BreakGlassProposalRecord, MetadataRecord};
use krabka_protocol::krabka::break_glass::ApproveBreakGlassRequest;

use super::{Attempt, decide};
use crate::{
    break_glass::{
        action_name,
        config::BreakGlassPolicy,
        handlers::{
            PrivilegedAudit, Refusal, from_wire_uuid, principal_name, require_privileged,
            submit_error,
        },
    },
    broker::Broker,
    codes,
    handlers::RequestContext,
};

/// Apply the request to the stored proposal and write the result.
pub(super) async fn settle(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    policy: BreakGlassPolicy<'_>,
    stored: Option<&BreakGlassProposalRecord>,
    req: &ApproveBreakGlassRequest,
) -> Result<BreakGlassProposalRecord, Refusal> {
    let stored = stored.ok_or_else(|| {
        Refusal::new(
            codes::RESOURCE_NOT_FOUND,
            format!(
                "no break-glass proposal {}",
                from_wire_uuid(req.proposal_id)
            ),
        )
    })?;
    let updated = decide(
        policy,
        &broker.config.operator_keys,
        stored,
        &Attempt {
            principal: &principal_name(ctx),
            key_id: &req.key_id,
            signature: &req.signature,
            withdraw: req.withdraw,
            now_ms: crate::time_util::now_ms(),
        },
    )?;
    let counterparties: Vec<String> = updated
        .approvals
        .iter()
        .map(|approval| approval.principal.clone())
        .collect();
    let audit = settlement_audit(req, &updated, &counterparties);
    require_privileged(broker.audit_log.as_ref(), ctx, policy.fingerprint(), &audit)
        .await
        .map_err(|error| {
            Refusal::new(
                codes::POLICY_VIOLATION,
                format!("privileged action refused: {error}"),
            )
        })?;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1BreakGlassProposal(updated.clone())])
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, "ApproveBreakGlass: submit_change failed");
            let (code, message) = submit_error(&error);
            Refusal::new(code, message)
        })?;
    Ok(updated)
}

pub(super) fn settlement_audit<'a>(
    req: &'a ApproveBreakGlassRequest,
    updated: &'a BreakGlassProposalRecord,
    counterparties: &'a [String],
) -> PrivilegedAudit<'a> {
    PrivilegedAudit {
        outcome: AuditOutcome::Success,
        phase: if req.withdraw {
            PrivilegedPhase::Consumed
        } else {
            PrivilegedPhase::Approved
        },
        action: action_name(updated.action),
        target: &updated.target,
        proposal_id: Some(updated.proposal_id),
        counterparties,
        key_id: &req.key_id,
        signature: &req.signature,
        signature_verified: !req.key_id.is_empty(),
        reason: &updated.reason,
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_audit::{AuditOutcome, PrivilegedPhase};
    use krabka_metadata::{BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord};
    use krabka_protocol::krabka::break_glass::ApproveBreakGlassRequest;

    use super::settlement_audit;
    use crate::break_glass::handlers::to_wire_uuid;

    #[test]
    fn settlement_audit_verifies_signature_and_sets_phase() {
        let proposal = BreakGlassProposalRecord {
            proposal_id: uuid::Uuid::from_u128(42),
            action: BreakGlassAction::DeleteTopic,
            target: "test-topic".to_owned(),
            proposer: "User:alice".to_owned(),
            reason: "emergency cleanup".to_owned(),
            created_at_ms: 100,
            expires_at_ms: 200,
            approvals: vec![BreakGlassApproval {
                principal: "User:bob".to_owned(),
                approved_at_ms: 150,
                key_id: "bob-key".to_owned(),
                signature: vec![1, 2, 3],
            }],
            consumed_at_ms: 0,
            withdrawn: false,
        };
        let counterparties = vec!["User:bob".to_owned()];

        let req_with_key = ApproveBreakGlassRequest {
            proposal_id: to_wire_uuid(proposal.proposal_id),
            key_id: "bob-key".to_owned(),
            signature: vec![1, 2, 3],
            withdraw: false,
            ..ApproveBreakGlassRequest::default()
        };
        let audit1 = settlement_audit(&req_with_key, &proposal, &counterparties);
        check!(audit1.outcome == AuditOutcome::Success);
        check!(audit1.phase == PrivilegedPhase::Approved);
        check!(audit1.action == "delete_topic");
        check!(audit1.target == "test-topic");
        check!(audit1.proposal_id == Some(uuid::Uuid::from_u128(42)));
        check!(audit1.counterparties == &["User:bob".to_owned()]);
        check!(audit1.key_id == "bob-key");
        check!(audit1.signature == &[1, 2, 3]);
        check!(audit1.signature_verified == true);
        check!(audit1.reason == "emergency cleanup");

        let req_withdraw_no_key = ApproveBreakGlassRequest {
            proposal_id: to_wire_uuid(proposal.proposal_id),
            key_id: String::new(),
            signature: Vec::new(),
            withdraw: true,
            ..ApproveBreakGlassRequest::default()
        };
        let audit2 = settlement_audit(&req_withdraw_no_key, &proposal, &counterparties);
        check!(audit2.outcome == AuditOutcome::Success);
        check!(audit2.phase == PrivilegedPhase::Consumed);
        check!(audit2.key_id == "");
        check!(audit2.signature_verified == false);
    }
}
