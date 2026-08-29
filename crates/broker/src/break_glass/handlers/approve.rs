//! `ApproveBreakGlass`, api key 1018.
//!
//! One request adds one approval to a break-glass proposal, or withdraws the
//! proposal. The `withdraw` flag picks between the two, which follows
//! `AlterableBarrierGroup` and its `delete` flag.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. A denied request
//! answers `CLUSTER_AUTHORIZATION_FAILED` (31).
//!
//! # Three checks make it a two-person rule
//!
//! The approver must be in `break_glass.approvers`, must not be the proposer,
//! and must not already appear in the approval list. Without all three the rule
//! is a two-click rule.
//!
//! # The broker reads the approver set here, and not when it acts
//!
//! `break_glass.approvers` comes from each broker's own file. This handler is
//! the only place that reads it. The gate that spends an approval never reads
//! it again, for two reasons.
//!
//! A second check at consumption time would make the consume non-deterministic
//! across brokers. The set is a per-node file value, and two nodes can
//! legitimately disagree during a rolling configuration change. Two brokers have
//! to reach the same answer about one record.
//!
//! The operator-facing consequence is also the right one. An operator who
//! removes a person stops that person from making new approvals. The removal
//! does not silently invalidate an incident response that is already under way.
//! The safety bound is `break_glass.proposal_ttl`: wait it out, and every
//! pending approval by that principal is dead.
//!
//! Each audit event records
//! [`BreakGlassPolicy::fingerprint`](crate::break_glass::config::BreakGlassPolicy::fingerprint),
//! so a broker that disagrees with its peers about the set is visible in the
//! audit log after the fact.

use bytes::Bytes;
use krabka_audit::{AuditOutcome, PrivilegedPhase};
use krabka_metadata::{BreakGlassApproval, BreakGlassProposalRecord, MetadataRecord};
use krabka_protocol::{
    Decode,
    krabka::break_glass::{ApproveBreakGlassRequest, ApproveBreakGlassResponse},
};
use uuid::Uuid;

use crate::{
    break_glass::{
        action_name,
        config::BreakGlassPolicy,
        gate::distinct_approvers,
        handlers::{
            PrivilegedAudit, Refusal, UNKNOWN_ACTION, audit_privileged, from_wire_uuid,
            principal_name, submit_error,
        },
        signing::approval_signing_bytes,
    },
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{RequestContext, cluster_alter_denied, encode_response},
    operator_keys::OperatorKeys,
};

#[tracing::instrument(
    name = "handle_approve_break_glass",
    level = "info",
    skip_all,
    fields(api = "ApproveBreakGlass"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut cur = req_bytes;
    let req = ApproveBreakGlassRequest::decode(&mut cur, version)?;

    let policy = BreakGlassPolicy::new(&broker.config.break_glass);
    let image = broker.controller.current_image();
    let stored = image.break_glass_proposal(from_wire_uuid(req.proposal_id));

    let outcome = if cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        Err(Refusal::new(
            codes::CLUSTER_AUTHORIZATION_FAILED,
            "approve-break-glass denied",
        ))
    } else {
        settle(broker, ctx, policy, stored, &req).await
    };

    let report = Report::of(stored, outcome.as_ref().ok(), policy);
    audit_privileged(
        broker.audit_log.as_ref(),
        ctx,
        policy.fingerprint(),
        &PrivilegedAudit {
            outcome: if outcome.is_ok() {
                AuditOutcome::Success
            } else {
                AuditOutcome::Failure
            },
            phase: phase_of(&outcome, req.withdraw),
            action: report.action,
            target: report.target,
            proposal_id: report.proposal_id,
            counterparties: &report.counterparties,
            key_id: &req.key_id,
            signature: &req.signature,
            signature_verified: outcome.is_ok() && !req.key_id.is_empty(),
            reason: reason(&outcome),
        },
    );

    let response = match outcome {
        Ok(_) => ApproveBreakGlassResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            approvals_held: report.held,
            approvals_required: report.required,
            ..ApproveBreakGlassResponse::default()
        },
        Err(refusal) => ApproveBreakGlassResponse {
            throttle_time_ms: 0,
            error_code: refusal.code,
            error_message: Some(refusal.message),
            approvals_held: report.held,
            approvals_required: report.required,
            ..ApproveBreakGlassResponse::default()
        },
    };
    encode_response(&response, version)
}

/// The phase an audit event records for one settled request.
fn phase_of(
    outcome: &Result<BreakGlassProposalRecord, Refusal>,
    withdraw: bool,
) -> PrivilegedPhase {
    match (outcome, withdraw) {
        (Err(_), _) => PrivilegedPhase::Refused,
        // A withdrawal spends the proposal without doing the action, so it is
        // the same end of the lifecycle that a consume reaches.
        (Ok(_), true) => PrivilegedPhase::Consumed,
        (Ok(_), false) => PrivilegedPhase::Approved,
    }
}

/// Apply the request to the stored proposal and write the result.
async fn settle(
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

/// What one caller asked to do to one proposal.
pub(crate) struct Attempt<'a> {
    /// The authenticated principal of the connection.
    pub principal: &'a str,
    /// The operator key that made `signature`, or an empty string.
    pub key_id: &'a str,
    /// The detached signature, or an empty slice.
    pub signature: &'a [u8],
    /// `true` withdraws the proposal instead of approving it.
    pub withdraw: bool,
    /// The controller's clock, in epoch milliseconds.
    pub now_ms: i64,
}

/// The proposal record that one approval or one withdrawal produces.
///
/// The result is the stored record with one approval appended, or with
/// `withdrawn` set. The caller writes it to the metadata log, where
/// [`MetadataImage::validate`](krabka_metadata::MetadataImage::validate)
/// refuses it if another approval landed first and this record does not extend
/// that list.
///
/// A withdrawal ignores `key_id` and `signature`. The proposer or any
/// configured approver may withdraw, because a withdrawal only takes authority
/// away and can never create any.
///
/// A caller that supplies a signature gets it verified even when the action
/// needs no signature. The broker never stores a signature it did not check.
/// So an `Ok` result with a non-empty `key_id` is proof that the signature
/// verified.
///
/// # Errors
///
/// Returns [`Refusal`] with:
///
/// - `POLICY_VIOLATION` (44) when the proposal is expired, withdrawn, or
///   consumed.
/// - `BREAK_GLASS_NOT_AN_APPROVER` (1008) when the principal is outside the
///   approver set.
/// - `BREAK_GLASS_DUPLICATE_APPROVER` (1007) when the principal proposed the
///   action, or already approved it.
/// - `OPERATOR_SIGNATURE_REQUIRED` (1010) when the action needs a signature and
///   the request carries none.
/// - `OPERATOR_SIGNATURE_INVALID` (1009) when the signature does not verify
///   against the trusted key set under the approving principal.
pub(crate) fn decide(
    policy: BreakGlassPolicy<'_>,
    keys: &OperatorKeys,
    stored: &BreakGlassProposalRecord,
    attempt: &Attempt<'_>,
) -> Result<BreakGlassProposalRecord, Refusal> {
    settled_state(stored)?;
    if attempt.withdraw {
        if !policy.is_approver(attempt.principal) && attempt.principal != stored.proposer {
            return Err(not_an_approver(attempt.principal));
        }
        return Ok(BreakGlassProposalRecord {
            withdrawn: true,
            ..stored.clone()
        });
    }
    if attempt.now_ms >= stored.expires_at_ms {
        return Err(Refusal::new(
            codes::POLICY_VIOLATION,
            format!(
                "break-glass proposal {} expired at {}",
                stored.proposal_id, stored.expires_at_ms
            ),
        ));
    }
    if !policy.is_approver(attempt.principal) {
        return Err(not_an_approver(attempt.principal));
    }
    if attempt.principal == stored.proposer {
        return Err(Refusal::new(
            codes::BREAK_GLASS_DUPLICATE_APPROVER,
            format!(
                "{} proposed this action and cannot also approve it",
                attempt.principal
            ),
        ));
    }
    if stored
        .approvals
        .iter()
        .any(|approval| approval.principal == attempt.principal)
    {
        return Err(Refusal::new(
            codes::BREAK_GLASS_DUPLICATE_APPROVER,
            format!("{} already approved this proposal", attempt.principal),
        ));
    }
    check_signature(policy, keys, stored, attempt)?;

    let mut approvals = stored.approvals.clone();
    approvals.push(BreakGlassApproval {
        principal: attempt.principal.to_owned(),
        approved_at_ms: attempt.now_ms,
        key_id: attempt.key_id.to_owned(),
        signature: attempt.signature.to_vec(),
    });
    Ok(BreakGlassProposalRecord {
        approvals,
        ..stored.clone()
    })
}

/// Refuse a principal that this broker does not know as an approver.
fn not_an_approver(principal: &str) -> Refusal {
    Refusal::new(
        codes::BREAK_GLASS_NOT_AN_APPROVER,
        format!("{principal} is not a break-glass approver"),
    )
}

/// Refuse a proposal that already reached the end of its lifecycle.
///
/// A withdrawal and an approval both stop here. The image validator refuses the
/// record either way, and a refusal that names the state is a better answer to
/// an operator than a rejected raft append.
fn settled_state(stored: &BreakGlassProposalRecord) -> Result<(), Refusal> {
    if stored.withdrawn {
        return Err(Refusal::new(
            codes::POLICY_VIOLATION,
            format!("break-glass proposal {} is withdrawn", stored.proposal_id),
        ));
    }
    if stored.consumed_at_ms != 0 {
        return Err(Refusal::new(
            codes::POLICY_VIOLATION,
            format!(
                "break-glass proposal {} was consumed at {}",
                stored.proposal_id, stored.consumed_at_ms
            ),
        ));
    }
    Ok(())
}

/// Check the signature that an approval carries, or the absence of one.
fn check_signature(
    policy: BreakGlassPolicy<'_>,
    keys: &OperatorKeys,
    stored: &BreakGlassProposalRecord,
    attempt: &Attempt<'_>,
) -> Result<(), Refusal> {
    let unsigned = attempt.key_id.is_empty() && attempt.signature.is_empty();
    if unsigned {
        if policy.needs_signature(stored.action) {
            return Err(Refusal::new(
                codes::OPERATOR_SIGNATURE_REQUIRED,
                format!(
                    "an approval of {} needs a detached operator signature",
                    action_name(stored.action)
                ),
            ));
        }
        return Ok(());
    }
    let message = approval_signing_bytes(stored);
    if keys.verify(
        attempt.key_id,
        attempt.principal,
        &message,
        attempt.signature,
    ) {
        Ok(())
    } else {
        Err(Refusal::new(
            codes::OPERATOR_SIGNATURE_INVALID,
            "the operator signature did not verify",
        ))
    }
}

/// The fields that the response and the audit event both read.
///
/// A refusal still reports the stored counts, so an operator sees how far the
/// proposal got even when this request did not move it.
struct Report<'a> {
    action: &'a str,
    target: &'a str,
    proposal_id: Option<Uuid>,
    counterparties: Vec<String>,
    held: i32,
    required: i32,
}

impl<'a> Report<'a> {
    fn of(
        stored: Option<&'a BreakGlassProposalRecord>,
        settled: Option<&BreakGlassProposalRecord>,
        policy: BreakGlassPolicy<'_>,
    ) -> Self {
        let required = count(policy.required_approvals());
        let Some(stored) = stored else {
            return Self {
                action: UNKNOWN_ACTION,
                target: "",
                proposal_id: None,
                counterparties: Vec::new(),
                held: 0,
                required,
            };
        };
        let latest = settled.unwrap_or(stored);
        Self {
            action: action_name(stored.action),
            target: &stored.target,
            proposal_id: Some(stored.proposal_id),
            counterparties: latest
                .approvals
                .iter()
                .map(|approval| approval.principal.clone())
                .collect(),
            held: count(distinct_approvers(latest)),
            required,
        }
    }
}

/// The text an audit event carries for one settled request.
fn reason(outcome: &Result<BreakGlassProposalRecord, Refusal>) -> &str {
    match outcome {
        Ok(record) => record.reason.as_str(),
        Err(refusal) => refusal.message.as_str(),
    }
}

/// A count as the wire carries it. A count beyond `i32::MAX` saturates, and no
/// reachable approver set comes near it.
fn count(value: usize) -> i32 {
    i32::try_from(value).unwrap_or(i32::MAX)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::BreakGlassAction;
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use tempfile::TempDir;

    use super::*;
    use crate::{
        break_glass::gate::tests::{EXPIRES_MS, NOW_MS, approval, config, proposal},
        config::BreakGlassConfig,
        operator_keys::OperatorKeyEntry,
    };

    fn attempt(principal: &str) -> Attempt<'_> {
        Attempt {
            principal,
            key_id: "",
            signature: &[],
            withdraw: false,
            now_ms: NOW_MS,
        }
    }

    fn pending() -> BreakGlassProposalRecord {
        BreakGlassProposalRecord {
            approvals: Vec::new(),
            ..proposal(1, BreakGlassAction::DeleteTopic, "doomed")
        }
    }

    // An operator key bound to `principal`, plus the signer for it.
    fn operator_key(
        dir: &TempDir,
        key_id: &str,
        principal: &str,
    ) -> (Ed25519KeyPair, OperatorKeyEntry) {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse pkcs8");
        let path = dir.path().join(format!("{key_id}.pub"));
        std::fs::write(&path, pair.public_key().as_ref()).expect("write key file");
        (
            pair,
            OperatorKeyEntry {
                key_id: key_id.to_owned(),
                principal: principal.to_owned(),
                public_key_path: path,
            },
        )
    }

    #[test]
    fn a_second_principal_adds_an_approval_to_the_list() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let stored = pending();

        let updated = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &attempt("User:bob"),
        )
        .expect("a second approver may approve");

        let expected = BreakGlassProposalRecord {
            approvals: vec![BreakGlassApproval {
                principal: "User:bob".to_owned(),
                approved_at_ms: NOW_MS,
                key_id: String::new(),
                signature: Vec::new(),
            }],
            ..stored
        };
        check!(updated == expected);
    }

    #[test]
    fn an_approval_appends_to_the_list_it_found() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let stored = BreakGlassProposalRecord {
            approvals: vec![approval("User:bob")],
            ..pending()
        };

        let updated = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &attempt("User:carol"),
        )
        .expect("a third principal may approve");

        let names: Vec<&str> = updated
            .approvals
            .iter()
            .map(|a| a.principal.as_str())
            .collect();
        check!(names == ["User:bob", "User:carol"]);
    }

    #[test]
    fn the_three_checks_that_make_it_a_two_person_rule() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let with_bob = BreakGlassProposalRecord {
            approvals: vec![approval("User:bob")],
            ..pending()
        };
        let cases = [
            (
                "the proposer approves their own proposal",
                pending(),
                "User:alice",
                codes::BREAK_GLASS_DUPLICATE_APPROVER,
            ),
            (
                "an approver approves twice",
                with_bob,
                "User:bob",
                codes::BREAK_GLASS_DUPLICATE_APPROVER,
            ),
            (
                "a principal outside the set approves",
                pending(),
                "User:mallory",
                codes::BREAK_GLASS_NOT_AN_APPROVER,
            ),
        ];
        for (label, stored, principal, expected) in cases {
            let outcome = decide(
                policy,
                &OperatorKeys::default(),
                &stored,
                &attempt(principal),
            );
            assert!(let Err(refusal) = outcome, "case {label}");
            check!(refusal.code == expected, "case {label}");
        }
    }

    #[test]
    fn a_settled_or_expired_proposal_takes_no_approval() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let cases = [
            (
                "an expired proposal",
                BreakGlassProposalRecord {
                    expires_at_ms: NOW_MS,
                    ..pending()
                },
            ),
            (
                "a withdrawn proposal",
                BreakGlassProposalRecord {
                    withdrawn: true,
                    ..pending()
                },
            ),
            (
                "a consumed proposal",
                BreakGlassProposalRecord {
                    consumed_at_ms: NOW_MS - 1,
                    ..pending()
                },
            ),
        ];
        for (label, stored) in cases {
            let outcome = decide(
                policy,
                &OperatorKeys::default(),
                &stored,
                &attempt("User:bob"),
            );
            assert!(let Err(refusal) = outcome, "case {label}");
            check!(refusal.code == codes::POLICY_VIOLATION, "case {label}");
        }
    }

    #[test]
    fn an_expired_proposal_still_takes_a_withdrawal() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let stored = BreakGlassProposalRecord {
            expires_at_ms: NOW_MS,
            ..pending()
        };

        let updated = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &Attempt {
                withdraw: true,
                ..attempt("User:alice")
            },
        )
        .expect("the proposer may withdraw an expired proposal");

        check!(updated.withdrawn);
    }

    #[test]
    fn the_proposer_and_every_approver_may_withdraw() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let cases = [
            ("the proposer", "User:alice", true),
            ("a configured approver", "User:carol", true),
            ("a principal outside the set", "User:mallory", false),
        ];
        for (label, principal, expected) in cases {
            let outcome = decide(
                policy,
                &OperatorKeys::default(),
                &pending(),
                &Attempt {
                    withdraw: true,
                    ..attempt(principal)
                },
            );
            check!(outcome.is_ok() == expected, "case {label}");
            if let Ok(updated) = outcome {
                check!(updated.withdrawn, "case {label}");
                check!(updated.approvals.is_empty(), "case {label}");
            }
        }
    }

    #[test]
    fn a_withdrawal_ignores_the_key_id_and_the_signature() {
        let config = BreakGlassConfig {
            signed_actions: vec!["delete_topic".to_owned()],
            ..config()
        };
        let policy = BreakGlassPolicy::new(&config);

        let updated = decide(
            policy,
            &OperatorKeys::default(),
            &pending(),
            &Attempt {
                withdraw: true,
                key_id: "nobody",
                signature: &[9; 64],
                ..attempt("User:bob")
            },
        )
        .expect("a withdrawal needs no signature");

        check!(updated.withdrawn);
        check!(updated.approvals.is_empty());
    }

    #[test]
    fn an_action_that_needs_a_signature_refuses_an_unsigned_approval() {
        let config = BreakGlassConfig {
            signed_actions: vec!["delete_topic".to_owned()],
            ..config()
        };
        let policy = BreakGlassPolicy::new(&config);

        let outcome = decide(
            policy,
            &OperatorKeys::default(),
            &pending(),
            &attempt("User:bob"),
        );

        assert!(let Err(refusal) = outcome);
        check!(refusal.code == codes::OPERATOR_SIGNATURE_REQUIRED);
    }

    #[test]
    fn a_signature_verifies_against_the_bound_principal_and_the_signed_bytes() {
        let dir = TempDir::new().expect("tempdir");
        let (bob, bob_entry) = operator_key(&dir, "bob-yubi", "User:bob");
        let (_, carol_entry) = operator_key(&dir, "carol-yubi", "User:carol");
        let keys = OperatorKeys::load(&[bob_entry, carol_entry]).expect("load the trust set");
        let config = BreakGlassConfig {
            signed_actions: vec!["delete_topic".to_owned()],
            ..config()
        };
        let policy = BreakGlassPolicy::new(&config);
        let stored = pending();
        let good = bob.sign(&approval_signing_bytes(&stored)).as_ref().to_vec();
        let other = bob
            .sign(&approval_signing_bytes(&BreakGlassProposalRecord {
                target: "another-topic".to_owned(),
                ..stored.clone()
            }))
            .as_ref()
            .to_vec();
        let cases = [
            (
                "bob's own signature",
                "User:bob",
                "bob-yubi",
                good.clone(),
                None,
            ),
            (
                "bob's key under carol's name",
                "User:carol",
                "bob-yubi",
                good.clone(),
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
            (
                "carol's key over bob's signature",
                "User:carol",
                "carol-yubi",
                good.clone(),
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
            (
                "a key that is not in the trust set",
                "User:bob",
                "mallory-yubi",
                good,
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
            (
                "a signature over another proposal",
                "User:bob",
                "bob-yubi",
                other,
                Some(codes::OPERATOR_SIGNATURE_INVALID),
            ),
        ];
        for (label, principal, key_id, signature, expected) in cases {
            let outcome = decide(
                policy,
                &keys,
                &stored,
                &Attempt {
                    key_id,
                    signature: &signature,
                    ..attempt(principal)
                },
            );
            match expected {
                None => {
                    assert!(let Ok(updated) = outcome, "case {label}");
                    check!(updated.approvals[0].key_id == key_id, "case {label}");
                    check!(!updated.approvals[0].signature.is_empty(), "case {label}");
                }
                Some(code) => {
                    assert!(let Err(refusal) = outcome, "case {label}");
                    check!(refusal.code == code, "case {label}");
                }
            }
        }
    }

    #[test]
    fn a_signature_on_an_action_that_needs_none_is_still_verified() {
        let dir = TempDir::new().expect("tempdir");
        let (bob, bob_entry) = operator_key(&dir, "bob-yubi", "User:bob");
        let keys = OperatorKeys::load(&[bob_entry]).expect("load the trust set");
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let stored = pending();
        let signature = bob.sign(&approval_signing_bytes(&stored)).as_ref().to_vec();

        let accepted = decide(
            policy,
            &keys,
            &stored,
            &Attempt {
                key_id: "bob-yubi",
                signature: &signature,
                ..attempt("User:bob")
            },
        );
        let refused = decide(
            policy,
            &keys,
            &stored,
            &Attempt {
                key_id: "bob-yubi",
                signature: &[0; 64],
                ..attempt("User:bob")
            },
        );

        check!(accepted.is_ok());
        assert!(let Err(refusal) = refused);
        check!(refusal.code == codes::OPERATOR_SIGNATURE_INVALID);
    }

    #[test]
    fn a_proposal_that_expires_exactly_now_is_expired() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let stored = pending();

        let before = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &Attempt {
                now_ms: EXPIRES_MS - 1,
                ..attempt("User:bob")
            },
        );
        let at = decide(
            policy,
            &OperatorKeys::default(),
            &stored,
            &Attempt {
                now_ms: EXPIRES_MS,
                ..attempt("User:bob")
            },
        );

        check!(before.is_ok());
        assert!(let Err(refusal) = at);
        check!(refusal.code == codes::POLICY_VIOLATION);
    }

    #[test]
    fn a_report_of_a_missing_proposal_names_no_action() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);

        let report = Report::of(None, None, policy);

        check!(report.action == UNKNOWN_ACTION);
        check!(report.proposal_id == None);
        check!(report.held == 0);
        check!(report.required == 2);
    }

    #[test]
    fn a_report_counts_the_approvals_the_request_leaves_behind() {
        let config = config();
        let policy = BreakGlassPolicy::new(&config);
        let stored = BreakGlassProposalRecord {
            approvals: vec![approval("User:bob")],
            ..pending()
        };
        let settled = BreakGlassProposalRecord {
            approvals: vec![approval("User:bob"), approval("User:carol")],
            ..pending()
        };

        let refused = Report::of(Some(&stored), None, policy);
        let approved = Report::of(Some(&stored), Some(&settled), policy);

        check!(refused.held == 1);
        check!(refused.counterparties == vec!["User:bob".to_owned()]);
        check!(approved.held == 2);
        check!(approved.counterparties == vec!["User:bob".to_owned(), "User:carol".to_owned()]);
        check!(approved.action == "delete_topic");
        check!(approved.target == "doomed");
    }

    #[test]
    fn the_audit_phase_follows_the_outcome_and_the_flag() {
        let cases = [
            (
                "an approval",
                Ok(pending()),
                false,
                PrivilegedPhase::Approved,
            ),
            (
                "a withdrawal",
                Ok(pending()),
                true,
                PrivilegedPhase::Consumed,
            ),
            (
                "a refused approval",
                Err(Refusal::new(codes::POLICY_VIOLATION, "no")),
                false,
                PrivilegedPhase::Refused,
            ),
            (
                "a refused withdrawal",
                Err(Refusal::new(codes::POLICY_VIOLATION, "no")),
                true,
                PrivilegedPhase::Refused,
            ),
        ];
        for (label, outcome, withdraw, expected) in cases {
            check!(phase_of(&outcome, withdraw) == expected, "case {label}");
        }
    }
}
