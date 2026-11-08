//! The lines this tool prints for a broker's answer, and the exit code that
//! answer becomes.
//!
//! One broker error code maps to one exit code here, so every subcommand hands
//! a runbook the same number for the same situation. A registry that the tool
//! checked locally reports what the check proved, entry by entry, and reserves
//! the signature exit code for a signature that failed rather than one it could
//! not check.
//!
//! Every line goes to a writer the caller hands in: stdout for what the broker
//! answered, stderr for why it refused. The binary passes the real streams, and
//! the scenarios pass buffers so they read what an operator would. A write that
//! fails is dropped, as `krabka-barrier` drops one: an operator who closed the
//! pipe has stopped reading, and the exit code still says what happened.

use {
    super::{
        EXIT_BAD_SIGNATURE, EXIT_MISMATCH, EXIT_NO_APPROVAL, EXIT_REFUSED,
        cli::{action_name, pattern_name},
        verify::{CheckedEntry, VerifyOutcome},
    },
    krabka_protocol::krabka::{break_glass as bg, freeze as api},
    std::io::Write,
};

/// The exit code that one broker error code becomes.
///
/// Three codes get their own number because a runbook acts on them
/// differently. An action that needs an approval sends the operator for a
/// second person. A signature failure sends them to their key material. Every
/// other refusal is a refusal.
#[must_use]
pub fn exit_for_code(code: i16) -> i32 {
    use crate::broker_codes as codes;

    match code {
        0 => 0,
        codes::BREAK_GLASS_APPROVAL_REQUIRED => EXIT_NO_APPROVAL,
        codes::OPERATOR_SIGNATURE_INVALID | codes::OPERATOR_SIGNATURE_REQUIRED => {
            EXIT_BAD_SIGNATURE
        }
        _ => EXIT_REFUSED,
    }
}

/// Describe an error code.
///
/// `krabka-broker` has no code-to-name table, so this prints the number and
/// names the krabka-private codes that no Kafka reference lists.
#[must_use]
pub fn code_name(code: i16) -> String {
    use crate::broker_codes as codes;

    let note = match code {
        codes::BREAK_GLASS_APPROVAL_REQUIRED => {
            Some("no approved break-glass proposal covers this")
        }
        codes::BREAK_GLASS_DUPLICATE_APPROVER => {
            Some("this principal proposed the action or already approved it")
        }
        codes::BREAK_GLASS_NOT_AN_APPROVER => Some("this principal is not a configured approver"),
        codes::OPERATOR_SIGNATURE_INVALID => Some("the operator signature did not verify"),
        codes::OPERATOR_SIGNATURE_REQUIRED => Some("this action needs an operator signature"),
        codes::FREEZE_SCOPE_INVALID => Some("the scope is empty or reaches an internal topic"),
        codes::FREEZE_LIMIT_EXCEEDED => Some("the registry is at freeze.max_entries"),
        _ => None,
    };
    note.map_or_else(
        || format!("error {code}"),
        |note| format!("error {code} ({note})"),
    )
}

/// One error code and its message, as a line.
pub fn described_error(code: i16, message: Option<&str>) -> String {
    format!(
        "{}{}",
        code_name(code),
        message.map_or_else(String::new, |m| format!(": {m}"))
    )
}

/// Write a refusal to `err` and turn it into an exit code.
pub fn report_error(err: &mut impl Write, code: i16, message: Option<&str>) -> i32 {
    _ = writeln!(err, "{}", described_error(code, message));
    exit_for_code(code)
}

/// The outcome of one freeze or thaw.
pub fn report_set_freeze(
    out: &mut impl Write,
    err: &mut impl Write,
    response: &api::SetTopicFreezeResponse,
    frozen: bool,
) -> i32 {
    if response.error_code != 0 {
        return report_error(err, response.error_code, response.error_message.as_deref());
    }
    _ = writeln!(out, "{}\tok", if frozen { "frozen" } else { "thawed" });
    0
}

/// One line per registry entry.
pub fn print_freeze(
    out: &mut impl Write,
    freeze: &api::DescribedTopicFreeze,
    checked: Option<&CheckedEntry>,
) {
    let proof = match checked.map(|entry| entry.unproved) {
        None => String::new(),
        Some(None) => format!("\tverified by {}", freeze.key_id),
        Some(Some(unproved)) => format!("\t{}", unproved.reason()),
    };
    _ = writeln!(
        out,
        "{}:{}\tset by {} at {}\t{}{proof}",
        pattern_name(freeze.pattern_type),
        freeze.scope,
        freeze.set_by,
        freeze.set_at_ms,
        freeze.reason,
    );
}

/// One block per proposal.
pub fn print_proposal(out: &mut impl Write, proposal: &bg::DescribedBreakGlassProposal) {
    let state = if proposal.withdrawn {
        "withdrawn"
    } else if proposal.consumed_at_ms > 0 {
        "consumed"
    } else {
        "open"
    };
    let id = uuid::Uuid::from_bytes(proposal.proposal_id.0);
    _ = writeln!(out, "proposal {id} {state}");
    _ = writeln!(out, "  action     {}", action_name(proposal.action));
    _ = writeln!(out, "  target     {}", proposal.target);
    _ = writeln!(out, "  proposer   {}", proposal.proposer);
    _ = writeln!(out, "  reason     {}", proposal.reason);
    _ = writeln!(out, "  created    {}", proposal.created_at_ms);
    _ = writeln!(out, "  expires    {}", proposal.expires_at_ms);
    for approval in &proposal.approvals {
        _ = writeln!(
            out,
            "  approved   {} at {} {}",
            approval.principal,
            approval.approved_at_ms,
            approval_evidence(approval)
        );
    }
}

/// What one approval offers as evidence of who made it.
///
/// The broker never stores a signature it did not check, so a `key_id` on a
/// stored approval is already proof that the signature verified there. The
/// first bytes of the signature are printed so an auditor can line one approval
/// up against the audit event that carries the whole of it.
fn approval_evidence(approval: &bg::BreakGlassApproval) -> String {
    /// How many leading signature bytes identify one approval in a report.
    const PREFIX: usize = 8;

    if approval.key_id.is_empty() {
        return "unsigned".to_owned();
    }
    format!(
        "signed by {} ({}...)",
        approval.key_id,
        hex::encode(
            approval
                .signature
                .get(..PREFIX)
                .unwrap_or(&approval.signature)
        )
    )
}

/// What a local verification of the registry found.
///
/// A signature that failed outranks a key this machine does not hold, because
/// the first says the tool checked and the answer is wrong, and the second says
/// the tool could not check.
pub fn report_verify(
    out: &mut impl Write,
    err: &mut impl Write,
    freezes: &[api::DescribedTopicFreeze],
    outcome: &VerifyOutcome,
) -> i32 {
    for (freeze, checked) in freezes.iter().zip(&outcome.entries) {
        print_freeze(out, freeze, Some(checked));
    }
    if outcome.any_signature_failed() {
        _ = writeln!(
            err,
            "the registry does not verify against the operator keys on this machine"
        );
        return EXIT_BAD_SIGNATURE;
    }
    if outcome.any_key_is_unknown() {
        _ = writeln!(
            err,
            "the registry names an operator key that this machine does not hold"
        );
        return EXIT_MISMATCH;
    }
    _ = writeln!(
        out,
        "{} of {} entries are proved by an operator signature",
        outcome.proved(),
        outcome.entries.len()
    );
    0
}

#[cfg(test)]
mod tests;
