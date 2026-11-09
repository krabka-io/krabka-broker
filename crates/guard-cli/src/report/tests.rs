//! Tests for the words the tool prints beside a broker error code. The exit code
//! each code becomes, and the note beside a private code, are scenarios in
//! `tests/features/outcomes`.

use {super::*, crate::broker_codes, assert2::check, krabka_broker::codes};

/// An approval says who made it, and a signed one says which key. The
/// broker never stores a signature it did not check, so a `key_id` here is
/// already proof that the signature verified on the broker.
#[test]
fn an_approval_reports_the_evidence_it_carries() {
    let unsigned = bg::BreakGlassApproval {
        principal: "User:bob".to_owned(),
        approved_at_ms: 1_770_000_000_000,
        ..bg::BreakGlassApproval::default()
    };
    check!(approval_evidence(&unsigned) == "unsigned");

    let signed = bg::BreakGlassApproval {
        key_id: "bob-yubi".to_owned(),
        signature: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06],
        ..unsigned
    };
    check!(approval_evidence(&signed) == "signed by bob-yubi (deadbeef01020304...)");
}

#[test]
fn cli_private_code_values_match_the_broker_wire_contract() {
    check!(broker_codes::BREAK_GLASS_APPROVAL_REQUIRED == codes::BREAK_GLASS_APPROVAL_REQUIRED);
    check!(broker_codes::BREAK_GLASS_DUPLICATE_APPROVER == codes::BREAK_GLASS_DUPLICATE_APPROVER);
    check!(broker_codes::BREAK_GLASS_NOT_AN_APPROVER == codes::BREAK_GLASS_NOT_AN_APPROVER);
    check!(broker_codes::OPERATOR_SIGNATURE_INVALID == codes::OPERATOR_SIGNATURE_INVALID);
    check!(broker_codes::OPERATOR_SIGNATURE_REQUIRED == codes::OPERATOR_SIGNATURE_REQUIRED);
    check!(broker_codes::FREEZE_SCOPE_INVALID == codes::FREEZE_SCOPE_INVALID);
    check!(broker_codes::FREEZE_LIMIT_EXCEEDED == codes::FREEZE_LIMIT_EXCEEDED);
    check!(broker_codes::INVALID_REQUEST == codes::INVALID_REQUEST);
}

/// A thaw the broker refuses with `INVALID_REQUEST` names a scope with no
/// freeze, and the line says so; a freeze keeps the plain Kafka code, because
/// the tool has no reason to read more into it there.
#[yare::parameterized(
    thaw = {
        false,
        "error 42 (the registry holds no freeze at this exact scope, so there is nothing to \
         thaw): literal:orders has no live freeze entry, so there is nothing to thaw"
    },
    freeze = {
        true,
        "error 42: literal:orders has no live freeze entry, so there is nothing to thaw"
    },
)]
fn a_refused_set_freeze_names_the_refusal(frozen: bool, line: &str) {
    let response = api::SetTopicFreezeResponse {
        error_code: codes::INVALID_REQUEST,
        error_message: Some(
            "literal:orders has no live freeze entry, so there is nothing to thaw".to_owned(),
        ),
        ..api::SetTopicFreezeResponse::default()
    };

    check!(
        set_freeze_report(&response, frozen)
            == SetFreezeReport {
                stdout: None,
                stderr: Some(line.to_owned()),
                exit: EXIT_REFUSED,
            }
    );
}
