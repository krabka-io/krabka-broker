//! Tests for the operator-signature gate.
//!
//! A thaw needs a signature whatever `freeze.require_signature` says, a freeze
//! needs one when that setting is on, and a signature the request carries is
//! verified whether or not the action needed it.

use assert2::check;
use krabka_metadata::{PatternType, TopicFreezeRecord};
use krabka_protocol::{
    krabka::freeze::{PATTERN_TYPE_LITERAL, SetTopicFreezeRequest},
    primitives::uuid::Uuid as ProtocolUuid,
};
use tempfile::TempDir;

use super::{
    super::checks::{FreezeEnv, check_signature, is_signed, prepare, record_of},
    ALICE, ALICE_KEY, ALICE_NAME, PROPOSAL, config_with_alice, context, freeze_request, image,
    peer, principal, sign,
};
use crate::{
    break_glass::handlers::principal_name,
    codes,
    config::{BrokerConfig, FreezeConfig},
    time_util::now_ms,
};

#[test]
fn a_signature_is_named_by_either_half_of_the_pair() {
    for (label, key_id, signature, expected) in [
        ("neither half", "", Vec::new(), false),
        ("both halves", ALICE_KEY, vec![0xAB; 64], true),
        ("a key with no signature", ALICE_KEY, Vec::new(), true),
        ("a signature with no key", "", vec![0xAB; 64], true),
    ] {
        check!(is_signed(key_id, &signature) == expected, "{label}");
    }
}

/// KFC-9 hoists `[[operator_keys]]` to the top level so one entry serves
/// both the freeze path and the break-glass path. That only holds if both
/// spell the same person the same way, because `OperatorKeys::verify`
/// compares the bound principal by equality.
///
/// A listener authenticates Alice as the bare `alice`. The break-glass path
/// has always named her `User:alice`, so the freeze path must too: one
/// entry bound to `User:alice` has to verify a signed freeze *and* a signed
/// approval. When the freeze path used the bare name, an operator needed
/// two key ids for one human, which is the thing the shared trust set
/// exists to avoid.
#[test]
fn the_freeze_path_names_the_author_the_way_the_break_glass_path_does() {
    let dir = TempDir::new().expect("tempdir");
    let (config, _) = config_with_alice(&dir);
    let image = image(&[]);
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = FreezeEnv {
        config: &config,
        image: &image,
        ctx: &ctx,
    };

    // What the listener actually authenticated, and what each path makes of it.
    check!(principal.name == ALICE_NAME);
    check!(principal_name(&ctx) == ALICE);

    let accepted = prepare(&env, &freeze_request("orders", PATTERN_TYPE_LITERAL))
        .expect("an unsigned freeze is accepted by default");

    // The record carries the Kafka form, so the one configured entry --
    // bound to `User:alice` -- is the entry that verifies it.
    check!(accepted.record.set_by == ALICE);
    // The audit event names her the same way, so an auditor can join a
    // freeze to the break-glass approval that authorized it by principal.
    check!(crate::break_glass::handlers::principal_name(&ctx) == accepted.record.set_by);
    check!(config.operator_keys.get(ALICE_KEY).is_some());
    check!(
        config
            .operator_keys
            .get(ALICE_KEY)
            .map(crate::operator_keys::OperatorKey::principal)
            == Some(accepted.record.set_by.as_str())
    );
}

#[test]
fn an_unsigned_freeze_is_accepted_by_default_and_refused_under_require_signature() {
    let dir = TempDir::new().expect("tempdir");
    let (base, _) = config_with_alice(&dir);
    let image = image(&[]);
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);

    for (label, require_signature, expected) in [
        ("the default accepts an unsigned freeze", false, None),
        (
            "require_signature refuses one",
            true,
            Some(codes::OPERATOR_SIGNATURE_REQUIRED),
        ),
    ] {
        let config = BrokerConfig {
            freeze: FreezeConfig {
                require_signature,
                ..FreezeConfig::default()
            },
            ..base.clone()
        };
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let record = record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        );
        let outcome = check_signature(&env, &record, None);
        check!(
            outcome.as_ref().err().map(|r| r.code) == expected,
            "{label}"
        );
        if expected.is_none() {
            check!(outcome == Ok(false), "{label}");
        }
    }
}

#[test]
fn an_unsigned_thaw_is_refused_whatever_require_signature_says() {
    let dir = TempDir::new().expect("tempdir");
    let (base, _) = config_with_alice(&dir);
    let image = image(&[("orders", PatternType::Literal)]);
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);

    for require_signature in [false, true] {
        let config = BrokerConfig {
            freeze: FreezeConfig {
                require_signature,
                ..FreezeConfig::default()
            },
            ..base.clone()
        };
        let env = FreezeEnv {
            config: &config,
            image: &image,
            ctx: &ctx,
        };
        let record = record_of(
            &SetTopicFreezeRequest {
                frozen: false,
                proposal_id: ProtocolUuid(PROPOSAL.into_bytes()),
                ..freeze_request("orders", PATTERN_TYPE_LITERAL)
            },
            PatternType::Literal,
            ALICE,
        );
        let outcome = check_signature(&env, &record, None);
        check!(
            outcome.err().map(|r| r.code) == Some(codes::OPERATOR_SIGNATURE_REQUIRED),
            "require_signature = {require_signature}"
        );
    }
}

#[test]
fn a_signed_freeze_verifies_and_a_tampered_one_answers_one_code() {
    let dir = TempDir::new().expect("tempdir");
    let (config, alice) = config_with_alice(&dir);
    let image = image(&[]);
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = FreezeEnv {
        config: &config,
        image: &image,
        ctx: &ctx,
    };
    let good = TopicFreezeRecord {
        key_id: ALICE_KEY.to_owned(),
        set_at_ms: now_ms(),
        ..record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        )
    };
    let signature = sign(&alice, &good);

    for (label, record, expected) in [
        (
            "the record the operator signed",
            TopicFreezeRecord {
                signature: signature.clone(),
                ..good.clone()
            },
            None,
        ),
        (
            "the same signature presented as the thaw",
            TopicFreezeRecord {
                frozen: false,
                signature: signature.clone(),
                ..good.clone()
            },
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
        (
            "another scope under the same signature",
            TopicFreezeRecord {
                scope: "payments".to_owned(),
                signature: signature.clone(),
                ..good.clone()
            },
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
        (
            "a key_id no trust set carries",
            TopicFreezeRecord {
                key_id: "carol-yubi".to_owned(),
                signature: signature.clone(),
                ..good.clone()
            },
            Some(codes::OPERATOR_SIGNATURE_INVALID),
        ),
    ] {
        let outcome = check_signature(&env, &record, None);
        check!(
            outcome.as_ref().err().map(|r| r.code) == expected,
            "{label}"
        );
        if expected.is_none() {
            check!(outcome == Ok(true), "{label}");
        }
    }
}
