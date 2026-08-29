//! Tests for the break-glass approval that a thaw spends.
//!
//! A thaw names one approved proposal, the gate matches that proposal against
//! the scope and the pattern type of the thaw, and a freeze needs none.

use assert2::{assert, check};
use krabka_metadata::{
    BreakGlassAction, BreakGlassApproval, BreakGlassProposalRecord, MetadataRecord, PatternType,
    TopicFreezeRecord,
};
use krabka_protocol::krabka::freeze::PATTERN_TYPE_LITERAL;
use tempfile::TempDir;
use uuid::Uuid;

use super::{
    super::checks::{FreezeEnv, check_approval, consumed_proposal_id, record_of},
    ALICE, PROPOSAL, config_with_alice, context, freeze_request, image, peer, principal,
};
use crate::{
    codes,
    config::{BreakGlassConfig, BrokerConfig},
};

#[test]
fn a_thaw_with_no_proposal_needs_a_break_glass_approval() {
    let dir = TempDir::new().expect("tempdir");
    let (config, _) = config_with_alice(&dir);
    let image = image(&[("orders", PatternType::Literal)]);
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = FreezeEnv {
        config: &config,
        image: &image,
        ctx: &ctx,
    };
    let thaw = TopicFreezeRecord {
        frozen: false,
        proposal_id: Uuid::nil(),
        ..record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        )
    };

    let outcome = check_approval(&env, &thaw);

    check!(outcome.err().map(|r| r.code) == Some(codes::BREAK_GLASS_APPROVAL_REQUIRED));
}

#[test]
fn a_freeze_needs_no_proposal() {
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
    let record = record_of(
        &freeze_request("orders", PATTERN_TYPE_LITERAL),
        PatternType::Literal,
        ALICE,
    );

    check!(check_approval(&env, &record) == Ok(None));
}

#[test]
fn a_thaw_spends_the_approved_proposal_that_covers_its_scope() {
    let dir = TempDir::new().expect("tempdir");
    let (base, _) = config_with_alice(&dir);
    let config = BrokerConfig {
        break_glass: BreakGlassConfig {
            approvers: ["User:alice", "User:bob", "User:carol"]
                .map(str::to_owned)
                .to_vec(),
            ..BreakGlassConfig::default()
        },
        ..base
    };
    let mut image = image(&[("orders", PatternType::Literal)]);
    image.apply(&MetadataRecord::V1BreakGlassProposal(approved_thaw(
        "literal:orders",
    )));
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = FreezeEnv {
        config: &config,
        image: &image,
        ctx: &ctx,
    };
    let thaw = TopicFreezeRecord {
        frozen: false,
        proposal_id: PROPOSAL,
        ..record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        )
    };

    assert!(let Ok(Some(consumed)) = check_approval(&env, &thaw));

    check!(consumed_proposal_id(&consumed) == Some(PROPOSAL));
    assert!(let MetadataRecord::V1BreakGlassProposal(proposal) = &consumed);
    check!(proposal.consumed_at_ms > 0);
}

#[test]
fn a_thaw_that_names_another_proposal_is_refused() {
    let dir = TempDir::new().expect("tempdir");
    let (base, _) = config_with_alice(&dir);
    let config = BrokerConfig {
        break_glass: BreakGlassConfig {
            approvers: ["User:alice", "User:bob", "User:carol"]
                .map(str::to_owned)
                .to_vec(),
            ..BreakGlassConfig::default()
        },
        ..base
    };
    let mut image = image(&[("orders", PatternType::Literal)]);
    image.apply(&MetadataRecord::V1BreakGlassProposal(approved_thaw(
        "literal:orders",
    )));
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = FreezeEnv {
        config: &config,
        image: &image,
        ctx: &ctx,
    };
    let thaw = TopicFreezeRecord {
        frozen: false,
        proposal_id: Uuid::from_u128(0xDEAD),
        ..record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        )
    };

    let outcome = check_approval(&env, &thaw);

    check!(outcome.err().map(|r| r.code) == Some(codes::BREAK_GLASS_APPROVAL_REQUIRED));
}

#[test]
fn a_proposal_for_one_scope_does_not_thaw_another() {
    let dir = TempDir::new().expect("tempdir");
    let (base, _) = config_with_alice(&dir);
    let config = BrokerConfig {
        break_glass: BreakGlassConfig {
            approvers: ["User:alice", "User:bob", "User:carol"]
                .map(str::to_owned)
                .to_vec(),
            ..BreakGlassConfig::default()
        },
        ..base
    };
    let mut image = image(&[
        ("orders", PatternType::Literal),
        ("orders", PatternType::Prefixed),
    ]);
    image.apply(&MetadataRecord::V1BreakGlassProposal(approved_thaw(
        "literal:orders",
    )));
    let principal = principal();
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = FreezeEnv {
        config: &config,
        image: &image,
        ctx: &ctx,
    };

    for (label, pattern_type, expected) in [
        ("the scope the proposal names", PatternType::Literal, None),
        (
            "the same name under the other pattern type",
            PatternType::Prefixed,
            Some(codes::BREAK_GLASS_APPROVAL_REQUIRED),
        ),
    ] {
        let thaw = TopicFreezeRecord {
            frozen: false,
            proposal_id: PROPOSAL,
            pattern_type,
            ..record_of(
                &freeze_request("orders", PATTERN_TYPE_LITERAL),
                pattern_type,
                ALICE,
            )
        };
        check!(
            check_approval(&env, &thaw).err().map(|r| r.code) == expected,
            "{label}"
        );
    }
}

// A proposal that two distinct principals approved, on `target`.
fn approved_thaw(target: &str) -> BreakGlassProposalRecord {
    BreakGlassProposalRecord {
        proposal_id: PROPOSAL,
        action: BreakGlassAction::ThawTopicFreeze,
        target: target.to_owned(),
        proposer: ALICE.to_owned(),
        reason: "restore finished".to_owned(),
        created_at_ms: 1,
        expires_at_ms: i64::MAX,
        approvals: vec![
            BreakGlassApproval {
                principal: "User:bob".to_owned(),
                approved_at_ms: 2,
                key_id: "bob-yubi".to_owned(),
                signature: vec![0xBB; 64],
            },
            BreakGlassApproval {
                principal: "User:carol".to_owned(),
                approved_at_ms: 3,
                key_id: "carol-yubi".to_owned(),
                signature: vec![0xCC; 64],
            },
        ],
        consumed_at_ms: 0,
        withdrawn: false,
    }
}
