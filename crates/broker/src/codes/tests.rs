//! Value checks for the wire-level error codes in [`super`], plus the
//! [`super::from_broker_error`] mapping.
//!
//! Each case pins the number the Apache Kafka error table assigns, because a
//! substitution here changes how a JVM client reacts. The mapping cases pin
//! the code that each `BrokerError` variant reaches the wire as.

use assert2::assert;

use super::*;
use crate::error::BrokerError;

#[test]
fn share_group_error_codes_match_kafka() {
    let cases = [
        ("INVALID_RECORD_STATE", INVALID_RECORD_STATE, 121),
        ("SHARE_SESSION_NOT_FOUND", SHARE_SESSION_NOT_FOUND, 122),
        (
            "INVALID_SHARE_SESSION_EPOCH",
            INVALID_SHARE_SESSION_EPOCH,
            123,
        ),
        ("FENCED_STATE_EPOCH", FENCED_STATE_EPOCH, 124),
        (
            "SHARE_SESSION_LIMIT_REACHED",
            SHARE_SESSION_LIMIT_REACHED,
            133,
        ),
    ];
    for (name, code, want) in cases {
        assert!(code == want, "{name}");
    }
}

#[test]
fn unknown_server_error_is_negative_one() {
    assert!(UNKNOWN_SERVER_ERROR == -1);
    assert!(UNKNOWN_SERVER_ERROR < NONE);
}

#[test]
fn from_broker_error_maps_variants_to_wire_codes() {
    let cases = [
        (
            BrokerError::UnsupportedApi {
                api_key: 0,
                version: 99,
            },
            UNSUPPORTED_VERSION, // 35
        ),
        (
            BrokerError::PartitionWriterDied {
                topic: "t".into(),
                partition: 0,
            },
            NOT_LEADER_OR_FOLLOWER, // 6
        ),
        (
            BrokerError::GroupInvalidState {
                group_id: "g".into(),
                state: "PreparingRebalance".into(),
            },
            REBALANCE_IN_PROGRESS, // 27
        ),
        (
            BrokerError::UnknownMember {
                group_id: "g".into(),
                member_id: "m".into(),
            },
            UNKNOWN_MEMBER_ID, // 25
        ),
        (
            BrokerError::GenerationMismatch {
                group_id: "g".into(),
                current: 5,
                requested: 4,
            },
            ILLEGAL_GENERATION, // 22
        ),
        (
            BrokerError::ProducerEpochFenced {
                producer_id: 1000,
                current: 2,
                requested: 1,
            },
            INVALID_PRODUCER_EPOCH, // 47
        ),
        (
            BrokerError::FencedLeaderEpoch {
                have: 0,
                current: 1,
            },
            FENCED_LEADER_EPOCH, // 74
        ),
        (BrokerError::UnknownLeaderEpoch(2), UNKNOWN_LEADER_EPOCH), // 75
        // Catch-all arm: internal variants map to the generic code.
        (BrokerError::Txn("test".into()), UNKNOWN_SERVER_ERROR), // -1
    ];
    for (err, want) in cases {
        assert!(from_broker_error(&err) == want, "{err:?}");
    }
    // Pin the concrete wire value for the producer-fence path: the Rust
    // producer client maps 47 to `ProducerError::FencedProducer`.
    assert!(INVALID_PRODUCER_EPOCH == 47);
}

#[test]
fn not_enough_replicas_codes_have_expected_values() {
    assert!(NOT_ENOUGH_REPLICAS == 19);
    assert!(NOT_ENOUGH_REPLICAS_AFTER_APPEND == 20);
}

#[test]
fn invalid_timestamp_code_matches_kafka() {
    assert!(INVALID_TIMESTAMP == 32);
}

#[test]
fn transaction_and_sasl_codes_match_kafka() {
    let cases = [
        ("INVALID_PRODUCER_EPOCH", INVALID_PRODUCER_EPOCH, 47),
        ("INVALID_TXN_STATE", INVALID_TXN_STATE, 48),
        (
            "INVALID_PRODUCER_ID_MAPPING",
            INVALID_PRODUCER_ID_MAPPING,
            49,
        ),
        ("CONCURRENT_TRANSACTIONS", CONCURRENT_TRANSACTIONS, 51),
        (
            "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
            TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
            53,
        ),
        ("SASL_AUTHENTICATION_FAILED", SASL_AUTHENTICATION_FAILED, 58),
    ];
    for (name, code, want) in cases {
        assert!(code == want, "{name}");
    }
}

#[test]
fn scram_error_code_numbers_match_kafka() {
    let cases = [
        ("UNSUPPORTED_SASL_MECHANISM", UNSUPPORTED_SASL_MECHANISM, 33),
        ("RESOURCE_NOT_FOUND", RESOURCE_NOT_FOUND, 91),
        ("DUPLICATE_RESOURCE", DUPLICATE_RESOURCE, 92),
        ("UNACCEPTABLE_CREDENTIAL", UNACCEPTABLE_CREDENTIAL, 93),
        ("ELECTION_NOT_NEEDED", ELECTION_NOT_NEEDED, 84),
    ];
    for (name, code, want) in cases {
        assert!(code == want, "{name}");
    }
}

#[test]
fn kip516_error_code_numbers_match_kafka() {
    let cases = [
        ("UNKNOWN_TOPIC_ID", super::UNKNOWN_TOPIC_ID, 100),
        ("INCONSISTENT_TOPIC_ID", super::INCONSISTENT_TOPIC_ID, 103),
    ];
    for (name, code, want) in cases {
        assert!(code == want, "{name}");
    }
}

#[test]
fn ineligible_replica_code_does_not_collide_with_duplicate_resource() {
    assert!(DUPLICATE_RESOURCE == 92);
    assert!(INELIGIBLE_REPLICA == 107);
}

/// Every krabka-private error code, with the name a failure reports.
const KRABKA_PRIVATE_CODES: [(&str, i16, i16); 8] = [
    (
        "BARRIER_INJECTION_IN_PROGRESS",
        BARRIER_INJECTION_IN_PROGRESS,
        1000,
    ),
    (
        "BREAK_GLASS_APPROVAL_REQUIRED",
        BREAK_GLASS_APPROVAL_REQUIRED,
        1006,
    ),
    (
        "BREAK_GLASS_DUPLICATE_APPROVER",
        BREAK_GLASS_DUPLICATE_APPROVER,
        1007,
    ),
    (
        "BREAK_GLASS_NOT_AN_APPROVER",
        BREAK_GLASS_NOT_AN_APPROVER,
        1008,
    ),
    (
        "OPERATOR_SIGNATURE_INVALID",
        OPERATOR_SIGNATURE_INVALID,
        1009,
    ),
    (
        "OPERATOR_SIGNATURE_REQUIRED",
        OPERATOR_SIGNATURE_REQUIRED,
        1010,
    ),
    ("FREEZE_SCOPE_INVALID", FREEZE_SCOPE_INVALID, 1011),
    ("FREEZE_LIMIT_EXCEEDED", FREEZE_LIMIT_EXCEEDED, 1012),
];

#[test]
fn krabka_private_error_codes_sit_above_every_kafka_code() {
    for (name, code, want) in KRABKA_PRIVATE_CODES {
        assert!(code == want, "{name}");
        // Above every code the Apache Kafka table assigns, and clear of
        // the two codes whose meaning a JVM client would act on.
        assert!(code > REBOOTSTRAP_REQUIRED, "{name}");
        assert!(code != CONCURRENT_TRANSACTIONS, "{name}");
        assert!(code != REBALANCE_IN_PROGRESS, "{name}");
    }
}

#[test]
fn krabka_private_error_codes_are_pairwise_distinct() {
    for (index, (left_name, left, _)) in KRABKA_PRIVATE_CODES.iter().enumerate() {
        for (right_name, right, _) in &KRABKA_PRIVATE_CODES[index + 1..] {
            assert!(left != right, "{left_name} and {right_name}");
        }
    }
}

#[test]
fn krabka_private_error_codes_leave_the_kfc6_range_free() {
    // KFC-6 proposes 1001 to 1005 for the coordination-primitives api, so
    // no code here takes one of them.
    for (name, code, _) in KRABKA_PRIVATE_CODES {
        assert!(!(1001..=1005).contains(&code), "{name}");
    }
}

#[test]
fn policy_violation_matches_the_kafka_error_table() {
    assert!(POLICY_VIOLATION == 44);
    // KFC-9 chose 44 and rejected both of these, because each one also
    // changes the JVM client's metadata cache. 29 marks the topic
    // unauthorized, and 17 marks the name permanently invalid.
    assert!(POLICY_VIOLATION != TOPIC_AUTHORIZATION_FAILED);
    assert!(POLICY_VIOLATION != INVALID_TOPIC_EXCEPTION);
}

#[test]
fn kip848_and_kip919_error_codes_match_kafka() {
    let cases = [
        ("UNRELEASED_INSTANCE_ID", UNRELEASED_INSTANCE_ID, 111),
        ("UNSUPPORTED_ASSIGNOR", UNSUPPORTED_ASSIGNOR, 112),
        ("STALE_MEMBER_EPOCH", STALE_MEMBER_EPOCH, 113),
        ("MISMATCHED_ENDPOINT_TYPE", MISMATCHED_ENDPOINT_TYPE, 114),
        ("UNKNOWN_CONTROLLER_ID", UNKNOWN_CONTROLLER_ID, 116),
    ];
    for (name, code, want) in cases {
        assert!(code == want, "{name}");
    }
}
