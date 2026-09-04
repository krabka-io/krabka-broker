//! Guard tests for the wire-level error codes in [`super`], plus the
//! [`super::from_broker_error`] mapping.
//!
//! The guards are total rather than sampled: every constant the module
//! declares is checked against the Apache Kafka table extracted from the
//! pinned container image, and every constant is checked against every other
//! for a collision. A new constant that invents a number therefore fails here
//! rather than reaching a JVM client.
//!
//! The table itself is checked in, so none of this needs a container. What
//! keeps a checked-in oracle honest is
//! [`the_error_table_records_the_image_the_build_pins`], which holds its
//! recorded provenance against the image the build still pins.

mod kafka_error_table;

use assert2::assert;
use kafka_error_table::{KAFKA_ERROR_TABLE, KAFKA_ERROR_TABLE_IMAGE_DIGEST};

use super::*;
use crate::error::BrokerError;

/// The digest `//MODULE.bazel` pins for `apache_kafka_4_3_1`.
///
/// The build supplies it -- `crates/broker/build.rs` under Cargo,
/// `//bazel/images:pinned_digests` under Bazel -- so the comparison below is
/// between two strings this compilation already holds. Reading `MODULE.bazel`
/// here instead would be a test asserting on the text of a source file, and
/// would not survive Bazel's sandbox, which hands a test only its declared
/// inputs.
const PINNED_KAFKA_ORACLE_IMAGE_DIGEST: &str = env!("KRABKA_PINNED_IMAGE_APACHE_KAFKA_4_3_1");

#[test]
fn the_error_table_records_the_image_the_build_pins() {
    assert!(
        KAFKA_ERROR_TABLE_IMAGE_DIGEST == PINNED_KAFKA_ORACLE_IMAGE_DIGEST,
        "the Kafka error table was extracted from an image the build no longer \
         pins; re-derive crates/broker/src/codes/tests/kafka_error_table.rs \
         from {PINNED_KAFKA_ORACLE_IMAGE_DIGEST} and record that digest there",
    );
}

#[test]
fn every_kafka_range_constant_appears_in_the_kafka_table() {
    for (name, code) in KAFKA_RANGE_CODES {
        let kafka = KAFKA_ERROR_TABLE
            .iter()
            .find(|(kafka_name, _)| kafka_name == name);
        assert!(kafka == Some(&(*name, *code)), "{name} = {code}");
    }
}

#[test]
fn no_two_error_code_constants_collide() {
    let all: Vec<(&str, i16)> = KAFKA_RANGE_CODES
        .iter()
        .chain(KRABKA_PRIVATE_CODES)
        .copied()
        .collect();
    for (index, (left_name, left)) in all.iter().enumerate() {
        for (right_name, right) in &all[index + 1..] {
            assert!(
                left != right,
                "{left_name} and {right_name} both use {left}"
            );
        }
    }
}

#[test]
fn krabka_private_codes_sit_above_the_whole_kafka_table() {
    let highest_kafka = KAFKA_ERROR_TABLE
        .iter()
        .map(|(_, code)| *code)
        .max()
        .expect("the extracted Kafka table is not empty");
    for (name, code) in KRABKA_PRIVATE_CODES {
        assert!(*code >= 1000, "{name}");
        assert!(*code > highest_kafka, "{name}");
    }
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
}

#[test]
fn policy_violation_is_not_a_code_that_poisons_the_metadata_cache() {
    // KFC-9 chose 44 and rejected both of these, because each one also
    // changes the JVM client's metadata cache. 29 marks the topic
    // unauthorized, and 17 marks the name permanently invalid.
    assert!(POLICY_VIOLATION != TOPIC_AUTHORIZATION_FAILED);
    assert!(POLICY_VIOLATION != INVALID_TOPIC_EXCEPTION);
}

#[test]
fn replica_not_available_is_not_the_controller_moved_code() {
    // A JVM AdminClient turns 11 into StaleControllerEpochException, which
    // makes `kafka-reassign-partitions` report a controller failure for a
    // replica this broker simply does not host.
    let stale_controller_epoch = KAFKA_ERROR_TABLE
        .iter()
        .find(|(name, _)| *name == "STALE_CONTROLLER_EPOCH")
        .map(|(_, code)| *code);
    assert!(stale_controller_epoch == Some(11));
    assert!(REPLICA_NOT_AVAILABLE == 9);
}
