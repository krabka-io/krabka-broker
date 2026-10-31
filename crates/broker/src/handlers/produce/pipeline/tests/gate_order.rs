//! Tests for the order of the gates ahead of the append.
//!
//! The order is part of Kafka compatibility: when one batch trips two gates,
//! the row carries the earlier gate's code. The authority gates (the topic ACL,
//! the KFC-9 freeze and the internal-topic gate) answer ahead of the
//! `max.message.bytes` cap, and the cap answers ahead of the leadership gate.

use {
    super::{non_leader_metadata, non_leader_row, process_partition, seed_batch},
    crate::{
        codes::{
            INVALID_TOPIC_EXCEPTION, MESSAGE_TOO_LARGE, POLICY_VIOLATION,
            TOPIC_AUTHORIZATION_FAILED,
        },
        freeze::resolve::{FreezeMutationResolution, FreezeVerdict, resolve_freeze_mutation},
        handlers::produce::{
            framing::PartitionPayload,
            test_support::{PipelineFixture, encode_batch, pipeline_input},
        },
        test_support::topic_freeze_record,
    },
    assert2::check,
    krabka_metadata::{MetadataRecord, PatternType},
    krabka_protocol::owned::produce_response::PartitionProduceResponse,
    krabka_verified::FreezeMutationKind,
    std::sync::Arc,
};

// A refusal ahead of the append: the -1 `base_offset` sentinel and no leader
// hint.
fn refused(error_code: i16, error_message: Option<String>) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        error_code,
        base_offset: -1,
        error_message,
        ..Default::default()
    }
}

#[tokio::test]
async fn the_earlier_gate_answers_when_one_batch_trips_two() {
    // `orders` is frozen and led by node 2, and the pipeline runs as node 1,
    // so a batch that passes every earlier gate meets the leadership gate.
    let mut image = non_leader_metadata();
    image.apply(&MetadataRecord::V1TopicFreeze(topic_freeze_record().call()));
    let image = Arc::new(image);
    let fixture = PipelineFixture::new(krabka_ids::NodeId(1));

    let frozen = resolve_freeze_mutation(&image, "orders", true, FreezeMutationKind::Produce);
    let freeze_message = FreezeVerdict {
        scope: "orders".to_owned(),
        pattern_type: PatternType::Literal,
        reason: "DR cutover".to_owned(),
    }
    .error_message();
    // Smaller than a v2 batch header, so every batch trips the size cap.
    let tiny_cap = krabka_units::bytes(16);
    let default_cap = krabka_log::DEFAULT_MAX_MESSAGE_SIZE;

    let cases = [
        (
            "the ACL denial answers ahead of the internal-topic gate and the cap",
            FreezeMutationResolution::AuthorizationDenied,
            true,
            tiny_cap,
            refused(TOPIC_AUTHORIZATION_FAILED, None),
        ),
        (
            "the freeze answers ahead of the internal-topic gate and the cap",
            frozen,
            true,
            tiny_cap,
            refused(POLICY_VIOLATION, Some(freeze_message)),
        ),
        (
            "the internal-topic gate answers ahead of the cap",
            FreezeMutationResolution::Admit,
            true,
            tiny_cap,
            refused(INVALID_TOPIC_EXCEPTION, None),
        ),
        (
            "the cap answers ahead of the leadership gate",
            FreezeMutationResolution::Admit,
            false,
            tiny_cap,
            refused(MESSAGE_TOO_LARGE, None),
        ),
        (
            "under every cap, the leadership gate answers with its hint",
            FreezeMutationResolution::Admit,
            false,
            default_cap,
            non_leader_row(),
        ),
    ];

    for (label, freeze, internal_topic_denied, max_message_bytes, want) in cases {
        let resp = process_partition(
            pipeline_input()
                .topic("orders")
                .payload(PartitionPayload::Slice(encode_batch(&seed_batch())))
                .freeze(freeze)
                .internal_topic_denied(internal_topic_denied)
                .max_message_bytes(max_message_bytes)
                .call(),
            fixture.services(&image),
        )
        .await
        .expect("process partition")
        .expect_done();
        check!(resp == want, "case: {label}");
    }
}
