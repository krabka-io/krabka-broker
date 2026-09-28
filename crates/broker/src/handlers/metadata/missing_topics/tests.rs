//! Kafka's `DefaultAutoTopicCreationManager` for a `Metadata` request that
//! asks to auto-create a missing coordinator topic.

use std::collections::BTreeMap;

use assert2::{assert, check};
use krabka_protocol::{
    owned::metadata_response::MetadataResponseTopic, primitives::uuid::Uuid as WireUuid,
};

use super::missing_topic_rows;
use crate::{
    codes,
    coordinator::bootstrap::OFFSETS_TOPIC,
    test_support::{peer, principal, request_context, start_broker_with},
};

fn row(error_code: i16, name: &str, is_internal: bool) -> MetadataResponseTopic {
    MetadataResponseTopic {
        error_code,
        name: Some(name.to_owned()),
        topic_id: WireUuid::ZERO,
        is_internal,
        ..Default::default()
    }
}

/// `creatableTopic` gives `__consumer_offsets` its configured partition count,
/// replication factor and topic configs, not the broker defaults, and
/// `filterCreatableTopics` skips it while its creation is in flight. Both
/// requests answer 3, as `createTopics` answers every name.
#[tokio::test]
async fn a_coordinator_topic_is_created_with_its_configured_shape_unless_in_flight() {
    let (handle, _dir) = start_broker_with(|config| {
        config.audit_enabled = false;
        config.num_partitions = 1;
        config.offsets_topic_num_partitions = 7;
    })
    .await;
    let broker = handle.broker_arc_for_test();
    let user = principal("alice");
    let address = peer();
    let ctx = request_context(&user, &address, "metadata-client");
    let expected = vec![row(codes::UNKNOWN_TOPIC_OR_PARTITION, OFFSETS_TOPIC, true)];

    assert!(broker.auto_topic_creation.begin(OFFSETS_TOPIC));
    let skipped = missing_topic_rows(&broker, &ctx, &[OFFSETS_TOPIC], true).await;
    check!(skipped == expected);
    check!(
        broker
            .controller
            .current_image()
            .topic(OFFSETS_TOPIC)
            .is_none()
    );
    broker.auto_topic_creation.end(OFFSETS_TOPIC);

    let created = missing_topic_rows(&broker, &ctx, &[OFFSETS_TOPIC], true).await;
    check!(created == expected);
    check!(!broker.auto_topic_creation.is_in_flight(OFFSETS_TOPIC));
    let image = broker.controller.current_image();
    check!(image.topic_partition_count(OFFSETS_TOPIC) == 7);
    let configs: BTreeMap<String, String> =
        crate::coordinator::bootstrap::offsets_topic_configs(&broker.config)
            .into_iter()
            .collect();
    check!(image.topic_config(OFFSETS_TOPIC) == Some(&configs));
    handle.shutdown().await;
}
