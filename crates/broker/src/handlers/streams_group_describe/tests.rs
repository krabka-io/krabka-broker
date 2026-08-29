//! End-to-end tests of the `StreamsGroupDescribe` handler against a running
//! broker, driven over the wire encoding.
//!
//! Each case pins the whole decoded response, so the per-group error rows the
//! KIP-1071 gates produce -- feature disabled, group unknown, streams actor
//! gone -- stay byte-for-byte what the JVM admin client expects.

use std::time::Duration;

use assert2::assert;
use krabka_protocol::UnknownTaggedFields;

use super::{
    test_support::{describe, error_group, finalize_streams_version, start_broker},
    *,
};
use crate::{codes, coordinator::unified::streams::actor::StreamsGroupActorMessage};

#[tokio::test]
async fn disabled_feature_returns_requested_group_error_rows() {
    let (broker_handle, _dir) = start_broker(true).await;
    let broker = broker_handle.broker_arc_for_test();

    let resp = describe(&broker, &["g-disabled-a", "g-disabled-b"]).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![
            error_group("g-disabled-a", codes::UNSUPPORTED_VERSION),
            error_group("g-disabled-b", codes::UNSUPPORTED_VERSION),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn enabled_missing_group_returns_not_found_rows() {
    let (broker_handle, _dir) = start_broker(true).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;

    let resp = describe(&broker, &["missing-a", "missing-b"]).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![
            error_group("missing-a", codes::GROUP_ID_NOT_FOUND),
            error_group("missing-b", codes::GROUP_ID_NOT_FOUND),
        ],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn closed_streams_actor_returns_load_in_progress_row() {
    let (broker_handle, _dir) = start_broker(true).await;
    let broker = broker_handle.broker_arc_for_test();
    finalize_streams_version(&broker).await;

    let actor = broker.group_coordinator.get_or_create_streams("stopped");
    let (tx, rx) = tokio::sync::oneshot::channel();
    actor
        .tx
        .send(StreamsGroupActorMessage::Shutdown(tx))
        .await
        .expect("send shutdown");
    rx.await.expect("actor shutdown");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !actor.tx.is_closed() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actor sender closed");

    let resp = describe(&broker, &["stopped"]).await;

    let expected = StreamsGroupDescribeResponse {
        throttle_time_ms: 0,
        groups: vec![error_group("stopped", codes::COORDINATOR_LOAD_IN_PROGRESS)],
        unknown_tagged_fields: UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected);
    broker_handle.shutdown().await;
}
