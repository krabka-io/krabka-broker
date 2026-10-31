//! Handler tests for what a consumer `Fetch` charges to its
//! `consumer_byte_rate` quota.
//!
//! Kafka's `KafkaApis.handleFetchRequest` records the whole response size
//! against `consumer_byte_rate`. When that throttles the fetch, it answers
//! with no records and the throttle, and `ClientQuotaManager.unrecordQuotaSensor`
//! takes the byte charge back.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::{
        test_support::{consumer_fetch_request, fetch_wire},
        throttle::fetch_response_size,
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        handlers::test_support::{
            create_topic, produce_records, set_user_quota, start_broker_with,
        },
    },
    assert2::assert,
    krabka_protocol::{owned::fetch_request::MAX_VERSION, primitives::uuid::Uuid as WireUuid},
    krabka_units::secs,
    std::sync::Arc,
};

/// The broker's quota window, in seconds. A byte-rate bucket holds this many
/// seconds of its rate. The window is long, so the bucket refills by a
/// negligible part of a response between two fetches of the test.
const QUOTA_WINDOW_SECS: u32 = 20;

/// The records the topic holds, in one batch. Enough that a response that
/// carries them is far larger than one that carries none.
const RECORDS: i32 = 500;

/// A broker with a [`QUOTA_WINDOW_SECS`] quota window.
async fn start() -> HandlerStartedBroker {
    start_broker_with(|config| {
        config.authorizer = Arc::new(AllowAllAuthorizer);
        config.quota_window = secs(QUOTA_WINDOW_SECS);
        config.quota_throttle_max = secs(QUOTA_WINDOW_SECS / 2);
    })
    .await
}

/// A sessionless consumer fetch of partition 0 of `topic_id` from
/// `fetch_offset`, as `user`.
async fn fetch_from(
    broker: &BrokerHandle,
    topic_id: WireUuid,
    fetch_offset: i64,
    user: &str,
) -> krabka_protocol::owned::fetch_response::FetchResponse {
    let mut request = consumer_fetch_request().topic_id(topic_id).call();
    request.topics[0].partitions[0].fetch_offset = fetch_offset;
    fetch_wire()
        .broker(broker)
        .request(&request)
        .user(user)
        .client_id("consumer-client")
        .call()
        .await
}

/// Three fetches by a user whose byte-rate bucket holds one and a half full
/// responses. The first fetch of the whole log fits. The second takes the
/// bucket past it, so it is throttled. Its bytes come off the quota again, so
/// the third fetch, from the end of the log, which carries no records, is not
/// throttled. Charged without the refund, it would be.
#[tokio::test]
async fn a_throttled_fetch_gives_its_bytes_back_to_the_next_fetch() {
    let HandlerStartedBroker { broker, dir: _dir } = start().await;
    let topic = "consumer-byte-rate";
    let user = "consumer-byte-rate-user";
    let topic_id = create_topic()
        .broker(&broker)
        .client_id("consumer-quota-test")
        .name(topic)
        .call()
        .await;
    produce_records()
        .broker(&broker)
        .topic(topic)
        .count(RECORDS)
        .call()
        .await;
    // Unquoted, the whole-log fetch measures the response it charges.
    let measured = fetch_from(&broker, topic_id, 0, "consumer-measure").await;
    let full_response = fetch_response_size(&measured, MAX_VERSION);
    let rate = 1.5 * f64::from(u32::try_from(full_response).expect("small response"))
        / f64::from(QUOTA_WINDOW_SECS);
    set_user_quota(&broker, user, "consumer_byte_rate", rate).await;

    let mut throttled = Vec::new();
    for fetch_offset in [0, 0, i64::from(RECORDS)] {
        let response = fetch_from(&broker, topic_id, fetch_offset, user).await;
        throttled.push(response.throttle_time_ms > 0);
    }

    assert!(throttled == [false, true, false]);
    broker.shutdown().await;
}
