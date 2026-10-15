//! Handler tests for what a consumer `Fetch` charges to its
//! `request_percentage` quota.
//!
//! KIP-124 meters the time a request holds a request-handler thread. Kafka's
//! `KafkaApis.handleFetchRequest` parks a fetch that waits for `MinBytes` in
//! the fetch purgatory, off the handler thread, so the long poll's wait is not
//! charged; only the work before and after it is.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    super::test_support::consumer_fetch_request,
    crate::{
        authorizer::AllowAllAuthorizer,
        handlers::test_support::{create_topic, set_user_quota, start_broker_with},
    },
    assert2::assert,
    krabka_units::secs,
};

/// How long the fetch waits for `MinBytes` on an empty partition.
const LONG_POLL_MS: i32 = 500;

/// What one long-polling fetch answered.
#[derive(Debug, PartialEq, Eq)]
struct Outcome {
    throttled: bool,
}

/// A long poll of `LONG_POLL_MS` on an empty partition, by a user whose
/// `request_percentage` bucket holds a tenth of a second of handler time. The
/// wait is ten times what the bucket holds, so charging it would throttle the
/// fetch; the handler's own work is far less. A starved quota still throttles
/// the same fetch, so the quota is charged at all.
#[tokio::test]
async fn a_long_poll_is_not_charged_to_the_request_quota() {
    let HandlerStartedBroker { broker, dir: _dir } = start_broker_with(|config| {
        config.authorizer = std::sync::Arc::new(AllowAllAuthorizer);
        // A one-second window: a bucket holds one second of its rate.
        config.quota_window = secs(1);
        config.quota_throttle_max = secs(1);
    })
    .await;

    let cases = [
        // 10% of a thread is 100 ms of handler time per second.
        ("ten-percent", 10.0, Outcome { throttled: false }),
        ("starved", 0.0001, Outcome { throttled: true }),
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, percentage, want) in cases {
        let topic = format!("long-poll-{name}");
        let user = format!("consumer-{name}");
        let topic_id = create_topic()
            .broker(&broker)
            .client_id("request-quota-test")
            .name(&topic)
            .call()
            .await;
        set_user_quota(&broker, &user, "request_percentage", percentage).await;

        let response = super::test_support::fetch_wire()
            .broker(&broker)
            .request(
                &consumer_fetch_request()
                    .topic_id(topic_id)
                    .max_wait_ms(LONG_POLL_MS)
                    .min_bytes(1)
                    .call(),
            )
            .user(&user)
            .client_id("consumer-client")
            .call()
            .await;
        actual.push((name, Outcome {
            throttled: response.throttle_time_ms > 0,
        }));
        expected.push((name, want));
    }

    assert!(actual == expected);
    broker.shutdown().await;
}
