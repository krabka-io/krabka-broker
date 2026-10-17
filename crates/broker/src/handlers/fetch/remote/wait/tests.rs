//! A fetch's cold reads share one `remote.fetch.max.wait.ms` deadline, armed
//! by the first of them, and the wait resolves per broker, then per cluster,
//! then to Kafka's 500 ms.

use {
    super::*,
    assert2::check,
    krabka_metadata::{
        BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord, NodeId,
    },
};

/// A read that takes `took`, and the value it answers with.
async fn read_taking(took: Duration) -> &'static str {
    tokio::time::sleep(took).await;
    "records"
}

/// What two reads in a row under one [`RemoteFetchWait`] answered, and the
/// time the pair took.
#[derive(Debug, PartialEq, Eq)]
struct Bounded {
    first: Option<&'static str>,
    second: Option<&'static str>,
    elapsed: Duration,
}

#[yare::parameterized(
    reads_inside_the_wait_both_answer = { 100, 100, Bounded {
        first: Some("records"), second: Some("records"), elapsed: Duration::from_millis(200),
    } },
    a_slow_first_read_expires_at_the_wait_and_the_second_with_it = { 5_000, 100, Bounded {
        first: None, second: None, elapsed: Duration::from_millis(500),
    } },
    the_second_read_gets_only_what_the_first_left = { 300, 300, Bounded {
        first: Some("records"), second: None, elapsed: Duration::from_millis(500),
    } },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn one_deadline_bounds_every_cold_read_of_a_fetch(
    first_ms: u64,
    second_ms: u64,
    expected: Bounded,
) {
    let mut wait = RemoteFetchWait::default();
    check!(!wait.dispatched());
    let started = Instant::now();
    let half_second = || Duration::from_millis(500);

    let first = wait
        .bound_with(half_second, read_taking(Duration::from_millis(first_ms)))
        .await;
    // The second read's own wait is never consulted: the deadline is armed.
    let second = wait
        .bound_with(
            || Duration::from_secs(3600),
            read_taking(Duration::from_millis(second_ms)),
        )
        .await;

    check!(wait.dispatched());
    check!(
        Bounded {
            first,
            second,
            elapsed: started.elapsed(),
        } == expected
    );
}

/// `(node, value)` dynamic configs of `remote.fetch.max.wait.ms`, the node
/// `None` for the cluster default.
type Configs = &'static [(Option<u64>, &'static str)];

#[yare::parameterized(
    unset_is_kafkas_default = { &[], 500 },
    the_cluster_default = { &[(None, "250")], 250 },
    the_broker_value_wins = { &[(None, "250"), (Some(1), "75")], 75 },
    another_brokers_value_is_ignored = { &[(Some(2), "75")], 500 },
    an_unusable_value_falls_back = { &[(None, "soon")], 500 },
    a_value_under_one_falls_back = { &[(Some(1), "0")], 500 },
)]
fn the_wait_resolves_broker_then_cluster_then_default(configs: Configs, expected_ms: u64) {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for (node, value) in configs {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: (*node).map_or(DEFAULT_BROKER_CONFIG_NODE_ID, NodeId),
            config_name: REMOTE_FETCH_MAX_WAIT_MS.into(),
            config_value: Some((*value).to_owned()),
        }));
    }
    check!(remote_fetch_max_wait(&image, NodeId(1)) == Duration::from_millis(expected_ms));
}
