//! Tests 3 and 4: the leader-side throttle caps the bytes a replica Fetch
//! returns, and an unthrottled partition is left alone.
//!
//! Both run on the PLAINTEXT cluster: the throttle decision is made on the
//! fetch path and needs no principal, so the compat shim that allows every
//! operation while there are no ACLs keeps the setup to the minimum. They are
//! the pair that gives the suite its name — the config tests only prove the key
//! round-trips, these two prove it is enforced.

use assert2::assert;

use crate::{
    cluster::{create_topic_plaintext, start_single_broker_plaintext, wait_partition_exists},
    configs::drive_incremental_alter_configs_plaintext,
    records::{fetch_plaintext_replica, produce_plaintext},
};

/// Test 3: After setting a very low leader throttle rate (512 bytes/sec) and
/// marking partition 0 as throttled for `replica_id=2`, a Fetch issued with
/// `replica_id=2` must return a response well under 8 KB.
///
/// The token bucket has a one-second burst capacity at the configured rate, so
/// the test sets the rate to 512 bytes/sec. An 8 KB response must be capped to
/// at most 512 bytes of record data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn throttle_rate_caps_fetch_response_size() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;
    let node_id = handle.node_id();

    // Create topic rf=1 so this broker is always the leader.
    create_topic_plaintext(addr, "bar", 1, 1).await;
    wait_partition_exists(&handle, "bar", 0).await;

    // Set the leader throttle rate to 512 bytes/sec.
    let err = drive_incremental_alter_configs_plaintext(
        addr,
        vec![(
            4, // resource_type = Broker
            node_id.to_string(),
            vec![(
                "leader.replication.throttled.rate".into(),
                Some("512".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert!(err == 0, "broker throttle alter failed: error_code={err}");

    // Mark partition 0 as throttled for follower replica_id=2.
    let err = drive_incremental_alter_configs_plaintext(
        addr,
        vec![(
            2, // resource_type = Topic
            "bar".into(),
            vec![(
                "leader.replication.throttled.replicas".into(),
                Some("0:2".into()),
                0, // OP_SET
            )],
        )],
    )
    .await;
    assert!(err == 0, "topic throttle alter failed: error_code={err}");

    // Wait for the configs to appear in the image before producing (so the
    // throttle enforcement is armed when the Fetch arrives).
    handle
        .wait_for_image(|img| {
            let rate = img.broker_throttle_rate(
                krabka_metadata::NodeId(node_id),
                krabka_metadata::ThrottleKind::Leader,
            );
            let throttle = krabka_broker::throttle::TopicThrottle::for_topic(img, "bar");
            rate == Some(krabka_units::bytes_per_sec(512))
                && throttle.leader.contains(0, krabka_broker::NodeId(2))
        })
        .await;

    // Produce 8 KB of data (8 records of 1 KB each).
    produce_plaintext(addr, "bar", 1024, 8).await;

    // Fetch with replica_id=2 (inter-broker follower path → leader throttle applies).
    let resp_bytes = fetch_plaintext_replica(addr, "bar", 2).await;

    // The throttled response must be much smaller than the 8 KB we produced.
    // We allow up to 2 KB as the upper bound to give headroom for framing
    // overhead (batch headers, response wrapper).
    assert!(
        resp_bytes <= 2048,
        "expected throttled fetch response <= 2048 bytes, got {resp_bytes} bytes"
    );

    handle.shutdown().await;
}

/// Test 4: Without any throttle config, a Fetch with `replica_id >= 0` delivers
/// all 8 KB of data unimpeded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unthrottled_partition_unaffected() {
    let (handle, _dir, addr) = start_single_broker_plaintext().await;

    // Create topic rf=1.
    create_topic_plaintext(addr, "baz", 1, 1).await;
    wait_partition_exists(&handle, "baz", 0).await;

    // Produce 8 KB of data (8 records of 1 KB each). No throttle configured.
    produce_plaintext(addr, "baz", 1024, 8).await;

    // Fetch with replica_id=2 (inter-broker path). No throttle → full data.
    let resp_bytes = fetch_plaintext_replica(addr, "baz", 2).await;

    // Full 8 KB data plus framing. The response should be well over 4 KB.
    assert!(
        resp_bytes >= 4096,
        "expected unthrottled fetch response >= 4096 bytes, got {resp_bytes} bytes"
    );

    handle.shutdown().await;
}
