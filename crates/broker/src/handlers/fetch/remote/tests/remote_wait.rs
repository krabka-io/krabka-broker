//! `remote.fetch.max.wait.ms` bounds a cold read that is slow to start, and
//! an alter of it reaches the next fetch on a running broker.
//!
//! The slow read is one that cannot get a reader slot: the broker reads with a
//! single reader thread, and the test holds that slot. That is the read Kafka's
//! `DelayedRemoteFetch.onExpiration` cancels, a `RemoteLogReader` still queued
//! on the reader pool when the wait runs out.

use {
    super::{
        super::{RemoteFetchWait, refused_read, try_remote_read, wait::remote_fetch_max_wait},
        TIERED_HIGH_WATERMARK, TieredBroker, committed_tiered_partition, out_of_range_at,
        tiered_broker_with,
    },
    crate::{
        broker::Broker,
        codes,
        test_support::{dispatch_wire, peer, principal, request_context},
    },
    assert2::check,
    krabka_protocol::owned::{
        fetch_response::PartitionData,
        incremental_alter_configs_request::{
            self, AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        incremental_alter_configs_response::IncrementalAlterConfigsResponse,
    },
    std::time::{Duration, Instant},
};

/// A tiered broker that reads the tier with one reader thread, so a test that
/// holds the slot makes every other cold read wait for it.
async fn single_reader_broker() -> TieredBroker {
    tiered_broker_with(|config| config.remote_reader_threads = 1).await
}

/// Sets `remote.fetch.max.wait.ms` on this broker through
/// `IncrementalAlterConfigs`, and waits for the image to carry it.
async fn alter_remote_fetch_max_wait(broker: &Broker, ms: u64) {
    let node = broker.config.node_id;
    let principal = principal("admin");
    let peer = peer();
    let response: IncrementalAlterConfigsResponse = dispatch_wire(
        broker,
        incremental_alter_configs_request::API_KEY,
        1,
        &IncrementalAlterConfigsRequest {
            resources: vec![AlterConfigsResource {
                resource_type: 4,
                resource_name: node.0.to_string(),
                configs: vec![AlterableConfig {
                    name: "remote.fetch.max.wait.ms".into(),
                    config_operation: 0,
                    value: Some(ms.to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        },
        &request_context(&principal, &peer, "admin"),
    )
    .await;
    check!(response.responses[0].error_code == codes::NONE);
    let want = Duration::from_millis(ms);
    let deadline = Instant::now() + Duration::from_secs(10);
    // intentional: the alter commits on the controller and reaches this
    // broker's image asynchronously; the image has no awaitable per-key signal.
    while remote_fetch_max_wait(&broker.controller.current_image(), node) != want {
        assert2::assert!(
            Instant::now() <= deadline,
            "the alter never reached the image"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_remote_fetch_wait_bounds_a_read_that_cannot_start_and_follows_an_alter() {
    let owners = single_reader_broker().await;
    let broker = owners.handle.broker_arc_for_test();
    let part = committed_tiered_partition(&broker, owners.dir.path()).await;
    let reader = broker.remote_reader.clone().expect("remote reader");

    alter_remote_fetch_max_wait(&broker, 300).await;
    let held = reader.pool.acquire().await.expect("the only reader slot");
    let mut queued_read = out_of_range_at(&part, 0);
    let started = Instant::now();
    let served = tokio::time::timeout(
        Duration::from_secs(20),
        try_remote_read(
            &broker,
            &mut queued_read,
            &part,
            &mut RemoteFetchWait::default(),
        ),
    )
    .await
    .expect("the remote wait must bound a read that never gets a slot");
    let waited = started.elapsed();

    // `DelayedRemoteFetch.onComplete` for an expired read: the local read
    // result of the remote band, so no error, no records, and the live bounds.
    check!(served == Some(0));
    check!(
        queued_read.out
            == PartitionData {
                high_watermark: TIERED_HIGH_WATERMARK,
                last_stable_offset: TIERED_HIGH_WATERMARK,
                log_start_offset: 0,
                ..refused_read(0, codes::NONE)
            }
    );
    check!(waited >= Duration::from_millis(300));
    check!(waited < Duration::from_secs(10));

    // Raised past the time the slot stays held, the same read now waits for
    // it and serves the batch: the new value applies to the next fetch.
    alter_remote_fetch_max_wait(&broker, 30_000).await;
    let mut waiting = out_of_range_at(&part, 0);
    let mut wait = RemoteFetchWait::default();
    let (served, ()) = tokio::join!(
        try_remote_read(&broker, &mut waiting, &part, &mut wait),
        async {
            tokio::time::sleep(Duration::from_millis(500)).await;
            drop(held);
        }
    );
    check!(served.is_some_and(|bytes| bytes > 0));
    check!(waiting.out.error_code == codes::NONE);
    check!(
        waiting
            .out
            .records
            .as_ref()
            .is_some_and(|records| records.payload_len() > 0)
    );
}
