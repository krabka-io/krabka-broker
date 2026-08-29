//! `DeleteRecords` (`api_key` 21): trimming a partition from a given offset, and
//! the agreement between the response's `low_watermark` and the partition's
//! `log_start_offset`.

use assert2::{assert, check};
use krabka_protocol::owned::delete_records_request::{
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsTopic,
};

use crate::{
    admin_harness::{build_client, create_topic_helper},
    support::start_n_node,
};

/// `DeleteRecords`: the test produces 100 records and then trims from offset
/// 50. The response carries a valid `low_watermark`, and the broker's
/// `log_start_offset` moves forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_trims_log_start() {
    let cluster = start_n_node(1).await.expect("start_n_node");
    let (broker, cfg, _dir) = &cluster[0];
    let client = build_client(cfg.listen_addr).await;

    create_topic_helper(&client, "t-dr", 1).await;

    // Produce 100 single-record batches through the broker's test helper.
    broker
        .produce_records_for_test("t-dr", 0, 100)
        .await
        .expect("produce_records_for_test");

    let req = DeleteRecordsRequest {
        topics: vec![DeleteRecordsTopic {
            name: "t-dr".into(),
            partitions: vec![DeleteRecordsPartition {
                partition_index: 0,
                offset: 50,
                ..Default::default()
            }],
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = client.send(req).await.expect("delete_records");
    let part_result = &resp.topics[0].partitions[0];
    check!(
        part_result.error_code == 0,
        "delete_records error: {:?}",
        part_result.error_code
    );
    // low_watermark must be the resulting log_start_offset after trim.
    check!(
        part_result.low_watermark >= 0,
        "low_watermark should be non-negative, got {}",
        part_result.low_watermark
    );
    check!(
        part_result.low_watermark <= 50,
        "low_watermark {} should be <= requested offset 50",
        part_result.low_watermark
    );

    let log_start = broker
        .partition_log_start_for_test("t-dr", 0)
        .expect("partition exists");
    assert!(
        log_start == part_result.low_watermark,
        "partition log_start_offset should equal low_watermark"
    );
}
