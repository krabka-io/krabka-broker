// rustc 1.95 clippy ICEs on annotate-snippets in pedantic lints on these
// raw-wire test files; match the opt-out used by compaction.rs / elect_leaders.rs.

//! End-to-end test of JBOD, multi-log-dir, and `DescribeLogDirs` (KIP-113).
//!
//! It boots a single broker with two log directories, creates a 6-partition
//! topic, and asserts:
//!   1. the partition data is spread across both directories, from
//!      least-loaded placement, and
//!   2. `DescribeLogDirs` reports one result per directory, and the union of
//!      those results covers every partition and matches what is on disk.

mod kafka_wire;

mod support;

use std::net::SocketAddr;

use assert2::assert;
use krabka_broker::BrokerHandle;
use krabka_protocol::owned::{
    describe_log_dirs_request::DescribeLogDirsRequest,
    describe_log_dirs_response::DescribeLogDirsResponse,
};
use tokio::net::TcpStream;

use crate::support::storage::start_two_dir_broker;

const CLIENT_ID: &str = "krabka-jbod-test";

async fn create_topic(addr: SocketAddr, topic: &str, partitions: i32) {
    kafka_wire::create_topic_plaintext(addr, CLIENT_ID, kafka_wire::topic(topic, partitions, 1))
        .await;
}

async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    kafka_wire::exchange(
        &mut stream,
        &DescribeLogDirsRequest {
            topics: None,
            ..Default::default()
        },
        35,
        4,
        1,
        CLIENT_ID,
        true,
    )
    .await
    .unwrap()
}

async fn wait_all_partitions(handle: &BrokerHandle, topic: &str, n: i32) {
    // The on-disk / DescribeLogDirs assertions below read partition directories
    // straight from the log dirs, so wait for each partition's LOCAL writer-actor
    // to materialize (which creates its dir) — not just the metadata image, which
    // can name the partition before the local replica exists. `min = 0` waits only
    // for the local replica/writer to appear.
    for p in 0..n {
        handle.wait_until_local_log_end_offset(topic, p, 0).await;
    }
}

/// Count `topic-partition` subdirs for `topic` directly under `dir`.
fn count_topic_dirs(dir: &std::path::Path, topic: &str) -> usize {
    crate::support::storage::count_partition_dirs(std::fs::read_dir(dir).unwrap(), topic, true)
}

#[tokio::test]
async fn partitions_spread_across_dirs_and_describe_log_dirs_reports_them() {
    let (handle, primary, extra, addr) = start_two_dir_broker().await;
    let n: i32 = 6;
    create_topic(addr, "t", n).await;
    wait_all_partitions(&handle, "t", n).await;

    // 1. Placement spread: both directories hold at least one partition of `t`.
    let in_primary = count_topic_dirs(primary.path(), "t");
    let in_extra = count_topic_dirs(extra.path(), "t");
    assert!(
        in_primary + in_extra == usize::try_from(n).unwrap(),
        "all partitions on disk"
    );
    assert!(
        in_primary > 0 && in_extra > 0,
        "partitions must spread across both dirs: primary={in_primary} extra={in_extra}"
    );

    // 2. DescribeLogDirs reports one result per configured dir, and the
    //    union of `t` partitions across results is the full 0..n set.
    let resp = describe_log_dirs(addr).await;
    assert!(resp.error_code == 0);
    assert!(resp.results.len() == 2, "one result per log dir");

    let mut reported: Vec<i32> = Vec::new();
    for result in &resp.results {
        assert!(result.error_code == 0);
        for topic in &result.topics {
            if topic.name == "t" {
                for p in &topic.partitions {
                    reported.push(p.partition_index);
                    assert!(p.partition_size >= 0);
                    assert!(!p.is_future_key);
                }
            }
        }
    }
    reported.sort_unstable();
    assert!(
        reported == (0..n).collect::<Vec<_>>(),
        "all partitions reported"
    );

    handle.shutdown().await;
}
