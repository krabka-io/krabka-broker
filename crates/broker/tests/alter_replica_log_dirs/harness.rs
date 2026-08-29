//! The two-directory broker every scenario boots on, and the observations the
//! scenarios make about where a partition currently lives.
//!
//! A log-dir move is broker-local physical state, so the only convergence
//! signal is `DescribeLogDirs` over the wire plus the partition directories on
//! disk. Both readings live here, next to the launcher that gives them the two
//! `log.dirs` they compare.

use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use assert2::assert;
use krabka_broker::{Broker, BrokerConfig, BrokerHandle};
use tempfile::TempDir;

use crate::wire::describe_log_dirs;

pub(crate) fn start_two_dir_broker()
-> impl std::future::Future<Output = (BrokerHandle, TempDir, TempDir, SocketAddr)> {
    let primary = tempfile::tempdir().unwrap();
    let extra = tempfile::tempdir().unwrap();
    let mut cfg = BrokerConfig::for_tests(primary.path().to_path_buf());
    cfg.extra_log_dirs = vec![extra.path().to_path_buf()];
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker start");
        let addr = handle.listen_addr();
        (handle, primary, extra, addr)
    })
}

pub(crate) async fn wait_all_partitions(handle: &BrokerHandle, topic: &str, n: i32) {
    for p in 0..n {
        handle.wait_until_partition_present(topic, p).await;
    }
}

pub(crate) fn count_topic_dirs(dir: &std::path::Path, topic: &str) -> usize {
    let prefix = format!("{topic}-");
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.filter_map(Result::ok)
        .filter(|e| e.file_type().is_ok_and(|t| t.is_dir()))
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with(&prefix) && !n.ends_with("-future"))
        })
        .count()
}

/// Wait until `DescribeLogDirs` reports both partitions of `topic`
/// under `target_dir` with `is_future_key == false`.
pub(crate) async fn wait_for_move_complete(
    addr: SocketAddr,
    target_dir: &std::path::Path,
    topic: &str,
    expected: &[i32],
) {
    let deadline = Instant::now() + Duration::from_secs(15);
    let target_canon = std::fs::canonicalize(target_dir).unwrap();
    loop {
        let resp = describe_log_dirs(addr).await;
        let mut current_in_target: Vec<i32> = Vec::new();
        let mut any_future = false;
        for result in &resp.results {
            let result_canon = std::fs::canonicalize(&result.log_dir)
                .unwrap_or_else(|_| std::path::PathBuf::from(&result.log_dir));
            if result_canon != target_canon {
                continue;
            }
            for t in &result.topics {
                if t.name != topic {
                    continue;
                }
                for p in &t.partitions {
                    if p.is_future_key {
                        any_future = true;
                    } else if expected.contains(&p.partition_index) {
                        current_in_target.push(p.partition_index);
                    }
                }
            }
        }
        current_in_target.sort_unstable();
        current_in_target.dedup();
        if !any_future && current_in_target == expected {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "move never completed: in_target={current_in_target:?} any_future={any_future}"
        );
        // intentional: log-dir move completion is broker-local physical state
        // (the `future_logs` map plus the on-disk partition-dir rename), not
        // reflected in the MetadataImage, exposed by any `*_for_test`
        // accessor, or surfaced as a metric. `DescribeLogDirs` over the wire
        // is the only observable, so we poll it with backoff.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
