//! Tests for the WAL fetch that `QuorumWalStore` serves out of its shard
//! engine, where the high watermark and the log end offset are separate
//! frontiers, and an empty read is not an out-of-range read.

use std::sync::{Arc, Mutex};

use assert2::assert;
use krabka_ids::{Offset, PartitionIndex};
use krabka_log::{Log, LogConfig};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};

use super::{QuorumWalStore, test_support::append_source};
use crate::wal::WalStore;

#[tokio::test]
async fn wal_fetch_serves_the_uncommitted_tail_with_separate_frontiers() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let store = QuorumWalStore::for_partition(
        "topic",
        None,
        PartitionIndex(0),
        dir.path(),
        source,
        None,
        3,
    )
    .unwrap();

    let (_results, first) = append_source(&store, 1).await;
    let (_results, second) = append_source(&store, 1).await;
    store.sync_durable(first).await.unwrap();

    let fetch = store
        .engine
        .serve_fetch(first, -1, ByteSize::from_bytes(u64::MAX))
        .unwrap();

    assert!(fetch.high_watermark == first);
    assert!(fetch.log_end_offset == second);
    assert!(fetch.log_start_offset == Offset(0));
    assert!(!fetch.offset_out_of_range);
    assert!(!fetch.records.is_empty());
}

#[tokio::test]
async fn wal_fetch_accepts_the_log_end_and_a_zero_byte_limit() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let store = QuorumWalStore::for_partition(
        "topic",
        None,
        PartitionIndex(0),
        dir.path(),
        source,
        None,
        3,
    )
    .unwrap();
    let (_results, log_end) = append_source(&store, 1).await;

    let at_end = store
        .engine
        .serve_fetch(log_end, -1, ByteSize::from_bytes(u64::MAX))
        .unwrap();
    assert!(!at_end.offset_out_of_range);
    assert!(at_end.records.is_empty());

    let zero_bytes = store
        .engine
        .serve_fetch(Offset(0), -1, ByteSize::ZERO)
        .unwrap();
    assert!(!zero_bytes.offset_out_of_range);
    assert!(zero_bytes.records.is_empty());
}
