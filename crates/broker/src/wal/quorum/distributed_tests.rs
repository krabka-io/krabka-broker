//! Tests for the distributed flavour of `QuorumWalStore`, where durability
//! comes from remote fsync acknowledgements: which acknowledgement counts
//! towards the watermark, how a voter-set change takes effect, and what a
//! reopen must not truncate.

use std::sync::{Arc, Mutex};

use assert2::assert;
use krabka_ids::{Offset, PartitionIndex};
use krabka_kraft_core::NodeId;
use krabka_log::{Log, LogConfig};
use uuid::Uuid;

use super::{
    QuorumWalStore,
    test_support::{append_source, batch},
};
use crate::wal::WalStore;

#[tokio::test]
async fn distributed_wal_waits_for_a_remote_fsync_ack() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let store = Arc::new(
        QuorumWalStore::for_distributed_partition(
            Uuid::from_u128(99),
            PartitionIndex(0),
            source,
            None,
            3,
        )
        .unwrap(),
    );
    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let (_results, leo) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let mut sync = tokio::spawn(async move { syncing.sync_durable(leo).await });

    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut sync)
            .await
            .is_err()
    );
    assert!(!store.engine.record_follower_ack(NodeId(9), leo));
    assert!(!store.engine.record_follower_ack(NodeId(1), leo));
    assert!(
        !store
            .engine
            .record_follower_ack(NodeId(2), Offset(leo.0 + 1))
    );
    assert!(store.engine.record_follower_ack(NodeId(2), leo));

    assert!(sync.await.unwrap().unwrap() == leo);
    assert!(store.engine.durable_watermark() == leo);
    assert!(store.engine.replica_end_offsets() == vec![leo]);
    assert!(store.trim_to_offset(leo).await.unwrap() == leo);
    assert!(store.engine.replica_start_offsets() == vec![leo]);
    assert!(
        !store
            .engine
            .record_follower_ack(NodeId(2), Offset(leo.0 - 1))
    );

    let (_results, next) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let mut sync = tokio::spawn(async move { syncing.sync_durable(next).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut sync)
            .await
            .is_err()
    );
    store.engine.configure_distributed(NodeId(1), &[]);
    let error = sync.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("placement disappeared"));
}

#[tokio::test]
async fn durable_advance_waits_for_an_offset_strictly_after_the_observation() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let store = Arc::new(
        QuorumWalStore::for_distributed_partition(
            Uuid::new_v4(),
            PartitionIndex(0),
            source,
            None,
            3,
        )
        .unwrap(),
    );
    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    let (_results, first) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let sync = tokio::spawn(async move { syncing.sync_durable(first).await });
    assert!(store.engine.record_follower_ack(NodeId(2), first));
    assert!(sync.await.unwrap().unwrap() == first);

    let engine = Arc::clone(&store.engine);
    let mut waiting = tokio::spawn(async move { engine.wait_for_durable_advance(first).await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut waiting)
            .await
            .is_err()
    );

    let (_results, second) = append_source(&store, 1).await;
    let syncing = Arc::clone(&store);
    let sync = tokio::spawn(async move { syncing.sync_durable(second).await });
    assert!(
        !store
            .engine
            .record_follower_ack(NodeId(2), Offset(first.0 - 1))
    );
    assert!(store.engine.record_follower_ack(NodeId(2), second));

    assert!(sync.await.unwrap().unwrap() == second);
    assert!(waiting.await.unwrap() == second);
}

#[tokio::test]
async fn distributed_wal_rejects_misordered_or_incomplete_voter_sets() {
    for voters in [
        vec![NodeId(2), NodeId(1), NodeId(3)],
        vec![NodeId(1), NodeId(2)],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
        ));
        let store = QuorumWalStore::for_distributed_partition(
            Uuid::new_v4(),
            PartitionIndex(0),
            source,
            None,
            3,
        )
        .unwrap();
        let (_results, leo) = append_source(&store, 1).await;

        store.engine.configure_distributed(NodeId(1), &voters);

        assert!(!store.engine.record_follower_ack(NodeId(2), leo));
        assert!(store.engine.durable_watermark() == Offset(0));
    }
}

#[tokio::test]
async fn distributed_wal_reconfiguration_replaces_the_remote_voter_set() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let store = QuorumWalStore::for_distributed_partition(
        Uuid::new_v4(),
        PartitionIndex(0),
        source,
        None,
        3,
    )
    .unwrap();
    let (_results, leo) = append_source(&store, 1).await;
    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);

    store
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(3), NodeId(4)]);

    assert!(!store.engine.record_follower_ack(NodeId(2), leo));
    assert!(store.engine.record_follower_ack(NodeId(3), leo));
    assert!(store.engine.durable_watermark() == leo);
}

#[tokio::test]
async fn distributed_wal_reopens_without_truncating_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let source_dir = dir.path().join("source");
    let source = Arc::new(Mutex::new(
        Log::open(&source_dir, LogConfig::default()).unwrap(),
    ));
    let mut batch = batch(2);
    source.lock().unwrap().append(&mut batch).unwrap();
    source.lock().unwrap().sync().unwrap();
    let store = QuorumWalStore::for_distributed_partition(
        Uuid::from_u128(100),
        PartitionIndex(0),
        source.clone(),
        None,
        3,
    )
    .unwrap();
    assert!(store.engine.durable_watermark() == Offset(0));
    assert!(store.engine.replica_end_offsets() == vec![Offset(2)]);
    drop(store);
    drop(source);

    let source = Arc::new(Mutex::new(
        Log::open(&source_dir, LogConfig::default()).unwrap(),
    ));
    let reopened = QuorumWalStore::for_distributed_partition(
        Uuid::from_u128(100),
        PartitionIndex(0),
        source,
        None,
        3,
    )
    .unwrap();

    assert!(reopened.engine.durable_watermark() == Offset(0));
    assert!(reopened.engine.replica_end_offsets() == vec![Offset(2)]);
    reopened
        .engine
        .configure_distributed(NodeId(1), &[NodeId(1), NodeId(2), NodeId(3)]);
    assert!(reopened.engine.record_follower_ack(NodeId(2), Offset(2)));
    assert!(reopened.engine.durable_watermark() == Offset(2));
}
