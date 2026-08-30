//! Tests for the durability path of `QuorumWalStore`: the majority rule and
//! the batch splitting it rests on, the commit that survives the loss of a
//! minority of replicas, the hot-tail run it mirrors, and the trim that follows
//! a committed prefix.

use std::sync::{Arc, Mutex};

use assert2::assert;
use krabka_compression::CompressionType;
use krabka_ids::{Offset, PartitionIndex};
use krabka_kraft_core::NodeId;
use krabka_log::{Log, LogConfig};
use uuid::Uuid;

use super::{
    HotTailTarget, QuorumWalStore,
    engine::{self, WalShardEngine},
    test_support::{append_source, batch},
};
use crate::{error::BrokerError, wal::WalStore};

#[test]
fn strict_majority_requires_more_than_half_of_every_voter_set() {
    for (voters, required) in [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3)] {
        assert!(engine::strict_majority(voters) == required);
    }
}

#[test]
fn split_batches_preserves_compressed_wire_boundaries() {
    let mut wire = bytes::BytesMut::new();
    for base_offset in [0, 20] {
        let mut compressed = batch(20);
        compressed.base_offset = base_offset;
        compressed.attributes = compressed.attributes.with_compression(CompressionType::Lz4);
        for record in &mut compressed.records {
            record.value = Some(bytes::Bytes::from(vec![b'x'; 256]));
        }
        compressed.encode(&mut wire).unwrap();
    }

    let batches = engine::split_batches(&wire.freeze()).unwrap();

    assert!(batches.len() == 2);
    assert!(batches[0].base_offset == Offset(0));
    assert!(batches[0].last_offset == Offset(19));
    assert!(batches[1].base_offset == Offset(20));
    assert!(batches[1].last_offset == Offset(39));
}

#[tokio::test]
async fn quorum_wal_store_commits_on_f_plus_1_and_survives_one_loss() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(source_dir.path(), LogConfig::default()).unwrap(),
    ));
    let replica_dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let engine = Arc::new(WalShardEngine::for_logs(
        [NodeId(1), NodeId(2), NodeId(3)]
            .into_iter()
            .zip(replica_dirs.iter().map(|dir| {
                Arc::new(Mutex::new(
                    Log::open(dir.path(), LogConfig::default()).unwrap(),
                ))
            }))
            .collect(),
    ));
    let store = QuorumWalStore::new(source.clone(), engine.clone());

    let (results, leo) = append_source(&store, 3).await;
    assert!(results.iter().all(Result::is_ok));
    assert!(leo == Offset(3));
    assert!(store.sync_durable(leo).await.unwrap() == Offset(3));
    assert!(engine.durable_watermark() == Offset(3));

    engine.set_replica_alive(NodeId(3), false);
    let (_results, leo) = append_source(&store, 2).await;
    assert!(store.sync_durable(leo).await.unwrap() == Offset(5));
    assert!(engine.durable_watermark() == Offset(5));

    engine.set_replica_alive(NodeId(2), false);
    let (_results, leo) = append_source(&store, 1).await;
    assert!(store.sync_durable(leo).await.is_err());
    assert!(engine.durable_watermark() == Offset(5));

    engine.set_replica_alive(NodeId(3), true);
    let (_results, leo) = append_source(&store, 1).await;
    assert!(store.sync_durable(leo).await.unwrap() == Offset(7));
    assert!(engine.replica_end_offsets()[2] == Offset(7));
}

#[tokio::test]
async fn five_voter_quorum_requires_three_durable_copies() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(source_dir.path(), LogConfig::default()).unwrap(),
    ));
    let replica_dirs = (0..5)
        .map(|_| tempfile::tempdir().unwrap())
        .collect::<Vec<_>>();
    let engine = Arc::new(WalShardEngine::for_logs(
        (1_u64..=5)
            .map(NodeId)
            .zip(replica_dirs.iter().map(|dir| {
                Arc::new(Mutex::new(
                    Log::open(dir.path(), LogConfig::default()).unwrap(),
                ))
            }))
            .collect(),
    ));
    let store = QuorumWalStore::new(source, engine.clone());
    for voter in [NodeId(3), NodeId(4), NodeId(5)] {
        engine.set_replica_alive(voter, false);
    }
    let (_results, leo) = append_source(&store, 1).await;

    assert!(store.sync_durable(leo).await.is_err());
    assert!(engine.durable_watermark() == Offset(0));

    engine.set_replica_alive(NodeId(3), true);
    assert!(store.sync_durable(leo).await.unwrap() == leo);
}

#[tokio::test]
async fn quorum_wal_store_populates_hot_tail_after_durable_sync() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(source_dir.path(), LogConfig::default()).unwrap(),
    ));
    let replica_dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let engine = Arc::new(WalShardEngine::for_logs(
        [NodeId(1), NodeId(2), NodeId(3)]
            .into_iter()
            .zip(replica_dirs.iter().map(|dir| {
                Arc::new(Mutex::new(
                    Log::open(dir.path(), LogConfig::default()).unwrap(),
                ))
            }))
            .collect(),
    ));
    let cache = Arc::new(crate::diskless::hot_tail::HotTailCache::default());
    let topic_id = Uuid::from_u128(9);
    let partition = PartitionIndex(0);
    let store = QuorumWalStore {
        source: source.clone(),
        engine,
        hot_tail: Some(HotTailTarget {
            topic_id,
            partition,
            cache: cache.clone(),
        }),
    };

    let (_results, leo) = append_source(&store, 2).await;
    assert!(store.sync_durable(leo).await.unwrap() == Offset(2));

    // `i64::MAX` as the visibility limit: this test asserts the cache
    // holds the run, not that a fetch window bounds it.
    assert!(
        cache
            .get(topic_id, partition, 1, i64::MAX, usize::MAX)
            .is_some()
    );
}

#[tokio::test]
async fn quorum_wal_store_can_commit_a_source_prefix_without_regressing() {
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
    assert!(store.sync_durable(first).await.unwrap() == Offset(1));
    assert!(store.sync_durable(second).await.unwrap() == Offset(2));
    assert!(store.sync_durable(first).await.unwrap() == Offset(2));
    assert!(store.engine.durable_watermark() == Offset(2));
}

#[tokio::test]
async fn quorum_wal_store_trims_every_replica_before_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let store = QuorumWalStore::for_partition(
        "topic",
        None,
        PartitionIndex(0),
        dir.path(),
        source.clone(),
        None,
        3,
    )
    .unwrap();
    let (_results, leo) = append_source(&store, 3).await;
    store.sync_durable(leo).await.unwrap();

    let start = store.trim_to_offset(Offset(2)).await.unwrap();

    assert!(start >= Offset(2));
    assert!(source.lock().unwrap().log_start_offset() == start);
    assert!(store.engine.replica_start_offsets() == vec![start, start, start]);
}

#[tokio::test]
async fn quorum_wal_store_rejects_a_source_outside_the_voter_set() {
    let dir = tempfile::tempdir().unwrap();
    let source = Arc::new(Mutex::new(
        Log::open(dir.path().join("source"), LogConfig::default()).unwrap(),
    ));
    let replica = Arc::new(Mutex::new(
        Log::open(dir.path().join("replica"), LogConfig::default()).unwrap(),
    ));
    let engine = Arc::new(WalShardEngine::for_logs(
        maplit::btreemap! {NodeId(1) => replica},
    ));
    let store = QuorumWalStore::new(source, engine);

    let error = store.trim_to_offset(Offset(0)).await.unwrap_err();

    assert!(
        matches!(error, BrokerError::Replication(message) if message == "wal quorum source is not its first replica")
    );
}
