//! Tiered-storage metadata-assignment integration: `KafkaMetadataEventLog` manual per-partition
//! fetch consumer honors a partition subset and a non-zero start offset.
//!
//! The test boots a bare loopback broker with no tiered-storage backend.
//! It constructs the `KafkaMetadataEventLog` directly, not through
//! `Broker::start`'s RLMM bootstrap. It publishes across three partitions,
//! then subscribes to a subset from a non-zero offset. It asserts that the
//! reworked consumer delivers exactly the assigned records, and never the
//! unassigned partition. A second test publishes one interleaved keyed group
//! and checks that each partition lands it in the order given.

use assert2::assert;
mod support;

use {
    bytes::Bytes,
    futures_util::StreamExt,
    krabka_broker::BrokerHandle,
    krabka_remote_storage_topic::{
        kafka_log::{KafkaMetadataEventLog, KafkaMetadataLogConfig},
        log::{KeyedEvent, MetadataEventLog, PartitionStart},
    },
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

krabka_macros::bound_start_fixture!(config, bare_config, ::krabka_broker);

/// A running bare broker and the log directory behind it.
struct BareBroker {
    broker: BrokerHandle,
    log_dir: TempDir,
}

/// A record the metadata log delivered.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Received {
    partition: i32,
    offset: i64,
    payload: Vec<u8>,
}

/// A keyed record the metadata log delivered.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
struct KeyedReceived {
    partition: i32,
    offset: i64,
    key: Vec<u8>,
    payload: Vec<u8>,
    tombstone: bool,
}

/// Boot a bare loopback broker with the pinned-port pattern from
/// `tiered_storage_topic_rlmm.rs::start_broker_with_topic_rlmm`. This
/// helper drops the tiered-storage backend and the `remote_log_metadata`
/// bootstrap, because the test wires `KafkaMetadataEventLog` by hand. It
/// returns the handle plus the log tempdir, which the caller keeps alive.
async fn start_bare_broker() -> BareBroker {
    support::init_tracing();

    // Pin a loopback port so the metadata-log bootstrap address is
    // knowable before the listener binds.
    let (clients, controllers, client_listeners, controller_listeners) =
        support::bind_and_hold_ports(1).await;
    let (listen, ctrl) = (clients[0], controllers[0]);

    let log_dir = TempDir::new().expect("log tempdir");

    let cfg = bare_config(log_dir.path(), listen, ctrl);

    let broker =
        support::start_first_held(cfg, client_listeners, controller_listeners, "broker start")
            .await;
    BareBroker { broker, log_dir }
}

/// A bare broker and a three-partition metadata event log on it.
async fn start_three_partition_log() -> (BareBroker, Arc<KafkaMetadataEventLog>) {
    let bare = start_bare_broker().await;
    let bootstrap = format!("127.0.0.1:{}", bare.broker.listen_addr().port());
    let mut cfg = KafkaMetadataLogConfig::new(bootstrap);
    cfg.num_partitions = 3;
    cfg.replication = 1;
    cfg.min_isr = Some(1);
    let log = KafkaMetadataEventLog::start(cfg).await.expect("log start");
    (bare, log)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_subset_from_nonzero_offset_yields_exact_records() {
    let (
        BareBroker {
            broker,
            log_dir: _log_dir,
        },
        log,
    ) = start_three_partition_log().await;
    let pc = log.partition_count();
    assert!(pc >= 3);

    // Publish: partition 0 -> a,b,c ; partition 1 -> x,y ; partition 2 -> z
    for v in [b"a".as_slice(), b"b", b"c"] {
        log.publish(0, Bytes::copy_from_slice(v)).await.unwrap();
    }
    for v in [b"x".as_slice(), b"y"] {
        log.publish(1, Bytes::copy_from_slice(v)).await.unwrap();
    }
    log.publish(2, Bytes::from_static(b"z")).await.unwrap();

    // Subscribe to partitions {0 from offset 1, 1 from offset 0}; not 2.
    let starts = [PartitionStart::new(0, 1), PartitionStart::new(1, 0)];
    let (mut stream, _handle) = log.subscribe(starts.to_vec());

    // Expect exactly: (0,1,b), (0,2,c), (1,0,x), (1,1,y). Collect with a
    // deadline; assert no partition-2 record ever arrives.
    let mut got: Vec<Received> = Vec::with_capacity(4);
    let deadline = Instant::now() + Duration::from_secs(15);
    while got.len() < 4 {
        let next = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        match next {
            Ok(Some(r)) => {
                assert!(r.partition != 2, "partition 2 was not assigned");
                got.push(Received {
                    partition: r.partition,
                    offset: r.offset,
                    payload: r.payload.to_vec(),
                });
            }
            Ok(None) => break,
            Err(_) => {} // timeout tick; keep waiting until deadline
        }
        assert!(
            Instant::now() <= deadline,
            "did not receive 4 records in 15s: {got:?}"
        );
    }
    got.sort_unstable();
    let expected = [
        Received {
            partition: 0,
            offset: 1,
            payload: b"b".to_vec(),
        },
        Received {
            partition: 0,
            offset: 2,
            payload: b"c".to_vec(),
        },
        Received {
            partition: 1,
            offset: 0,
            payload: b"x".to_vec(),
        },
        Received {
            partition: 1,
            offset: 1,
            payload: b"y".to_vec(),
        },
    ];
    assert!(got == expected);

    // Brief grace: ensure no stray partition-2 record sneaks in.
    if let Ok(Some(extra)) = tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
        assert!(extra.partition != 2, "partition 2 leaked: {extra:?}");
    }

    log.shutdown().await;
    broker.shutdown().await;
}

/// `publish_keyed_group` enqueues the whole group before it waits. Each
/// partition still assigns offsets in the order the group lists its records,
/// and every key, value and tombstone lands as given.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_group_lands_each_partition_in_order() {
    let (
        BareBroker {
            broker,
            log_dir: _log_dir,
        },
        log,
    ) = start_three_partition_log().await;
    assert!(log.partition_count() >= 3);

    assert!(log.publish_keyed_group(Vec::new()).await.unwrap() == Vec::<i64>::new());

    let group = [
        (0, "k-a", Some("a")),
        (1, "k-x", Some("x")),
        (0, "k-b", Some("b")),
        (2, "k-z", Some("z")),
        (1, "k-y", Some("y")),
        (0, "k-a", None),
    ];
    let offsets = log
        .publish_keyed_group(
            group
                .iter()
                .map(|&(partition, key, value)| KeyedEvent {
                    partition,
                    key: Bytes::from_static(key.as_bytes()),
                    event: value.map(|value| Bytes::from_static(value.as_bytes())),
                })
                .collect(),
        )
        .await
        .unwrap();
    assert!(offsets == [0, 0, 1, 0, 1, 2]);

    let starts = (0..3).map(|partition| PartitionStart::new(partition, 0));
    let (mut stream, _handle) = log.subscribe(starts.collect());
    let mut got = Vec::with_capacity(group.len());
    let deadline = Instant::now() + Duration::from_secs(15);
    while got.len() < group.len() {
        if let Ok(Some(r)) = tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
            got.push(KeyedReceived {
                partition: r.partition,
                offset: r.offset,
                key: r.key.map(|key| key.to_vec()).unwrap_or_default(),
                payload: r.payload.to_vec(),
                tombstone: r.tombstone,
            });
        }
        assert!(
            Instant::now() <= deadline,
            "did not receive {} records in 15s: {got:?}",
            group.len()
        );
    }
    got.sort_unstable();
    let mut expected: Vec<_> = group
        .iter()
        .zip(offsets)
        .map(|(&(partition, key, value), offset)| KeyedReceived {
            partition,
            offset,
            key: key.as_bytes().to_vec(),
            payload: value.unwrap_or_default().as_bytes().to_vec(),
            tombstone: value.is_none(),
        })
        .collect();
    expected.sort_unstable();
    assert!(got == expected);

    log.shutdown().await;
    broker.shutdown().await;
}
