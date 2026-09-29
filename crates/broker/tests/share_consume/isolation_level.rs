//! Share fetches of a group whose `share.isolation.level` is
//! `read_committed`. The acquire window is clamped to the last stable offset,
//! so the records of an open transaction stay invisible until that
//! transaction commits, and the broker surfaces them afterwards rather than
//! losing them. The data of an aborted transaction is archived and never
//! acquired.
//!
//! Kafka has no broker isolation key, so each test sets the group override,
//! and a group without one reads `read_uncommitted`.

use std::{collections::BTreeSet, time::Duration};

use assert2::assert;
use krabka_broker::{Broker, BrokerHandle};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_metadata::{GroupConfigRecord, MetadataRecord};

use crate::{
    harness::{
        bootstrap_share_state, broker_config, broker_test_permit, connect, create_topic, join,
        produce_n, topic_id, wait_for_share_init,
    },
    share_rpc::{acquired_count, share_fetch},
};

/// Kafka's `GroupConfig.SHARE_ISOLATION_LEVEL_CONFIG` values.
const READ_COMMITTED: &str = "read_committed";
const READ_UNCOMMITTED: &str = "read_uncommitted";

/// Sets the `share.isolation.level` of `group` in the metadata image, beside
/// the `share.auto.offset.reset=earliest` that `bootstrap_share_state` sets,
/// in the record `IncrementalAlterConfigs` writes for a group config.
///
/// The record replaces the group's whole override map, so it carries both
/// keys, and it goes after `bootstrap_share_state`.
async fn set_isolation_level(broker: &BrokerHandle, group: &str, level: &str) {
    broker
        .submit_metadata_record_for_test(MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: group.into(),
            configs: [
                ("share.auto.offset.reset".to_owned(), "earliest".to_owned()),
                ("share.isolation.level".to_owned(), level.to_owned()),
            ]
            .into(),
        }))
        .await
        .expect("set share.isolation.level");
}

/// F2 (`read_committed`): with `share.isolation.level = read_committed`, a
/// share fetch never surfaces records from an OPEN transaction (offsets past
/// the LSO).
///
/// A transactional producer begins a txn and sends 3 records but does NOT
/// commit. The partition's HWM is then 3 while the LSO stays at 0. A
/// `read_committed` share fetch clamps its read window to `min(LSO, HWM) = 0`,
/// so it acquires nothing. After the txn commits, the LSO advances to 3 and the
/// same group then acquires all 3. This proves the clamp tracked the LSO, and
/// that the broker merely deferred the records and did not lose them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_skips_open_txn_then_sees_committed() {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = connect(&bootstrap).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1").await;
    set_isolation_level(&broker, "g1", READ_COMMITTED).await;
    // The krabka producer does not retry a `FindCoordinator` that answers
    // `COORDINATOR_NOT_AVAILABLE`, so the transaction coordinator must serve
    // before `init_transactions`.
    broker.wait_until_transaction_coordinator_ready().await;

    // Open a transaction and send 3 records WITHOUT committing: HWM=3, LSO=0.
    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("share-rc-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "t".into(),
                    value: Some(bytes::Bytes::from(v.to_string())),
                    ..Default::default()
                })
                .await,
        );
    }
    // Flush the records to the log (advances HWM) but keep the txn OPEN (LSO=0).
    producer.flush().await.unwrap();

    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // A read_committed share fetch must acquire NOTHING: every record is past
    // the LSO (still 0). Poll a few times to be sure it never spuriously acquires.
    for epoch in 0..6 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        assert!(
            acquired_count(&row) == 0,
            "read_committed must not surface open-txn records, got {:?}",
            row.acquired_records
        );
        // intentional: deliberately observe that nothing is acquired across a
        // window while the txn stays open (behavior under test, not a
        // state-settle guess).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Commit the transaction → the LSO advances past the records (a commit
    // control marker is appended, so HWM == LSO). The same group now acquires
    // the committed records (proving they were deferred, not dropped). The
    // acquired window also covers the control-marker offset, whose bytes the
    // read path filters out — so we assert on the surfaced record VALUES.
    txn.commit().await.unwrap();
    let mut values: Vec<String> = Vec::new();
    for epoch in 6..30 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        if acquired_count(&row) > 0
            && let Some(batches) = row.records.as_ref().and_then(|r| r.as_v2())
        {
            values = batches
                .iter()
                .flat_map(|b| b.records.iter())
                .filter_map(|r| r.value.as_ref())
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .collect();
            values.sort();
            if values == vec!["a", "b", "c"] {
                break;
            }
        }
        // intentional: bounded RPC poll for the post-commit LSO advance
        // (transaction-coordinator state, not in the metadata image) to surface
        // via ShareFetch.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        values == vec!["a", "b", "c"],
        "after commit the read_committed fetch must surface the 3 committed \
         records, got {values:?}"
    );

    producer.close().await.unwrap();
}

/// What a share group sees of one transaction followed by one plain record:
/// the offsets it acquired and the record values that it got.
#[derive(Debug, PartialEq, Eq)]
struct Seen {
    acquired: BTreeSet<i64>,
    values: BTreeSet<String>,
}

/// Kafka's `SharePartition.filterAbortedTransactionalAcquiredRecords`: under
/// `read_committed`, a share group never acquires the data of an aborted
/// transaction. The share consumer cannot drop it itself, because a
/// `ShareFetch` response has no aborted-transactions field.
///
/// Each row writes a transaction of three records at offsets 0-2 and ends it,
/// which puts the marker at offset 3. It then produces one plain record `v0`
/// at offset 4. The marker is never acquired.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_transaction_data_is_archived_under_read_committed() {
    let cases = [
        (
            "read_committed, commit",
            Some(READ_COMMITTED),
            true,
            Seen {
                acquired: BTreeSet::from([0, 1, 2, 4]),
                values: ["a", "b", "c", "v0"].map(String::from).into(),
            },
        ),
        (
            "read_committed, abort",
            Some(READ_COMMITTED),
            false,
            Seen {
                acquired: BTreeSet::from([4]),
                values: ["v0"].map(String::from).into(),
            },
        ),
        (
            "read_uncommitted, abort",
            Some(READ_UNCOMMITTED),
            false,
            Seen {
                acquired: BTreeSet::from([0, 1, 2, 4]),
                values: ["a", "b", "c", "v0"].map(String::from).into(),
            },
        ),
        (
            "no override reads uncommitted, abort",
            None,
            false,
            Seen {
                acquired: BTreeSet::from([0, 1, 2, 4]),
                values: ["a", "b", "c", "v0"].map(String::from).into(),
            },
        ),
    ];

    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (name, isolation_level, commit, want) in cases {
        actual.push((name, transaction_then_record(isolation_level, commit).await));
        expected.push((name, want));
    }
    assert!(actual == expected);
}

/// `isolation_level` is the group's `share.isolation.level`, or `None` for a
/// group with no override.
async fn transaction_then_record(isolation_level: Option<&str>, commit: bool) -> Seen {
    let _permit = broker_test_permit().await;
    let dir = tempfile::TempDir::new().unwrap();
    let broker = Broker::start(broker_config(dir.path().to_path_buf()))
        .await
        .unwrap();
    let bootstrap = broker.listen_addr().to_string();
    let client = connect(&bootstrap).await;
    create_topic(&broker, &client, "t", 1).await;
    let tid = topic_id(&broker, "t");
    bootstrap_share_state(&broker, &client, "g1").await;
    if let Some(level) = isolation_level {
        set_isolation_level(&broker, "g1", level).await;
    }
    broker.wait_until_transaction_coordinator_ready().await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("share-aborted-tid")
        .build()
        .await
        .unwrap();
    producer.init_transactions().await.unwrap();
    let txn = producer.begin_transaction().await.unwrap();
    for v in ["a", "b", "c"] {
        drop(
            producer
                .send(ProducerRecord {
                    topic: "t".into(),
                    value: Some(bytes::Bytes::from(v.to_string())),
                    ..Default::default()
                })
                .await,
        );
    }
    producer.flush().await.unwrap();
    if commit {
        txn.commit().await.unwrap();
    } else {
        txn.abort().await.unwrap();
    }
    produce_n(&client, "t", tid, 0, 1).await;

    let (member, member_epoch) = join(&client, "g1", "t").await;
    wait_for_share_init(&broker, &client, &member, member_epoch, tid).await;

    // The member never acknowledges, so each offset is acquired at most once
    // while its lock holds. Fetch until the plain record at offset 4 arrives.
    let mut seen = Seen {
        acquired: BTreeSet::new(),
        values: BTreeSet::new(),
    };
    for epoch in 0..30 {
        let row = share_fetch(&client, "g1", &member, tid, 0, epoch, 0).await;
        let row_acquired: BTreeSet<i64> = row
            .acquired_records
            .iter()
            .flat_map(|range| range.first_offset..=range.last_offset)
            .collect();
        if let Some(batches) = row.records.as_ref().and_then(|r| r.as_v2()) {
            for batch in batches {
                for record in &batch.records {
                    let offset = batch.base_offset + i64::from(record.offset_delta);
                    if let Some(value) = record.value.as_ref()
                        && row_acquired.contains(&offset)
                    {
                        seen.values
                            .insert(String::from_utf8_lossy(value).into_owned());
                    }
                }
            }
        }
        seen.acquired.extend(row_acquired);
        if seen.acquired.contains(&4) {
            break;
        }
        // intentional: bounded RPC poll for the LSO to pass the marker and the
        // plain record to become acquirable; no image or metric signals it.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    producer.close().await.unwrap();
    broker.shutdown().await;
    seen
}
