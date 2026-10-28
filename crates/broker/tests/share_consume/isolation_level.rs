//! Share fetches of a group whose `share.isolation.level` is
//! `read_committed`. The acquire window is clamped to the last stable offset,
//! so the records of an open transaction stay invisible until that
//! transaction commits, and the broker surfaces them afterwards rather than
//! losing them. The data of an aborted transaction is archived and never
//! acquired.
//!
//! Kafka has no broker isolation key, so each test sets the group override,
//! and a group without one reads `read_uncommitted`.

use {
    crate::{
        harness::{bootstrap_share_state, produce_n},
        share_rpc::{acquired_count, share_fetch},
        support::{
            producer::{producer_record, transactional_producer},
            share::{
                ShareFetchSetup, ShareSessionEpoch, join_consume_member, permitted_topic_fixture,
                record_values,
            },
        },
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_producer::{OwnedTransaction, Producer},
    krabka_metadata::{GroupConfigRecord, MetadataRecord},
    std::{collections::BTreeSet, sync::Arc, time::Duration},
    tokio::{sync::OwnedSemaphorePermit, time},
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

/// A transaction-ready broker and the owners that keep it running.
struct IsolationFixture {
    permit: OwnedSemaphorePermit,
    broker: BrokerHandle,
    client: Arc<krabka_client_core::Client>,
    dir: tempfile::TempDir,
    tid: uuid::Uuid,
    bootstrap: String,
}

/// Prepare a transaction-ready empty share group with its exact isolation override.
async fn isolation_fixture(level: Option<&str>) -> IsolationFixture {
    let (permit, broker, client, dir, tid) = Box::pin(permitted_topic_fixture().call()).await;
    let bootstrap = broker.listen_addr().to_string();
    bootstrap_share_state(&broker, &client, "g1").await;
    if let Some(level) = level {
        set_isolation_level(&broker, "g1", level).await;
    }
    broker.wait_until_transaction_coordinator_ready().await;
    IsolationFixture {
        permit,
        broker,
        client,
        dir,
        tid,
        bootstrap,
    }
}

use {
    krabka_protocol::owned::share_fetch_response::PartitionData,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Debug, Default, PartialEq, Eq)]
struct Seen {
    acquired: BTreeSet<i64>,
    values: BTreeSet<String>,
}

#[derive(Default)]
struct ShareIsolation {
    fixture: Option<IsolationFixture>,
    producer: Option<Arc<Producer>>,
    transaction: Option<OwnedTransaction>,
    member: Option<String>,
    open_reads: Vec<PartitionData>,
    committed_values: Option<Vec<String>>,
    seen: Option<Seen>,
}

#[fixture]
fn share_isolation() -> ShareIsolation {
    // The transaction guard remains live while actual share fetches observe its isolation.
    ShareIsolation::default()
}

#[given("a share group whose isolation override is {level}")]
async fn share_isolation_policy(share_isolation: &mut ShareIsolation, level: String) {
    let level = match level.as_str() {
        READ_COMMITTED => Some(READ_COMMITTED),
        READ_UNCOMMITTED => Some(READ_UNCOMMITTED),
        "unset" => None,
        _ => panic!("unknown isolation policy {level}"),
    };
    share_isolation.fixture = Some(isolation_fixture(level).await);
}

#[when("transactional producer {transactional_id} opens and flushes records a,b,c")]
async fn open_share_transaction(share_isolation: &mut ShareIsolation, transactional_id: String) {
    share_isolation.producer = Some(Arc::new(
        transactional_producer(
            share_isolation.fixture().bootstrap.clone(),
            &transactional_id,
        )
        .await,
    ));
    share_isolation.transaction = Some(
        Arc::clone(
            share_isolation
                .producer
                .as_ref()
                .expect("transaction producer"),
        )
        .begin_transaction_owned()
        .await
        .unwrap(),
    );
    enqueue_transaction_values(
        share_isolation
            .producer
            .as_ref()
            .expect("transaction producer"),
    )
    .await;
    share_isolation
        .producer
        .as_ref()
        .expect("transaction producer")
        .flush()
        .await
        .unwrap();
}

#[when("a share consumer joins the isolation-test group")]
async fn join_isolated_share_group(share_isolation: &mut ShareIsolation) {
    share_isolation.member = Some(
        join_consume_member(
            &share_isolation.fixture().broker,
            &share_isolation.fixture().client,
            share_isolation.fixture().tid,
        )
        .await
        .member,
    );
}

#[when("the committed share consumer fetches six times while the transaction stays open")]
async fn observe_open_share_transaction(share_isolation: &mut ShareIsolation) {
    let member = share_isolation
        .member
        .as_ref()
        .expect("joined share consumer");
    for epoch in 0..6 {
        share_isolation.open_reads.push(
            share_fetch(
                &share_isolation.fixture().client,
                ShareFetchSetup::at_epoch(
                    member,
                    share_isolation.fixture().tid,
                    ShareSessionEpoch(epoch),
                ),
            )
            .await,
        );
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[then("all six open-transaction share fetches acquire zero records")]
fn open_share_transaction_invisible(share_isolation: &ShareIsolation) {
    assert!(share_isolation.open_reads.len() == 6);
    for response in &share_isolation.open_reads {
        assert!(
            acquired_count(response) == 0,
            "open transaction leaked: {response:?}"
        );
    }
}

#[when("the share-test transaction is committed: {commit}")]
async fn end_share_transaction(share_isolation: &mut ShareIsolation, commit: bool) {
    let transaction = share_isolation
        .transaction
        .take()
        .expect("open transaction");
    if commit {
        transaction.commit().await.unwrap();
    } else {
        transaction.abort().await.unwrap();
    }
}

#[when("the same consumer fetches committed records using epochs six through twenty-nine")]
async fn read_committed_share_transaction(share_isolation: &mut ShareIsolation) {
    let member = share_isolation
        .member
        .as_ref()
        .expect("joined share consumer");
    let mut values = Vec::new();
    for epoch in 6..30 {
        let response = share_fetch(
            &share_isolation.fixture().client,
            ShareFetchSetup::at_epoch(
                member,
                share_isolation.fixture().tid,
                ShareSessionEpoch(epoch),
            ),
        )
        .await;
        if acquired_count(&response) > 0
            && let Some(batches) = response
                .records
                .as_ref()
                .and_then(|records| records.as_v2())
        {
            values = record_values(batches);
            values.sort();
            if values == ["a", "b", "c"] {
                break;
            }
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    share_isolation.committed_values = Some(values);
}

#[then("the previously deferred share values are exactly a,b,c")]
fn committed_share_transaction_visible(share_isolation: &ShareIsolation) {
    assert!(
        share_isolation
            .committed_values
            .as_ref()
            .expect("post-commit reads finished")
            == &["a", "b", "c"]
    );
}

#[when("a plain v0 record is appended after the transaction marker")]
async fn append_share_transaction_suffix(share_isolation: &ShareIsolation) {
    produce_n()
        .client(&share_isolation.fixture().client)
        .topic_id(share_isolation.fixture().tid)
        .call()
        .await;
}

#[when("the share consumer reads until plain offset four is acquired")]
async fn read_share_transaction_suffix(share_isolation: &mut ShareIsolation) {
    let member = share_isolation
        .member
        .as_ref()
        .expect("joined share consumer");
    let mut seen = Seen::default();
    for epoch in 0..30 {
        let row = share_fetch(
            &share_isolation.fixture().client,
            ShareFetchSetup::at_epoch(
                member,
                share_isolation.fixture().tid,
                ShareSessionEpoch(epoch),
            ),
        )
        .await;
        let acquired: BTreeSet<i64> = row
            .acquired_records
            .iter()
            .flat_map(|range| range.first_offset..=range.last_offset)
            .collect();
        if let Some(batches) = row.records.as_ref().and_then(|records| records.as_v2()) {
            for batch in batches {
                for record in &batch.records {
                    let offset = batch.base_offset + i64::from(record.offset_delta);
                    if let Some(value) = record.value.as_ref()
                        && acquired.contains(&offset)
                    {
                        seen.values
                            .insert(String::from_utf8_lossy(value).into_owned());
                    }
                }
            }
        }
        seen.acquired.extend(acquired);
        if seen.acquired.contains(&4) {
            break;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
    share_isolation.seen = Some(seen);
}

#[then("the acquired offsets are {offsets} and their visible values are {values}")]
fn share_transaction_visibility(share_isolation: &ShareIsolation, offsets: String, values: String) {
    let expected = Seen {
        acquired: offsets
            .split(',')
            .map(|offset| offset.parse().unwrap())
            .collect(),
        values: values.split(',').map(str::to_owned).collect(),
    };
    assert!(
        share_isolation
            .seen
            .as_ref()
            .expect("transaction and suffix read")
            == &expected
    );
}

impl ShareIsolation {
    async fn shutdown(&mut self) {
        self.transaction.take();
        Arc::try_unwrap(self.producer.take().expect("transaction producer"))
            .unwrap_or_else(|_| panic!("completed transaction released its producer"))
            .close()
            .await
            .unwrap();
        let fixture = self.fixture.take().expect("isolation fixture");
        fixture.broker.shutdown().await;
        drop((fixture.client, fixture.dir, fixture.permit));
    }
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/isolation_level.feature",
    name = "read committed skips open txn then sees committed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_skips_open_txn_then_sees_committed(share_isolation: ShareIsolation) {
    share_isolation.shutdown().await;
    drop(share_isolation);
}

#[scenario(
    path = "tests/features/migrated/tests/share_consume/isolation_level.feature",
    name = "aborted transaction data is archived under read committed"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aborted_transaction_data_is_archived_under_read_committed(
    share_isolation: ShareIsolation,
    level: String,
    commit: bool,
    offsets: String,
    values: String,
) {
    let _ = (level, commit, offsets, values);
    share_isolation.shutdown().await;
    drop(share_isolation);
}

async fn enqueue_transaction_values(producer: &Producer) {
    for v in ["a", "b", "c"] {
        drop(
            producer
                .enqueue(
                    producer_record()
                        .topic(("t").into())
                        .maybe_value(Some(Some(bytes::Bytes::from(v.to_owned()))))
                        .call(),
                )
                .await
                .expect("record is queued"),
        );
    }
}

impl ShareIsolation {
    fn fixture(&self) -> &IsolationFixture { self.fixture.as_ref().expect("live scenario fixture") }
}
