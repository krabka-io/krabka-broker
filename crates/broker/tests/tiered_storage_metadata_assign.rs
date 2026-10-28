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
#![cfg(test)]

use {assert2::assert, tokio::time};
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

use {
    krabka_remote_storage_topic::log::{AssignmentHandle, MetadataEventStream},
    rstest::fixture,
    rstest_bdd::{Slot, datatable::Rows},
    rstest_bdd_macros::{DataTableRow, given, scenario, then, when},
};

#[derive(Default)]
struct MetadataAssignment {
    bare: Option<BareBroker>,
    log: Option<Arc<KafkaMetadataEventLog>>,
    stream: Option<MetadataEventStream>,
    assignment: Option<Arc<dyn AssignmentHandle>>,
    records: Vec<KeyedReceived>,
    offsets: Option<Vec<i64>>,
    extra_partition: Slot<Option<i32>>,
}

#[fixture]
fn metadata_assignment() -> MetadataAssignment {
    // Keep the real subscription handle alive while its assigned records are observed.
    MetadataAssignment::default()
}

impl MetadataAssignment {
    fn log(&self) -> &KafkaMetadataEventLog { self.log.as_ref().expect("metadata log running") }

    async fn shutdown(&mut self) {
        self.log().shutdown().await;
        let BareBroker { broker, log_dir } = self.bare.take().expect("bare broker");
        broker.shutdown().await;
        self.stream.take();
        self.assignment.take();
        self.log.take();
        drop(log_dir);
    }
}

#[derive(DataTableRow)]
struct PublishedValue {
    partition: i32,
    payload: String,
}
#[derive(DataTableRow)]
struct PublishedKey {
    partition: i32,
    key: String,
    value: String,
}
#[derive(DataTableRow)]
struct AssignedStart {
    partition: i32,
    offset: i64,
}
#[derive(DataTableRow)]
struct ExpectedValue {
    partition: i32,
    offset: i64,
    payload: String,
}
#[derive(DataTableRow)]
struct ExpectedKey {
    partition: i32,
    offset: i64,
    key: String,
    payload: String,
    tombstone: bool,
}

#[given("a bare broker hosts a metadata event log with at least three partitions")]
async fn metadata_assignment_log(metadata_assignment: &mut MetadataAssignment) {
    let (bare, log) = start_three_partition_log().await;
    assert!(log.partition_count() >= 3);
    metadata_assignment.bare = Some(bare);
    metadata_assignment.log = Some(log);
}

#[when("these unkeyed metadata records are published in order")]
async fn publish_assigned_metadata(
    metadata_assignment: &MetadataAssignment,
    #[datatable] rows: Rows<PublishedValue>,
) {
    for row in rows {
        metadata_assignment
            .log()
            .publish(row.partition, Bytes::from(row.payload))
            .await
            .unwrap();
    }
}

#[when("an empty keyed metadata group is published")]
async fn publish_empty_metadata_group(metadata_assignment: &mut MetadataAssignment) {
    metadata_assignment.offsets = Some(
        metadata_assignment
            .log()
            .publish_keyed_group(Vec::new())
            .await
            .unwrap(),
    );
}

#[then("the empty metadata group receives no offsets")]
fn empty_metadata_group_offsets(metadata_assignment: &MetadataAssignment) {
    assert!(
        metadata_assignment
            .offsets
            .as_ref()
            .expect("group published")
            .is_empty()
    );
}

#[when("this interleaved keyed metadata group is published")]
async fn publish_keyed_metadata_group(
    metadata_assignment: &mut MetadataAssignment,
    #[datatable] rows: Rows<PublishedKey>,
) {
    let group = rows
        .into_iter()
        .map(|row| KeyedEvent {
            partition: row.partition,
            key: Bytes::from(row.key),
            event: (row.value != "tombstone").then(|| Bytes::from(row.value)),
        })
        .collect();
    metadata_assignment.offsets = Some(
        metadata_assignment
            .log()
            .publish_keyed_group(group)
            .await
            .unwrap(),
    );
}

#[then("the metadata group offsets are {offsets}")]
fn assigned_metadata_offsets(metadata_assignment: &MetadataAssignment, offsets: String) {
    let expected: Vec<i64> = offsets
        .split(',')
        .map(|offset| offset.parse().unwrap())
        .collect();
    assert!(
        metadata_assignment
            .offsets
            .as_ref()
            .expect("group published")
            == &expected
    );
}

#[when("a metadata consumer subscribes at these partition offsets")]
fn subscribe_assigned_metadata(
    metadata_assignment: &mut MetadataAssignment,
    #[datatable] rows: Rows<AssignedStart>,
) {
    let (stream, assignment) = metadata_assignment.log().subscribe(
        rows.into_iter()
            .map(|row| PartitionStart::new(row.partition, row.offset))
            .collect(),
    );
    metadata_assignment.stream = Some(stream);
    metadata_assignment.assignment = Some(assignment);
}

#[when("the metadata consumer reads {records} records within fifteen seconds")]
async fn read_assigned_metadata(metadata_assignment: &mut MetadataAssignment, records: usize) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while metadata_assignment.records.len() < records {
        let next = time::timeout(
            Duration::from_millis(500),
            metadata_assignment
                .stream
                .as_mut()
                .expect("metadata subscription")
                .next(),
        )
        .await;
        match next {
            Ok(Some(record)) => metadata_assignment.records.push(KeyedReceived {
                partition: record.partition,
                offset: record.offset,
                key: record.key.map(|key| key.to_vec()).unwrap_or_default(),
                payload: record.payload.to_vec(),
                tombstone: record.tombstone,
            }),
            Ok(None) => break,
            Err(_) => {}
        }
        assert!(
            Instant::now() <= deadline,
            "metadata records did not arrive: {:?}",
            metadata_assignment.records
        );
    }
    metadata_assignment.records.sort_unstable();
}

#[then("the assigned metadata values are exactly")]
fn assigned_metadata_values(
    metadata_assignment: &MetadataAssignment,
    #[datatable] rows: Rows<ExpectedValue>,
) {
    let actual: Vec<_> = metadata_assignment
        .records
        .iter()
        .map(|record| Received {
            partition: record.partition,
            offset: record.offset,
            payload: record.payload.clone(),
        })
        .collect();
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|row| Received {
            partition: row.partition,
            offset: row.offset,
            payload: row.payload.into_bytes(),
        })
        .collect();
    expected.sort_unstable();
    assert!(actual == expected);
}

#[then("the assigned keyed metadata records are exactly")]
fn assigned_keyed_metadata_values(
    metadata_assignment: &MetadataAssignment,
    #[datatable] rows: Rows<ExpectedKey>,
) {
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|row| KeyedReceived {
            partition: row.partition,
            offset: row.offset,
            key: row.key.into_bytes(),
            payload: if row.tombstone {
                vec![]
            } else {
                row.payload.into_bytes()
            },
            tombstone: row.tombstone,
        })
        .collect();
    expected.sort_unstable();
    assert!(metadata_assignment.records == expected);
}

#[when("the metadata consumer observes another 500-millisecond grace window")]
async fn assigned_metadata_grace(metadata_assignment: &mut MetadataAssignment) {
    let extra = match time::timeout(
        Duration::from_millis(500),
        metadata_assignment
            .stream
            .as_mut()
            .expect("metadata subscription")
            .next(),
    )
    .await
    {
        Ok(Some(record)) => Some(record.partition),
        _ => None,
    };
    metadata_assignment.extra_partition.set(extra);
}

#[then("no unassigned partition-two record arrived in the grace window")]
fn unassigned_metadata_absent(metadata_assignment: &MetadataAssignment) {
    assert!(
        metadata_assignment
            .extra_partition
            .get()
            .expect("grace window observed")
            != Some(2)
    );
}

#[scenario(
    path = "tests/features/migrated/tests/tiered_storage_metadata_assign.feature",
    name = "subscribe subset from nonzero offset yields exact records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscribe_subset_from_nonzero_offset_yields_exact_records(
    metadata_assignment: MetadataAssignment,
) {
    metadata_assignment.shutdown().await;
    drop(metadata_assignment);
}

#[scenario(
    path = "tests/features/migrated/tests/tiered_storage_metadata_assign.feature",
    name = "keyed group lands each partition in order"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn keyed_group_lands_each_partition_in_order(metadata_assignment: MetadataAssignment) {
    metadata_assignment.shutdown().await;
    drop(metadata_assignment);
}
