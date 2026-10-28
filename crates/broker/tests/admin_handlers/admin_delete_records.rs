//! `DeleteRecords` (`api_key` 21): trimming a partition from a given offset,
//! the agreement between the response's `low_watermark` and the partition's
//! `log_start_offset`, and the durability of a trim that lands where no segment
//! name can record it.

use {
    crate::{
        admin_harness::{AdminCluster, admin, create_topic_helper},
        support::{
            self,
            fetch::fetch_topic_row,
            offsets::{
                delete_records_partition, delete_records_request, delete_records_topic,
                list_offset_partition, single_partition_list_offsets,
            },
            records::record_count,
        },
    },
    assert2::{assert, check},
    krabka_broker::{BrokerHandle, NodeId, codes},
    krabka_client_core::Client,
    krabka_log::name,
    krabka_protocol::{
        owned::{
            delete_records_response::DeleteRecordsPartitionResult,
            fetch_request::{FetchPartition, FetchRequest},
            fetch_response::PartitionData,
            list_offsets_request::ListOffsetsRequest,
            list_offsets_response::ListOffsetsPartitionResponse,
        },
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{fs, path::Path},
    tempfile::TempDir,
};

/// Request timestamp sentinel (-2) asking where the log starts. Kafka's
/// `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
const EARLIEST_TIMESTAMP: i64 = -2;

/// The topic the restart case trims, kept clear of the one above so the two
/// cases never share a partition directory.
const RESTART_TOPIC: &str = "t-dr-restart";

/// Records produced before the restart case trims. `segment.bytes` defaults to
/// 1 GiB, so every one of them lands in the first, still-active segment.
const RESTART_RECORDS: i64 = 100;

/// Where the restart case moves the log start. It is strictly inside the one
/// segment, so no segment name witnesses the trim: the records below it are
/// still in the same file, and only the partition's own checkpoint says they
/// are gone.
const RESTART_TRIM_TO: i64 = 50;

/// What a client can see of a trimmed partition: where `ListOffsets` says the
/// log starts, and what a `Fetch` aimed below that point is answered with.
///
/// The two readings are one value so a case asserts against a whole expected
/// struct rather than against four fields in sequence. A broker that forgot the
/// trim answers offset 0, no error and 50 served records here, so the whole
/// difference shows up in one failure.
#[derive(Debug, PartialEq, Eq)]
struct TrimmedView {
    earliest_error_code: i16,
    earliest_offset: i64,
    fetch_error_code: i16,
    fetch_log_start_offset: i64,
    fetched_records: usize,
}

/// Trim partition 0 of `topic` from `offset` and return the partition row.
async fn trim(
    client: &krabka_client_core::Client,
    topic: &str,
    offset: i64,
) -> DeleteRecordsPartitionResult {
    let mut resp = client
        .send(delete_records_request(
            vec![delete_records_topic(topic, vec![delete_records_partition(
                0, offset,
            )])],
            5_000,
        ))
        .await
        .expect("delete_records");
    resp.topics.remove(0).partitions.remove(0)
}

/// Base offsets of the `.log` segment files in partition 0 of `topic`, sorted.
///
/// A case reads this to say out loud which segments exist, so "the trim landed
/// inside the active segment" is a checked precondition rather than an
/// assumption about the default `segment.bytes`.
fn segment_base_offsets(log_dir: &Path, topic: &str) -> Vec<i64> {
    let dir = name::partition_dir(log_dir, topic, 0);
    let mut bases: Vec<i64> = fs::read_dir(&dir)
        .expect("read partition dir")
        .map(|entry| entry.expect("partition dir entry").file_name())
        .filter_map(|name| name::parse_log_filename(&name.to_string_lossy()).ok())
        .collect();
    bases.sort_unstable();
    bases
}

/// Read partition 0 of `topic` the way a client would: `ListOffsets` at the
/// `EARLIEST` sentinel, and a `Fetch` from `fetch_offset`.
async fn trimmed_view(
    client: &krabka_client_core::Client,
    topic: &str,
    fetch_offset: i64,
) -> TrimmedView {
    let topic_id = support::topic_id_for(client, topic).await;
    let earliest = list_earliest(client, topic).await;
    let fetched = fetch_from(client, topic, topic_id, fetch_offset).await;
    TrimmedView {
        earliest_error_code: earliest.error_code,
        earliest_offset: earliest.offset,
        fetch_error_code: fetched.error_code,
        fetch_log_start_offset: fetched.log_start_offset,
        fetched_records: record_count(fetched.records.as_ref()),
    }
}

/// `ListOffsets` for partition 0 of `topic` at Kafka's `EARLIEST_TIMESTAMP`,
/// which is where the log starts.
async fn list_earliest(
    client: &krabka_client_core::Client,
    topic: &str,
) -> ListOffsetsPartitionResponse {
    let mut resp = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            timeout_ms: 5_000,
            ..single_partition_list_offsets(topic, list_offset_partition(0, EARLIEST_TIMESTAMP))
        })
        .await
        .expect("ListOffsets");
    resp.topics.remove(0).partitions.remove(0)
}

/// One non-blocking `Fetch` of partition 0 of `topic` from `fetch_offset`.
async fn fetch_from(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: Uuid,
    fetch_offset: i64,
) -> PartitionData {
    let mut resp = client
        .send(FetchRequest {
            // A snapshot read: an empty answer comes back at once rather than
            // parking in the long poll.
            max_wait_ms: 0,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![fetch_topic_row(topic, topic_id, vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }])],
            ..Default::default()
        })
        .await
        .expect("Fetch");
    resp.responses.remove(0).partitions.remove(0)
}
struct RecordTrimming {
    broker: Option<BrokerHandle>,
    client: Option<Client>,
    directory: Option<TempDir>,
    topic: &'static str,
    trimmed: Option<DeleteRecordsPartitionResult>,
    view: Option<TrimmedView>,
}

impl RecordTrimming {
    fn broker<'a>(&'a self, admin: &'a AdminCluster) -> &'a BrokerHandle {
        self.broker.as_ref().unwrap_or_else(|| admin.broker())
    }

    fn client<'a>(&'a self, admin: &'a AdminCluster) -> &'a Client {
        self.client.as_ref().unwrap_or_else(|| admin.client())
    }
}

#[fixture]
fn trimming() -> RecordTrimming {
    // Start this scenario with fresh inputs and observations.

    RecordTrimming {
        broker: None,
        client: None,
        directory: None,
        topic: "",
        trimmed: None,
        view: None,
    }
}

#[given("topic t-dr contains 100 records on the admin broker")]
async fn records_to_trim(admin: &AdminCluster, trimming: &mut RecordTrimming) {
    create_topic_helper(admin.client(), "t-dr", 1).await;
    admin
        .broker()
        .produce_records_for_test("t-dr", 0, 100)
        .await
        .expect("produce_records_for_test");
    trimming.topic = "t-dr";
}

#[given("a restartable topic contains 100 committed records entirely in segment 0")]
async fn restartable_records(trimming: &mut RecordTrimming) {
    let directory = tempfile::tempdir().expect("tempdir");
    let (broker, client) = support::start_with_dir(directory.path()).await;
    create_topic_helper(&client, RESTART_TOPIC, 1).await;
    broker
        .wait_until_local_partition_leader(RESTART_TOPIC, 0, NodeId(1))
        .await;
    broker
        .produce_records_for_test(
            RESTART_TOPIC,
            0,
            usize::try_from(RESTART_RECORDS).expect("record count fits usize"),
        )
        .await
        .expect("produce_records_for_test");
    broker
        .wait_until_high_watermark(RESTART_TOPIC, 0, RESTART_RECORDS)
        .await;
    check!(
        segment_base_offsets(directory.path(), RESTART_TOPIC) == vec![0],
        "every record must be in one segment or a deleted segment name would carry the trim"
    );
    trimming.topic = RESTART_TOPIC;
    trimming.directory = Some(directory);
    trimming.broker = Some(broker);
    trimming.client = Some(client);
}

#[when("DeleteRecords trims the topic before offset 50")]
async fn delete_before_50(admin: &AdminCluster, trimming: &mut RecordTrimming) {
    trimming.trimmed = Some(trim(trimming.client(admin), trimming.topic, RESTART_TRIM_TO).await);
}

#[then("the successful trim reports a valid low watermark equal to the local log start")]
fn trim_watermark(admin: &AdminCluster, trimming: &RecordTrimming) {
    let result = trimming
        .trimmed
        .as_ref()
        .expect("DeleteRecords was submitted");
    check!(
        result.error_code == 0,
        "delete_records error: {:?}",
        result.error_code
    );
    check!(
        result.low_watermark >= 0,
        "low_watermark should be non-negative, got {}",
        result.low_watermark
    );
    check!(
        result.low_watermark <= 50,
        "low_watermark {} should be <= requested offset 50",
        result.low_watermark
    );
    let log_start = trimming
        .broker(admin)
        .partition_log_start_for_test(trimming.topic, 0)
        .expect("partition exists");
    assert!(
        log_start == result.low_watermark,
        "partition log_start_offset should equal low_watermark"
    );
}

#[then("the live trim succeeds and advances both low watermark and log start to 50")]
fn active_segment_trim(admin: &AdminCluster, trimming: &RecordTrimming) {
    let result = trimming
        .trimmed
        .as_ref()
        .expect("DeleteRecords was submitted");
    check!(
        result.error_code == codes::NONE,
        "delete_records: {result:?}"
    );
    check!(result.low_watermark == RESTART_TRIM_TO);
    check!(
        trimming
            .broker(admin)
            .partition_log_start_for_test(RESTART_TOPIC, 0)
            == Some(RESTART_TRIM_TO),
        "the trim moved the live partition's log start"
    );
}

#[when("the trimmed broker shuts down")]
async fn stop_trimmed_broker(trimming: &mut RecordTrimming) {
    drop(trimming.client.take());
    trimming
        .broker
        .take()
        .expect("a restartable broker")
        .shutdown()
        .await;
}

#[then("the partition still has only segment 0 to witness the trim")]
fn same_segment_after_trim(trimming: &RecordTrimming) {
    let directory = trimming
        .directory
        .as_ref()
        .expect("the persistent log directory");
    check!(segment_base_offsets(directory.path(), RESTART_TOPIC) == vec![0]);
}

#[when("the broker restarts from that same log directory")]
async fn restart_trimmed_broker(trimming: &mut RecordTrimming) {
    let directory = trimming
        .directory
        .as_ref()
        .expect("the persistent log directory");
    let (broker, client) = support::start_with_dir(directory.path()).await;
    broker
        .wait_until_local_partition_leader(RESTART_TOPIC, 0, NodeId(1))
        .await;
    trimming.broker = Some(broker);
    trimming.client = Some(client);
}

#[when("the client asks for the earliest offset and fetches from offset 0")]
async fn observe_trimmed_log(admin: &AdminCluster, trimming: &mut RecordTrimming) {
    trimming.view = Some(trimmed_view(trimming.client(admin), RESTART_TOPIC, 0).await);
}

#[then("the restarted log begins at 50 and refuses the old fetch without serving records")]
fn durable_trim(trimming: &RecordTrimming) {
    assert!(
        trimming.view.as_ref()
            == Some(&TrimmedView {
                earliest_error_code: codes::NONE,
                earliest_offset: RESTART_TRIM_TO,
                fetch_error_code: codes::OFFSET_OUT_OF_RANGE,
                fetch_log_start_offset: -1,
                fetched_records: 0,
            })
    );
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_delete_records.feature",
    name = "delete records trims log start"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_trims_log_start(admin: AdminCluster, trimming: RecordTrimming) {
    drop(trimming);
    admin.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/admin_handlers/admin_delete_records.feature",
    name = "a trim inside the active segment survives a restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trim_inside_the_active_segment_survives_a_restart(
    admin: AdminCluster,
    trimming: RecordTrimming,
) {
    trimming
        .broker
        .expect("a restarted broker")
        .shutdown()
        .await;
    admin.shutdown().await;
}
