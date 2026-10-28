//! Multi-RPC sequences against an in-process broker, driven through
//! `krabka-client-core`. These run on every push (no Docker required).
#![cfg(test)]

use {
    crate::support::{
        client::connect_client,
        discovery::{api_versions_request_for, topic_metadata_request},
        fetch::{FetchLimits, RequestWaitMillis, check_record_count, single_partition_fetch},
        offsets::{list_offset_partition, single_partition_list_offsets},
        produce::{ProduceAcknowledgements, SinglePartitionProduceSetup, batch_request},
        records::value_record,
        topics::{creatable_topic, create_topic_request},
        wire::write_frame,
    },
    assert2::{assert, check},
    krabka_broker::{BootstrapMode, Broker, BrokerConfig, BrokerHandle},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    support::InProcess,
    tempfile::{TempDir, tempdir},
    tokio::{net::TcpStream, time},
};
mod support;

use {
    bytes::{Bytes, BytesMut},
    krabka_protocol::{
        Encode,
        owned::{
            api_versions_response::ApiVersionsResponse,
            create_topics_response::CreateTopicsResponse,
            fetch_response::FetchResponse,
            list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest},
            list_offsets_response::ListOffsetsResponse,
            metadata_request::MetadataRequest,
            metadata_response::MetadataResponse,
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid,
        records::{Record, RecordBatch},
    },
    std::{collections::BTreeMap, io, time::Duration},
    support::topic_id_for,
    tokio::io::AsyncReadExt,
};

/// Build a `RecordBatch` with one entry per provided value. Codegen's
/// `PartitionProduceData.records` is `Option<RecordsPayload>`. Callers
/// pass the batch by value and `.into()` it at the assignment site.
fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let len_i32 = i32::try_from(values.len()).expect("test fixture small enough for i32");
    let len_i64 = i64::try_from(values.len()).expect("test fixture small enough for i64");
    let mut batch = RecordBatch {
        last_offset_delta: (len_i32 - 1).max(0),
        max_timestamp: len_i64,
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(value_record(
            i32::try_from(i).expect("test fixture small enough for i32"),
            Some(Bytes::from(v.to_string())),
        ));
    }
    batch
}

/// A record value and the absolute timestamp it carries.
struct TimestampedValue<'a> {
    value: &'a str,
    timestamp: i64,
}

/// One record per value and timestamp. `base_timestamp` is the
/// first timestamp. Each record's `timestamp_delta` reconstructs the
/// requested absolute timestamp. `max_timestamp` is the largest
/// timestamp.
fn timestamped_batch(entries: &[TimestampedValue<'_>]) -> RecordBatch {
    let base_ts = entries.first().map_or(0, |entry| entry.timestamp);
    let max_ts = entries
        .iter()
        .map(|entry| entry.timestamp)
        .max()
        .unwrap_or(0);
    let len_i32 = i32::try_from(entries.len()).expect("small");
    let mut batch = RecordBatch {
        base_timestamp: base_ts,
        max_timestamp: max_ts,
        last_offset_delta: (len_i32 - 1).max(0),
        ..RecordBatch::default()
    };
    for (i, entry) in entries.iter().enumerate() {
        batch.records.push(Record {
            timestamp_delta: entry.timestamp - base_ts,
            ..value_record(
                i32::try_from(i).expect("small"),
                Some(Bytes::from(entry.value.to_owned())),
            )
        });
    }
    batch
}

struct TimestampLookup {
    process: Option<InProcess>,
    responses: BTreeMap<i64, ListOffsetsResponse>,
}

#[fixture]
fn timestamps() -> TimestampLookup {
    // Start this scenario with fresh inputs and observations.

    TimestampLookup {
        process: None,
        responses: BTreeMap::new(),
    }
}

#[given("the local topic contains these timestamped records")]
async fn timestamped_records(timestamps: &mut TimestampLookup, datatable: Vec<Vec<String>>) {
    let process = support::start().await;
    process
        .client
        .send(create_topic_request(
            creatable_topic().name("by_ts".into()).call(),
        ))
        .await
        .unwrap();
    let topic_id = topic_id_for(&process.client, "by_ts").await;
    let entries = datatable
        .into_iter()
        .skip(1)
        .map(|row| {
            let [value, timestamp]: [String; 2] = row.try_into().expect("value and timestamp");
            (value, timestamp.parse().expect("record timestamp"))
        })
        .collect::<Vec<_>>();
    let borrowed = entries
        .iter()
        .map(|(value, timestamp)| TimestampedValue {
            value,
            timestamp: *timestamp,
        })
        .collect::<Vec<_>>();
    process
        .client
        .send(batch_request(
            timestamped_batch(&borrowed),
            SinglePartitionProduceSetup {
                topic: "by_ts".into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    timestamps.process = Some(process);
}

#[when("local offsets are queried for timestamps {queries}")]
async fn query_timestamps(timestamps: &mut TimestampLookup, queries: String) {
    let process = timestamps
        .process
        .as_ref()
        .expect("timestamped records were written");
    for query in queries.split(',') {
        let timestamp = query.trim().parse().expect("lookup timestamp");
        let response = process
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                ..single_partition_list_offsets("by_ts", list_offset_partition(0, timestamp))
            })
            .await
            .unwrap();
        timestamps.responses.insert(timestamp, response);
    }
}

#[then("timestamp query {query} returns offset {offset} and record timestamp {timestamp}")]
fn timestamp_result(timestamps: &TimestampLookup, query: i64, offset: i64, timestamp: i64) {
    let response = timestamps
        .responses
        .get(&query)
        .expect("the timestamp was queried");
    let partition = &response.topics[0].partitions[0];
    check!(partition.error_code == 0);
    check!(partition.offset == offset);
    check!(partition.timestamp == timestamp);
}

#[then("the earliest local timestamp query returns offset zero")]
fn earliest_local(timestamps: &TimestampLookup) {
    let response = timestamps
        .responses
        .get(&-4)
        .expect("EARLIEST_LOCAL was queried");
    assert!(response.topics[0].partitions[0].offset == 0);
}

struct RpcLifecycle {
    process: Option<InProcess>,
    topic_id: Uuid,
    versions: Option<ApiVersionsResponse>,
    created: Option<CreateTopicsResponse>,
    metadata: Option<MetadataResponse>,
    produced: Option<ProduceResponse>,
    latest: Option<ListOffsetsResponse>,
    fetched: Option<FetchResponse>,
}

#[fixture]
fn lifecycle() -> RpcLifecycle {
    // Start this scenario with fresh inputs and observations.

    RpcLifecycle {
        process: None,
        topic_id: Uuid::ZERO,
        versions: None,
        created: None,
        metadata: None,
        produced: None,
        latest: None,
        fetched: None,
    }
}

#[given("an in-process broker serves ordinary client requests")]
async fn client_broker(lifecycle: &mut RpcLifecycle) {
    lifecycle.process = Some(support::start().await);
}

#[when("the client negotiates supported API versions")]
async fn negotiate(lifecycle: &mut RpcLifecycle) {
    let process = lifecycle.process.as_ref().expect("a broker was started");
    lifecycle.versions = Some(
        process
            .client
            .send(api_versions_request_for("krabka", "0.0.0"))
            .await
            .unwrap(),
    );
}

#[then("API version negotiation succeeds")]
fn negotiated(lifecycle: &RpcLifecycle) {
    assert!(
        lifecycle
            .versions
            .as_ref()
            .expect("ApiVersions completed")
            .error_code
            == 0
    );
}

#[when("the client creates topic e2e and requests topic metadata")]
async fn create_e2e(lifecycle: &mut RpcLifecycle) {
    let process = lifecycle.process.as_ref().expect("a broker was started");
    lifecycle.created = Some(
        process
            .client
            .send(create_topic_request(
                creatable_topic().name("e2e".into()).call(),
            ))
            .await
            .unwrap(),
    );
    lifecycle.metadata = Some(
        process
            .client
            .send(topic_metadata_request(None))
            .await
            .unwrap(),
    );
    lifecycle.topic_id = topic_id_for(&process.client, "e2e").await;
}

#[then("topic creation succeeds and e2e is visible in metadata")]
fn visible_topic(lifecycle: &RpcLifecycle) {
    assert!(
        lifecycle
            .created
            .as_ref()
            .expect("CreateTopics completed")
            .topics[0]
            .error_code
            == 0
    );
    assert!(
        lifecycle
            .metadata
            .as_ref()
            .expect("Metadata completed")
            .topics
            .iter()
            .any(|topic| topic.name.as_deref() == Some("e2e"))
    );
}

#[when("records a b and c are produced to e2e")]
async fn append_e2e(lifecycle: &mut RpcLifecycle) {
    let process = lifecycle.process.as_ref().expect("a broker was started");
    lifecycle.produced = Some(
        process
            .client
            .send(batch_request(
                record_batch_with_values(&["a", "b", "c"]),
                SinglePartitionProduceSetup {
                    topic: "e2e".into(),
                    topic_id: lifecycle.topic_id,
                    ..Default::default()
                },
            ))
            .await
            .unwrap(),
    );
}

#[then("the produce is acknowledged successfully")]
fn appended_e2e(lifecycle: &RpcLifecycle) {
    assert!(
        lifecycle
            .produced
            .as_ref()
            .expect("Produce completed")
            .responses[0]
            .partition_responses[0]
            .error_code
            == 0
    );
}

#[when("the latest offset of e2e is requested")]
async fn latest_e2e(lifecycle: &mut RpcLifecycle) {
    let process = lifecycle.process.as_ref().expect("a broker was started");
    lifecycle.latest = Some(
        process
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                ..single_partition_list_offsets("e2e", ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -1,
                    ..Default::default()
                })
            })
            .await
            .unwrap(),
    );
}

#[then("the latest offset succeeds at three")]
fn log_end_e2e(lifecycle: &RpcLifecycle) {
    let partition = &lifecycle
        .latest
        .as_ref()
        .expect("ListOffsets completed")
        .topics[0]
        .partitions[0];
    assert!(partition.error_code == 0);
    assert!(partition.offset == 3);
}

#[when("the client fetches the e2e partition")]
async fn fetch_e2e(lifecycle: &mut RpcLifecycle) {
    let process = lifecycle.process.as_ref().expect("a broker was started");
    lifecycle.fetched = Some(
        process
            .client
            .send(
                single_partition_fetch()
                    .topic("e2e".into())
                    .topic_id(lifecycle.topic_id)
                    .limits(FetchLimits::one_mebibyte(RequestWaitMillis(100)))
                    .call(),
            )
            .await
            .unwrap(),
    );
}

#[then("the fetch contains all three records")]
fn fetched_e2e(lifecycle: &RpcLifecycle) {
    check_record_count(
        &lifecycle
            .fetched
            .as_ref()
            .expect("Fetch completed")
            .responses[0]
            .partitions[0],
        3,
    );
}

krabka_macros::request_frame_fixture!(one_way_request);

struct OneWayProduce {
    process: Option<InProcess>,
    stream: Option<TcpStream>,
    readiness: Option<Result<io::Result<()>, time::error::Elapsed>>,
    metadata_reply: Vec<u8>,
    latest: Option<ListOffsetsResponse>,
}

#[fixture]
fn one_way() -> OneWayProduce {
    // Start this scenario with fresh inputs and observations.

    OneWayProduce {
        process: None,
        stream: None,
        readiness: None,
        metadata_reply: Vec::new(),
        latest: None,
    }
}

#[given("topic one-way-produce exists for a raw protocol client")]
async fn one_way_topic(one_way: &mut OneWayProduce) {
    let process = support::start().await;
    let created = process
        .client
        .send(create_topic_request(
            creatable_topic().name("one-way-produce".into()).call(),
        ))
        .await
        .expect("create topic");
    assert!(created.topics[0].error_code == 0);
    one_way.stream = Some(
        TcpStream::connect(process.broker.listen_addr())
            .await
            .expect("connect raw client"),
    );
    one_way.process = Some(process);
}

async fn send_one_way_frame(
    one_way: &mut OneWayProduce,
    api: i16,
    version: i16,
    correlation: i32,
    body: &[u8],
) {
    let frame = one_way_request()
        .api_key(api)
        .api_version(version)
        .correlation_id(correlation)
        .maybe_client_id(Some(b"acks-zero-test"))
        .maybe_tagged(Some(&[0]))
        .body(body)
        .call();
    write_frame(
        one_way.stream.as_mut().expect("a raw client connected"),
        &frame,
        None,
    )
    .await
    .expect("write request frame");
}

#[when("a Produce v9 frame sends one record with acks zero")]
async fn unacknowledged_produce(one_way: &mut OneWayProduce) {
    let produce = batch_request(
        record_batch_with_values(&["value"]),
        SinglePartitionProduceSetup {
            topic: "one-way-produce".into(),
            acknowledgements: ProduceAcknowledgements::NoResponse,
            ..Default::default()
        },
    );
    let mut body = BytesMut::new();
    produce.encode(&mut body, 9).expect("encode Produce v9");
    send_one_way_frame(one_way, 0, 9, 1, &body).await;
    let stream = one_way.stream.as_mut().expect("a raw client connected");
    one_way.readiness = Some(time::timeout(Duration::from_millis(150), stream.readable()).await);
}

#[then("no response frame becomes readable for the unacknowledged produce")]
fn no_produce_reply(one_way: &OneWayProduce) {
    assert!(
        one_way
            .readiness
            .as_ref()
            .expect("the raw response was observed")
            .is_err(),
        "acks=0 must not make a response frame readable"
    );
}

#[when("a Metadata v12 frame with correlation two reuses the same connection")]
async fn reuse_one_way_connection(one_way: &mut OneWayProduce) {
    let mut body = BytesMut::new();
    MetadataRequest::default()
        .encode(&mut body, 12)
        .expect("encode Metadata v12");
    send_one_way_frame(one_way, 3, 12, 2, &body).await;
    let stream = one_way.stream.as_mut().expect("a raw client connected");
    let response_len = time::timeout(Duration::from_secs(5), stream.read_u32())
        .await
        .expect("Metadata response arrives")
        .expect("read Metadata frame length");
    let mut response = vec![0_u8; response_len as usize];
    stream.read_exact(&mut response).await.unwrap();
    one_way.metadata_reply = response;
}

#[then("the connection returns the Metadata correlation id two")]
fn metadata_correlation(one_way: &OneWayProduce) {
    assert!(i32::from_be_bytes(one_way.metadata_reply[..4].try_into().unwrap()) == 2);
}

#[when("the one-way-produce log end is queried")]
async fn one_way_log_end(one_way: &mut OneWayProduce) {
    let process = one_way.process.as_ref().expect("a topic was created");
    one_way.latest = Some(
        process
            .client
            .send(ListOffsetsRequest {
                replica_id: -1,
                ..single_partition_list_offsets("one-way-produce", list_offset_partition(0, -1))
            })
            .await
            .expect("read log end"),
    );
}

#[then("the unacknowledged record is present at log end one")]
fn one_way_record_present(one_way: &OneWayProduce) {
    assert!(
        one_way
            .latest
            .as_ref()
            .expect("ListOffsets completed")
            .topics[0]
            .partitions[0]
            .offset
            == 1
    );
}

struct DiskRecovery {
    directory: Option<TempDir>,
    broker: Option<BrokerHandle>,
    created: Option<CreateTopicsResponse>,
    metadata: Option<MetadataResponse>,
}

#[fixture]
fn recovery() -> DiskRecovery {
    // Start this scenario with fresh inputs and observations.

    DiskRecovery {
        directory: None,
        broker: None,
        created: None,
        metadata: None,
    }
}

#[given("a fresh broker log directory")]
fn fresh_directory(recovery: &mut DiskRecovery) { recovery.directory = Some(tempdir().unwrap()); }

#[when("a broker creates persisted with two partitions and shuts down")]
async fn persist_topic(recovery: &mut DiskRecovery) {
    let directory = recovery
        .directory
        .as_ref()
        .expect("a log directory was allocated");
    let broker = Broker::start(BrokerConfig::for_tests(directory.path().to_path_buf()))
        .await
        .unwrap();
    let client = connect_client(broker.listen_addr().to_string(), Some("recovery-test")).await;
    recovery.created = Some(
        client
            .send(create_topic_request(
                creatable_topic()
                    .name("persisted".into())
                    .partitions(2)
                    .call(),
            ))
            .await
            .unwrap(),
    );
    broker.shutdown().await;
}

#[then("the persisted topic creation was accepted")]
fn accepted_persistence(recovery: &DiskRecovery) {
    assert!(
        recovery
            .created
            .as_ref()
            .expect("CreateTopics completed")
            .topics[0]
            .error_code
            == 0
    );
}

#[when("the broker rejoins using the same log directory and metadata is requested")]
async fn reopen_broker(recovery: &mut DiskRecovery) {
    let directory = recovery
        .directory
        .as_ref()
        .expect("a log directory was allocated");
    let mut config = BrokerConfig::for_tests(directory.path().to_path_buf());
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = Broker::start(config).await.unwrap();
    let client = connect_client(broker.listen_addr().to_string(), Some("recovery-test")).await;
    recovery.metadata = Some(client.send(topic_metadata_request(None)).await.unwrap());
    recovery.broker = Some(broker);
}

#[then("the recovered topic metadata still has two partitions")]
fn recovered_partitions(recovery: &DiskRecovery) {
    let topic = recovery
        .metadata
        .as_ref()
        .expect("Metadata completed")
        .topics
        .iter()
        .find(|topic| topic.name.as_deref() == Some("persisted"))
        .expect("recovered topic visible in metadata");
    assert!(topic.partitions.len() == 2);
}

#[scenario(
    path = "tests/features/migrated/tests/integration.feature",
    name = "Timestamp lookups return the first matching record and Kafka local sentinels"
)]
#[tokio::test]
async fn list_offsets_by_timestamp_local(timestamps: TimestampLookup) {
    timestamps
        .process
        .expect("records were written")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/integration.feature",
    name = "A negotiated client creates a topic produces records and fetches them"
)]
#[tokio::test]
async fn end_to_end_create_produce_fetch_delete(lifecycle: RpcLifecycle) {
    lifecycle
        .process
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/integration.feature",
    name = "A one-way produce appends without a reply and leaves its connection usable"
)]
#[tokio::test]
async fn produce_acks_zero_sends_no_frame_and_keeps_connection_usable(one_way: OneWayProduce) {
    one_way
        .process
        .expect("a broker was started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/integration.feature",
    name = "A broker recovers persisted topic partitions when it rejoins its log directory"
)]
#[tokio::test]
async fn second_open_recovers_partitions_from_disk(recovery: DiskRecovery) {
    recovery.broker.expect("a broker rejoined").shutdown().await;
}
