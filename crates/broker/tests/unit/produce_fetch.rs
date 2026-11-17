//! Record offsets and fetch visibility through one Kafka broker.

use {
    crate::{
        harness::{create_topic, topic_id_for},
        support::{
            self, InProcess,
            fetch::{FetchLimits, RequestWaitMillis, single_partition_fetch},
            offsets::{list_offset_partition, single_partition_list_offsets},
            produce::{single_partition_produce, single_partition_response},
            records::empty_record_batch,
        },
    },
    assert2::assert,
    krabka_protocol::{
        owned::{
            fetch_response::FetchResponse, list_offsets_request::ListOffsetsRequest,
            list_offsets_response::ListOffsetsResponse, produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
};

#[derive(Default)]
struct RecordRoundTrip {
    broker: Option<InProcess>,
    topic: String,
    topic_id: Uuid,
    produced: Vec<ProduceResponse>,
    fetched: Option<FetchResponse>,
    offset: Option<ListOffsetsResponse>,
}

impl RecordRoundTrip {
    fn broker(&self) -> &InProcess { self.broker.as_ref().expect("the record broker is running") }

    async fn produce(&self, count: i32) -> ProduceResponse {
        let request = single_partition_produce()
            .records(empty_record_batch(count))
            .topic(self.topic.clone())
            .topic_id(self.topic_id)
            .call();
        self.broker().client.send(request).await.expect("Produce")
    }

    async fn shutdown(self) {
        self.broker
            .expect("the record broker is running")
            .broker
            .shutdown()
            .await;
    }
}

#[fixture]
fn records() -> RecordRoundTrip {
    // Each scenario owns fresh services and observations.
    RecordRoundTrip {
        broker: None,
        ..Default::default()
    }
}

#[given("a single broker for record requests")]
async fn record_broker(records: &mut RecordRoundTrip) {
    records.broker = Some(support::start().await);
}

#[given("an empty single-partition topic named {topic}")]
async fn record_topic(records: &mut RecordRoundTrip, topic: String) {
    create_topic(records.broker(), &topic, 1).await;
    records.topic_id = topic_id_for(&records.broker().client, &topic).await;
    records.topic = topic;
}

#[when("the client produces {count} records to the topic")]
async fn append_records(records: &mut RecordRoundTrip, count: i32) {
    let response = records.produce(count).await;
    records.produced.push(response);
}

#[given("the topic already contains {count} records")]
async fn stored_records(records: &mut RecordRoundTrip, count: i32) -> Result<(), String> {
    if count > 0 {
        let response = records.produce(count).await;
        let partition = single_partition_response(&response);
        if partition.error_code != 0 {
            return Err(format!(
                "could not seed {count} records in topic {}: Kafka error {}",
                records.topic, partition.error_code
            ));
        }
    }
    Ok(())
}

#[when("the client produces without a topic id")]
async fn produce_missing_topic_id(records: &mut RecordRoundTrip) {
    let request = single_partition_produce()
        .records(empty_record_batch(1))
        .topic("nope")
        .call();
    let response = records
        .broker()
        .client
        .send(request)
        .await
        .expect("Produce unknown");
    records.produced.push(response);
}

#[then("the latest produce response contains one successful partition at offset {offset}")]
fn produced_offset(records: &RecordRoundTrip, offset: i64) {
    let response = records.produced.last().expect("records were produced");
    let partition = single_partition_response(response);
    assert!((partition.error_code, partition.base_offset) == (0, offset));
}

#[then("produce without a topic id returns UNKNOWN_TOPIC_ID")]
fn unknown_produce_topic(records: &RecordRoundTrip) {
    let response = records.produced.last().expect("Produce completed");
    assert!(single_partition_response(response).error_code == 100);
}

#[when("the client fetches the topic from offset zero")]
async fn fetch_records(records: &mut RecordRoundTrip) {
    let request = single_partition_fetch()
        .topic(records.topic.clone())
        .topic_id(records.topic_id)
        .limits(FetchLimits::wait_for_data(RequestWaitMillis(100)))
        .call();
    records.fetched = Some(records.broker().client.send(request).await.expect("Fetch"));
}

#[then("fetch returns the topic partition successfully at offsets {offsets}")]
fn fetched_records(records: &RecordRoundTrip, offsets: String) {
    let response = records.fetched.as_ref().expect("Fetch completed");
    assert!(response.responses.len() == 1);
    let topic = &response.responses[0];
    assert!(topic.partitions.len() == 1);
    let partition = &topic.partitions[0];
    let batches = partition
        .records
        .as_ref()
        .expect("Fetch returned records")
        .as_v2()
        .expect("Fetch returned v2 record batches");
    let actual: Vec<_> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .records
                .iter()
                .map(move |record| batch.base_offset + i64::from(record.offset_delta))
        })
        .collect();
    let expected: Vec<i64> = offsets
        .split(',')
        .map(|offset| offset.parse().expect("an expected record offset"))
        .collect();
    assert!(
        (
            response.error_code,
            topic.topic_id,
            partition.partition_index,
            partition.error_code,
            actual,
        ) == (0, records.topic_id, 0, 0, expected)
    );
}

#[when("the client requests the {bound} offset")]
async fn request_offset_bound(records: &mut RecordRoundTrip, bound: String) {
    let timestamp = match bound.as_str() {
        "earliest" => -2,
        "latest" => -1,
        _ => panic!("unknown offset bound {bound}"),
    };
    let request = ListOffsetsRequest {
        replica_id: -1,
        ..single_partition_list_offsets(&records.topic, list_offset_partition(0, timestamp))
    };
    records.offset = Some(
        records
            .broker()
            .client
            .send(request)
            .await
            .expect("ListOffsets"),
    );
}

#[then("the requested offset is {offset} without a Kafka error")]
fn offset_bound(records: &RecordRoundTrip, offset: i64) {
    let response = records.offset.as_ref().expect("ListOffsets completed");
    assert!(response.topics.len() == 1);
    assert!(response.topics[0].partitions.len() == 1);
    let partition = &response.topics[0].partitions[0];
    assert!((partition.error_code, partition.offset) == (0, offset));
}

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "Appends follow the records already stored"
)]
#[tokio::test]
async fn produce_assigns_base_offsets(
    records: RecordRoundTrip,
    case: String,
    stored: String,
    appended: String,
    offset: String,
) {
    let _ = (case, stored, appended, offset);
    records.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "Producing without a topic id is refused"
)]
#[tokio::test]
async fn produce_without_a_topic_id_returns_unknown_topic_id(records: RecordRoundTrip) {
    records.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "Fetch reads the records from offset zero"
)]
#[tokio::test]
async fn produce_then_fetch_round_trip(records: RecordRoundTrip) { records.shutdown().await; }

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "Earliest and latest offsets bound the stored records"
)]
#[tokio::test]
async fn list_offsets_earliest_and_latest(
    records: RecordRoundTrip,
    case: String,
    stored: String,
    bound: String,
    offset: String,
) {
    let _ = (case, stored, bound, offset);
    records.shutdown().await;
}
