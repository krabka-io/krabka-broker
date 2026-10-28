//! Record offsets and fetch visibility through one Kafka broker.

use {
    crate::{
        harness::{create_topic, topic_id_for},
        support::{
            self, InProcess,
            fetch::{FetchLimits, RequestWaitMillis, check_record_count, single_partition_fetch},
            offsets::{list_offset_partition, single_partition_list_offsets},
            produce::{SinglePartitionProduceSetup, batch_request},
            records::empty_record_batch,
        },
    },
    assert2::{assert, check},
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
    offsets: Vec<ListOffsetsResponse>,
}

impl RecordRoundTrip {
    fn broker(&self) -> &InProcess { self.broker.as_ref().expect("the record broker is running") }

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
    let request = batch_request(empty_record_batch(count), SinglePartitionProduceSetup {
        topic: records.topic.clone(),
        topic_id: records.topic_id,
        ..Default::default()
    });
    let response = records
        .broker()
        .client
        .send(request)
        .await
        .expect("Produce");
    records.produced.push(response);
}

#[when("the client produces without a topic id")]
async fn produce_missing_topic_id(records: &mut RecordRoundTrip) {
    let request = batch_request(empty_record_batch(1), SinglePartitionProduceSetup {
        topic: "nope".into(),
        ..Default::default()
    });
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
    assert!(response.responses.len() == 1);
    assert!(response.responses[0].partition_responses.len() == 1);
    check!(response.responses[0].partition_responses[0].error_code == 0);
    check!(response.responses[0].partition_responses[0].base_offset == offset);
}

#[then("produce without a topic id returns UNKNOWN_TOPIC_ID")]
fn unknown_produce_topic(records: &RecordRoundTrip) {
    assert!(
        records
            .produced
            .last()
            .expect("Produce completed")
            .responses[0]
            .partition_responses[0]
            .error_code
            == 100
    );
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

#[then("fetch returns one topic containing {count} records")]
fn fetched_records(records: &RecordRoundTrip, count: usize) {
    let response = records.fetched.as_ref().expect("Fetch completed");
    assert!(response.responses.len() == 1);
    check_record_count(&response.responses[0].partitions[0], count);
}

#[when("the client requests the earliest and latest offsets")]
async fn request_offset_bounds(records: &mut RecordRoundTrip) {
    for timestamp in [-2, -1] {
        let request = ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets(&records.topic, list_offset_partition(0, timestamp))
        };
        let response = records
            .broker()
            .client
            .send(request)
            .await
            .expect("ListOffsets");
        records.offsets.push(response);
    }
}

#[then("both offset bounds are zero without a Kafka error")]
fn empty_offset_bounds(records: &RecordRoundTrip) {
    for (label, response) in ["earliest", "latest"].into_iter().zip(&records.offsets) {
        check!(response.topics[0].partitions[0].error_code == 0, "{label}");
        check!(response.topics[0].partitions[0].offset == 0, "{label}");
    }
    assert!(records.offsets.len() == 2);
}

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "produce assigns base offsets"
)]
#[tokio::test]
async fn produce_assigns_base_offsets(records: RecordRoundTrip) { records.shutdown().await; }

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "produce without a topic id returns unknown topic id"
)]
#[tokio::test]
async fn produce_without_a_topic_id_returns_unknown_topic_id(records: RecordRoundTrip) {
    records.shutdown().await;
}

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "produce then fetch round trip"
)]
#[tokio::test]
async fn produce_then_fetch_round_trip(records: RecordRoundTrip) { records.shutdown().await; }

#[scenario(
    path = "tests/features/migrated/tests/unit/produce_fetch.feature",
    name = "list offsets earliest and latest"
)]
#[tokio::test]
async fn list_offsets_earliest_and_latest(records: RecordRoundTrip) { records.shutdown().await; }
