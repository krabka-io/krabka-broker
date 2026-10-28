//! Invalid producer batches are refused before either append path writes them.

use {
    crate::{
        harness::{
            batch, boot, create_topic, encode_batch, produce_one, produce_payload, topic_id_for,
        },
        support::{client::connect_client, records::value_record},
    },
    assert2::check,
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_compression::CompressionType,
    krabka_protocol::{
        primitives::uuid::Uuid,
        records::{
            Attributes, CRC_COVERAGE_START, HEADER_LEN, Record, RecordBatch, RecordsPayload,
        },
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

enum CandidateBatch {
    Control(RecordBatch),
    Raw(Bytes),
}

struct RejectedBatch {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    topic: String,
    topic_id: Uuid,
    candidate: Option<CandidateBatch>,
    answer: Option<Result<i64, i16>>,
}

#[fixture]
fn rejection() -> RejectedBatch {
    // Start this scenario with fresh inputs and observations.

    RejectedBatch {
        broker: None,
        directory: None,
        client: None,
        topic: String::new(),
        topic_id: Uuid::ZERO,
        candidate: None,
        answer: None,
    }
}

async fn rejection_topic(rejection: &mut RejectedBatch, topic: &str) {
    let (broker, bootstrap, directory) = boot().await;
    create_topic(&broker, &bootstrap, topic).await;
    let client = connect_client(bootstrap, None).await;
    rejection.topic_id = topic_id_for(&client, topic).await;
    rejection.broker = Some(broker);
    rejection.directory = Some(directory);
    rejection.client = Some(client);
    rejection.topic = topic.into();
}

#[given("a client authored a control batch containing one transaction marker-shaped record")]
async fn client_control(rejection: &mut RejectedBatch) {
    rejection_topic(rejection, "ctrl").await;
    let mut batch = RecordBatch {
        last_offset_delta: 0,
        max_timestamp: 99,
        producer_id: -1,
        ..Default::default()
    };
    batch.attributes = Attributes::default().with_control(true);
    batch.records.push(Record {
        key: Some(Bytes::from_static(&[0, 0, 0, 0])),
        ..value_record(0, Some(Bytes::from_static(&[0, 0, 0, 0])))
    });
    rejection.candidate = Some(CandidateBatch::Control(batch));
}

#[given("a producer batch has a zero-length record body but a recomputed valid CRC")]
async fn crc_valid_bad_body(rejection: &mut RejectedBatch) {
    rejection_topic(rejection, "malformed-body").await;
    let mut wire = encode_batch(&batch(CompressionType::None, 1, b"valid")).to_vec();
    wire[HEADER_LEN] = 0;
    let crc = crc32c::crc32c(&wire[CRC_COVERAGE_START..]);
    wire[CRC_COVERAGE_START - 4..CRC_COVERAGE_START].copy_from_slice(&crc.to_be_bytes());
    rejection.candidate = Some(CandidateBatch::Raw(Bytes::from(wire)));
}

#[when("the client submits this candidate producer batch")]
async fn submit_candidate(rejection: &mut RejectedBatch) {
    let client = rejection
        .client
        .as_ref()
        .expect("a producer topic was created");
    rejection.answer = Some(
        match rejection
            .candidate
            .take()
            .expect("a candidate batch was supplied")
        {
            CandidateBatch::Control(batch) => {
                produce_one(client, &rejection.topic, rejection.topic_id, batch).await
            }
            CandidateBatch::Raw(bytes) => {
                produce_payload(
                    client,
                    &rejection.topic,
                    rejection.topic_id,
                    RecordsPayload::Raw(bytes),
                )
                .await
            }
        },
    );
}

#[then("the batch is refused as INVALID_RECORD 87 without appending anything")]
fn refused_before_append(rejection: &RejectedBatch) {
    check!(rejection.answer.as_ref().expect("Produce completed") == &Err(87));
    check!(
        rejection
            .broker
            .as_ref()
            .expect("a broker was started")
            .local_log_end_offset(&rejection.topic, 0)
            == Some(0)
    );
}

async fn stop_rejection(rejection: RejectedBatch) {
    rejection
        .broker
        .expect("a broker was started")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/rejection.feature",
    name = "Clients cannot append broker-only transaction control batches"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_control_batch_is_rejected(rejection: RejectedBatch) {
    stop_rejection(rejection).await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/rejection.feature",
    name = "A valid checksum cannot make an invalid record body appendable"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crc_valid_malformed_record_body_is_rejected(rejection: RejectedBatch) {
    stop_rejection(rejection).await;
}
