//! The gate itself: a batch at the cap appends, a batch one byte over it is
//! refused with `MESSAGE_TOO_LARGE` (10) and appends nothing.

use {
    crate::{
        support,
        wire::{
            COMPRESSION_LZ4_LEVEL, COMPRESSION_TYPE, MAX_MESSAGE_BYTES, SEGMENT_BYTES, accepted,
            create_topic, gzip_batch, produce_batch, produce_batch_of_wire_len,
            record_list_too_large, too_large, wire_len,
        },
    },
    assert2::check,
    krabka_compression::CompressionType,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse, primitives::uuid::Uuid,
        records::RecordBatch,
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::collections::BTreeMap,
};

struct BatchGate {
    process: Option<support::InProcess>,
    topics: BTreeMap<String, Uuid>,
    response: Option<PartitionProduceResponse>,
    gzip: Option<RecordBatch>,
}
#[fixture]
fn gate() -> BatchGate {
    // Start this scenario with fresh inputs and observations.

    BatchGate {
        process: None,
        topics: BTreeMap::new(),
        response: None,
        gzip: None,
    }
}
#[given("a broker hosts topics with these message and segment limits")]
async fn limits(gate: &mut BatchGate, datatable: Vec<Vec<String>>) {
    let process = support::start().await;
    for row in datatable.into_iter().skip(1) {
        let [name, message, segment, codec]: [String; 4] =
            row.try_into().expect("topic, cap, segment size and codec");
        let mut settings = Vec::new();
        if message != "default" {
            settings.push((MAX_MESSAGE_BYTES, message.as_str()));
        }
        if segment != "default" {
            settings.push((SEGMENT_BYTES, segment.as_str()));
        }
        if codec != "producer" {
            settings.push((COMPRESSION_TYPE, codec.as_str()));
        }
        let id = create_topic(&process.broker, &process.client, &name, &settings).await;
        gate.topics.insert(name, id);
    }
    gate.process = Some(process);
}
#[given("a gzip batch of twenty thousand bytes is smaller than its level nine LZ4 storage form")]
async fn recompression_limits(gate: &mut BatchGate) {
    let batch = gzip_batch(20_000);
    let mut stored = batch.clone();
    stored.attributes = stored.attributes.with_compression(CompressionType::Lz4);
    let mut bytes = bytes::BytesMut::new();
    stored
        .encode_with_compression_level(&mut bytes, Some(9))
        .expect("encode lz4");
    let stored_length = bytes.len();
    check!(wire_len(&batch) < stored_length - 1);
    let process = support::start().await;
    for (name, cap) in [("under", stored_length - 1), ("at", stored_length)] {
        let id = create_topic(&process.broker, &process.client, name, &[
            (MAX_MESSAGE_BYTES, &cap.to_string()),
            (COMPRESSION_TYPE, "lz4"),
            (COMPRESSION_LZ4_LEVEL, "9"),
        ])
        .await;
        gate.topics.insert(name.into(), id);
    }
    gate.gzip = Some(batch);
    gate.process = Some(process);
}
#[when("a producer appends a {size} byte wire batch to {topic}")]
async fn wire_append(gate: &mut BatchGate, size: usize, topic: String) {
    let process = gate.process.as_ref().expect("topic limits configured");
    gate.response =
        Some(produce_batch_of_wire_len(&process.client, &topic, gate.topics[&topic], size).await);
}
#[when("a producer appends the gzip batch to {topic}")]
async fn recompressed_append(gate: &mut BatchGate, topic: String) {
    let process = gate.process.as_ref().expect("topic limits configured");
    gate.response = Some(
        produce_batch(
            &process.client,
            &topic,
            gate.topics[&topic],
            gate.gzip.as_ref().expect("gzip input prepared").clone(),
        )
        .await,
    );
}
#[when("a producer appends gzip compressed content of {size} bytes to orders")]
async fn expanded_append(gate: &mut BatchGate, size: usize) {
    let process = gate.process.as_ref().expect("topic limits configured");
    let batch = gzip_batch(size);
    if size == 100_000 {
        check!(wire_len(&batch) < 2048);
        check!(batch.encoded_batch_len() > 2048);
    }
    gate.response =
        Some(produce_batch(&process.client, "orders", gate.topics["orders"], batch).await);
}
#[then("the batch is accepted at offset {offset}")]
fn appended(gate: &BatchGate, offset: i64) {
    check!(*gate.response.as_ref().expect("append completed") == accepted(offset, 0));
}
#[then("the batch is refused as a message too large")]
fn message_refused(gate: &BatchGate) {
    check!(*gate.response.as_ref().expect("append completed") == too_large());
}
#[then("the batch is refused as a record list too large for the segment")]
fn segment_refused(gate: &BatchGate) {
    check!(*gate.response.as_ref().expect("append completed") == record_list_too_large());
}
#[then("topic {topic} has log end offset {offset}")]
fn log_end(gate: &BatchGate, topic: String, offset: i64) {
    let process = gate.process.as_ref().expect("topic limits configured");
    check!(process.broker.local_log_end_offset(&topic, 0) == Some(offset));
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/produce_gate.feature",
    name = "An exact message cap accepts its boundary and refuses one byte more"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_at_the_cap_appends_and_one_byte_over_it_is_refused(gate: BatchGate) {
    gate.process
        .expect("batch gate broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/produce_gate.feature",
    name = "Recompression measures the exact stored LZ4 batch size"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recompressed_batch_is_held_to_the_cap_at_its_stored_size_exactly(gate: BatchGate) {
    gate.process
        .expect("batch gate broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/produce_gate.feature",
    name = "Segment size independently limits a whole record batch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_larger_than_segment_bytes_is_refused_even_when_the_message_cap_allows_it(
    gate: BatchGate,
) {
    gate.process
        .expect("batch gate broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/produce_gate.feature",
    name = "An unconfigured topic inherits the broker message cap"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_with_no_override_inherits_the_brokers_message_max_bytes(gate: BatchGate) {
    gate.process
        .expect("batch gate broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/produce_gate.feature",
    name = "Topic overrides tighten and loosen the broker default"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_topic_override_binds_below_and_above_the_broker_default(gate: BatchGate) {
    gate.process
        .expect("batch gate broker started")
        .broker
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/max_message_bytes/produce_gate.feature",
    name = "An uncompressed storage policy measures the expanded batch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_the_topic_expands_is_measured_after_the_broker_re_encodes_it(gate: BatchGate) {
    gate.process
        .expect("batch gate broker started")
        .broker
        .shutdown()
        .await;
}
