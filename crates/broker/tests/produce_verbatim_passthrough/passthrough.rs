//! Verbatim producer bytes and topic-directed recompression over the wire.

use {
    crate::{
        harness::{
            batch, boot, create_topic, create_topic_with_configs, encode_batch, fetch_first_batch,
            produce_one, topic_id_for, wait_for_compression,
        },
        support::client::connect_client,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_compression::CompressionType,
    krabka_protocol::{
        owned::create_topics_request::CreatableTopicConfig,
        primitives::uuid::Uuid,
        records::{HEADER_LEN, RecordBatch},
    },
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

struct BatchStorage {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    client: Option<Client>,
    topic: String,
    topic_id: Uuid,
    producer_batch: Option<RecordBatch>,
    producer_wire: Bytes,
    value: Vec<u8>,
    count: usize,
    answer: Option<Result<i64, i16>>,
    fetched: Option<RecordBatch>,
}

#[fixture]
fn storage() -> BatchStorage {
    // Start this scenario with fresh inputs and observations.

    BatchStorage {
        broker: None,
        directory: None,
        client: None,
        topic: String::new(),
        topic_id: Uuid::ZERO,
        producer_batch: None,
        producer_wire: Bytes::new(),
        value: Vec::new(),
        count: 0,
        answer: None,
        fetched: None,
    }
}

enum StoragePolicy {
    PreserveProducer,
    ForceZstd,
}

async fn storage_topic(storage: &mut BatchStorage, topic: &str, policy: StoragePolicy) {
    let (broker, bootstrap, directory) = boot().await;
    if matches!(policy, StoragePolicy::ForceZstd) {
        create_topic_with_configs(&broker, &bootstrap, topic, vec![CreatableTopicConfig {
            name: "compression.type".into(),
            value: Some("zstd".into()),
            ..Default::default()
        }])
        .await;
        wait_for_compression(&broker, topic, Some(CompressionType::Zstd)).await;
    } else {
        create_topic(&broker, &bootstrap, topic).await;
    }
    let client = connect_client(bootstrap, None).await;
    storage.topic_id = topic_id_for(&client, topic).await;
    storage.broker = Some(broker);
    storage.directory = Some(directory);
    storage.client = Some(client);
    storage.topic = topic.into();
}

fn input_batch(storage: &mut BatchStorage, codec: CompressionType, count: usize, value: Vec<u8>) {
    let batch = batch(codec, count, &value);
    storage.producer_wire = encode_batch(&batch);
    storage.producer_batch = Some(batch);
    storage.value = value;
    storage.count = count;
}

#[given("a small LZ4 producer batch represents two hundred 512-byte Z records")]
async fn producer_lz4(storage: &mut BatchStorage) {
    storage_topic(storage, "lz4t", StoragePolicy::PreserveProducer).await;
    input_batch(storage, CompressionType::Lz4, 200, vec![b'Z'; 512]);
    assert!(
        storage.producer_wire.len() < (200 * 512) / 8,
        "the LZ4 fixture must be far smaller than its raw records"
    );
}

#[given("an uncompressed producer batch contains three payload records")]
async fn producer_uncompressed(storage: &mut BatchStorage) {
    storage_topic(storage, "raw", StoragePolicy::PreserveProducer).await;
    input_batch(storage, CompressionType::None, 3, b"payload".to_vec());
}

#[given("a zstd topic receives an LZ4 producer batch containing ten 256-byte Q records")]
async fn topic_recompression(storage: &mut BatchStorage) {
    storage_topic(storage, "recmp", StoragePolicy::ForceZstd).await;
    input_batch(storage, CompressionType::Lz4, 10, vec![b'Q'; 256]);
}

#[when("the producer batch is appended to the topic")]
async fn append_batch(storage: &mut BatchStorage) {
    let client = storage
        .client
        .as_ref()
        .expect("a producer topic was created");
    storage.answer = Some(
        produce_one(
            client,
            &storage.topic,
            storage.topic_id,
            storage
                .producer_batch
                .as_ref()
                .expect("a producer batch was prepared")
                .clone(),
        )
        .await,
    );
}

#[then("the producer batch is accepted at offset zero")]
fn accepted_batch(storage: &BatchStorage) {
    assert!(storage.answer.as_ref().expect("Produce completed") == &Ok(0));
}

#[when("the stored producer batch is fetched")]
async fn read_stored_batch(storage: &mut BatchStorage) {
    storage.fetched = Some(
        fetch_first_batch(
            storage.broker.as_ref().expect("a broker was started"),
            storage.client.as_ref().expect("a client connected"),
            &storage.topic,
            storage.topic_id,
            i64::try_from(storage.count).expect("record count fits in offset"),
        )
        .await,
    );
}

fn matching_values(storage: &BatchStorage, codec: CompressionType) {
    let fetched = storage.fetched.as_ref().expect("Fetch completed");
    check!(
        fetched.attributes.compression() == codec,
        "stored batch codec: {:?}",
        fetched.attributes.compression()
    );
    assert!(fetched.records.len() == storage.count);
    check!(
        fetched
            .records
            .iter()
            .all(|record| record.value.as_deref() == Some(storage.value.as_slice())),
        "every record value must round trip"
    );
    check!(fetched.base_offset == 0);
}

#[then("LZ4 compression and every large record value are preserved")]
fn preserved_lz4(storage: &BatchStorage) { matching_values(storage, CompressionType::Lz4); }

#[then("the topic's zstd compression replaces LZ4 without changing record values")]
fn forced_zstd(storage: &BatchStorage) { matching_values(storage, CompressionType::Zstd); }

#[then("the record body CRC-covered header and CRC bytes are unchanged")]
fn preserved_bytes(storage: &BatchStorage) {
    let fetched = encode_batch(storage.fetched.as_ref().expect("Fetch completed"));
    check!(
        fetched[HEADER_LEN..] == storage.producer_wire[HEADER_LEN..],
        "record body must be verbatim"
    );
    check!(
        fetched[21..HEADER_LEN] == storage.producer_wire[21..HEADER_LEN],
        "CRC-covered header must be verbatim"
    );
    check!(
        fetched[17..21] == storage.producer_wire[17..21],
        "CRC field unchanged"
    );
}

async fn stop_batch_storage(storage: BatchStorage) {
    storage
        .broker
        .expect("a broker was started")
        .shutdown()
        .await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/passthrough.feature",
    name = "Producer LZ4 compression survives the verbatim append and Fetch"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lz4_batch_passes_through_and_roundtrips(storage: BatchStorage) {
    stop_batch_storage(storage).await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/passthrough.feature",
    name = "An uncompressed producer batch retains its CRC and record bytes"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn uncompressed_batch_roundtrips_byte_identically(storage: BatchStorage) {
    stop_batch_storage(storage).await;
}

#[scenario(
    path = "tests/features/migrated/tests/produce_verbatim_passthrough/passthrough.feature",
    name = "A topic codec override recompresses producer records without data loss"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recompression_config_takes_owned_path(storage: BatchStorage) {
    stop_batch_storage(storage).await;
}
