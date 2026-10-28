//! The one wire exchange both halves of the `fetch.min.bytes` evidence run:
//! a topic, one small record, and a Fetch whose `min_bytes` floor is far above
//! what that record weighs.
//!
//! Both halves send it with the same client code, so the only difference
//! between them is which broker is on the other end.

use {
    crate::support::{
        client::connect_owned,
        discovery::named_topic_metadata,
        fetch::{FetchByteLimit, FetchLimits, RequestWaitMillis, single_partition_fetch},
        produce::{ProduceTimeoutMillis, SinglePartitionProduceSetup, send_batch},
        records::batch_from_records,
        topics::{creatable_topic, create_topic_request_with_setup},
    },
    assert2::assert,
    krabka_client_core::Client,
    krabka_protocol::{
        primitives::uuid::Uuid as WireUuid,
        records::{Record, RecordsPayload},
    },
    std::time::{Duration, Instant},
    tokio::time,
};

/// Topic-created-already, which a re-run of the ignored differential meets.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// What a fetch answered with, whole, so two brokers' answers compare as one
/// value.
#[derive(Debug, PartialEq, Eq)]
pub struct FetchFacts {
    pub(crate) error_code: i16,
    pub(crate) high_watermark: i64,
    /// The base offset of every batch the response carried.
    pub(crate) base_offsets: Vec<i64>,
}

/// A client on `bootstrap`.
async fn connect(bootstrap: &str) -> Client {
    connect_owned(bootstrap, "krabka-fetch-min-bytes", "client build").await
}

/// Append one small record -- small enough that `MIN_BYTES` stays out of reach
/// whatever the batch overhead of the broker on the other end is.
async fn produce_one(client: &Client, topic: &str, topic_id: WireUuid, value: &'static [u8]) {
    let produced = send_batch(
        &client,
        batch_from_records(vec![Record {
            value: Some(bytes::Bytes::from_static(value)),
            ..Default::default()
        }]),
        SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            timeout: ProduceTimeoutMillis(10_000),
            ..Default::default()
        },
    )
    .await;
    assert!(
        produced.responses[0].partition_responses[0].error_code == 0,
        "Produce"
    );
}

/// Create `topic` with one partition, tolerating a topic a previous run left
/// behind.
async fn create_topic(client: &Client, topic: &str) {
    let response = client
        .send(
            create_topic_request_with_setup()
                .topic(creatable_topic().name(topic.to_owned()).call())
                .timeout(10_000)
                .call(),
        )
        .await
        .expect("CreateTopics");
    let code = response.topics[0].error_code;
    assert!(code == 0 || code == TOPIC_ALREADY_EXISTS, "CreateTopics");
}

/// Poll `Metadata` until the topic has a leader, and answer with its id.
///
/// A real Kafka broker publishes the topic through its own metadata log, so
/// the id is not there the instant `CreateTopics` returns.
async fn topic_id(client: &Client, topic: &str) -> WireUuid {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .send(named_topic_metadata(topic))
            .await
            .expect("Metadata");
        if let Some(found) = response
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic) && t.error_code == 0)
            && found.partitions.iter().all(|p| p.leader_id >= 0)
        {
            return found.topic_id;
        }
        assert!(Instant::now() < deadline, "topic never got a leader");
        time::sleep(Duration::from_millis(200)).await;
    }
}

/// The base offset of every batch in a fetched partition's records field.
fn base_offsets(records: Option<&RecordsPayload>) -> Vec<i64> {
    let Some(payload) = records else {
        return Vec::new();
    };
    let batches = payload.as_v2().map_or_else(
        || {
            let RecordsPayload::Raw(raw) = payload else {
                panic!("unexpected records payload: {payload:?}");
            };
            let decoded =
                RecordsPayload::from_bytes(raw.clone()).expect("decode the fetched records");
            let Some(batches) = decoded.as_v2() else {
                panic!("fetched records are not v2 batches: {decoded:?}");
            };
            batches.to_vec()
        },
        <[_]>::to_vec,
    );
    batches.iter().map(|batch| batch.base_offset).collect()
}

/// A topic seeded with one record and a connection reserved for its Fetch.
pub struct SeededTopic {
    client: Client,
    bootstrap: String,
    topic: String,
    topic_id: WireUuid,
}

impl SeededTopic {
    pub async fn create(bootstrap: &str, topic: &str) -> Self {
        let client = connect(bootstrap).await;
        create_topic(&client, topic).await;
        let topic_id = topic_id(&client, topic).await;
        produce_one(&client, topic, topic_id, b"first").await;
        Self {
            client,
            bootstrap: bootstrap.to_owned(),
            topic: topic.to_owned(),
            topic_id,
        }
    }

    /// Fetch while another connection appends the second record partway through its wait.
    pub async fn fetch(
        &self,
        minimum: i32,
        wait: i32,
        append_after_ms: u64,
    ) -> (Duration, FetchFacts) {
        let producer = connect(&self.bootstrap).await;
        let topic = self.topic.clone();
        let topic_id = self.topic_id;
        let appended = tokio::spawn(async move {
            time::sleep(Duration::from_millis(append_after_ms)).await;
            produce_one(&producer, &topic, topic_id, b"second").await;
        });
        let started = Instant::now();
        let fetched = self
            .client
            .send(
                single_partition_fetch()
                    .topic(self.topic.clone())
                    .topic_id(self.topic_id)
                    .limits(FetchLimits {
                        wait: RequestWaitMillis(wait),
                        minimum: FetchByteLimit(minimum),
                        ..Default::default()
                    })
                    .call(),
            )
            .await
            .expect("Fetch");
        let held = started.elapsed();
        appended.await.expect("the mid-fetch append");
        let partition = &fetched.responses[0].partitions[0];
        (held, FetchFacts {
            error_code: partition.error_code,
            high_watermark: partition.high_watermark,
            base_offsets: base_offsets(partition.records.as_ref()),
        })
    }
}
