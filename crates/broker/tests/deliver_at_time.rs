//! KFC-1 deliver-at-time visibility, end to end against an in-process broker.
//!
//! Every case here drives the real Kafka wire path — `CreateTopics`,
//! `Produce`, `Fetch`, `ListOffsets` — against a live broker, and every case
//! runs twice: once on a topic with `delivery.mode=scheduled` and once on a
//! topic with `delivery.mode=immediate`. The immediate half is the control. It
//! is what shows that a scheduled topic's behaviour is the configuration and
//! not the code path every topic now takes.
//!
//! # Real time, not a mock clock
//!
//! KFC-1's test plan asks for a mock clock, and the delivery unit tests have
//! one. An integration test cannot reach it. `DeliveryHandles::now_ms` reads
//! the clock its handles were built with, `DeliveryHandles::new` in the
//! partition-spawn path builds them on the system clock, and `BrokerConfig`
//! carries no seam to change that. So these cases run on wall-clock time.
//!
//! That is only honest if no assertion is a race, so none of them is one. A
//! record that has to activate mid-test is stamped [`ACTIVATION_DELAY_MS`]
//! ahead, and the read that must find it still pending is taken immediately
//! after the produce and then *checked against the clock*: the case fails if
//! that read finished after the delivery time, rather than passing on a read
//! that proved nothing. A record that has to stay pending for a whole case is
//! stamped [`PENDING_HORIZON_MS`] ahead, which no test run approaches. And
//! nothing waits on a fixed sleep: a case that expects a record to appear polls
//! for it and reports the clock reading of the poll that found it, so "never
//! early" is an assertion on that reading and not on how long the test slept.

mod support;

use std::time::{Duration, Instant};

use assert2::{assert, check};
use bytes::Bytes;
use crabka_broker::{BrokerHandle, NodeId};
use crabka_client_core::Client;
use crabka_log::DeliveryPolicy;
use crabka_protocol::{
    owned::{
        create_topics_request::{CreatableTopic, CreatableTopicConfig, CreateTopicsRequest},
        fetch_request::{FetchPartition, FetchRequest, FetchTopic},
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    },
    primitives::uuid::Uuid,
    records::{Record, RecordBatch},
};
use qubit_clock::{Clock, SystemClock};

/// How far ahead of produce time a record that must activate during a case is
/// stamped.
///
/// It is long enough that the read taken right after the produce is
/// unambiguously before the delivery time on a loaded machine, and short enough
/// that a case does not spend long waiting for it.
const ACTIVATION_DELAY_MS: i64 = 2_000;

/// How far ahead a record that must stay pending for a whole case is stamped.
/// One hour: no run of this suite comes near it.
const PENDING_HORIZON_MS: i64 = 3_600_000;

/// How long a delivery time is allowed to sit in the past for a record that
/// must be due the moment it is produced.
const ALREADY_DUE_MS: i64 = 60_000;

/// Ceiling on how long a poll waits for a record to become visible.
const VISIBILITY_DEADLINE: Duration = Duration::from_secs(30);

/// `max.wait.ms` of the long poll in
/// [`a_parked_long_poll_wakes_when_the_record_comes_due`]. A consumer that the
/// delivery advance does not wake waits all of it out.
const LONG_POLL_MS: i32 = 20_000;

/// What the long poll has to beat to prove that it woke rather than expired.
/// The record it waits for comes due after [`ACTIVATION_DELAY_MS`] plus the
/// declared clock bound, so the two outcomes are seconds apart.
const LONG_POLL_WOKE_MS: u128 = 10_000;

/// One delivery mode: the `delivery.mode` value that selects it, and the
/// [`DeliveryPolicy`] the partition's log reports once the topic config has
/// reached it.
#[derive(Debug, Clone, Copy)]
struct Mode {
    value: &'static str,
    policy: DeliveryPolicy,
}

const IMMEDIATE: Mode = Mode {
    value: "immediate",
    policy: DeliveryPolicy::Immediate,
};

const SCHEDULED: Mode = Mode {
    value: "scheduled",
    policy: DeliveryPolicy::Scheduled,
};

/// Everything a consumer can learn about one partition without a group: the
/// offset `ListOffsets` LATEST reports, which is where a seek-to-end lands, and
/// the record values a fetch from the start of the log serves.
#[derive(Debug, PartialEq, Eq)]
struct Visible {
    latest: i64,
    values: Vec<String>,
}

impl Visible {
    fn of(latest: i64, values: &[&str]) -> Self {
        Self {
            latest,
            values: values.iter().map(|value| (*value).to_owned()).collect(),
        }
    }
}

fn now_ms() -> i64 {
    SystemClock::new().millis()
}

// One batch whose delivery time — its `max_timestamp` — is `delivery_ms`, with
// one record per entry of `values`.
fn batch_at(delivery_ms: i64, values: &[&str]) -> RecordBatch {
    let count = i32::try_from(values.len()).expect("a test batch is small");
    let mut batch = RecordBatch {
        base_timestamp: delivery_ms,
        max_timestamp: delivery_ms,
        last_offset_delta: count - 1,
        ..RecordBatch::default()
    };
    for (index, value) in values.iter().enumerate() {
        batch.records.push(Record {
            offset_delta: i32::try_from(index).expect("a test batch is small"),
            value: Some(Bytes::from((*value).to_owned())),
            ..Record::default()
        });
    }
    batch
}

async fn create_topic(client: &Client, topic: &str, mode: Mode) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.to_owned(),
                num_partitions: 1,
                replication_factor: 1,
                configs: vec![CreatableTopicConfig {
                    name: "delivery.mode".to_owned(),
                    value: Some(mode.value.to_owned()),
                    ..CreatableTopicConfig::default()
                }],
                ..CreatableTopic::default()
            }],
            timeout_ms: 5_000,
            ..CreateTopicsRequest::default()
        })
        .await
        .expect("CreateTopics");
    let created = response.topics.first().expect("one topic result");
    assert!(
        created.error_code == 0,
        "create {topic}: {:?}",
        created.error_message
    );
}

// Wait until `delivery.mode` has travelled from the metadata image through the
// supervisor's reconcile loop into the partition's own `LogConfig`.
//
// `CreateTopics` materializes the partition from the broker's base log config
// and the overrides land on the next reconcile, so a read taken between the two
// would see an immediate-delivery log on a scheduled topic. Waiting on the
// partition's live config is what removes that window; waiting on the metadata
// image would not, because the image is upstream of the value the fetch cap
// reads.
async fn wait_for_delivery_policy(broker: &BrokerHandle, topic: &str, mode: Mode) {
    broker
        .wait_for_metrics("delivery.mode reaches the partition LogConfig", |_| {
            broker
                .partition_log_config_for_test(topic, 0)
                .is_some_and(|config| config.delivery_policy == mode.policy)
        })
        .await;
}

// Create `topic` in `mode` and return its id once the partition is led here and
// carries the mode.
async fn ready_topic(broker: &BrokerHandle, client: &Client, topic: &str, mode: Mode) -> Uuid {
    create_topic(client, topic, mode).await;
    broker
        .wait_until_local_partition_leader(topic, 0, NodeId(1))
        .await;
    wait_for_delivery_policy(broker, topic, mode).await;
    support::topic_id_for(client, topic).await
}

async fn produce(client: &Client, topic: &str, topic_id: Uuid, batch: RecordBatch) {
    let response = client
        .send(ProduceRequest {
            acks: 1,
            timeout_ms: 5_000,
            topic_data: vec![TopicProduceData {
                name: topic.to_owned(),
                topic_id,
                partition_data: vec![PartitionProduceData {
                    index: 0,
                    records: Some(batch.into()),
                    ..PartitionProduceData::default()
                }],
                ..TopicProduceData::default()
            }],
            ..ProduceRequest::default()
        })
        .await
        .expect("Produce");
    let written = response
        .responses
        .first()
        .and_then(|topic| topic.partition_responses.first())
        .expect("one partition result");
    assert!(
        written.error_code == 0,
        "produce to {topic}: error {}",
        written.error_code
    );
}

// Fetch `topic` from offset 0 and return the record values it served, in order.
async fn fetch_values(
    client: &Client,
    topic: &str,
    topic_id: Uuid,
    max_wait_ms: i32,
) -> Vec<String> {
    let response = client
        .send(FetchRequest {
            max_wait_ms,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![FetchTopic {
                topic: topic.to_owned(),
                topic_id,
                partitions: vec![FetchPartition {
                    partition: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1 << 20,
                    ..FetchPartition::default()
                }],
                ..FetchTopic::default()
            }],
            ..FetchRequest::default()
        })
        .await
        .expect("Fetch");
    let served = response
        .responses
        .first()
        .and_then(|topic| topic.partitions.first())
        .expect("one partition result");
    assert!(
        served.error_code == 0,
        "fetch {topic}: error {}",
        served.error_code
    );
    served
        .records
        .as_ref()
        .and_then(crabka_protocol::records::RecordsPayload::as_v2)
        .map(|batches| {
            batches
                .iter()
                .flat_map(|batch| batch.records.iter())
                .map(|record| {
                    String::from_utf8_lossy(record.value.as_deref().unwrap_or_default())
                        .into_owned()
                })
                .collect()
        })
        .unwrap_or_default()
}

// The offset `ListOffsets` LATEST reports, which is where a seek-to-end lands.
async fn latest_offset(client: &Client, topic: &str) -> i64 {
    let response = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            topics: vec![ListOffsetsTopic {
                name: topic.to_owned(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -1,
                    ..ListOffsetsPartition::default()
                }],
                ..ListOffsetsTopic::default()
            }],
            ..ListOffsetsRequest::default()
        })
        .await
        .expect("ListOffsets");
    let end = response
        .topics
        .first()
        .and_then(|topic| topic.partitions.first())
        .expect("one partition result");
    assert!(
        end.error_code == 0,
        "list offsets {topic}: error {}",
        end.error_code
    );
    end.offset
}

async fn visible(client: &Client, topic: &str, topic_id: Uuid) -> Visible {
    Visible {
        latest: latest_offset(client, topic).await,
        // `max_wait_ms` of zero keeps this a snapshot: an empty read returns at
        // once rather than parking in the long poll.
        values: fetch_values(client, topic, topic_id, 0).await,
    }
}

// Poll until `topic` serves exactly `want`, and report the clock reading taken
// after the read that first saw it.
//
// The reading is taken *after* the read rather than before it, which makes
// "never early" a claim the broker cannot satisfy by luck: a reading below the
// delivery time means the whole round trip finished before that time and still
// came back with the record.
async fn wait_until_visible(client: &Client, topic: &str, topic_id: Uuid, want: &Visible) -> i64 {
    let deadline = Instant::now() + VISIBILITY_DEADLINE;
    loop {
        let seen = visible(client, topic, topic_id).await;
        let at_ms = now_ms();
        if seen == *want {
            return at_ms;
        }
        assert!(
            Instant::now() < deadline,
            "{topic} never served {want:?}; the last read was {seen:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn a_record_stamped_in_the_future_waits_for_its_delivery_time() {
    struct Case {
        mode: Mode,
        /// What a read taken before the record's delivery time serves.
        before_delivery: Visible,
        /// Whether the record first reached a consumer at or after its delivery
        /// time. Only a scheduled topic holds it that long.
        held_until_delivery: bool,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            before_delivery: Visible::of(0, &[]),
            held_until_delivery: true,
        },
        Case {
            mode: IMMEDIATE,
            before_delivery: Visible::of(1, &["due-soon"]),
            held_until_delivery: false,
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-wait-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let deliver_at_ms = now_ms() + ACTIVATION_DELAY_MS;
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(deliver_at_ms, &["due-soon"]),
        )
        .await;

        let seen = visible(&p.client, &topic, topic_id).await;
        let read_at_ms = now_ms();
        check!(
            read_at_ms < deliver_at_ms,
            "{topic}: the read finished {} ms after the delivery time, so it proves nothing",
            read_at_ms - deliver_at_ms
        );
        check!(
            seen == case.before_delivery,
            "{topic}: before the delivery time"
        );

        let delivered = Visible::of(1, &["due-soon"]);
        let served_at_ms = wait_until_visible(&p.client, &topic, topic_id, &delivered).await;
        check!(
            (served_at_ms >= deliver_at_ms) == case.held_until_delivery,
            "{topic}: first served at {served_at_ms}, delivery time {deliver_at_ms}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn a_parked_long_poll_wakes_when_the_record_comes_due() {
    struct Case {
        mode: Mode,
        /// Whether the poll returned at or after the record's delivery time.
        returns_after_delivery: bool,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            returns_after_delivery: true,
        },
        Case {
            mode: IMMEDIATE,
            returns_after_delivery: false,
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-longpoll-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let deliver_at_ms = now_ms() + ACTIVATION_DELAY_MS;
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(deliver_at_ms, &["due-soon"]),
        )
        .await;

        // The record is already in the log, so nothing appends and no watermark
        // this consumer reads moves while it waits. On a scheduled topic the
        // only thing that can end the wait early is the delivery advance.
        let started = Instant::now();
        let served = fetch_values(&p.client, &topic, topic_id, LONG_POLL_MS).await;
        let elapsed = started.elapsed();
        let returned_at_ms = now_ms();

        check!(
            served == vec!["due-soon".to_owned()],
            "{topic}: the long poll served {served:?}"
        );
        check!(
            elapsed.as_millis() < LONG_POLL_WOKE_MS,
            "{topic}: the long poll took {elapsed:?}, so it expired rather than woke"
        );
        check!(
            (returned_at_ms >= deliver_at_ms) == case.returns_after_delivery,
            "{topic}: returned at {returned_at_ms}, delivery time {deliver_at_ms}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn list_offsets_latest_lands_a_seek_to_end_on_the_first_pending_record() {
    struct Case {
        mode: Mode,
        expected: Visible,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            expected: Visible::of(2, &["due-a", "due-b"]),
        },
        Case {
            mode: IMMEDIATE,
            expected: Visible::of(3, &["due-a", "due-b", "pending"]),
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-latest-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let now = now_ms();
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now - ALREADY_DUE_MS, &["due-a", "due-b"]),
        )
        .await;
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now + PENDING_HORIZON_MS, &["pending"]),
        )
        .await;

        // On a scheduled topic LATEST is the delivery watermark, so a consumer
        // that seeks to end lands on offset 2 and receives the pending record
        // when it activates. On an immediate topic it is the log end offset.
        check!(
            visible(&p.client, &topic, topic_id).await == case.expected,
            "{topic}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn a_batch_scheduled_far_out_holds_back_a_later_batch_that_is_already_due() {
    struct Case {
        mode: Mode,
        expected: Visible,
    }

    let p = support::start().await;
    let cases = [
        Case {
            mode: SCHEDULED,
            expected: Visible::of(0, &[]),
        },
        Case {
            mode: IMMEDIATE,
            expected: Visible::of(2, &["pending", "already-due"]),
        },
    ];

    for case in cases {
        let topic = format!("deliver-at-time-head-of-line-{}", case.mode.value);
        let topic_id = ready_topic(&p.broker, &p.client, &topic, case.mode).await;

        let now = now_ms();
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now + PENDING_HORIZON_MS, &["pending"]),
        )
        .await;
        produce(
            &p.client,
            &topic,
            topic_id,
            batch_at(now - ALREADY_DUE_MS, &["already-due"]),
        )
        .await;

        // The record at offset 1 is due, and a scheduled topic still serves
        // nothing: a classic group's position is one offset per partition, so
        // delivering offset 1 first would put offset 0 permanently behind that
        // position. Head-of-line order is the contract, not a defect.
        check!(
            visible(&p.client, &topic, topic_id).await == case.expected,
            "{topic}"
        );
    }

    p.broker.shutdown().await;
}

#[tokio::test]
async fn the_delivery_watermark_survives_a_broker_restart() {
    struct Case {
        mode: Mode,
        expected: Visible,
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let cases = [
        Case {
            mode: SCHEDULED,
            expected: Visible::of(1, &["due"]),
        },
        Case {
            mode: IMMEDIATE,
            expected: Visible::of(2, &["due", "pending"]),
        },
    ];
    let topic_of = |mode: Mode| format!("deliver-at-time-restart-{}", mode.value);

    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        for case in &cases {
            let topic = topic_of(case.mode);
            let topic_id = ready_topic(&broker, &client, &topic, case.mode).await;

            let now = now_ms();
            produce(
                &client,
                &topic,
                topic_id,
                batch_at(now - ALREADY_DUE_MS, &["due"]),
            )
            .await;
            produce(
                &client,
                &topic,
                topic_id,
                batch_at(now + PENDING_HORIZON_MS, &["pending"]),
            )
            .await;

            check!(
                visible(&client, &topic, topic_id).await == case.expected,
                "{topic}: before the restart"
            );
        }
        broker.shutdown().await;
    }

    // Nothing about the schedule was written anywhere but the records
    // themselves, so the reopened log has to derive the same answer from the
    // batch timestamps and the clock.
    let (broker, client) = support::start_with_dir(dir.path()).await;
    for case in &cases {
        let topic = topic_of(case.mode);
        broker
            .wait_until_local_partition_leader(&topic, 0, NodeId(1))
            .await;
        wait_for_delivery_policy(&broker, &topic, case.mode).await;
        let topic_id = support::topic_id_for(&client, &topic).await;

        check!(
            visible(&client, &topic, topic_id).await == case.expected,
            "{topic}: after the restart"
        );
    }

    broker.shutdown().await;
}
