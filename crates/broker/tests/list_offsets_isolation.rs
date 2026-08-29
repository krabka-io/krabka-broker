//! Every `ListOffsets` answer against the bound Kafka measures it by, proved
//! over the wire against a live broker with a real transaction open.
//!
//! # Why this suite exists
//!
//! `read_committed` is the isolation level a consumer picks when it must not
//! see the records of a transaction that has not resolved. `ListOffsets` with
//! the `LATEST` sentinel is how such a consumer finds the end of a partition --
//! it is what `seekToEnd`, `endOffsets` and every lag calculation are built on.
//! A broker that answers `LATEST` from the log end offset whatever the request
//! asked for hands a `read_committed` consumer a position past records it is
//! not allowed to read, which is precisely the distinction the isolation level
//! exists to draw. Kafka answers such a request with the partition's last
//! stable offset instead, and this suite is the tier that says so on a real
//! socket through the real Kafka codecs.
//!
//! # Both isolation levels are asked on both sides of the resolution
//!
//! Every case reads `LATEST` at `read_uncommitted` *and* at `read_committed`,
//! while the transaction is open and again after it ends. The pair has to
//! disagree in the first reading and agree in the second. A case that compared
//! them only after the commit would pass against a broker that ignored
//! `isolation_level` entirely, because a resolved transaction pins nothing --
//! the disagreement while the transaction is open is the whole assertion.
//!
//! # One bound, three sentinels
//!
//! `Partition.fetchOffsetForTimestamp` picks `lastFetchableOffset` once per
//! request -- the last stable offset for a `read_committed` client, the high
//! watermark for a `read_uncommitted` one -- and then uses it twice over.
//! LATEST *is* that offset. `MAX_TIMESTAMP` and a positive-timestamp lookup
//! resolve against record data and are refused with `UNKNOWN_OFFSET` when the
//! record they matched sits at or above it. The refusal carries no error code,
//! which is what separates it from a partition that is unavailable.
//!
//! The cases below drive all three through one open transaction, because a
//! bound that is right for LATEST and ignored by the other two would let a
//! `read_committed` consumer read past its own end of partition simply by
//! asking a different way.
//!
//! # Records precede the transaction
//!
//! Each case writes two ordinary records before it opens the transaction. The
//! `read_committed` answer is then the offset the transaction started at rather
//! than zero, so a broker that answered the log start offset, or a constant, is
//! caught here too.

mod support;

use assert2::check;
use bytes::Bytes;
use krabka_broker::{BrokerHandle, codes};
use krabka_client_core::Client;
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_protocol::owned::{
    create_topics_request::{CreatableTopic, CreateTopicsRequest},
    list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest, ListOffsetsTopic},
    list_offsets_response::ListOffsetsPartitionResponse,
};

/// Request timestamp sentinel (-1) asking for the end of the partition.
/// Kafka's `ListOffsetsRequest.LATEST_TIMESTAMP`.
const LATEST_TIMESTAMP: i64 = -1;
/// Request `replica_id` (-1) that marks an ordinary client. Kafka's
/// `ListOffsetsRequest.CONSUMER_REPLICA_ID`.
const CONSUMER_REPLICA_ID: i32 = -1;
/// Request `isolation_level` (0). Kafka's `IsolationLevel.READ_UNCOMMITTED`.
const READ_UNCOMMITTED: i8 = 0;
/// Request `isolation_level` (1). Kafka's `IsolationLevel.READ_COMMITTED`.
const READ_COMMITTED: i8 = 1;

/// How the transaction ends.
///
/// Both resolutions write a control marker to the partition and both release
/// the last stable offset past it, so `ListOffsets` must answer the same way
/// for either. Which of the records a later `Fetch` then hands back is the
/// difference between them, and that is `Fetch`'s business rather than this
/// suite's.
#[derive(Clone, Copy, Debug)]
enum Resolution {
    Commit,
    Abort,
}

/// The end of one partition as the two isolation levels see it at one instant.
///
/// The pair is one value so a case asserts against a whole expected struct
/// rather than against two offsets in sequence, which is what makes "they
/// disagree here and agree there" a single readable claim.
#[derive(Debug, PartialEq, Eq)]
struct EndOfPartition {
    read_uncommitted: ListOffsetsPartitionResponse,
    read_committed: ListOffsetsPartitionResponse,
}

/// The whole row a partition 0 answers with for a sentinel that matched a
/// record: the offset it matched and that record's timestamp.
fn matched_row(offset: i64, timestamp: i64) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: codes::NONE,
        timestamp,
        offset,
        leader_epoch: -1,
        ..Default::default()
    }
}

/// The whole row a partition 0 answers with when the record a sentinel matched
/// sits at or above the request's bound.
///
/// `ReplicaManager.fetchOffset` builds this with
/// `buildErrorResponse(Errors.NONE, partition)`, so the refusal reports *no
/// error*: a client is told the partition has no answer for it, not that the
/// partition is unavailable. Asserting the error code is `NONE` here is what
/// keeps a future implementation from turning a fence into a retryable failure
/// that would spin a consumer forever.
fn refused_row() -> ListOffsetsPartitionResponse {
    matched_row(-1, -1)
}

/// The whole `LATEST` row a healthy partition 0 answers with.
///
/// `LATEST` matches no record, so the response echoes Kafka's
/// `UNKNOWN_TIMESTAMP` (-1), and the handler leaves the leader epoch at the
/// same sentinel. Spelling the full row out is what makes an isolation level
/// that quietly changed the error code or the timestamp fail here too.
fn latest_row(offset: i64) -> ListOffsetsPartitionResponse {
    matched_row(offset, -1)
}

async fn create_topic(client: &Client, name: &str) {
    let response = client
        .send(CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: name.into(),
                num_partitions: 1,
                replication_factor: 1,
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("CreateTopics");
    check!(
        response.topics[0].error_code == codes::NONE,
        "create_topic({name}): {response:?}"
    );
}

/// One `ListOffsets` for partition 0 of `topic` at one sentinel and one
/// isolation level.
async fn list_offset(
    client: &Client,
    topic: &str,
    timestamp: i64,
    isolation_level: i8,
) -> ListOffsetsPartitionResponse {
    let mut response = client
        .send(ListOffsetsRequest {
            replica_id: CONSUMER_REPLICA_ID,
            isolation_level,
            topics: vec![ListOffsetsTopic {
                name: topic.into(),
                partitions: vec![ListOffsetsPartition {
                    partition_index: 0,
                    timestamp,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            timeout_ms: 5_000,
            ..Default::default()
        })
        .await
        .expect("ListOffsets");
    response.topics.remove(0).partitions.remove(0)
}

/// Ask one sentinel of `topic` at both isolation levels.
async fn both_levels(client: &Client, topic: &str, timestamp: i64) -> EndOfPartition {
    EndOfPartition {
        read_uncommitted: list_offset(client, topic, timestamp, READ_UNCOMMITTED).await,
        read_committed: list_offset(client, topic, timestamp, READ_COMMITTED).await,
    }
}

/// Ask for the end of `topic` at both isolation levels.
async fn end_of_partition(client: &Client, topic: &str) -> EndOfPartition {
    both_levels(client, topic, LATEST_TIMESTAMP).await
}

/// Wait until partition 0 of `topic` holds `offset` records and has committed
/// all of them, so a `ListOffsets` taken next is reading settled state.
///
/// Both bounds are needed. The log end offset says the append landed, and the
/// high watermark says it is acknowledged -- and the last stable offset a
/// `read_committed` client is answered with is capped at the high watermark, so
/// a reading taken before the watermark caught up would measure the wrong
/// thing.
async fn wait_for_settled_log(broker: &BrokerHandle, topic: &str, offset: i64) {
    broker
        .wait_until_local_log_end_offset(topic, 0, offset)
        .await;
    broker.wait_until_high_watermark(topic, 0, offset).await;
}

fn record(topic: &str, value: &'static str) -> ProducerRecord {
    ProducerRecord {
        topic: topic.into(),
        value: Some(Bytes::from_static(value.as_bytes())),
        ..Default::default()
    }
}

async fn send_ok(producer: &Producer, topic: &str, value: &'static str) {
    producer
        .send(record(topic, value))
        .await
        .await
        .expect("producer delivery channel open")
        .expect("produce acknowledged");
}

/// Produce one record carrying an explicit `timestamp_ms`.
///
/// The timestamp cases need to know what they are looking up, and a record left
/// to the producer's wall clock cannot be named in an assertion. Fixed
/// timestamps also keep the ordinary records far below the transactional ones,
/// so a lookup can aim either side of the bound on purpose.
async fn send_at(producer: &Producer, topic: &str, value: &'static str, timestamp_ms: i64) {
    producer
        .send(ProducerRecord {
            timestamp_ms: Some(timestamp_ms),
            ..record(topic, value)
        })
        .await
        .await
        .expect("producer delivery channel open")
        .expect("produce acknowledged");
}

/// `LATEST` under `read_committed` stops at the first record of an open
/// transaction, and catches up with `read_uncommitted` once that transaction
/// resolves either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_committed_latest_stops_at_an_open_transaction() {
    let p = support::start().await;
    let bootstrap = p.broker.listen_addr().to_string();

    for (label, resolution) in [
        // A commit publishes the records. The last stable offset moves past
        // the commit marker, so the end of the partition is the whole log.
        ("commit", Resolution::Commit),
        // An abort discards them for a `read_committed` reader, but it still
        // writes a marker and still releases the last stable offset. A broker
        // that released it only on commit would leave every `read_committed`
        // consumer of the partition pinned behind a transaction that is over.
        ("abort", Resolution::Abort),
    ] {
        let topic = format!("list-offsets-isolation-{label}");
        create_topic(&p.client, &topic).await;
        p.broker.wait_until_partition_present(&topic, 0).await;

        // Two ordinary records first, so the transaction does not start at
        // offset 0 and the `read_committed` answer below cannot be confused
        // with the log start offset.
        let plain = Producer::builder()
            .bootstrap(bootstrap.clone())
            .build()
            .await
            .expect("plain producer build");
        for value in ["settled-a", "settled-b"] {
            send_ok(&plain, &topic, value).await;
        }
        wait_for_settled_log(&p.broker, &topic, 2).await;

        // With no transaction anywhere on the partition the two isolation
        // levels must already agree. This is the control reading: it separates
        // "the last stable offset holds a `read_committed` client back" from
        // "the `read_committed` answer is broken".
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(2),
                    read_committed: latest_row(2),
                },
            "{label}: a partition with no transaction on it has one end"
        );

        let producer = Producer::builder()
            .bootstrap(bootstrap.clone())
            .transactional_id(format!("list-offsets-isolation-{label}-tid"))
            .build()
            .await
            .expect("transactional producer build");
        producer
            .init_transactions()
            .await
            .expect("init_transactions");
        let txn = producer
            .begin_transaction()
            .await
            .expect("begin_transaction");
        for value in ["open-c", "open-d"] {
            send_ok(&producer, &topic, value).await;
        }
        wait_for_settled_log(&p.broker, &topic, 4).await;

        // The transaction is open. `read_uncommitted` sees the whole log, and
        // `read_committed` stops at offset 2, the first record the transaction
        // wrote. This is the reading the bug got wrong: it answered 4 to both.
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(4),
                    read_committed: latest_row(2),
                },
            "{label}: an open transaction holds read_committed at its first record"
        );

        match resolution {
            Resolution::Commit => txn.commit().await.expect("commit"),
            Resolution::Abort => txn.abort().await.expect("abort"),
        }

        // The marker is the fifth record on the partition, and it releases the
        // last stable offset. Both isolation levels are back to one answer.
        wait_for_settled_log(&p.broker, &topic, 5).await;
        check!(
            end_of_partition(&p.client, &topic).await
                == EndOfPartition {
                    read_uncommitted: latest_row(5),
                    read_committed: latest_row(5),
                },
            "{label}: a resolved transaction releases read_committed"
        );

        producer
            .close()
            .await
            .expect("transactional producer close");
        plain.close().await.expect("plain producer close");
    }

    p.broker.shutdown().await;
}

/// `LATEST` under `read_uncommitted` answers the high watermark, not the log
/// end offset, so a consumer cannot seek past what the ISR acknowledged.
///
/// This is the older of the two divergences the bound closed, and the one with
/// the wider reach: it applies to every consumer on every topic, transactions
/// or not. A leader whose follower is behind holds records that are durable
/// locally and that no `Fetch` will serve. Answering `LATEST` from the log end
/// offset hands a plain `read_uncommitted` consumer a position inside that
/// unreplicated tail, where it waits for records that may yet be truncated away
/// by a leader election.
///
/// Both isolation levels are asked, because the last stable offset is capped at
/// the high watermark too -- a `read_committed` client must not read past it
/// either, and a bound that fenced only the transactional path would leave the
/// larger hole open.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn latest_answers_the_high_watermark_not_the_unreplicated_log_end() {
    const TOPIC: &str = "list-offsets-isolation-unreplicated";

    let p = support::start().await;
    let producer = Producer::builder()
        .bootstrap(p.broker.listen_addr().to_string())
        .build()
        .await
        .expect("producer build");

    create_topic(&p.client, TOPIC).await;
    p.broker.wait_until_partition_present(TOPIC, 0).await;
    for value in ["a", "b", "c", "d"] {
        send_ok(&producer, TOPIC, value).await;
    }
    wait_for_settled_log(&p.broker, TOPIC, 4).await;

    // The control. Every record is acknowledged, so the high watermark and the
    // log end offset are the same number and no bound can be told from the
    // other. Without this reading a broker that answered a constant 2 below
    // would look correct.
    check!(
        end_of_partition(&p.client, TOPIC).await
            == EndOfPartition {
                read_uncommitted: latest_row(4),
                read_committed: latest_row(4),
            },
        "a fully replicated partition ends at its log end offset"
    );

    // Now the last two records are unreplicated: durable on the leader, not yet
    // acknowledged by the ISR, exactly as they are while a follower catches up.
    check!(
        p.broker.hold_high_watermark_for_test(TOPIC, 0, 2).await,
        "the partition is hosted here"
    );

    check!(
        end_of_partition(&p.client, TOPIC).await
            == EndOfPartition {
                read_uncommitted: latest_row(2),
                read_committed: latest_row(2),
            },
        "neither isolation level may seek into the unreplicated tail"
    );

    producer.close().await.expect("producer close");
    p.broker.shutdown().await;
}

/// An open transaction fences `MAX_TIMESTAMP` and a by-timestamp lookup for a
/// `read_committed` client, while a `read_uncommitted` client still resolves
/// them, and a lookup that lands below the bound is answered for both.
///
/// LATEST is not the only way to ask where a partition ends. A record written
/// inside an open transaction has a timestamp like any other, so a
/// `read_committed` consumer that asked `MAX_TIMESTAMP` -- or offered the
/// transaction's own timestamp to `offsetsForTimes` -- would be handed the
/// offset of a record it is not allowed to read, and would seek straight into
/// the open transaction that LATEST was careful to keep it out of. Kafka
/// refuses both with `UNKNOWN_OFFSET` and no error code.
///
/// The unfenced control matters as much as the fenced readings: a lookup
/// matching one of the settled records must still be answered normally at both
/// isolation levels. Without it, a broker that answered -1 to every timestamp
/// request would pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_open_transaction_fences_max_timestamp_and_a_timestamp_lookup() {
    const TOPIC: &str = "list-offsets-isolation-timestamps";
    /// Request timestamp sentinel (-3, KIP-734) asking for the offset of the
    /// record with the highest timestamp. Kafka's
    /// `ListOffsetsRequest.MAX_TIMESTAMP`.
    const MAX_TIMESTAMP: i64 = -3;
    /// The two settled records' timestamps, far below the transactional ones.
    const SETTLED: [i64; 2] = [1_000, 1_100];
    /// The two transactional records' timestamps. The higher of them is the
    /// partition's maximum while the transaction is open, which is what makes
    /// `MAX_TIMESTAMP` land above the bound.
    const IN_TXN: [i64; 2] = [5_000, 5_100];

    let p = support::start().await;
    let bootstrap = p.broker.listen_addr().to_string();

    create_topic(&p.client, TOPIC).await;
    p.broker.wait_until_partition_present(TOPIC, 0).await;

    let plain = Producer::builder()
        .bootstrap(bootstrap.clone())
        .build()
        .await
        .expect("plain producer build");
    send_at(&plain, TOPIC, "settled-a", SETTLED[0]).await;
    send_at(&plain, TOPIC, "settled-b", SETTLED[1]).await;
    wait_for_settled_log(&p.broker, TOPIC, 2).await;

    let producer = Producer::builder()
        .bootstrap(bootstrap.clone())
        .transactional_id("list-offsets-isolation-timestamps-tid")
        .build()
        .await
        .expect("transactional producer build");
    producer
        .init_transactions()
        .await
        .expect("init_transactions");
    let txn = producer
        .begin_transaction()
        .await
        .expect("begin_transaction");
    send_at(&producer, TOPIC, "open-c", IN_TXN[0]).await;
    send_at(&producer, TOPIC, "open-d", IN_TXN[1]).await;
    wait_for_settled_log(&p.broker, TOPIC, 4).await;

    // The highest timestamp on the partition belongs to the last transactional
    // record, at offset 3. The `read_committed` bound is offset 2, so 3 is out
    // of reach and the answer is refused; `read_uncommitted` is bounded at the
    // high watermark, 4, and gets the record.
    check!(
        both_levels(&p.client, TOPIC, MAX_TIMESTAMP).await
            == EndOfPartition {
                read_uncommitted: matched_row(3, IN_TXN[1]),
                read_committed: refused_row(),
            },
        "MAX_TIMESTAMP inside an open transaction is refused to read_committed"
    );

    // The same shape by timestamp. The transaction's first record is the
    // earliest at or after its own timestamp, and it sits exactly *on* the
    // bound -- Kafka's test is `offset >= lastFetchableOffset`, so the bound
    // itself is already out of reach.
    check!(
        both_levels(&p.client, TOPIC, IN_TXN[0]).await
            == EndOfPartition {
                read_uncommitted: matched_row(2, IN_TXN[0]),
                read_committed: refused_row(),
            },
        "a lookup landing on the bound is refused to read_committed"
    );

    // The control: a timestamp matching a settled record is below the bound at
    // both isolation levels, so both are answered normally. A broker that
    // refused every timestamp request would fail here.
    check!(
        both_levels(&p.client, TOPIC, SETTLED[0]).await
            == EndOfPartition {
                read_uncommitted: matched_row(0, SETTLED[0]),
                read_committed: matched_row(0, SETTLED[0]),
            },
        "a lookup below the bound is answered to both isolation levels"
    );

    txn.commit().await.expect("commit");
    wait_for_settled_log(&p.broker, TOPIC, 5).await;

    // The commit marker releases the last stable offset to 5, so the bound is
    // the same for both isolation levels and every sentinel answers alike. The
    // rows are compared to each other rather than to a literal because the
    // marker carries a wall-clock timestamp, which makes it the partition's
    // maximum and unnameable here.
    for (label, timestamp) in [
        ("MAX_TIMESTAMP", MAX_TIMESTAMP),
        ("the transaction's own timestamp", IN_TXN[0]),
        ("a settled record's timestamp", SETTLED[0]),
    ] {
        let answers = both_levels(&p.client, TOPIC, timestamp).await;
        check!(
            answers.read_committed == answers.read_uncommitted,
            "{label}: a resolved transaction leaves one answer"
        );
        check!(
            answers.read_committed != refused_row(),
            "{label}: and it is a real answer rather than a refusal"
        );
    }

    producer
        .close()
        .await
        .expect("transactional producer close");
    plain.close().await.expect("plain producer close");
    p.broker.shutdown().await;
}
