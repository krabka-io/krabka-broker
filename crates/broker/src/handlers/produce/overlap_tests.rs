//! The append stage of one Produce request overlaps distinct partitions: a
//! writer busy with an earlier message holds back only its own partition,
//! while a partition named twice still appends in request order and every row
//! lands in the slot the request gave it.

use {
    super::{
        AdmittedPartition, PendingPartition, complete_admitted,
        framing::PartitionPayload,
        pipeline::{Admission, PartitionInput, admit_partition},
        producer_checks::TransactionRequest,
        test_support::{PipelineFixture, PipelinePartitionSetup, encode_batch, pipeline_input},
    },
    crate::{
        codes, handlers::test_support::replicated_partition, metrics::PartitionLabel,
        partition::Partition, test_support::replica_nodes,
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord},
    krabka_protocol::{
        owned::produce_response::{PartitionProduceResponse, TopicProduceResponse},
        records::{Record, RecordBatch},
    },
    std::{sync::Arc, time::Duration},
};

/// Long enough that a test which waits this long has failed, not raced.
const PATIENCE: Duration = Duration::from_secs(10);

/// One single-replica partition 0 per topic, each led by this broker under
/// its own topic id.
fn wide_image(topics: &[&str]) -> Arc<MetadataImage> {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for (n, topic) in (1_u128..).zip(topics) {
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: (*topic).into(),
            topic_id: uuid::Uuid::from_u128(n),
            partitions: 1,
            replication_factor: 1,
        }));
        image.apply(&MetadataRecord::V1Partition(
            replicated_partition()
                .topic(topic)
                .replicas(&replica_nodes(&[1]))
                .call(),
        ));
    }
    Arc::new(image)
}

/// An idempotent single-record batch at `base_sequence`.
fn batch(base_sequence: i32) -> Bytes {
    encode_batch(&RecordBatch {
        producer_id: 7,
        producer_epoch: 0,
        base_sequence,
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    })
}

/// One request entry: the topic, its row in the response, and the batch.
struct Entry<'a> {
    topic: &'a str,
    topic_row: usize,
    base_sequence: i32,
}

/// The request shape every test drives: its admitted entries and the rows
/// they reserved, one partition per topic row.
struct Request {
    admitted: Vec<AdmittedPartition>,
    topic_results: Vec<TopicProduceResponse>,
}

async fn admit(
    fixture: &PipelineFixture,
    image: &Arc<MetadataImage>,
    entries: &[Entry<'_>],
    acks: i16,
) -> Request {
    let mut admitted = Vec::new();
    let mut topic_results = Vec::new();
    for entry in entries {
        let input = PartitionInput {
            acks,
            ..pipeline_input(
                entry.topic,
                PartitionPayload::Slice(batch(entry.base_sequence)),
            )
        };
        let Admission::Admitted(admitted_batch) = admit_partition(input, fixture.services(image))
            .await
            .expect("admit")
        else {
            panic!("{} must pass every gate before the append", entry.topic);
        };
        admitted.push(AdmittedPartition {
            topic_row: entry.topic_row,
            partition_row: 0,
            topic_name: entry.topic.into(),
            idx: 0,
            micros: 0,
            batch: admitted_batch,
        });
        topic_results.push(TopicProduceResponse {
            name: entry.topic.to_owned(),
            partition_responses: vec![PartitionProduceResponse {
                index: 0,
                base_offset: -1,
                ..Default::default()
            }],
            ..Default::default()
        });
    }
    Request {
        admitted,
        topic_results,
    }
}

fn no_transaction() -> TransactionRequest<'static> {
    pipeline_input("unused", PartitionPayload::Slice(Bytes::new())).transaction
}

/// The row of an `acks=1` append that took offset `base_offset`.
fn appended(base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse {
        index: 0,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset: 0,
        ..Default::default()
    }
}

fn expected_rows(names: &[&str], rows: Vec<PartitionProduceResponse>) -> Vec<TopicProduceResponse> {
    names
        .iter()
        .zip(rows)
        .map(|(name, row)| TopicProduceResponse {
            name: (*name).to_owned(),
            partition_responses: vec![row],
            ..Default::default()
        })
        .collect()
}

async fn until_appended(partitions: &[Arc<Partition>]) {
    while partitions
        .iter()
        .any(|partition| partition.log_end_offset().0 == 0)
    {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_busy_writer_holds_back_only_its_own_partition() {
    let topics = ["slow", "fast-1", "fast-2", "fast-3"];
    let dir = tempfile::tempdir().expect("tempdir");
    let image = wide_image(&topics);
    let fixture = PipelineFixture::new(krabka_ids::NodeId(1));
    let mut partitions = Vec::new();
    for topic in topics {
        partitions.push(fixture.register_partition(dir.path(), topic, &image).await);
    }
    let entries: Vec<Entry<'_>> = (0..)
        .zip(topics)
        .map(|(topic_row, topic)| Entry {
            topic,
            topic_row,
            base_sequence: 0,
        })
        .collect();
    let Request {
        admitted,
        mut topic_results,
    } = admit(&fixture, &image, &entries, 1).await;
    let mut awaiting: Vec<PendingPartition> = Vec::new();

    // The slow partition's writer is in the middle of a message, so its
    // append queues behind it; the others' writers are idle.
    let busy = partitions[0].writer_gate.writer_busy().await;
    {
        let completing = complete_admitted(
            admitted,
            fixture.services(&image),
            no_transaction(),
            &mut topic_results,
            &mut awaiting,
        );
        tokio::pin!(completing);
        tokio::select! {
            finished = &mut completing => {
                panic!("the request finished under a busy writer: {finished:?}");
            }
            waited = tokio::time::timeout(PATIENCE, until_appended(&partitions[1..])) => {
                waited.expect("every partition behind the busy writer appends while it is busy");
            }
        }
        check!(partitions[0].log_end_offset().0 == 0);
        drop(busy);
        tokio::time::timeout(PATIENCE, completing)
            .await
            .expect("the request finishes once the writer is free")
            .expect("append");
    }

    check!(awaiting.is_empty(), "acks=1 answers without a wait");
    let rows = (0..topics.len()).map(|_| appended(0)).collect();
    assert!(topic_results == expected_rows(&topics, rows));
}

/// Appends that wait at once share the request's handler time between them:
/// the per-partition CPU charge of four writers busy over the same interval
/// adds up to that interval, not four times it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlapping_waits_share_the_handler_time_they_spent_together() {
    let topics = ["a", "b", "c", "d"];
    let dir = tempfile::tempdir().expect("tempdir");
    let image = wide_image(&topics);
    let fixture = PipelineFixture::new(krabka_ids::NodeId(1));
    let mut partitions = Vec::new();
    for topic in topics {
        partitions.push(fixture.register_partition(dir.path(), topic, &image).await);
    }
    let entries: Vec<Entry<'_>> = (0..)
        .zip(topics)
        .map(|(topic_row, topic)| Entry {
            topic,
            topic_row,
            base_sequence: 0,
        })
        .collect();
    let Request {
        admitted,
        mut topic_results,
    } = admit(&fixture, &image, &entries, 1).await;
    let mut awaiting: Vec<PendingPartition> = Vec::new();

    let mut busy = Vec::new();
    for partition in &partitions {
        busy.push(partition.writer_gate.writer_busy().await);
    }
    let started = std::time::Instant::now();
    let (completed, ()) = tokio::join!(
        complete_admitted(
            admitted,
            fixture.services(&image),
            no_transaction(),
            &mut topic_results,
            &mut awaiting,
        ),
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            drop(busy);
        },
    );
    let wall = started.elapsed();
    completed.expect("append");

    let charged: u64 = topics
        .iter()
        .map(|topic| {
            fixture
                .metrics
                .partition_cpu_micros
                .get_or_create(&PartitionLabel {
                    topic: (*topic).into(),
                    partition: 0,
                })
                .get()
        })
        .sum();
    let wall_micros = u64::try_from(wall.as_micros()).expect("a short test");
    check!(charged >= 40_000, "the waits are charged");
    check!(
        charged <= wall_micros,
        "{charged}us charged for {wall_micros}us of request time"
    );
}

/// A partition the request names twice, around a partition that is refused,
/// keeps its entries in request order: the second takes the next offset, and
/// its idempotent sequence follows the first rather than racing it. Under
/// `acks=-1` the appended rows wait in the slots they reserved and the refused
/// row is final.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partition_named_twice_appends_in_request_order_beside_a_refusal() {
    let dir = tempfile::tempdir().expect("tempdir");
    let image = wide_image(&["orders", "tiny"]);
    let fixture = PipelineFixture::new(krabka_ids::NodeId(1));
    let orders = fixture
        .register_partition(dir.path(), "orders", &image)
        .await;
    // A segment smaller than any batch refuses the append with
    // `RECORD_LIST_TOO_LARGE`, after admission.
    let tiny = fixture
        .partition_with_config(dir.path(), &image, PipelinePartitionSetup {
            topic: "tiny",
            log_config: krabka_log::LogConfig {
                segment_size: krabka_units::bytes(1),
                ..Default::default()
            },
        })
        .await;
    fixture
        .partitions
        .insert("tiny".into(), krabka_ids::PartitionIndex(0), tiny);
    let names = ["orders", "tiny", "orders"];
    let entries = [
        Entry {
            topic: "orders",
            topic_row: 0,
            base_sequence: 0,
        },
        Entry {
            topic: "tiny",
            topic_row: 1,
            base_sequence: 0,
        },
        Entry {
            topic: "orders",
            topic_row: 2,
            base_sequence: 1,
        },
    ];
    let Request {
        admitted,
        mut topic_results,
    } = admit(&fixture, &image, &entries, -1).await;
    let mut awaiting: Vec<PendingPartition> = Vec::new();

    tokio::time::timeout(
        PATIENCE,
        complete_admitted(
            admitted,
            fixture.services(&image),
            no_transaction(),
            &mut topic_results,
            &mut awaiting,
        ),
    )
    .await
    .expect("the stage finishes")
    .expect("append");

    // Each appended row finishes once the high watermark covers it, as the
    // request's one wait would finish it.
    let mut waited = Vec::new();
    for pending in awaiting {
        let row = pending
            .ack
            .finish(
                std::time::Instant::now() + PATIENCE,
                |_, _| Ok(1),
                std::future::pending(),
            )
            .await;
        waited.push((pending.topic_row, pending.partition_row, row));
    }
    assert!(waited == [(0, 0, appended(0)), (2, 0, appended(1))]);
    check!(orders.log_end_offset().0 == 2);
    let unanswered = PartitionProduceResponse {
        index: 0,
        base_offset: -1,
        ..Default::default()
    };
    let refused = PartitionProduceResponse {
        error_code: codes::RECORD_LIST_TOO_LARGE,
        ..unanswered.clone()
    };
    assert!(topic_results == expected_rows(&names, vec![unanswered.clone(), refused, unanswered]));
}
