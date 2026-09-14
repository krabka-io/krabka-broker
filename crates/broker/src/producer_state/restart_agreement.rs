//! The produce-path sequence decision before and after a restart.
//!
//! Each case appends data batches and transaction markers through a real
//! partition writer, exactly as the Produce handler and the marker fan-out do.
//! It then asks the live tracker for a decision on each probe batch. It stops
//! the writer, opens the log again from disk, rebuilds a new tracker from that
//! log as startup does, and asks the same questions. Both answers must be the
//! Kafka answer from `ProducerAppendInfo.checkSequence` and
//! `checkProducerEpoch`.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig, ProducerId};
use krabka_protocol::records::{Attributes, Record, RecordBatch};

use super::{Decision, ProducerState};
use crate::txn::marker::{MarkerType, build_marker_batch};

const TOPIC: &str = "orders";
const PARTITION: PartitionIndex = PartitionIndex(0);
const PRODUCER_ID: i64 = 42;

#[derive(Debug, Clone, Copy)]
enum Step {
    /// A data batch of `records` records at `(epoch, base_sequence)`.
    Data {
        epoch: i16,
        base_sequence: i32,
        records: i32,
        transactional: bool,
    },
    /// A COMMIT or ABORT marker at `epoch`.
    Marker { epoch: i16, marker: MarkerType },
}

/// One probe batch: `(epoch, base_sequence, last_offset_delta)`.
type Probe = (i16, i32, i32);

struct Case {
    name: &'static str,
    steps: &'static [Step],
    expected: &'static [(Probe, Decision)],
}

fn data_batch(epoch: i16, base_sequence: i32, records: i32, transactional: bool) -> RecordBatch {
    RecordBatch {
        attributes: Attributes::default().with_transactional(transactional),
        last_offset_delta: records - 1,
        base_timestamp: 1_700_000_000_000,
        max_timestamp: 1_700_000_000_000,
        producer_id: PRODUCER_ID,
        producer_epoch: epoch,
        base_sequence,
        records: (0..records)
            .map(|offset_delta| Record {
                offset_delta,
                value: Some(Bytes::from_static(b"v")),
                ..Record::default()
            })
            .collect(),
        ..RecordBatch::default()
    }
}

/// Run the steps on a live partition, as the Produce handler and the marker
/// fan-out do. The handler records each accepted data batch in the tracker
/// after the append.
async fn run_steps(partition: &crate::partition::Partition, state: &ProducerState, steps: &[Step]) {
    for step in steps {
        match *step {
            Step::Data {
                epoch,
                base_sequence,
                records,
                transactional,
            } => {
                let batch = data_batch(epoch, base_sequence, records, transactional);
                let timestamp = batch.max_timestamp;
                let offset = partition
                    .produce_batch(batch)
                    .await
                    .expect("append data batch");
                state
                    .commit(
                        TOPIC,
                        PARTITION,
                        (PRODUCER_ID, epoch),
                        (base_sequence, records - 1),
                        (offset.get(), timestamp),
                    )
                    .await;
            }
            Step::Marker { epoch, marker } => {
                let batch = build_marker_batch(
                    ProducerId(PRODUCER_ID),
                    epoch,
                    partition.log_end_offset(),
                    marker,
                    0,
                );
                partition
                    .produce_control_batch(batch)
                    .await
                    .expect("append marker");
            }
        }
    }
}

async fn decisions(state: &ProducerState, probes: &[(Probe, Decision)]) -> Vec<(Probe, Decision)> {
    let mut answers = Vec::with_capacity(probes.len());
    for &(probe, _) in probes {
        let (epoch, base_sequence, last_offset_delta) = probe;
        let decision = state
            .check(
                TOPIC,
                PARTITION,
                PRODUCER_ID,
                epoch,
                base_sequence,
                last_offset_delta,
            )
            .await;
        answers.push((probe, decision));
    }
    answers
}

/// Decisions from the live tracker, then from a tracker rebuilt from the
/// reopened log.
async fn live_and_recovered(case: &Case) -> (Vec<(Probe, Decision)>, Vec<(Probe, Decision)>) {
    let directory = tempfile::tempdir().expect("tempdir");
    let state = Arc::new(ProducerState::new());
    let log = Log::open(directory.path(), LogConfig::default()).expect("open log");
    let partition = crate::broker::spawn_partition(
        TOPIC.to_string(),
        PARTITION,
        directory.path().to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::clone(&state),
        false,
    );
    run_steps(&partition, &state, case.steps).await;
    let live = decisions(&state, case.expected).await;

    partition
        .log
        .lock()
        .expect("partition log")
        .sync()
        .expect("sync log");
    let writer = partition.take_writer_handle().expect("writer handle");
    drop(partition);
    writer.await.expect("writer stops");

    let reopened = Log::open(directory.path(), LogConfig::default()).expect("reopen log");
    let recovered_state = ProducerState::new();
    recovered_state
        .rebuild_from_log(TOPIC, PARTITION, &reopened)
        .await
        .expect("rebuild producer state");
    let recovered = decisions(&recovered_state, case.expected).await;
    (live, recovered)
}

const TV2_COMMIT: &[Step] = &[
    Step::Data {
        epoch: 3,
        base_sequence: 0,
        records: 3,
        transactional: true,
    },
    Step::Marker {
        epoch: 4,
        marker: MarkerType::Commit,
    },
];

const TV2_ABORT: &[Step] = &[
    Step::Data {
        epoch: 3,
        base_sequence: 0,
        records: 3,
        transactional: true,
    },
    Step::Marker {
        epoch: 4,
        marker: MarkerType::Abort,
    },
];

const TV2_TWO_TRANSACTIONS: &[Step] = &[
    Step::Data {
        epoch: 3,
        base_sequence: 0,
        records: 3,
        transactional: true,
    },
    Step::Marker {
        epoch: 4,
        marker: MarkerType::Commit,
    },
    Step::Data {
        epoch: 4,
        base_sequence: 0,
        records: 2,
        transactional: true,
    },
    Step::Marker {
        epoch: 5,
        marker: MarkerType::Commit,
    },
];

const TV1_COMMIT: &[Step] = &[
    Step::Data {
        epoch: 3,
        base_sequence: 0,
        records: 3,
        transactional: true,
    },
    Step::Marker {
        epoch: 3,
        marker: MarkerType::Commit,
    },
];

const IDEMPOTENT_EPOCH_BUMP: &[Step] = &[
    Step::Data {
        epoch: 0,
        base_sequence: 0,
        records: 3,
        transactional: false,
    },
    Step::Data {
        epoch: 1,
        base_sequence: 0,
        records: 2,
        transactional: false,
    },
];

const CASES: &[Case] = &[
    Case {
        name: "transaction version 2 commit bumps the epoch",
        steps: TV2_COMMIT,
        expected: &[
            ((4, 0, 0), Decision::Append),
            ((4, 3, 0), Decision::OutOfOrder),
            ((3, 0, 2), Decision::Fenced),
            ((3, 3, 0), Decision::Fenced),
            ((5, 0, 0), Decision::Append),
            ((5, 3, 0), Decision::OutOfOrder),
        ],
    },
    Case {
        name: "transaction version 2 abort bumps the epoch",
        steps: TV2_ABORT,
        expected: &[
            ((4, 0, 0), Decision::Append),
            ((4, 3, 0), Decision::OutOfOrder),
            ((3, 0, 2), Decision::Fenced),
        ],
    },
    Case {
        name: "transaction version 2 second transaction restarts at 0",
        steps: TV2_TWO_TRANSACTIONS,
        expected: &[
            ((5, 0, 0), Decision::Append),
            ((5, 2, 0), Decision::OutOfOrder),
            ((5, 5, 0), Decision::OutOfOrder),
            ((4, 0, 1), Decision::Fenced),
        ],
    },
    Case {
        name: "transaction version 1 commit keeps the epoch and the sequence",
        steps: TV1_COMMIT,
        expected: &[
            ((3, 3, 0), Decision::Append),
            ((3, 0, 2), Decision::Duplicate { base_offset: 0 }),
            ((3, 0, 0), Decision::OutOfOrder),
            ((4, 0, 0), Decision::Append),
            ((4, 3, 0), Decision::OutOfOrder),
            ((2, 3, 0), Decision::Fenced),
        ],
    },
    Case {
        name: "idempotent producer epoch bump restarts at 0",
        steps: IDEMPOTENT_EPOCH_BUMP,
        expected: &[
            ((1, 2, 0), Decision::Append),
            ((1, 0, 1), Decision::Duplicate { base_offset: 3 }),
            ((1, 5, 0), Decision::OutOfOrder),
            ((2, 0, 0), Decision::Append),
            ((2, 2, 0), Decision::OutOfOrder),
            ((0, 3, 0), Decision::Fenced),
        ],
    },
];

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_and_recovered_sequence_decisions_agree() {
    for case in CASES {
        let (live, recovered) = live_and_recovered(case).await;
        assert!(live == case.expected, "live decisions: {}", case.name);
        assert!(
            recovered == case.expected,
            "recovered decisions: {}",
            case.name
        );
    }
}
