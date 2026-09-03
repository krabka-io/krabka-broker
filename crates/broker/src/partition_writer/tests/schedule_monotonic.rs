//! Writer-loop tests for KFC-1 `delivery.schedule.monotonic`, which the log
//! enforces under the lock that writes the batch.
//!
//! The writer batches up to `max_produce_group` queued jobs into one
//! `append_produce_batch` call, so a rule checked anywhere above the writer is
//! a rule two jobs of one group can walk straight past. These tests drive that
//! exact shape: two produces, one group, descending delivery times.

use assert2::check;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    codes,
    delivery::test_support::{NOW_MS, batch_at},
    partition::{ProduceData, ProduceJob},
};

/// A scheduled, monotonic log.
fn monotonic_config() -> LogConfig {
    LogConfig {
        delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
        schedule_order: krabka_log::ScheduleOrder::Monotonic,
        ..LogConfig::default()
    }
}

/// Spawn the writer over `log` and hand back its sender.
///
/// Every argument but the log is the default the other writer-loop tests use;
/// nothing in this file reads a watermark, a notification or a producer state.
fn spawn_writer(
    dir: &std::path::Path,
    log: &Arc<Mutex<Log>>,
    rx: mpsc::Receiver<WriterMessage>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(run_writer!(
        "scheduled".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.to_path_buf())),
        rx,
        Arc::new(Notify::new()),
        Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        Arc::new(Notify::new()),
        DeliveryHandles::new(),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ))
}

/// Two produces whose delivery times descend, queued before the writer runs so
/// that its group drain takes both into one append call.
///
/// The later batch is admitted and the earlier one is refused with the error
/// the broker maps to `INVALID_TIMESTAMP` (32). The log holds exactly the one
/// batch that was admitted: two records, one per record of `batch_at`.
#[tokio::test]
async fn a_backwards_delivery_time_in_one_writer_group_is_refused() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), monotonic_config()).expect("open the scheduled log"),
    ));

    // Both jobs are on the queue before the writer starts, so its first
    // `recv` and the `try_recv` behind it drain them into one group.
    let (tx, rx) = mpsc::channel(2);
    let (later_ack, later) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS + 60_000)),
        ack: later_ack,
    }))
    .await
    .expect("queue the later batch");
    let (earlier_ack, earlier) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS)),
        ack: earlier_ack,
    }))
    .await
    .expect("queue the earlier batch");

    let writer = spawn_writer(dir.path(), &log, rx);

    let later = later.await.expect("ack the later batch");
    check!(later.expect("the later batch is admitted").base_offset == Offset(0));

    let earlier = earlier
        .await
        .expect("ack the earlier batch")
        .expect_err("a batch that runs the schedule backwards is refused");
    check!(matches!(
        &earlier,
        crate::error::BrokerError::Log(krabka_log::LogError::ScheduleRunsBackwards {
            delivery_ms
        }) if *delivery_ms == NOW_MS
    ));
    check!(codes::from_broker_error(&earlier) == codes::INVALID_TIMESTAMP);

    // The refusal appended nothing: the log holds the admitted batch alone.
    check!(log.lock().unwrap().log_end_offset() == Offset(2));

    drop(tx);
    writer.await.expect("writer join");
}

/// The same two produces on a scheduled topic that did not ask for the
/// setting. Both are admitted, because KFC-1 leaves a backwards schedule legal
/// by default and only reports it when an operator asks.
#[tokio::test]
async fn a_backwards_delivery_time_is_admitted_without_the_setting() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(
            dir.path(),
            LogConfig {
                schedule_order: krabka_log::ScheduleOrder::Unordered,
                ..monotonic_config()
            },
        )
        .expect("open the scheduled log"),
    ));

    let (tx, rx) = mpsc::channel(2);
    let (later_ack, later) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS + 60_000)),
        ack: later_ack,
    }))
    .await
    .expect("queue the later batch");
    let (earlier_ack, earlier) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS)),
        ack: earlier_ack,
    }))
    .await
    .expect("queue the earlier batch");

    let writer = spawn_writer(dir.path(), &log, rx);

    check!(
        later
            .await
            .expect("ack the later batch")
            .expect("the later batch is admitted")
            .base_offset
            == Offset(0)
    );
    check!(
        earlier
            .await
            .expect("ack the earlier batch")
            .expect("the earlier batch is admitted too")
            .base_offset
            == Offset(2)
    );
    check!(log.lock().unwrap().log_end_offset() == Offset(4));

    drop(tx);
    writer.await.expect("writer join");
}
