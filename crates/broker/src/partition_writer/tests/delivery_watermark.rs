//! Writer-loop tests for the delivery-watermark refresh that runs after
//! every message, on both a scheduled and an immediate topic.

use assert2::check;
use krabka_log::{LogConfig, Offset};
use tempfile::tempdir;
use tokio::sync::oneshot;

use super::*;
use crate::{
    partition::{ProduceData, ProduceJob},
    partition_writer::test_support::sample_batch,
};

#[tokio::test]
async fn a_produce_to_a_scheduled_topic_refreshes_the_mirror_and_rearms_the_scheduler() {
    use crate::delivery::{
        DeliveryWaker,
        test_support::{BOUND_MS, NOW_MS, batch_at, wait_until},
    };

    let dir = tempdir().expect("tempdir");
    let config = LogConfig {
        delivery_policy: krabka_log::DeliveryPolicy::Scheduled,
        ..LogConfig::default()
    };
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), config).expect("open scheduled log"),
    ));
    let time = qubit_clock::MockTime::at(
        qubit_clock::DateTime::from_timestamp_millis(NOW_MS).expect("a representable instant"),
    );
    let delivery = DeliveryHandles::with_clock(Arc::new(time.clock()));
    // The partition is adopted, and the scheduler sleeps for a full second.
    let waker = Arc::new(DeliveryWaker::new());
    waker.arm(NOW_MS + 1_000);
    delivery.adopt(&waker);

    let (tx, rx) = mpsc::channel(1);
    let writer = tokio::spawn(run_writer!(
        "scheduled".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        Arc::new(Notify::new()),
        Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        Arc::new(Notify::new()),
        delivery.clone(),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    // A batch that is already active moves the watermark to the log end.
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS - 60_000)),
        ack,
    }))
    .await
    .expect("send the active batch");
    ack_rx.await.expect("ack").expect("append ok");
    // The writer publishes the watermark after it acks, so poll for it.
    check!(wait_until(|| delivery.watermark() == Offset(2)).await);

    // A batch that comes due inside the scheduler's sleep re-arms it.
    let woken = waker.woken();
    tokio::pin!(woken);
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS + 200)),
        ack,
    }))
    .await
    .expect("send the scheduled batch");
    ack_rx.await.expect("ack").expect("append ok");

    // The pending batch holds the watermark where it was.
    tokio::time::timeout(std::time::Duration::from_secs(1), woken)
        .await
        .expect("the produce did not re-arm the delivery scheduler");
    check!(delivery.watermark() == Offset(2));
    check!(log.lock().unwrap().log_end_offset() == Offset(4));

    // Past the activation instant, the writer's own refresh releases it.
    time.advance(std::time::Duration::from_millis(
        u64::try_from(200 + BOUND_MS).expect("positive"),
    ));
    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(batch_at(NOW_MS - 60_000)),
        ack,
    }))
    .await
    .expect("send a third batch");
    ack_rx.await.expect("ack").expect("append ok");
    check!(wait_until(|| delivery.watermark() == Offset(6)).await);

    drop(tx);
    writer.await.expect("writer join");
}

#[tokio::test]
async fn a_produce_to_an_immediate_topic_keeps_the_mirror_at_the_log_end() {
    let dir = tempdir().expect("tempdir");
    let log = Arc::new(Mutex::new(
        Log::open(dir.path(), LogConfig::default()).expect("open log"),
    ));
    let delivery = DeliveryHandles::new();
    let (tx, rx) = mpsc::channel(1);
    let writer = tokio::spawn(run_writer!(
        "immediate".to_string(),
        PartitionIndex(0),
        log.clone(),
        Arc::new(ArcSwap::from_pointee(dir.path().to_path_buf())),
        rx,
        Arc::new(Notify::new()),
        Arc::new(tokio::sync::Mutex::new(
            crate::replica_state::ReplicaState::new(),
        )),
        Arc::new(Notify::new()),
        delivery.clone(),
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(ProducerState::new()),
        None,
    ));

    let (ack, ack_rx) = oneshot::channel();
    tx.send(WriterMessage::Produce(ProduceJob {
        data: ProduceData::Owned(sample_batch(3)),
        ack,
    }))
    .await
    .expect("send job");
    ack_rx.await.expect("ack").expect("append ok");

    check!(crate::delivery::test_support::wait_until(|| delivery.watermark() == Offset(3)).await);

    drop(tx);
    writer.await.expect("writer join");
}
