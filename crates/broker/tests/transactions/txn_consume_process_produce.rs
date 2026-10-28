//! Atomic output and input-offset enrollment in a consume-process-produce transaction.

use {
    crate::{
        support::producer::{default_producer, transactional_producer},
        txn_harness::{boot_single, create_topic, rec, send_ok},
    },
    assert2::assert,
    krabka_broker::BrokerHandle,
    krabka_client_consumer::{AutoOffsetReset, Consumer, IsolationLevel},
    krabka_client_producer::{OwnedTransaction, Producer},
    krabka_units::millis,
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tempfile::TempDir,
};

#[derive(Default)]
struct ProcessingTransaction {
    broker: Option<BrokerHandle>,
    directory: Option<TempDir>,
    bootstrap: String,
    producer: Option<Arc<Producer>>,
    transaction: Option<OwnedTransaction>,
    input: Option<Consumer>,
    output: Option<Consumer>,
    last_offset: Option<((String, i32), i64)>,
    read: usize,
    seen: usize,
}

#[fixture]
fn processing() -> ProcessingTransaction {
    // Each scenario owns fresh services and observations.
    ProcessingTransaction {
        broker: None,
        ..Default::default()
    }
}

#[given("input contains i0 through i4 and transactional output is empty")]
async fn seeded_input(processing: &mut ProcessingTransaction) {
    let (broker, bootstrap, directory) = boot_single().await;
    create_topic(&bootstrap, "input").await;
    create_topic(&bootstrap, "output").await;
    {
        let producer = default_producer(bootstrap.clone()).await;
        for value in ["i0", "i1", "i2", "i3", "i4"] {
            send_ok(&producer, rec("input", value)).await;
        }
        producer.flush().await.unwrap();
        producer.close().await
    }
    .unwrap();
    processing.broker = Some(broker);
    processing.directory = Some(directory);
    processing.bootstrap = bootstrap;
}

#[when("group cpp-g transforms its input records into output within one open transaction")]
async fn process_input(processing: &mut ProcessingTransaction) {
    let mut consumer = Consumer::builder()
        .bootstrap(processing.bootstrap.clone())
        .group_id("cpp-g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["input".to_owned()])
        .build()
        .await
        .unwrap();
    let producer = Arc::new(transactional_producer(processing.bootstrap.clone(), "cpp-tid").await);
    let transaction = Arc::clone(&producer)
        .begin_transaction_owned()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while processing.read < 5 && Instant::now() < deadline {
        for record in consumer.poll(millis(200)).await.unwrap() {
            let value = format!(
                "{}_v",
                String::from_utf8_lossy(record.value.as_deref().unwrap_or(b""))
            );
            send_ok(&producer, rec("output", &value)).await;
            processing.last_offset = Some((("input".into(), record.partition), record.offset + 1));
            processing.read += 1;
        }
    }
    processing.input = Some(consumer);
    processing.producer = Some(producer);
    processing.transaction = Some(transaction);
}

#[then("the processing transaction has consumed all five input records")]
fn all_inputs_processed(processing: &ProcessingTransaction) {
    assert!(processing.read == 5, "expected to read 5 input records");
}

#[when("the producer enrolls the input offset and commits the output transaction")]
async fn commit_output_and_offsets(processing: &mut ProcessingTransaction) {
    if let Some(offset) = processing.last_offset.take() {
        processing
            .producer
            .as_ref()
            .expect("the processing producer is running")
            .send_offsets_to_transaction(
                [offset],
                &processing
                    .input
                    .as_ref()
                    .expect("the input consumer is joined")
                    .group_metadata(),
            )
            .await
            .unwrap();
    }
    processing
        .transaction
        .take()
        .expect("the output transaction is open")
        .commit()
        .await
        .unwrap();
    processing
        .broker
        .as_ref()
        .expect("the processing broker is running")
        .wait_until_local_log_end_offset("output", 0, 5)
        .await;
    processing
        .input
        .take()
        .expect("the input consumer is joined")
        .close()
        .await
        .unwrap();
    Arc::try_unwrap(processing.producer.take().expect("the producer is running"))
        .unwrap_or_else(|_| panic!("the completed transaction released its producer"))
        .close()
        .await
        .unwrap();
}

#[when("a read_committed verifier polls output for the five transformed records")]
async fn read_committed_output(processing: &mut ProcessingTransaction) {
    let mut consumer = Consumer::builder()
        .bootstrap(processing.bootstrap.clone())
        .group_id("cpp-verify")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .isolation_level(IsolationLevel::ReadCommitted)
        .subscribe(["output".to_owned()])
        .build()
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while processing.seen < 5 && Instant::now() < deadline {
        processing.seen += consumer.poll(millis(200)).await.unwrap().len();
    }
    processing.output = Some(consumer);
}

#[then("all five output records are visible as committed data")]
fn output_is_atomic(processing: &ProcessingTransaction) {
    assert!(processing.seen == 5, "expected 5 records on output topic");
}

#[scenario(
    path = "tests/features/migrated/tests/transactions/txn_consume_process_produce.feature",
    name = "send offsets to transaction atomic with records"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_offsets_to_transaction_atomic_with_records(processing: ProcessingTransaction) {
    processing
        .output
        .take()
        .expect("the committed output was polled")
        .close()
        .await
        .unwrap();
    processing
        .broker
        .take()
        .expect("the processing broker is running")
        .shutdown()
        .await;
    drop(processing.directory.take());
    drop(processing);
}
