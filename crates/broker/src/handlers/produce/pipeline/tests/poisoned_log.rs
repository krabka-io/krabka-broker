//! Tests for the size gates when a panic elsewhere has poisoned the
//! partition's log mutex.
//!
//! The writer recovers a poisoned log guard and still appends, so the gates
//! ahead of it must recover it the same way and keep refusing the batches they
//! exist to refuse.

use {
    super::{process_partition, seed_batch},
    crate::{
        codes::RECORD_LIST_TOO_LARGE,
        handlers::produce::{
            framing::PartitionPayload,
            test_support::{
                PipelineFixture, PipelinePartitionSetup, encode_batch, image_with_topic,
                pipeline_input,
            },
        },
        partition::Partition,
    },
    assert2::{assert, check},
    krabka_protocol::owned::produce_response::PartitionProduceResponse,
    std::{panic, sync::Arc, thread},
};

/// Poison the partition's log mutex the way a production panic would: a
/// thread panics while it holds the guard.
fn poison_log(partition: &Arc<Partition>) {
    let partition = Arc::clone(partition);
    let outcome = thread::spawn(move || {
        let _guard = partition.log.lock();
        panic::resume_unwind(Box::new("poison the partition log"));
    })
    .join();
    assert!(outcome.is_err());
}

#[tokio::test]
async fn the_segment_bytes_gate_refuses_an_oversize_batch_on_a_poisoned_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let image = Arc::new(image_with_topic("orders", &[1]));
    let fixture = PipelineFixture::new(krabka_ids::NodeId(1));
    // A segment smaller than any batch: the gate refuses every append.
    let partition = fixture
        .partition_with_config(dir.path(), &image, PipelinePartitionSetup {
            topic: "orders",
            log_config: krabka_log::LogConfig {
                segment_size: krabka_units::bytes(1),
                ..Default::default()
            },
        })
        .await;
    fixture.partitions.insert(
        "orders".into(),
        krabka_ids::PartitionIndex(0),
        Arc::clone(&partition),
    );
    poison_log(&partition);
    check!(partition.log.is_poisoned());

    let resp = process_partition(
        pipeline_input()
            .topic("orders")
            .payload(PartitionPayload::Slice(encode_batch(&seed_batch())))
            .call(),
        fixture.services(&image),
    )
    .await
    .expect("process partition")
    .expect_done();

    let want = PartitionProduceResponse {
        index: 0,
        error_code: RECORD_LIST_TOO_LARGE,
        base_offset: -1,
        ..Default::default()
    };
    assert!(resp == want);
    check!(partition.log_end_offset().0 == 0);
}
