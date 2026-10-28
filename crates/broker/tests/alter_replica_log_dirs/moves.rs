//! The happy-path moves: `AlterReplicaLogDirs` relocates live partition
//! directories to the requested `log.dirs` entry, and the records already in
//! the log survive the relocation.
//!
//! Both scenarios pick the target directory from where the partition happens
//! to sit, because placement is least-loaded and therefore not fixed.

use {
    crate::{
        ReplicaDirectoryScenario,
        harness::{count_topic_dirs, wait_for_move_complete},
        replica_directory,
        support::producer::enqueue_unkeyed_values,
        wire::alter_replica_log_dirs,
    },
    assert2::assert,
    bytes::Bytes,
    krabka_client_consumer::{AutoOffsetReset, Consumer},
    krabka_client_producer::Producer,
    std::{
        fs,
        time::{Duration, Instant},
    },
};
#[rstest_bdd_macros::given("topic t partition 0 contains the fifty records v0 through v49")]
async fn persisted_records_before_directory_move(replica_directory: &mut ReplicaDirectoryScenario) {
    let producer = Producer::builder()
        .bootstrap(replica_directory.address().to_string())
        .build()
        .await
        .expect("producer");
    enqueue_unkeyed_values(
        &producer,
        "t",
        (0..50i32).map(|index| Bytes::from(format!("v{index}"))),
    )
    .await;
    producer.flush().await.expect("flush source records");
    producer.close().await.expect("close source producer");
    drop(producer);
}
#[rstest_bdd_macros::then(
    "partitions {partitions} acknowledge success, DescribeLogDirs places them in the destination, the source is empty and no future directory remains"
)]
async fn moved_replica_directory_state(
    replica_directory: &ReplicaDirectoryScenario,
    partitions: String,
) {
    let expected = partitions
        .split(',')
        .map(|partition| partition.parse::<i32>().expect("partition index"))
        .collect::<Vec<_>>();
    let response = replica_directory.response.as_ref().expect("move response");
    let rows = response
        .results
        .iter()
        .filter(|row| row.topic_name == "t")
        .collect::<Vec<_>>();
    assert!(rows.len() == 1, "one topic result");
    for partition in &rows[0].partitions {
        assert!(
            partition.error_code == 0,
            "partition {}: {}",
            partition.partition_index,
            partition.error_code
        );
    }
    wait_for_move_complete(
        replica_directory.address(),
        &replica_directory.target,
        "t",
        &expected,
    )
    .await;
    assert!(count_topic_dirs(&replica_directory.target, "t") == expected.len());
    assert!(count_topic_dirs(&replica_directory.source, "t") == 0);
    for directory in [replica_directory.primary(), replica_directory.extra()] {
        for entry in fs::read_dir(directory)
            .expect("directory contents")
            .flatten()
        {
            let name = entry.file_name();
            assert!(
                !name.to_string_lossy().ends_with("-future"),
                "future directory remains in {}: {name:?}",
                directory.display()
            );
        }
    }
}
#[rstest_bdd_macros::when("the client repeats the move of t partition 0 to the same destination")]
async fn repeat_completed_directory_move(replica_directory: &mut ReplicaDirectoryScenario) {
    replica_directory.repeated = Some(
        alter_replica_log_dirs(
            replica_directory.address(),
            &replica_directory.target,
            "t",
            vec![0],
        )
        .await,
    );
}
#[rstest_bdd_macros::then("the repeated directory move is an idempotent success for partition 0")]
async fn repeated_directory_move_response(replica_directory: &mut ReplicaDirectoryScenario) {
    let topic = replica_directory
        .repeated
        .as_ref()
        .expect("repeated move response")
        .results
        .iter()
        .find(|topic| topic.topic_name == "t")
        .expect("topic t");
    assert!(topic.partitions[0].error_code == 0);
    replica_directory.shutdown().await;
}
#[rstest_bdd_macros::when("a new earliest-offset consumer reads t from the destination directory")]
async fn consume_moved_partition_records(replica_directory: &mut ReplicaDirectoryScenario) {
    let mut consumer = Consumer::builder()
        .bootstrap(replica_directory.address().to_string())
        .group_id("arld-move-consumer")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .subscribe(["t".to_owned()])
        .build()
        .await
        .expect("consumer");
    let deadline = Instant::now() + Duration::from_secs(15);
    while replica_directory.records.len() < 50 && Instant::now() < deadline {
        for record in consumer
            .poll(krabka_units::millis(200))
            .await
            .expect("poll moved records")
        {
            if let Some(value) = record.value {
                replica_directory
                    .records
                    .push(String::from_utf8(value.to_vec()).expect("UTF-8 record value"));
            }
        }
    }
    consumer.close().await.expect("close moved-record consumer");
}
#[rstest_bdd_macros::then(
    "the consumer receives exactly the fifty original records v0 through v49"
)]
async fn moved_partition_record_contents(replica_directory: &mut ReplicaDirectoryScenario) {
    replica_directory.records.sort();
    let mut expected = (0..50).map(|index| format!("v{index}")).collect::<Vec<_>>();
    expected.sort();
    assert!(
        replica_directory.records == expected,
        "all records survive the move"
    );
    replica_directory.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_moves.feature",
    name = "Moving both partitions empties the source and repeated moves succeed"
)]
#[tokio::test]
async fn alter_replica_log_dirs_moves_partitions_to_target_dir(
    replica_directory: ReplicaDirectoryScenario,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_moves.feature",
    name = "A physical directory move preserves every stored record"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alter_replica_log_dirs_preserves_records_across_move(
    replica_directory: ReplicaDirectoryScenario,
) {
}
