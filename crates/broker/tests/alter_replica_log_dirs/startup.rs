//! What `Broker::start` does with a `<topic>-<partition>-future` directory it
//! finds on disk: resume the move when the real partition still exists, and
//! sweep the directory away when it does not.
//!
//! Both scenarios plant the future directory themselves rather than crashing a
//! broker mid-move, because the planted directory is exactly the state a crash
//! between the rename and the catch-up leaves behind.

use {
    crate::{
        ReplicaDirectoryScenario,
        harness::{count_topic_dirs, wait_for_move_complete},
        replica_directory,
        support::{
            producer::enqueue_unkeyed_values,
            storage::{start_two_dir_broker, two_dir_config},
        },
        two_dir_topic::{Setup, create_and_wait},
        wire::{CLIENT_ID, describe_log_dirs},
    },
    assert2::{assert, check},
    bytes::Bytes,
    krabka_broker::Broker,
    krabka_client_producer::Producer,
    std::fs,
};
#[rstest_bdd_macros::given(
    "a stopped two-directory broker stores t partition 0 with records v0 through v4 and an empty t-0-future directory exists in the other log directory"
)]
async fn interrupted_replica_directory_move(replica_directory: &mut ReplicaDirectoryScenario) {
    // First boot: create topic, produce a handful of records, then
    // shut down cleanly so the partition directory is left on disk.
    let (handle, primary, extra, addr) = start_two_dir_broker().await;
    create_and_wait(&handle, addr, Setup {
        client_id: CLIENT_ID,
        ..Default::default()
    })
    .await;

    {
        let producer = Producer::builder()
            .bootstrap(addr.to_string())
            .build()
            .await
            .expect("producer");
        enqueue_unkeyed_values(
            &producer,
            "t",
            (0..5i32).map(|i| Bytes::from(format!("v{i}"))),
        )
        .await;
        producer.flush().await.expect("flush");
        producer.close().await
    }
    .expect("producer close");

    // Find which dir holds `t-0` and pick the OTHER as the target.
    let primary_has_0 = primary.path().join("t-0").exists();
    let (current_dir, target_dir) = if primary_has_0 {
        (primary.path(), extra.path())
    } else {
        (extra.path(), primary.path())
    };
    handle.shutdown().await;

    // Plant an empty future dir to simulate a crash mid-ARLD before
    // the move task got a chance to copy anything. On restart, the
    // broker discovers it and resumes the move, copying the
    // already-produced batches into it.
    let future_path = target_dir.join("t-0-future");
    fs::create_dir_all(&future_path).expect("plant future dir");
    assert!(future_path.exists());
    assert!(
        current_dir.join("t-0").exists(),
        "source must still be here"
    );

    replica_directory.target = target_dir.to_path_buf();
    replica_directory.source = current_dir.to_path_buf();
    replica_directory.future = future_path;
    replica_directory.primary = Some(primary);
    replica_directory.extra = Some(extra);
}
#[rstest_bdd_macros::given(
    "ghost was never created but ghost-0-future exists in the extra configured log directory"
)]
fn orphaned_replica_future_directory(replica_directory: &mut ReplicaDirectoryScenario) {
    let primary = tempfile::tempdir().expect("primary directory");
    let extra = tempfile::tempdir().expect("extra directory");
    let future = extra.path().join("ghost-0-future");
    fs::create_dir_all(&future).expect("plant orphan future directory");
    assert!(future.exists());
    replica_directory.future = future;
    replica_directory.primary = Some(primary);
    replica_directory.extra = Some(extra);
}
#[rstest_bdd_macros::when("the broker starts against both log directories in {mode} mode")]
async fn start_with_replica_future_directories(
    replica_directory: &mut ReplicaDirectoryScenario,
    mode: String,
) {
    let mut config = two_dir_config(replica_directory.primary(), replica_directory.extra());
    match mode.as_str() {
        "rejoin" => config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin,
        "fresh-bootstrap" => {}
        other => panic!("unknown startup mode {other}"),
    }
    let handle = Broker::start(config).await.expect("broker start");
    replica_directory.described = Some(describe_log_dirs(handle.listen_addr()).await);
    replica_directory.handle = Some(handle);
}
#[rstest_bdd_macros::then(
    "the interrupted move completes in the target directory, the source has no t partitions and t-0-future is renamed away"
)]
async fn resumed_replica_directory_move(replica_directory: &mut ReplicaDirectoryScenario) {
    wait_for_move_complete(
        replica_directory.address(),
        &replica_directory.target,
        "t",
        &[0],
    )
    .await;
    check!(count_topic_dirs(&replica_directory.target, "t") == 1);
    check!(count_topic_dirs(&replica_directory.source, "t") == 0);
    check!(
        !replica_directory.future.exists(),
        "future directory is renamed away"
    );
    replica_directory.shutdown().await;
}
#[rstest_bdd_macros::then(
    "startup removes ghost-0-future and DescribeLogDirs reports no future entries"
)]
async fn swept_orphan_replica_directory(replica_directory: &mut ReplicaDirectoryScenario) {
    assert!(
        !replica_directory.future.exists(),
        "orphan future directory remains: {}",
        replica_directory.future.display()
    );
    let response = replica_directory
        .described
        .as_ref()
        .expect("startup directory description");
    assert!(
        !response
            .results
            .iter()
            .flat_map(|result| result.topics.iter())
            .flat_map(|topic| topic.partitions.iter())
            .any(|partition| partition.is_future_key)
    );
    replica_directory.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_startup.feature",
    name = "Restart resumes a planted future directory for an existing partition"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_resumes_move_for_existing_partition(replica_directory: ReplicaDirectoryScenario) {}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_startup.feature",
    name = "Startup removes future directories for topics that never existed"
)]
#[tokio::test]
async fn startup_cleans_up_stranded_future_dir(replica_directory: ReplicaDirectoryScenario) {}
