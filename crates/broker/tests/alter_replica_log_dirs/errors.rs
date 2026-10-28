//! The rejection paths of `AlterReplicaLogDirs`: a target that is not a
//! configured `log.dirs` entry, a replica this broker does not host, and a
//! principal without Cluster Alter.
//!
//! Each scenario asserts the Kafka error code per requested partition, because
//! that per-partition row is what the JVM admin client reads.

use {
    crate::{
        ReplicaDirectoryScenario, replica_directory,
        support::{acl::use_simple_acl_authorizer, storage::two_dir_config},
    },
    assert2::assert,
    krabka_broker::Broker,
    std::collections::HashSet,
};
#[rstest_bdd_macros::given(
    "a two-log-directory broker grants no principal Cluster Alter and hosts no requested topic"
)]
async fn denied_directory_move_broker(replica_directory: &mut ReplicaDirectoryScenario) {
    let primary = tempfile::tempdir().expect("primary directory");
    let extra = tempfile::tempdir().expect("extra directory");
    let mut config = two_dir_config(primary.path(), extra.path());
    config.super_users = HashSet::new();
    use_simple_acl_authorizer(&mut config);
    replica_directory.handle = Some(Broker::start(config).await.expect("broker start"));
    replica_directory.primary = Some(primary);
    replica_directory.extra = Some(extra);
}
#[rstest_bdd_macros::then(
    "the move response returns exactly these partition error codes for the requested topic"
)]
async fn refused_directory_move(
    replica_directory: &mut ReplicaDirectoryScenario,
    datatable: Vec<Vec<String>>,
) {
    let topic = replica_directory
        .response
        .as_ref()
        .expect("move response")
        .results
        .iter()
        .find(|row| row.topic_name == replica_directory.topic)
        .expect("requested topic response");
    let actual = topic
        .partitions
        .iter()
        .map(|row| (row.partition_index, row.error_code))
        .collect::<Vec<_>>();
    let expected = datatable
        .into_iter()
        .skip(1)
        .map(|cells| {
            (
                cells[0].parse().expect("partition index"),
                cells[1].parse().expect("partition error"),
            )
        })
        .collect::<Vec<_>>();
    assert!(actual == expected);
    replica_directory.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_errors.feature",
    name = "Unknown destination directories are refused"
)]
#[tokio::test]
async fn alter_replica_log_dirs_rejects_unknown_target(
    replica_directory: ReplicaDirectoryScenario,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_errors.feature",
    name = "Configured destinations cannot move unhosted replicas"
)]
#[tokio::test]
async fn alter_replica_log_dirs_rejects_unknown_replica(
    replica_directory: ReplicaDirectoryScenario,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_errors.feature",
    name = "Cluster Alter authorization precedes replica lookup"
)]
#[tokio::test]
async fn alter_replica_log_dirs_denied_without_cluster_alter(
    replica_directory: ReplicaDirectoryScenario,
) {
}
