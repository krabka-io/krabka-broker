//! Cluster Describe authorization controls credential descriptions independently of super users.
use crate::scram_cluster::{ScramDescriptions, scram};

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_user_scram_credentials/scram_authorization.feature",
    name = "describe allows cluster describe acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_allows_cluster_describe_acl(scram: ScramDescriptions) { scram.shutdown().await; }

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_user_scram_credentials/scram_authorization.feature",
    name = "describe rejects without cluster describe acl"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_rejects_without_cluster_describe_acl(scram: ScramDescriptions) {
    scram.shutdown().await;
}
