#![cfg(test)]

//! KIP-113: `AlterReplicaLogDirs` (`api_key` 34) end-to-end.
//!
//! The test starts a single broker with two `log.dirs`, creates a
//! 2-partition topic, then sends `AlterReplicaLogDirs` to move both
//! partitions into the second directory. It asserts that:
//!   1. the partition directories migrate on disk to the target dir,
//!   2. `DescribeLogDirs` polls converge with `is_future_key = false`
//!      in the target dir for both partitions,
//!   3. an invalid target and a missing replica return the correct Kafka
//!      error codes.
//!
//! # Module layout
//!
//! This file is the test-binary root and holds the module wiring alone. The
//! `wire` child holds the request drivers, `harness` the two-directory broker
//! and the on-disk and `DescribeLogDirs` readings the scenarios make of it,
//! and `moves`, `errors` and `startup` one scenario group each.

mod kafka_wire;
// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `alter_replica_log_dirs/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
mod support;
#[path = "support/two_dir_topic.rs"]
mod two_dir_topic;

#[path = "alter_replica_log_dirs/cordoned.rs"]
mod cordoned;
#[path = "alter_replica_log_dirs/errors.rs"]
mod errors;
#[path = "alter_replica_log_dirs/harness.rs"]
mod harness;
#[path = "alter_replica_log_dirs/moves.rs"]
mod moves;
#[path = "alter_replica_log_dirs/startup.rs"]
mod startup;
#[path = "alter_replica_log_dirs/wire.rs"]
mod wire;

#[derive(Default)]
struct ReplicaDirectoryScenario {
    handle: Option<krabka_broker::BrokerHandle>,
    primary: Option<tempfile::TempDir>,
    extra: Option<tempfile::TempDir>,
    unconfigured: Option<tempfile::TempDir>,
    target: PathBuf,
    source: PathBuf,
    future: PathBuf,
    topic: String,
    response: Option<AlterReplicaLogDirsResponse>,
    repeated: Option<AlterReplicaLogDirsResponse>,
    config_result: Option<wire::ConfigAlterOutcome>,
    described: Option<DescribeLogDirsResponse>,
    records: Vec<String>,
}
#[rstest::fixture]
fn replica_directory() -> ReplicaDirectoryScenario {
    let handle = None;
    ReplicaDirectoryScenario {
        handle,
        ..Default::default()
    }
}
impl ReplicaDirectoryScenario {
    fn primary(&self) -> &Path { self.primary.as_ref().expect("primary log directory").path() }

    fn extra(&self) -> &Path { self.extra.as_ref().expect("extra log directory").path() }

    fn address(&self) -> SocketAddr {
        self.handle
            .as_ref()
            .expect("two-directory broker")
            .listen_addr()
    }

    async fn shutdown(&mut self) {
        self.handle
            .take()
            .expect("two-directory broker")
            .shutdown()
            .await;
    }
}
#[rstest_bdd_macros::given("a two-log-directory broker hosts topic t with {partitions} partitions")]
async fn hosted_directory_move_topic(
    replica_directory: &mut ReplicaDirectoryScenario,
    partitions: i32,
) {
    let (handle, primary, extra, _) = two_dir_topic::start()
        .client_id(wire::CLIENT_ID)
        .partitions(partitions)
        .call()
        .await;
    replica_directory.handle = Some(handle);
    replica_directory.primary = Some(primary);
    replica_directory.extra = Some(extra);
}
#[rstest_bdd_macros::given("a two-log-directory broker hosts no requested topic")]
async fn empty_directory_move_broker(replica_directory: &mut ReplicaDirectoryScenario) {
    let (handle, primary, extra, _) = harness::start_two_dir_broker().await;
    replica_directory.handle = Some(handle);
    replica_directory.primary = Some(primary);
    replica_directory.extra = Some(extra);
}
#[rstest_bdd_macros::when(
    "AlterReplicaLogDirs moves topic {topic} partitions {partitions} to the {destination} directory"
)]
async fn request_replica_directory_move(
    replica_directory: &mut ReplicaDirectoryScenario,
    topic: String,
    partitions: String,
    destination: String,
) {
    replica_directory.target = match destination.as_str() {
        "extra" => replica_directory.extra().to_path_buf(),
        "unconfigured" => {
            let directory = tempfile::tempdir().expect("unconfigured destination");
            let path = directory.path().to_path_buf();
            replica_directory.unconfigured = Some(directory);
            path
        }
        "opposite" => {
            if replica_directory.primary().join("t-0").exists() {
                replica_directory.extra().to_path_buf()
            } else {
                replica_directory.primary().to_path_buf()
            }
        }
        other => panic!("unknown destination resource {other}"),
    };
    replica_directory.source = if replica_directory.target == replica_directory.primary() {
        replica_directory.extra().to_path_buf()
    } else {
        replica_directory.primary().to_path_buf()
    };
    let partitions = partitions
        .split(',')
        .map(|partition| partition.parse().expect("partition index"))
        .collect();
    replica_directory.response = Some(
        wire::alter_replica_log_dirs(
            replica_directory.address(),
            &replica_directory.target,
            &topic,
            partitions,
        )
        .await,
    );
    replica_directory.topic = topic;
}

use {
    krabka_protocol::owned::{
        alter_replica_log_dirs_response::AlterReplicaLogDirsResponse,
        describe_log_dirs_response::DescribeLogDirsResponse,
    },
    std::{
        net::SocketAddr,
        path::{Path, PathBuf},
    },
};
