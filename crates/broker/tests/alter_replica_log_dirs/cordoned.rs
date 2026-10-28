//! KIP-1066 cordoned log directories over the wire, on the two-directory
//! broker: `cordoned.log.dirs` set through `IncrementalAlterConfigs`, the
//! `DescribeLogDirs` v5 `is_cordoned` flag, and the `AlterReplicaLogDirs`
//! refusal of a cordoned destination.
//!
//! Each expectation is what `apache/kafka:4.3.1` answers with
//! `log.dirs=/tmp/d1,/tmp/d2`: `kafka-log-dirs --describe` shows the flag,
//! and `kafka-reassign-partitions` moving a replica into a cordoned directory
//! fails with `InvalidReplicaAssignmentException` (39).

use {
    crate::wire::{ConfigAlterOutcome, describe_log_dirs_at, set_broker_config},
    assert2::{assert, check},
    krabka_protocol::{
        UnknownTaggedFields,
        owned::alter_replica_log_dirs_response::{
            AlterReplicaLogDirPartitionResult, AlterReplicaLogDirTopicResult,
            AlterReplicaLogDirsResponse,
        },
    },
    std::{
        fs,
        net::SocketAddr,
        path::{Path, PathBuf},
        time::{Duration, Instant},
    },
    tokio::time,
};

/// One directory `DescribeLogDirs` reports, and whether it is cordoned.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct DirFlag {
    log_dir: PathBuf,
    is_cordoned: bool,
}

impl DirFlag {
    fn new(log_dir: PathBuf, is_cordoned: bool) -> Self {
        Self {
            log_dir,
            is_cordoned,
        }
    }
}

/// The flag of every directory `DescribeLogDirs` reports at `version`,
/// canonicalized so a symbolic-link temp root compares equal.
async fn cordoned_flags(addr: SocketAddr, version: i16) -> Vec<DirFlag> {
    let mut flags: Vec<_> = describe_log_dirs_at(addr, version)
        .await
        .results
        .into_iter()
        .map(|result| {
            let dir = fs::canonicalize(&result.log_dir)
                .unwrap_or_else(|_| PathBuf::from(&result.log_dir));
            DirFlag::new(dir, result.is_cordoned)
        })
        .collect();
    flags.sort();
    flags
}

/// Wait until `DescribeLogDirs` v5 reports `want`. The dynamic config reaches
/// the broker's cordoned set through the metadata image, so the flag follows
/// the committed alter by an image publication.
async fn wait_for_flags(addr: SocketAddr, want: &[DirFlag]) {
    let mut want = want.to_vec();
    want.sort();
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let got = cordoned_flags(addr, 5).await;
        if got == want {
            return;
        }
        assert!(Instant::now() <= deadline, "flags never converged: {got:?}");
        // intentional: the cordoned set is broker-local state behind an image
        // watcher, and `DescribeLogDirs` is its only wire observable.
        time::sleep(Duration::from_millis(50)).await;
    }
}

fn canonical(dir: &Path) -> PathBuf { fs::canonicalize(dir).unwrap() }

fn move_result(topic: &str, error_code: i16) -> AlterReplicaLogDirsResponse {
    AlterReplicaLogDirsResponse {
        throttle_time_ms: 0,
        results: vec![AlterReplicaLogDirTopicResult {
            topic_name: topic.to_owned(),
            partitions: vec![AlterReplicaLogDirPartitionResult {
                partition_index: 0,
                error_code,
                unknown_tagged_fields: UnknownTaggedFields(vec![]),
            }],
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        }],
        unknown_tagged_fields: UnknownTaggedFields(vec![]),
    }
}

use crate::{ReplicaDirectoryScenario, replica_directory};
#[rstest_bdd_macros::given("both configured log directories initially report uncordoned")]
async fn initially_uncordoned_directories(replica_directory: &ReplicaDirectoryScenario) {
    wait_for_flags(replica_directory.address(), &[
        DirFlag::new(canonical(replica_directory.primary()), false),
        DirFlag::new(canonical(replica_directory.extra()), false),
    ])
    .await;
}
#[rstest_bdd_macros::when(
    "broker 1 sets cordoned.log.dirs to \"{value}\" through IncrementalAlterConfigs"
)]
async fn alter_cordoned_replica_directories(
    replica_directory: &mut ReplicaDirectoryScenario,
    value: String,
) {
    let value = if value == "extra configured path" {
        replica_directory.extra().display().to_string()
    } else if value == "<empty>" {
        String::new()
    } else {
        value
    };
    replica_directory.config_result =
        Some(set_broker_config(replica_directory.address(), 1, "cordoned.log.dirs", &value).await);
}
#[rstest_bdd_macros::then(
    "the alter succeeds and DescribeLogDirs v5 reports primary uncordoned and extra cordoned {cordoned}"
)]
async fn described_cordoned_replica_directory(
    replica_directory: &ReplicaDirectoryScenario,
    cordoned: bool,
) {
    assert!(
        replica_directory
            .config_result
            .as_ref()
            .expect("config alter response")
            == &ConfigAlterOutcome::accepted()
    );
    wait_for_flags(replica_directory.address(), &[
        DirFlag::new(canonical(replica_directory.primary()), false),
        DirFlag::new(canonical(replica_directory.extra()), cordoned),
    ])
    .await;
}
#[rstest_bdd_macros::then(
    "DescribeLogDirs v4 omits cordoning and reports both directories uncordoned"
)]
async fn legacy_directory_cordon_flags(replica_directory: &ReplicaDirectoryScenario) {
    let actual = cordoned_flags(replica_directory.address(), 4).await;
    let mut expected = vec![
        DirFlag::new(canonical(replica_directory.primary()), false),
        DirFlag::new(canonical(replica_directory.extra()), false),
    ];
    expected.sort();
    check!(actual == expected);
}
#[rstest_bdd_macros::then("the complete replica move response has partition 0 error code {code}")]
async fn cordoned_destination_move_response(
    replica_directory: &mut ReplicaDirectoryScenario,
    code: i16,
) {
    check!(replica_directory.response.as_ref().expect("move response") == &move_result("t", code));
    if code == 0 {
        replica_directory.shutdown().await;
    }
}
#[rstest_bdd_macros::then("cordoning refuses with code 42 and message \"{message}\"")]
async fn unknown_cordoned_directory_response(
    replica_directory: &mut ReplicaDirectoryScenario,
    message: String,
) {
    check!(
        replica_directory
            .config_result
            .as_ref()
            .expect("config refusal")
            == &ConfigAlterOutcome {
                error_code: 42,
                error_message: Some(message)
            }
    );
    replica_directory.shutdown().await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_cordoning.feature",
    name = "Cordoning is versioned in descriptions and blocks moves until removed"
)]
#[tokio::test]
async fn a_cordoned_dir_is_flagged_and_refuses_replica_moves(
    replica_directory: ReplicaDirectoryScenario,
) {
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/storage/replica_directory_cordoning.feature",
    name = "Cordoning cannot name an unconfigured directory"
)]
#[tokio::test]
async fn an_unknown_cordoned_dir_is_refused(replica_directory: ReplicaDirectoryScenario) {}
