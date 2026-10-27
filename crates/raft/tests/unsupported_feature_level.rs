//! A controller refuses to run at a feature level that it does not support.
//!
//! Kafka's `FeatureControlManager.replay(FeatureLevelRecord)` throws when a
//! record's level lies outside the controller's supported range, so a node
//! started with `unstable.feature.versions.enable` off never serves a log that
//! was finalized at an unstable `metadata.version`. The tests finalize
//! `4.4-IV2` (level 33), which is unstable in Kafka 4.3.
#![cfg(test)]

use {
    assert2::{assert, check},
    krabka_metadata::{FeatureLevelRecord, MetadataRecord, NodeId},
    krabka_raft::{
        BootstrapMode, Controller, ControllerConfig, ControllerHandle, RaftError,
        UnstableFeatureVersions, metadata_partition_dir,
    },
    krabka_units::prelude::{Time, millis},
    std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
        time::{Duration, Instant},
    },
    tempfile::TempDir,
    tokio::time,
    uuid::Uuid,
};

/// Single-voter elections are instant, and a short timeout keeps each boot well
/// inside the 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

krabka_macros::topic_record_fixture!(single_partition_topic);

const UNSTABLE_METADATA_VERSION: i16 = 33;

const REFUSAL: &str = "Tried to apply FeatureLevelRecord \
    FeatureLevelRecord(name='metadata.version', featureLevel=33), \
    but this controller only supports versions 7-30";

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct FeatureControllerSetup {
    cluster_id: Uuid,
    #[default(BootstrapMode::Bootstrap)]
    mode: BootstrapMode,
    #[default(UnstableFeatureVersions::Disabled)]
    unstable: UnstableFeatureVersions,
}

#[bon::builder]
fn config(
    dir: &TempDir,
    #[builder(default)] cluster_id: Uuid,
    #[builder(default = BootstrapMode :: Bootstrap)] mode: BootstrapMode,
    #[builder(default = UnstableFeatureVersions :: Disabled)] unstable: UnstableFeatureVersions,
) -> ControllerConfig {
    let setup = FeatureControllerSetup {
        cluster_id,
        mode,
        unstable,
    };
    let FeatureControllerSetup {
        cluster_id,
        mode,
        unstable,
    } = setup;
    let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
    cfg.election_timeout = FAST_ELECTION_TIMEOUT;
    cfg.cluster_id = Some(cluster_id);
    cfg.bootstrap_mode = mode;
    cfg.unstable_feature_versions = unstable;
    cfg
}

async fn wait_for_leader(controller: &ControllerHandle) {
    let mut rx = controller.watch_leader();
    time::timeout(Duration::from_secs(30), rx.wait_for(Option::is_some))
        .await
        .expect("no leader elected within 30s")
        .expect("leader watch channel closed");
}

fn finalize_unstable_metadata_version() -> Vec<MetadataRecord> {
    vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: "metadata.version".into(),
        level: UNSTABLE_METADATA_VERSION,
    })]
}

fn protected_metadata_files(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fs::read_dir(dir)
        .expect("read metadata directory")
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.is_file()
                && !matches!(
                    path.extension().and_then(|extension| extension.to_str()),
                    Some("snapshot" | "index" | "timeindex")
                )
        })
        .map(|path| {
            let bytes = fs::read(&path).expect("read protected metadata file");
            (path, bytes)
        })
        .collect()
}

/// Boot a node with the flag on, commit `level`, then commit `then`, and stop
/// it. Returns the high-watermark file as it was when `level` had committed.
async fn finalize_unstable_level(
    dir: &TempDir,
    cluster_id: Uuid,
    level: Vec<MetadataRecord>,
    then: Vec<MetadataRecord>,
) -> Vec<u8> {
    let controller = Controller::start(
        config()
            .dir(dir)
            .cluster_id(cluster_id)
            .unstable(UnstableFeatureVersions::Enabled)
            .call(),
    )
    .await
    .expect("first boot");
    wait_for_leader(&controller).await;
    controller
        .submit_change(level)
        .await
        .expect("finalize the unstable level");
    let at_level = fs::read(metadata_partition_dir(dir.path()).join("high-watermark"))
        .expect("read the high watermark the level committed under");
    if !then.is_empty() {
        controller
            .submit_change(then)
            .await
            .expect("commit the records after the level");
    }
    controller.shutdown().await;
    at_level
}

/// Start the node of `dir` with the flag off, and return its refusal.
async fn refused_start(dir: &TempDir, cluster_id: Uuid) -> RaftError {
    let Err(refused) = Controller::start(
        config()
            .dir(dir)
            .cluster_id(cluster_id)
            .mode(BootstrapMode::Rejoin)
            .call(),
    )
    .await
    else {
        panic!("a controller without the flag started over an unstable log");
    };
    refused
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_finalized_at_an_unstable_level_starts_only_with_the_flag_on() {
    let dir = TempDir::new().unwrap();
    let cluster_id = Uuid::new_v4();

    // The first boot has the flag on and finalizes the unstable level.
    finalize_unstable_level(
        &dir,
        cluster_id,
        finalize_unstable_metadata_version(),
        vec![],
    )
    .await;

    let metadata_dir = metadata_partition_dir(dir.path());
    let before = protected_metadata_files(&metadata_dir);
    assert!(!before.is_empty());

    // With the flag off the node refuses the log, with Kafka's message.
    let refused = refused_start(&dir, cluster_id).await;
    check!(matches!(&refused, RaftError::FatalFault(message) if message == REFUSAL));
    check!(refused.to_string() == format!("Encountered fatal fault: {REFUSAL}"));
    check!(protected_metadata_files(&metadata_dir) == before);

    // The refusal changed nothing on disk: the same log starts with the flag on.
    let controller = Controller::start(
        config()
            .dir(&dir)
            .cluster_id(cluster_id)
            .mode(BootstrapMode::Rejoin)
            .unstable(UnstableFeatureVersions::Enabled)
            .call(),
    )
    .await
    .expect("restart with the flag on");
    wait_for_leader(&controller).await;
    check!(controller.current_image().finalized_metadata_version() == Some(33));
    controller.shutdown().await;
}

/// What the high-watermark file says when the node restarts.
#[derive(Clone, Copy, Debug)]
enum HighWatermarkFile {
    /// The file records the log start, below the finalized level.
    AtLogStart,
    /// The file is gone, so the restart starts from the log start.
    Missing,
    /// The file records the level as committed, and an uncommitted record
    /// that lowers it to a supported level follows. The engine starts from
    /// the committed image, so that record must not hide the level from the
    /// guard.
    BelowASupersedingLevel,
}

/// A `share.version` record at `level`. Kafka 4.3's latest production level
/// is 1, and 2 is unstable. A `metadata.version` downgrade would not do here:
/// it commits with a KIP-1155 checkpoint, which a restart reads first.
fn share_version(level: i16) -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: "share.version".into(),
        level,
    })
}

const SHARE_VERSION_REFUSAL: &str = "Tried to apply FeatureLevelRecord \
    FeatureLevelRecord(name='share.version', featureLevel=2), \
    but this controller only supports versions 0-1";

/// The high-watermark file is a cache, and a restart can recover a high
/// watermark below the finalized level. The refusal must still come before
/// the engine writes anything: an engine that started would elect itself,
/// append to the log, commit the level, and only then stop.
#[yare::parameterized(
    at_log_start = { HighWatermarkFile::AtLogStart },
    missing = { HighWatermarkFile::Missing },
    below_a_superseding_level = { HighWatermarkFile::BelowASupersedingLevel },
)]
#[test_macro(tokio::test(flavor = "multi_thread", worker_threads = 2))]
async fn a_level_above_the_recovered_high_watermark_is_refused_before_any_write(
    high_watermark: HighWatermarkFile,
) {
    let dir = TempDir::new().unwrap();
    let cluster_id = Uuid::new_v4();
    let (level, then, refusal) = match high_watermark {
        HighWatermarkFile::AtLogStart | HighWatermarkFile::Missing => {
            (finalize_unstable_metadata_version(), vec![], REFUSAL)
        }
        HighWatermarkFile::BelowASupersedingLevel => (
            vec![share_version(2)],
            vec![share_version(1)],
            SHARE_VERSION_REFUSAL,
        ),
    };
    let at_level = finalize_unstable_level(&dir, cluster_id, level, then).await;

    let metadata_dir = metadata_partition_dir(dir.path());
    let high_watermark_path = metadata_dir.join("high-watermark");
    match high_watermark {
        // Version 0 of the file: the version, then the offset.
        HighWatermarkFile::AtLogStart => fs::write(&high_watermark_path, "0\n0\n").unwrap(),
        HighWatermarkFile::Missing => fs::remove_file(&high_watermark_path).unwrap(),
        HighWatermarkFile::BelowASupersedingLevel => {
            fs::write(&high_watermark_path, at_level).unwrap();
        }
    }
    let before = protected_metadata_files(&metadata_dir);

    let refused = refused_start(&dir, cluster_id).await;
    check!(matches!(&refused, RaftError::FatalFault(message) if message == refusal));
    check!(protected_metadata_files(&metadata_dir) == before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_stops_when_it_replays_a_level_it_does_not_support() {
    let dir = TempDir::new().unwrap();
    let controller = Controller::start(config().dir(&dir).cluster_id(Uuid::new_v4()).call())
        .await
        .expect("boot");
    wait_for_leader(&controller).await;
    let fatal = controller.watch_fatal();
    check!(
        fatal.borrow().is_none(),
        "no fault before the level commits"
    );

    // The level commits and applies, since the check reads the image the engine
    // publishes. Then the controller stops, and no later change is accepted.
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("commit the unsupported level");
    let deadline = Instant::now() + Duration::from_secs(30);
    for probe in 0.. {
        let topic = MetadataRecord::V1Topic(single_partition_topic(
            &format!("probe-{probe}"),
            Uuid::new_v4(),
        ));
        if let Err(error) = controller.submit_change(vec![topic]).await {
            check!(matches!(error, RaftError::Shutdown), "{error}");
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "the controller kept accepting changes at an unsupported level"
        );
        // intentional: the stop follows the image publication on another task,
        // and nothing but a refused submit says that it has happened.
        time::sleep(Duration::from_millis(20)).await;
    }
    // The refusal is on the fatal channel by the time a submit fails: the host
    // that halts over it never sees the shutdown without the reason.
    check!(fatal.borrow().as_deref() == Some(REFUSAL));
    controller.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_that_stops_for_any_other_reason_reports_no_fault() {
    let dir = TempDir::new().unwrap();
    let controller = Controller::start(
        config()
            .dir(&dir)
            .cluster_id(Uuid::new_v4())
            .unstable(UnstableFeatureVersions::Enabled)
            .call(),
    )
    .await
    .expect("boot");
    wait_for_leader(&controller).await;
    // A level within the range is not a fault.
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("commit a supported level");
    let mut fatal = controller.watch_fatal();
    controller.shutdown().await;

    // The channel closes without ever carrying a value.
    check!(fatal.changed().await.is_err());
    check!(fatal.borrow().is_none());
}
