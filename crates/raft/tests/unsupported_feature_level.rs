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
use rstest_bdd_macros::{given, then, when};
struct FeatureRecovery {
    dir: TempDir,
    cluster: Uuid,
    before: BTreeMap<PathBuf, Vec<u8>>,
    refusal: Option<RaftError>,
    expected_refusal: &'static str,
    controller: Option<ControllerHandle>,
    fault: Option<String>,
    channel_closed: Option<bool>,
    rejected_change: Option<RaftError>,
}
#[rstest::fixture]
fn recovery() -> FeatureRecovery {
    // Start this scenario with fresh inputs and observations.

    FeatureRecovery {
        dir: TempDir::new().expect("metadata directory"),
        cluster: Uuid::new_v4(),
        before: BTreeMap::new(),
        refusal: None,
        expected_refusal: REFUSAL,
        controller: None,
        fault: None,
        channel_closed: None,
        rejected_change: None,
    }
}
#[given("a stopped controller log finalized at unstable metadata version 33")]
async fn unstable_log(recovery: &mut FeatureRecovery) {
    finalize_unstable_level(
        &recovery.dir,
        recovery.cluster,
        finalize_unstable_metadata_version(),
        vec![],
    )
    .await;
    recovery.before = protected_metadata_files(&metadata_partition_dir(recovery.dir.path()));
}
#[given("the finalized unstable log has recovered high watermark {watermark}")]
async fn watermark_log(recovery: &mut FeatureRecovery, watermark: String) {
    let watermark = match watermark.as_str() {
        "at-log-start" => HighWatermarkFile::AtLogStart,
        "missing" => HighWatermarkFile::Missing,
        "below-superseding-level" => HighWatermarkFile::BelowASupersedingLevel,
        _ => panic!("unknown high watermark condition"),
    };
    let (level, after, refusal) = match watermark {
        HighWatermarkFile::AtLogStart | HighWatermarkFile::Missing => {
            (finalize_unstable_metadata_version(), vec![], REFUSAL)
        }
        HighWatermarkFile::BelowASupersedingLevel => (
            vec![share_version(2)],
            vec![share_version(1)],
            SHARE_VERSION_REFUSAL,
        ),
    };
    let at_level = finalize_unstable_level(&recovery.dir, recovery.cluster, level, after).await;
    let path = metadata_partition_dir(recovery.dir.path()).join("high-watermark");
    match watermark {
        HighWatermarkFile::AtLogStart => fs::write(path, "0\n0\n").expect("cache log start"),
        HighWatermarkFile::Missing => fs::remove_file(path).expect("remove watermark cache"),
        HighWatermarkFile::BelowASupersedingLevel => {
            fs::write(path, at_level).expect("cache committed level");
        }
    }
    recovery.expected_refusal = refusal;
    recovery.before = protected_metadata_files(&metadata_partition_dir(recovery.dir.path()));
}
#[when("the controller rejoins with unstable features disabled")]
async fn refuse_unstable_restart(recovery: &mut FeatureRecovery) {
    recovery.refusal = Some(refused_start(&recovery.dir, recovery.cluster).await);
}
#[then(
    "startup reports the exact unsupported feature fault before changing any protected metadata file"
)]
fn unchanged_refused_restart(recovery: &FeatureRecovery) {
    let refused = recovery.refusal.as_ref().expect("restart attempted");
    check!(
        matches!(refused, RaftError::FatalFault(message) if message == recovery.expected_refusal)
    );
    check!(
        refused.to_string() == format!("Encountered fatal fault: {}", recovery.expected_refusal)
    );
    check!(!recovery.before.is_empty());
    check!(
        protected_metadata_files(&metadata_partition_dir(recovery.dir.path())) == recovery.before
    );
}
#[when("the same controller log rejoins with unstable features enabled")]
async fn restart_unstable_enabled(recovery: &mut FeatureRecovery) {
    let controller = Controller::start(
        config()
            .dir(&recovery.dir)
            .cluster_id(recovery.cluster)
            .mode(BootstrapMode::Rejoin)
            .unstable(UnstableFeatureVersions::Enabled)
            .call(),
    )
    .await
    .expect("restart with unstable features");
    wait_for_leader(&controller).await;
    recovery.controller = Some(controller);
}
#[then("the recovered controller publishes finalized metadata version 33")]
fn recovered_unstable_version(recovery: &FeatureRecovery) {
    check!(
        recovery
            .controller
            .as_ref()
            .expect("controller restarted")
            .current_image()
            .finalized_metadata_version()
            == Some(33)
    );
}
#[given("a running controller with unstable features enabled {enabled}")]
async fn live_feature_controller(recovery: &mut FeatureRecovery, enabled: bool) {
    let policy = if enabled {
        UnstableFeatureVersions::Enabled
    } else {
        UnstableFeatureVersions::Disabled
    };
    let controller = Controller::start(
        config()
            .dir(&recovery.dir)
            .cluster_id(recovery.cluster)
            .unstable(policy)
            .call(),
    )
    .await
    .expect("boot controller");
    wait_for_leader(&controller).await;
    recovery.controller = Some(controller);
}
#[then("the controller has not reported a fatal fault")]
fn no_initial_fault(recovery: &FeatureRecovery) {
    check!(
        recovery
            .controller
            .as_ref()
            .expect("controller running")
            .watch_fatal()
            .borrow()
            .is_none()
    );
}
#[when("metadata version 33 commits and subsequent submissions observe the controller stopping")]
async fn commit_unsupported_live(recovery: &mut FeatureRecovery) {
    let controller = recovery.controller.as_ref().expect("controller running");
    let fatal = controller.watch_fatal();
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("commit unsupported level");
    let deadline = Instant::now() + Duration::from_secs(30);
    for probe in 0.. {
        let topic = MetadataRecord::V1Topic(single_partition_topic(
            &format!("probe-{probe}"),
            Uuid::new_v4(),
        ));
        let submitted = time::timeout_at(
            time::Instant::from_std(deadline),
            controller.submit_change(vec![topic]),
        )
        .await
        .expect("a submission went unanswered while the controller stopped");
        if let Err(error) = submitted {
            recovery.rejected_change = Some(error);
            break;
        }
        assert!(
            Instant::now() <= deadline,
            "controller continued accepting unsupported metadata"
        );
        time::sleep(Duration::from_millis(20)).await;
    }
    recovery.fault = fatal.borrow().as_deref().map(str::to_owned);
}
#[then(
    "the stopped controller rejects changes with Shutdown and publishes the unsupported feature reason"
)]
fn stopped_feature_fault(recovery: &FeatureRecovery) {
    check!(matches!(
        recovery.rejected_change,
        Some(RaftError::Shutdown)
    ));
    check!(recovery.fault.as_deref() == Some(REFUSAL));
}
#[when("metadata version 33 commits and the supported controller shuts down normally")]
async fn normal_feature_shutdown(recovery: &mut FeatureRecovery) {
    let controller = recovery.controller.take().expect("controller running");
    controller
        .submit_change(finalize_unstable_metadata_version())
        .await
        .expect("commit supported level");
    let mut fatal = controller.watch_fatal();
    controller.shutdown().await;
    recovery.channel_closed = Some(fatal.changed().await.is_err());
    recovery.fault = fatal.borrow().as_deref().map(str::to_owned);
}
#[then("the fatal channel closes without ever reporting a fault")]
fn normal_feature_fault(recovery: &FeatureRecovery) {
    check!(recovery.channel_closed == Some(true));
    check!(recovery.fault.is_none());
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unsupported_feature_level.feature",
    name = "a log finalized at an unstable level starts only with the flag on"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_log_finalized_at_an_unstable_level_starts_only_with_the_flag_on(
    recovery: FeatureRecovery,
) {
    if let Some(controller) = recovery.controller.take() {
        controller.shutdown().await;
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unsupported_feature_level.feature",
    name = "a level above the recovered high watermark is refused before any write"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_level_above_the_recovered_high_watermark_is_refused_before_any_write(
    recovery: FeatureRecovery,
    watermark: String,
) {
    let _ = watermark;
    if let Some(controller) = recovery.controller.take() {
        controller.shutdown().await;
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unsupported_feature_level.feature",
    name = "a controller stops when it replays a level it does not support"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_stops_when_it_replays_a_level_it_does_not_support(recovery: FeatureRecovery) {
    if let Some(controller) = recovery.controller.take() {
        controller.shutdown().await;
    }
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/unsupported_feature_level.feature",
    name = "a controller that stops for any other reason reports no fault"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_controller_that_stops_for_any_other_reason_reports_no_fault(recovery: FeatureRecovery) {
    if let Some(controller) = recovery.controller.take() {
        controller.shutdown().await;
    }
}
