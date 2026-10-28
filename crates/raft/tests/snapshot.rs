//! End-to-end snapshot generation and restart recovery for a single-voter
//! controller.
//!
//! The test proves that a committed metadata change survives a manual snapshot
//! and a full process-style restart. That restart rebuilds the image from the
//! on-disk checkpoint, and not from in-memory state.
//!
//! The scope here is the manual `trigger_snapshot` path and the recovery that
//! follows it. The engine's own byte- and interval-triggered snapshotting is
//! covered in `krabka_raft::kraft::controller`, and the cross-node catch-up
//! over KIP-595 `FetchSnapshot` is covered by the engine simulator.
#![cfg(test)]

use {
    assert2::check,
    krabka_metadata::{
        FeatureLevelRecord, KRaftVersionRange, KRaftVersionRecord, MetadataImage, MetadataRecord,
        NodeId, TopicRecord, Voter, VoterEndpoint, VoterSet, VotersRecord,
    },
    krabka_raft::{
        BootstrapMode, Controller, ControllerConfig, ControllerHandle, RaftError,
        deserialize_metadata_snapshot, deserialize_metadata_snapshot_image,
        serialize_metadata_snapshot,
    },
    krabka_units::prelude::{Time, millis},
    std::{fs, path::Path, slice, time::Duration},
    tempfile::TempDir,
    tokio::time,
    uuid::Uuid,
};

krabka_macros::topic_record_fixture!(single_partition_topic);

/// Single-voter elections are instant, and a short timeout keeps each boot well
/// inside the 30-second leader deadline.
const FAST_ELECTION_TIMEOUT: Time = millis(200);

async fn wait_for_leader(controller: &ControllerHandle) {
    let mut leader_watch = controller.watch_leader();
    time::timeout(Duration::from_secs(30), async {
        while leader_watch.borrow().is_none() {
            leader_watch.changed().await.expect("leader watch closed");
        }
    })
    .await
    .expect("no leader elected within 30s");
}

// A lagging follower catching up through the real KIP-595 `FetchSnapshot` is
// covered by `kraft_engine_sim::lagging_follower_catches_up_via_snapshot`.

fn has_checkpoint(meta_dir: &Path) -> bool {
    fs::read_dir(meta_dir)
        .into_iter()
        .flatten()
        .flatten()
        .any(|e| e.file_name().to_string_lossy().ends_with(".checkpoint"))
}

use {
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, scenarios, then, when},
};

struct SnapshotCodec {
    cluster_id: Uuid,
    topic: Option<MetadataRecord>,
    image: Option<MetadataImage>,
    restored: Option<Vec<MetadataRecord>>,
    whole: Option<Vec<MetadataRecord>>,
}

#[fixture]
fn codec() -> SnapshotCodec {
    // Start this scenario with fresh inputs and observations.

    SnapshotCodec {
        cluster_id: Uuid::nil(),
        topic: None,
        image: None,
        restored: None,
        whole: None,
    }
}

#[given("a metadata image containing topic {name}")]
fn topic_image(codec: &mut SnapshotCodec, name: String) {
    codec.cluster_id = Uuid::new_v4();
    let record = MetadataRecord::V1Topic(TopicRecord {
        name,
        topic_id: Uuid::new_v4(),
        partitions: 0,
        replication_factor: 1,
    });
    codec.image = Some(MetadataImage::from_records(
        codec.cluster_id,
        slice::from_ref(&record),
    ));
    codec.topic = Some(record);
}

#[given("the image also contains KRaft version 1 and voter 3001 at controller.example port 9592")]
fn quorum_control_image(codec: &mut SnapshotCodec) {
    let controls = [
        MetadataRecord::V1KRaftVersion(KRaftVersionRecord { kraft_version: 1 }),
        MetadataRecord::V1Voters(VotersRecord {
            voters: VoterSet::from_voters([Voter {
                id: NodeId(3001),
                directory_id: Uuid::from_u128(3001),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".into(),
                    host: "controller.example".into(),
                    port: 9592,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }]),
        }),
    ];
    codec.image = Some(MetadataImage::from_records(codec.cluster_id, &[
        controls[0].clone(),
        controls[1].clone(),
        codec
            .topic
            .as_ref()
            .expect("a topic image was given")
            .clone(),
    ]));
}

#[when("the public snapshot codec serializes and decodes the image")]
fn snapshot_roundtrip(codec: &mut SnapshotCodec) {
    let bytes = serialize_metadata_snapshot(
        codec.image.as_ref().expect("a metadata image was given"),
        1_700_000_000_000,
    )
    .expect("serialize snapshot");
    codec.restored = Some(deserialize_metadata_snapshot(&bytes).expect("decode restore metadata"));
    codec.whole =
        Some(deserialize_metadata_snapshot_image(&bytes).expect("decode observer metadata"));
}

#[then("the restored topic metadata exactly matches the original image")]
fn restored_topic(codec: &SnapshotCodec) {
    let restored = codec.restored.as_ref().expect("the snapshot was decoded");
    check!(!restored.is_empty());
    check!(
        MetadataImage::from_records(codec.cluster_id, restored)
            == *codec.image.as_ref().expect("an image was given")
    );
}

#[then("observer recovery retains the quorum controls while archive restore keeps only the topic")]
fn restored_control_state(codec: &SnapshotCodec) {
    let whole = codec
        .whole
        .as_ref()
        .expect("the observer snapshot was decoded");
    let restored = codec
        .restored
        .as_ref()
        .expect("the restore snapshot was decoded");
    check!(
        MetadataImage::from_records(codec.cluster_id, whole)
            == *codec.image.as_ref().expect("an image was given")
    );
    check!(
        MetadataImage::from_records(codec.cluster_id, restored)
            == MetadataImage::from_records(codec.cluster_id, &[codec
                .topic
                .as_ref()
                .expect("a topic was given")
                .clone()])
    );
}

scenarios!("tests/features/migrated/tests/snapshot.feature", tags = "@codec", fixtures = [codec: SnapshotCodec]);

struct SnapshotRecovery {
    directory: Option<TempDir>,
    cluster_id: Uuid,
    controller: Option<ControllerHandle>,
    requested: Option<Result<(), RaftError>>,
    checkpoint_written: Option<bool>,
}

#[fixture]
fn recovery() -> SnapshotRecovery {
    // Start this scenario with fresh inputs and observations.

    SnapshotRecovery {
        directory: None,
        cluster_id: Uuid::nil(),
        controller: None,
        requested: None,
        checkpoint_written: None,
    }
}

#[given("a fresh single-voter metadata controller")]
async fn snapshot_controller(recovery: &mut SnapshotRecovery) {
    let dir = TempDir::new().unwrap();
    recovery.cluster_id = Uuid::new_v4();
    let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
    cfg.election_timeout = FAST_ELECTION_TIMEOUT;
    cfg.cluster_id = Some(recovery.cluster_id);
    cfg.bootstrap_mode = BootstrapMode::Bootstrap;
    let controller = Controller::start(cfg).await.expect("first boot start");
    wait_for_leader(&controller).await;
    recovery.directory = Some(dir);
    recovery.controller = Some(controller);
}

#[given("topic t and finalized metadata.version 25 are committed")]
async fn committed_snapshot_records(recovery: &mut SnapshotRecovery) {
    recovery
        .controller
        .as_ref()
        .expect("a controller was started")
        .submit_change(vec![
            MetadataRecord::V1Topic(single_partition_topic("t", Uuid::new_v4())),
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 25,
            }),
        ])
        .await
        .expect("submit records");
}

#[when("the operator requests a manual metadata snapshot")]
async fn trigger_snapshot(recovery: &mut SnapshotRecovery) {
    recovery.requested = Some(
        recovery
            .controller
            .as_ref()
            .expect("a controller was started")
            .trigger_snapshot()
            .await,
    );
    recovery.checkpoint_written = Some(has_checkpoint(&krabka_raft::metadata_partition_dir(
        recovery
            .directory
            .as_ref()
            .expect("a metadata directory was allocated")
            .path(),
    )));
}

#[then("the committed topic feature level and voter are present and the checkpoint exists on disk")]
fn snapshot_persisted(recovery: &SnapshotRecovery) {
    check!(
        recovery
            .requested
            .as_ref()
            .expect("a snapshot was requested")
            .is_ok()
    );
    let image = recovery
        .controller
        .as_ref()
        .expect("a controller was started")
        .current_image();
    check!(
        (
            image.topic("t").is_some(),
            image.finalized_metadata_version(),
            image.voters().contains(NodeId(1))
        ) == (true, Some(25), true)
    );
    assert2::assert!(recovery.checkpoint_written == Some(true));
}

#[when("the controller stops and rejoins from that same metadata directory")]
async fn restart_snapshot_controller(recovery: &mut SnapshotRecovery) {
    recovery
        .controller
        .take()
        .expect("the original controller was running")
        .shutdown()
        .await;
    let directory = recovery
        .directory
        .as_ref()
        .expect("a metadata directory was allocated");
    let mut cfg = ControllerConfig::for_tests(NodeId(1), directory.path().to_path_buf());
    cfg.election_timeout = FAST_ELECTION_TIMEOUT;
    cfg.cluster_id = Some(recovery.cluster_id);
    cfg.bootstrap_mode = BootstrapMode::Rejoin;
    let controller = Controller::start(cfg).await.expect("rejoin start");
    wait_for_leader(&controller).await;
    recovery.controller = Some(controller);
}

#[then("restart recovers topic t metadata.version 25 its finalized epoch and voter 1")]
fn snapshot_recovered(recovery: &SnapshotRecovery) {
    let image = recovery
        .controller
        .as_ref()
        .expect("the controller rejoined")
        .current_image();
    assert2::assert!(image.topic("t").is_some());
    check!(
        image.finalized_metadata_version() == Some(25),
        "finalized metadata.version must survive snapshot + restart"
    );
    check!(
        image.finalized_features_epoch() >= 0,
        "finalized-features epoch must survive snapshot + restart, got {}",
        image.finalized_features_epoch()
    );
    check!(
        image.voters().contains(NodeId(1)),
        "voter set must survive snapshot + restart"
    );
}

#[scenario(
    path = "tests/features/migrated/tests/snapshot.feature",
    name = "A manual snapshot survives a full controller restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_then_restart_recovers_image(recovery: SnapshotRecovery) {
    if let Some(controller) = recovery.controller.take() {
        controller.shutdown().await;
    }
}
