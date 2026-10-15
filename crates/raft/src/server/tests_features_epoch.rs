//! The finalized-features epoch of the controller listener's `ApiVersions`
//! answer, over a live controller.
//!
//! Kafka 4.3.1's `ControllerServer` answers from its `FeaturesPublisher`. The
//! publisher starts at `FinalizedFeatures.fromKRaftVersion(MINIMUM_VERSION)`,
//! whose epoch is -1. It rebuilds its `FinalizedFeatures`, with
//! `newImage.provenance().lastContainedOffset()` as the epoch, only for a
//! metadata delta that has a `featuresDelta`. A delta has one when it replays
//! a `FeatureLevelRecord`, when it loads a snapshot, and when the loader
//! initializes the publisher with the whole image after a restart. Other
//! records do not move the epoch.

use {
    super::{
        Unstable, answer_api_versions,
        test_support::{open_engine_in, single_voter, wait_for_leader},
    },
    crate::{
        LATEST_PRODUCTION_METADATA_VERSION,
        kraft::KraftController,
        test_support::{single_partition_topic, single_replica_partition},
    },
    krabka_ids::PartitionIndex,
    krabka_metadata::{
        FeatureLevelRecord, MetadataRecord, NodeId,
        metadata_version::{METADATA_VERSION_FEATURE, METADATA_VERSION_MIN},
    },
    krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse,
        },
    },
    uuid::Uuid,
};

/// One action on the controller. Every step is followed by an `ApiVersions`
/// request whose epoch the test records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    /// Commit a batch that finalizes `metadata.version` at this level.
    MetadataVersion(i16),
    /// Commit a batch that creates a topic of this name.
    Topic(&'static str),
    /// Write a checkpoint of the applied image.
    Snapshot,
    /// Stop the controller and open a new one over the same directory, then
    /// elect it.
    Restart,
}

/// The epoch the controller listener answers with after `step`.
#[derive(Debug, PartialEq, Eq)]
struct EpochAfter {
    step: Option<Step>,
    finalized_features_epoch: i64,
}

fn api_versions_body() -> Vec<u8> {
    let mut body = bytes::BytesMut::new();
    ApiVersionsRequest {
        client_software_name: "krabka".into(),
        client_software_version: "1.0".into(),
        ..Default::default()
    }
    .encode(&mut body, 4)
    .expect("encode");
    body.to_vec()
}

/// The `finalized_features_epoch` the controller listener answers with now.
fn listener_epoch(engine: &KraftController) -> i64 {
    let body = answer_api_versions(engine, None, Unstable::default(), 4, &api_versions_body())
        .expect("ApiVersions answer");
    ApiVersionsResponse::decode(&mut &body[..], 4)
        .expect("decode")
        .finalized_features_epoch
}

/// The offset of the last record the controller has applied.
fn last_applied_offset(engine: &KraftController) -> i64 {
    engine.quorum_snapshot().high_watermark - 1
}

fn step_records(step: Step) -> Vec<MetadataRecord> {
    match step {
        Step::MetadataVersion(level) => vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
            name: METADATA_VERSION_FEATURE.into(),
            level,
        })],
        Step::Topic(name) => {
            let id = Uuid::from_u128(u128::try_from(name.len()).expect("a short name"));
            vec![
                MetadataRecord::V1Topic(single_partition_topic(name, id)),
                MetadataRecord::V1Partition(single_replica_partition(
                    name,
                    PartitionIndex(0),
                    NodeId(1),
                )),
            ]
        }
        Step::Snapshot | Step::Restart => Vec::new(),
    }
}

/// Run `steps` against a fresh single-voter controller and return the epoch
/// the listener answers with after each, together with the epoch Kafka's
/// `FeaturesPublisher` would hold at the same point.
async fn run_steps(steps: &[Step]) -> (Vec<EpochAfter>, Vec<EpochAfter>) {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut engine = open_engine_in(dir.path(), 1, single_voter());
    // The election commits a `LeaderChange` control batch, which is not a
    // features delta.
    wait_for_leader(&engine).await;
    let mut observed = vec![EpochAfter {
        step: None,
        finalized_features_epoch: listener_epoch(&engine),
    }];
    let mut expected = vec![EpochAfter {
        step: None,
        finalized_features_epoch: -1,
    }];
    let mut kafka_epoch = -1;
    let mut metadata_version_finalized = false;
    for &step in steps {
        match step {
            Step::MetadataVersion(_) => {
                engine
                    .submit_change(step_records(step))
                    .await
                    .expect("commit the feature level");
                metadata_version_finalized = true;
                kafka_epoch = last_applied_offset(&engine);
            }
            Step::Topic(_) => {
                engine
                    .submit_change(step_records(step))
                    .await
                    .expect("commit the topic");
            }
            Step::Snapshot => engine.trigger_snapshot().await.expect("snapshot"),
            Step::Restart => {
                engine.shutdown().await;
                engine = open_engine_in(dir.path(), 1, single_voter());
                // The loader initializes the publisher with the whole
                // recovered image once it has caught up: a features delta
                // whose last contained offset is the image's.
                if metadata_version_finalized {
                    kafka_epoch = last_applied_offset(&engine);
                }
                wait_for_leader(&engine).await;
            }
        }
        observed.push(EpochAfter {
            step: Some(step),
            finalized_features_epoch: listener_epoch(&engine),
        });
        expected.push(EpochAfter {
            step: Some(step),
            finalized_features_epoch: kafka_epoch,
        });
    }
    engine.shutdown().await;
    (observed, expected)
}

/// Each row is a sequence of controller actions. After every action the
/// listener's epoch is the one Kafka's `FeaturesPublisher` would hold.
#[yare::parameterized(
    fresh_controller = { &[] },
    topics_after_a_feature_level_keep_its_epoch = {
        &[
            Step::MetadataVersion(METADATA_VERSION_MIN),
            Step::Topic("a"),
            Step::Topic("bb"),
        ]
    },
    a_feature_level_at_the_same_level_moves_the_epoch = {
        &[
            Step::MetadataVersion(METADATA_VERSION_MIN),
            Step::Topic("a"),
            Step::MetadataVersion(METADATA_VERSION_MIN),
            Step::Topic("bb"),
        ]
    },
    a_feature_level_upgrade_moves_the_epoch = {
        &[
            Step::MetadataVersion(METADATA_VERSION_MIN),
            Step::Topic("a"),
            Step::MetadataVersion(LATEST_PRODUCTION_METADATA_VERSION),
        ]
    },
    topics_without_metadata_version_keep_the_initial_epoch = {
        &[Step::Topic("a"), Step::Restart, Step::Topic("bb")]
    },
    a_restart_restores_the_recovered_image_offset = {
        &[
            Step::MetadataVersion(METADATA_VERSION_MIN),
            Step::Topic("a"),
            Step::Restart,
            Step::Topic("bb"),
        ]
    },
    a_restart_from_a_checkpoint_restores_the_recovered_image_offset = {
        &[
            Step::MetadataVersion(METADATA_VERSION_MIN),
            Step::Topic("a"),
            Step::Snapshot,
            Step::Topic("bb"),
            Step::Restart,
            Step::Topic("ccc"),
            Step::MetadataVersion(METADATA_VERSION_MIN),
        ]
    },
)]
#[test_macro(tokio::test)]
async fn the_controller_listener_epoch_moves_only_on_a_features_delta(steps: &[Step]) {
    let (observed, expected) = run_steps(steps).await;

    assert2::assert!(observed == expected);
}
