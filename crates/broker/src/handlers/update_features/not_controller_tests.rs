//! The answer of an `UpdateFeatures` request whose write reaches a node that
//! is not the active controller: through the `kraft.version` upgrade, and
//! through the guarded append of the feature records.

use {
    super::{apply_updates, kraft_upgrade_refusal, preconditions::LocalController},
    crate::{
        codes,
        features::METADATA_VERSION,
        handlers::update_features::test_support::{apply_request, elr_update},
        test_support::FakeMetadataSource,
    },
    assert2::check,
    krabka_metadata::{FeatureLevelRecord, MetadataImage, MetadataRecord},
    krabka_protocol::owned::update_features_response::UpdateFeaturesResponse,
    krabka_raft::{NodeId, RaftError, ReconfigOutcome, UnstableFeatureVersions},
};

/// The whole response for a top-level `error_code` and `error_message`, with
/// no per-feature results.
fn top_level(error_code: i16, error_message: &str) -> UpdateFeaturesResponse {
    UpdateFeaturesResponse {
        error_code,
        error_message: Some(error_message.to_owned()),
        ..Default::default()
    }
}

/// The answer to a `kraft.version` upgrade that the Raft layer refused.
///
/// Kafka's `ControllerApis.handleUpdateFeatures` answers a failed
/// `updateFeatures` through `requestHelper.handleError`, and
/// `UpdateFeaturesRequest.getErrorResponse` builds
/// `UpdateFeaturesResponse.createWithErrors(ApiError.fromThrowable(e), ...)`:
/// a top-level code and message, no per-feature results. Raft's
/// `NotLeaderException` becomes `ControllerExceptions.newWrongControllerException`
/// through `EventHandlerExceptionInfo.fromInternal`, so the message names the
/// leader the refusal redirected to. The mapping reads no other leader, so the
/// rows that differ only by the leader this node knows are left out; an
/// uncommitted tail is not a leadership refusal on this path and keeps its
/// failure answer.
#[yare::parameterized(
    reconfig_not_leader = {
        Ok(ReconfigOutcome::NotLeader { leader: Some(NodeId(2)) }),
        Some(top_level(codes::NOT_CONTROLLER, "The active controller appears to be node 2.")),
    },
    reconfig_not_leader_without_hint = {
        Ok(ReconfigOutcome::NotLeader { leader: None }),
        Some(top_level(codes::NOT_CONTROLLER, "No controller appears to be active.")),
    },
    not_leader = {
        Err(RaftError::NotLeader { current_leader: Some(NodeId(2)) }),
        Some(top_level(codes::NOT_CONTROLLER, "The active controller appears to be node 2.")),
    },
    not_leader_without_hint = {
        Err(RaftError::NotLeader { current_leader: None }),
        Some(top_level(codes::NOT_CONTROLLER, "No controller appears to be active.")),
    },
    leader_unknown = {
        Err(RaftError::LeaderUnknown),
        Some(top_level(codes::NOT_CONTROLLER, "No controller appears to be active.")),
    },
    uncommitted_tail = {
        Err(RaftError::UncommittedTail),
        Some(top_level(codes::FEATURE_UPDATE_FAILED, "Failed to activate kraft.version.")),
    },
    committed = { Ok(ReconfigOutcome::Committed), None },
)]
fn a_refused_kraft_upgrade_answers_kafkas_response(
    outcome: Result<ReconfigOutcome, RaftError>,
    expected: Option<UpdateFeaturesResponse>,
) {
    check!(kraft_upgrade_refusal(outcome) == expected);
}

/// The answer to a request whose guarded append of the feature records failed
/// with `error`.
///
/// Kafka answers it as for a refused `kraft.version` upgrade: through
/// `UpdateFeaturesRequest.getErrorResponse`, with the message of
/// `ControllerExceptions.newWrongControllerException` at the top level and no
/// per-feature results. A guarded append retries `UncommittedTail` until the
/// deadline, so that row answers `REQUEST_TIMED_OUT` whatever leader this node
/// knows; the case without a known leader is the same row and is left out.
#[yare::parameterized(
    not_leader = {
        || RaftError::NotLeader { current_leader: Some(NodeId(2)) },
        top_level(codes::NOT_CONTROLLER, "The active controller appears to be node 2."),
    },
    not_leader_without_hint = {
        || RaftError::NotLeader { current_leader: None },
        top_level(codes::NOT_CONTROLLER, "No controller appears to be active."),
    },
    leader_unknown = {
        || RaftError::LeaderUnknown,
        top_level(codes::NOT_CONTROLLER, "No controller appears to be active."),
    },
    uncommitted_tail = {
        || RaftError::UncommittedTail,
        top_level(codes::REQUEST_TIMED_OUT, "The request timed out."),
    },
    shutdown = {
        || RaftError::Shutdown,
        top_level(codes::FEATURE_UPDATE_FAILED, "Failed to persist the feature update."),
    },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn a_failed_feature_append_answers_kafkas_response(
    error: fn() -> RaftError,
    expected: UpdateFeaturesResponse,
) {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
        name: METADATA_VERSION.into(),
        level: 30,
    }));
    let source = FakeMetadataSource::builder()
        .image(image)
        .leader(Some(NodeId(1)))
        .on_submit(move |_| Err(error()))
        .build();
    let local_controller = LocalController {
        node_id: NodeId(1),
        unstable_features: UnstableFeatureVersions::Disabled,
    };

    let outcome = apply_updates(
        &source,
        &apply_request(vec![elr_update(1, 1)]),
        local_controller,
        2,
    )
    .await;

    check!(outcome == Err(expected));
}
