//! The KFC-9 metric families, watched across the requests that move them.
//!
//! A metric that is defined but never fed passes every unit test, so the
//! registry gauge, the proposal-state gauge and the refusal counter are each
//! observed here over a real freeze, a real approval and a real refusal, on
//! the same cluster the audit cases use.

use {
    crate::{
        freeze_workflow::{
            Cluster, FREEZE_REASON, FROZEN_TARGET, PROPOSE_REASON, SignedFreeze, THAW_REASON,
            boot_workflow_cluster, now_ms, propose_thaw, send_signed_freeze, settle_proposal,
        },
        support::{
            self,
            topics::{creatable_topic, create_topic_request},
        },
    },
    assert2::check,
    krabka_broker::{
        codes,
        metrics::{
            BreakGlassAction as ActionLabel, BreakGlassActionLabel, BreakGlassState,
            BreakGlassStateLabel, BrokerMetrics,
        },
    },
    krabka_client_core::Client,
    krabka_metadata::BreakGlassAction,
    krabka_protocol::{
        krabka::{
            break_glass::{ApproveBreakGlassResponse, ProposeBreakGlassResponse},
            freeze::SetTopicFreezeResponse,
        },
        owned::{
            create_topics_response::CreateTopicsResponse,
            delete_topics_request::{DeleteTopicState, DeleteTopicsRequest},
            delete_topics_response::DeleteTopicsResponse,
        },
    },
};
fn proposal_counts(metrics: &BrokerMetrics) -> [i64; 3] {
    [
        BreakGlassState::Pending,
        BreakGlassState::Approved,
        BreakGlassState::Consumed,
    ]
    .map(|state| {
        metrics
            .break_glass_proposals
            .get_or_create(&BreakGlassStateLabel { state })
            .get()
    })
}
fn refusals(metrics: &BrokerMetrics, action: BreakGlassAction) -> u64 {
    metrics
        .break_glass_refusals
        .get_or_create(&BreakGlassActionLabel {
            action: ActionLabel(action),
        })
        .get()
}
struct IncidentActors {
    alice: Client,
    bob: Client,
    carol: Client,
    cluster_id: String,
}
struct FreezeMetrics {
    cluster: Option<Cluster>,
    actors: Option<IncidentActors>,
    set_at: Option<i64>,
    freeze: Option<SetTopicFreezeResponse>,
    proposal: Option<ProposeBreakGlassResponse>,
    approvals: Vec<ApproveBreakGlassResponse>,
    created: Option<CreateTopicsResponse>,
    deleted: Option<DeleteTopicsResponse>,
    refusal_before: Option<u64>,
}
#[rstest::fixture]
fn freeze_metrics() -> FreezeMetrics {
    // Start this scenario with fresh inputs and observations.

    FreezeMetrics {
        cluster: None,
        actors: None,
        set_at: None,
        freeze: None,
        proposal: None,
        approvals: Vec::new(),
        created: None,
        deleted: None,
        refusal_before: None,
    }
}
#[rstest_bdd_macros::given(
    "alice bob and carol authenticate separately to a broker with signed freeze and two person approval policy"
)]
async fn freeze_metric_actors(freeze_metrics: &mut FreezeMetrics) {
    let cluster = boot_workflow_cluster().await;
    let (alice, bob, carol, cluster_id) = cluster.workflow_clients().await;
    freeze_metrics.actors = Some(IncidentActors {
        alice,
        bob,
        carol,
        cluster_id,
    });
    freeze_metrics.cluster = Some(cluster);
}
#[rstest_bdd_macros::then("the active freeze gauge is initially zero")]
fn no_initial_freeze(freeze_metrics: &FreezeMetrics) {
    check!(
        freeze_metrics
            .cluster
            .as_ref()
            .expect("workflow broker started")
            .broker
            .metrics()
            .topic_freezes_active
            .get()
            == 0
    );
}
#[rstest_bdd_macros::when("alice signs and submits a freeze for orders")]
async fn metric_signed_freeze(freeze_metrics: &mut FreezeMetrics) {
    let cluster = freeze_metrics
        .cluster
        .as_ref()
        .expect("workflow broker started");
    let actors = freeze_metrics
        .actors
        .as_ref()
        .expect("principals authenticated");
    let set_at_ms = now_ms();
    let freeze = SignedFreeze {
        key: &cluster.alice_key,
        cluster_id: &actors.cluster_id,
        frozen: true,
        reason: FREEZE_REASON,
        proposal_id: uuid::Uuid::nil(),
        set_at_ms,
    };
    freeze_metrics.freeze = Some(send_signed_freeze(&actors.alice, &freeze).await);
    freeze_metrics.set_at = Some(set_at_ms);
}
#[rstest_bdd_macros::then("the freeze succeeds and the active freeze gauge reaches one")]
async fn metric_freeze_active(freeze_metrics: &FreezeMetrics) {
    check!(
        freeze_metrics
            .freeze
            .as_ref()
            .expect("freeze submitted")
            .error_code
            == 0
    );
    freeze_metrics
        .cluster
        .as_ref()
        .expect("workflow broker started")
        .broker
        .wait_for_metrics("the registry gauge counts the freeze", |metrics| {
            metrics.topic_freezes_active.get() == 1
        })
        .await;
}
#[rstest_bdd_macros::when("alice proposes thawing orders")]
async fn metric_propose_thaw(freeze_metrics: &mut FreezeMetrics) {
    freeze_metrics.proposal = Some(
        propose_thaw(
            &freeze_metrics
                .actors
                .as_ref()
                .expect("principals authenticated")
                .alice,
            FROZEN_TARGET,
            PROPOSE_REASON,
        )
        .await,
    );
}
#[rstest_bdd_macros::then("the proposal succeeds and the pending proposal gauge reaches one")]
async fn metric_thaw_pending(freeze_metrics: &FreezeMetrics) {
    check!(
        freeze_metrics
            .proposal
            .as_ref()
            .expect("thaw proposed")
            .error_code
            == 0
    );
    freeze_metrics
        .cluster
        .as_ref()
        .expect("workflow broker started")
        .broker
        .wait_for_metrics("the proposal counts as pending", |metrics| {
            proposal_counts(metrics)[0] == 1
        })
        .await;
}
#[rstest_bdd_macros::when("bob and carol independently approve the thaw proposal")]
async fn metric_approve_thaw(freeze_metrics: &mut FreezeMetrics) {
    let actors = freeze_metrics
        .actors
        .as_ref()
        .expect("principals authenticated");
    let proposal = freeze_metrics.proposal.as_ref().expect("thaw proposed");
    for client in [&actors.bob, &actors.carol] {
        freeze_metrics
            .approvals
            .push(settle_proposal(client, proposal.proposal_id, false).await);
    }
}
#[rstest_bdd_macros::then("both approvals succeed and the proposal moves from pending to approved")]
async fn metric_thaw_approved(freeze_metrics: &FreezeMetrics) {
    check!(freeze_metrics.approvals.len() == 2);
    for approval in &freeze_metrics.approvals {
        check!(approval.error_code == 0);
    }
    freeze_metrics
        .cluster
        .as_ref()
        .expect("workflow broker started")
        .broker
        .wait_for_metrics("two approvals move it to approved", |metrics| {
            proposal_counts(metrics)[1] == 1 && proposal_counts(metrics)[0] == 0
        })
        .await;
}
#[rstest_bdd_macros::when("alice signs and submits the thaw using the approved proposal")]
async fn metric_consume_thaw(freeze_metrics: &mut FreezeMetrics) {
    let cluster = freeze_metrics
        .cluster
        .as_ref()
        .expect("workflow broker started");
    let actors = freeze_metrics
        .actors
        .as_ref()
        .expect("principals authenticated");
    let proposal = freeze_metrics.proposal.as_ref().expect("thaw proposed");
    let thaw = SignedFreeze {
        key: &cluster.alice_key,
        cluster_id: &actors.cluster_id,
        frozen: false,
        reason: THAW_REASON,
        proposal_id: uuid::Uuid::from_bytes(proposal.proposal_id.0),
        set_at_ms: now_ms().max(freeze_metrics.set_at.expect("freeze timestamp") + 1),
    };
    freeze_metrics.freeze = Some(send_signed_freeze(&actors.alice, &thaw).await);
}
#[rstest_bdd_macros::then(
    "the thaw succeeds and clears the freeze gauge while consuming the proposal"
)]
async fn metric_thaw_consumed(freeze_metrics: &FreezeMetrics) {
    check!(
        freeze_metrics
            .freeze
            .as_ref()
            .expect("thaw submitted")
            .error_code
            == 0
    );
    freeze_metrics
        .cluster
        .as_ref()
        .expect("workflow broker started")
        .broker
        .wait_for_metrics(
            "the thaw drops the gauge and spends the proposal",
            |metrics| metrics.topic_freezes_active.get() == 0 && proposal_counts(metrics)[2] == 1,
        )
        .await;
}
#[rstest_bdd_macros::when("alice creates doomed without a deletion approval")]
async fn metric_doomed_topic(freeze_metrics: &mut FreezeMetrics) {
    freeze_metrics.created = Some(
        freeze_metrics
            .actors
            .as_ref()
            .expect("principals authenticated")
            .alice
            .send(create_topic_request(
                creatable_topic().name("doomed".into()).call(),
            ))
            .await
            .expect("CreateTopics"),
    );
}
#[rstest_bdd_macros::then("doomed is created successfully")]
fn metric_doomed_created(freeze_metrics: &FreezeMetrics) {
    check!(
        freeze_metrics
            .created
            .as_ref()
            .expect("topic creation attempted")
            .topics[0]
            .error_code
            == 0
    );
}
#[rstest_bdd_macros::when("alice attempts to delete doomed without approval")]
async fn metric_unapproved_delete(freeze_metrics: &mut FreezeMetrics) {
    freeze_metrics.refusal_before = Some(refusals(
        freeze_metrics
            .cluster
            .as_ref()
            .expect("workflow broker started")
            .broker
            .metrics(),
        BreakGlassAction::DeleteTopic,
    ));
    let req = DeleteTopicsRequest {
        topics: vec![DeleteTopicState {
            name: Some("doomed".into()),
            ..Default::default()
        }],
        topic_names: vec!["doomed".into()],
        timeout_ms: 5_000,
        ..Default::default()
    };
    freeze_metrics.deleted = Some(
        freeze_metrics
            .actors
            .as_ref()
            .expect("principals authenticated")
            .alice
            .send(req)
            .await
            .expect("DeleteTopics"),
    );
}
#[rstest_bdd_macros::then(
    "deletion is refused by policy and increments the refusal counter once with an audit record"
)]
async fn metric_refused_deletion(freeze_metrics: &FreezeMetrics) {
    check!(
        freeze_metrics
            .deleted
            .as_ref()
            .expect("deletion attempted")
            .responses[0]
            .error_code
            == codes::POLICY_VIOLATION
    );
    check!(
        refusals(
            freeze_metrics
                .cluster
                .as_ref()
                .expect("workflow broker started")
                .broker
                .metrics(),
            BreakGlassAction::DeleteTopic
        ) == freeze_metrics.refusal_before.expect("refusal baseline") + 1,
        "the refusal counter did not move"
    );
    support::wait_for_audit_record(
        &freeze_metrics
            .actors
            .as_ref()
            .expect("principals authenticated")
            .alice,
        "the refused deletion",
        |row| row["api"]["operation"] == "delete_topic.refused" && row["status_id"] == 2,
    )
    .await;
}
/// Verifies that the KFC-9 metric families move on a real request.
///
/// A metric that is defined but never fed passes every unit test, and the
/// KFC-7 suite found exactly that gap the hard way. These four series are the
/// ones an operator alerts on, so each is watched across the request that is
/// supposed to move it: the registry gauge up on the freeze and back down on
/// the thaw, and a proposal walking `pending` → `approved` → `consumed`.
///
/// The refusal counter is the fourth, and it needs a Kafka transition rather
/// than a private one: `DeleteTopics` on a gated broker with no approval is
/// refused with `POLICY_VIOLATION`, bumps `break_glass_refusals`, and writes
/// its own refused row to the audit topic. Counting the refusal and auditing
/// it are two different promises, and this asserts both of them at once.
#[cfg(test)]
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/audit/freeze_metrics.feature",
    name = "the kfc9 gauges and counters move on real requests"
)]
#[tokio::test]
async fn the_kfc9_gauges_and_counters_move_on_real_requests(freeze_metrics: FreezeMetrics) {
    freeze_metrics
        .cluster
        .expect("workflow broker started")
        .broker
        .shutdown()
        .await;
}
