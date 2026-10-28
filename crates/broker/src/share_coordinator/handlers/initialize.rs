//! `InitializeShareGroupState` (`api_key=83`). It seeds the durable share state
//! for each `(group, topic, partition)` at the requested `state_epoch` and
//! `start_offset`. It gates every partition on local leadership of that
//! partition's `__share_group_state` partition.

use {
    crate::share_coordinator::coordinator::ShareCoordinator,
    krabka_log::Offset,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        initialize_share_group_state_request::InitializeShareGroupStateRequest,
        initialize_share_group_state_response::{
            InitializeShareGroupStateResponse, InitializeStateResult, PartitionResult,
        },
    },
};

krabka_macros::share_state_handler!(
    InitializeShareGroupStateRequest,
    InitializeShareGroupStateResponse,
    InitializeStateResult,
    PartitionResult,
    initialize_state
);

/// Initializes every partition of `req`, as Kafka's
/// `ShareCoordinatorService.initializeState` does.
///
/// An empty group id or an empty topic list gets a response with no results.
async fn initialize_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: InitializeShareGroupStateRequest,
) -> InitializeShareGroupStateResponse {
    if req.group_id.is_empty() || req.topics.is_empty() {
        return InitializeShareGroupStateResponse::default();
    }
    let results = krabka_macros::share_state_results!(
        req,
        InitializeStateResult,
        |group_id, topic_id, pd| {
            let result = coordinator
                .initialize(
                    image,
                    group_id,
                    topic_id,
                    pd.partition,
                    pd.state_epoch,
                    Offset(pd.start_offset),
                )
                .await;
            let verdict = super::operation_result(result, "initialize");
            PartitionResult {
                partition: pd.partition,
                error_code: verdict.code,
                error_message: verdict.message,
                ..Default::default()
            }
        }
    );

    InitializeShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_coordinator::{
            config::ShareCoordinatorConfig,
            coordinator::{
                ShareStateSummary,
                test_support::{Logged, image_with_topic, logged_records, logged_since},
            },
            handlers::test_support::{
                StateFixture, StateResponseRow, coordinator_with, retain_leadership,
                state_response_rows,
            },
            persistence::{ShareSnapshotValue, StateBatch},
        },
        assert2::check,
        krabka_protocol::{
            owned::initialize_share_group_state_request::{InitializeStateData, PartitionData},
            primitives::uuid::Uuid as ProtoUuid,
        },
        rstest_bdd::datatable::Rows,
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([32; 16]);
    #[derive(Default)]
    struct InitializeStateStory {
        state: Option<StateFixture>,
        directory: Option<tempfile::TempDir>,
        observed: Option<InitializeShareGroupStateResponse>,
        summary: rstest_bdd::Slot<Option<ShareStateSummary>>,
        appended: Option<Vec<Logged>>,
    }
    impl InitializeStateStory {
        fn state(&self) -> &StateFixture {
            self.state.as_ref().expect("seeded initialization state")
        }
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct InitializeInput {
        group: String,
        topic: String,
        partition: i32,
        state_epoch: i32,
        start_offset: i64,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct InitializeSnapshot {
        snapshot_epoch: i32,
        state_epoch: i32,
        leader_epoch: i32,
        start_offset: i64,
        delivery_complete_count: i32,
        create_timestamp: i64,
        write_timestamp: i64,
        batches: String,
    }
    #[rstest::fixture]
    fn initialize_state_story() -> InitializeStateStory {
        // The seed, request and durable observations belong to one initialization.
        InitializeStateStory::default()
    }
    #[rstest_bdd_macros::given(
        "Share initialization uses {rules} rules at clock 1700000000000 with topic 20202020-2020-2020-2020-202020202020 having 3 partitions and group g partition 0 at state epoch 5 start 10"
    )]
    async fn initialized_seed(initialize_state_story: &mut InitializeStateStory, rules: String) {
        let trunk_rules = match rules.as_str() {
            "Kafka4.3.1" => false,
            "trunk" => true,
            unknown => panic!("unknown initialization rules {unknown}"),
        };
        initialize_state_story.directory = Some(tempfile::TempDir::new().expect("state logs"));
        initialize_state_story.state = Some(StateFixture {
            coordinator: coordinator_with(
                initialize_state_story
                    .directory
                    .as_ref()
                    .expect("logs")
                    .path(),
                ShareCoordinatorConfig {
                    trunk_rules,
                    ..ShareCoordinatorConfig::default()
                },
            ),
            image: image_with_topic(TOPIC, 3),
        });
        initialize_state_story
            .state()
            .coordinator
            .lead_all_partitions_for_test()
            .await;
        initialize_state_story
            .state()
            .coordinator
            .initialize(
                &initialize_state_story.state().image,
                "g",
                TOPIC,
                0,
                5,
                Offset(10),
            )
            .await
            .expect("seed state");
    }
    #[rstest_bdd_macros::given("the InitializeShareGroupState coordinator leadership is {led}")]
    async fn initialize_leadership(initialize_state_story: &InitializeStateStory, led: bool) {
        retain_leadership(&initialize_state_story.state().coordinator, led).await;
    }
    #[rstest_bdd_macros::when(
        "InitializeShareGroupState applies this request and observes its durable state"
    )]
    async fn issue_state_initialization(
        initialize_state_story: &mut InitializeStateStory,
        #[datatable] datatable: Rows<InitializeInput>,
    ) {
        let mut inputs = datatable.into_iter();
        let input = inputs.next().expect("one initialization request");
        assert2::assert!(inputs.next().is_none(), "exactly one request row");
        let topic = input
            .topic
            .parse::<uuid::Uuid>()
            .expect("requested topic UUID");
        let key_group = if input.group.is_empty() {
            "g"
        } else {
            &input.group
        };
        let state_partition = initialize_state_story
            .state()
            .coordinator
            .state_partition_for(key_group, &topic, input.partition)
            .unwrap();
        let before =
            logged_records(&initialize_state_story.state().coordinator, state_partition).len();
        initialize_state_story.observed = Some(
            initialize_state(
                &initialize_state_story.state().coordinator,
                &initialize_state_story.state().image,
                InitializeShareGroupStateRequest {
                    group_id: input.group.clone(),
                    topics: vec![InitializeStateData {
                        topic_id: ProtoUuid(*topic.as_bytes()),
                        partitions: vec![PartitionData {
                            partition: input.partition,
                            state_epoch: input.state_epoch,
                            start_offset: input.start_offset,
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )
            .await,
        );
        initialize_state_story.appended = Some(logged_since(
            &initialize_state_story.state().coordinator,
            state_partition,
            before,
        ));
        initialize_state_story.summary.set(
            initialize_state_story
                .state()
                .coordinator
                .read_summary(key_group, topic, input.partition)
                .await
                .ok()
                .flatten(),
        );
    }
    #[rstest_bdd_macros::then(
        "the complete InitializeShareGroupState response has these rows and default tags"
    )]
    fn initialize_response(
        initialize_state_story: &InitializeStateStory,
        #[datatable] datatable: Rows<StateResponseRow>,
    ) {
        let expected: InitializeShareGroupStateResponse = state_response_rows(datatable);
        check!(
            initialize_state_story
                .observed
                .as_ref()
                .expect("RPC completed")
                == &expected
        );
    }
    #[rstest_bdd_macros::then("the initialized Share state summary is {summary}")]
    fn initialize_summary(initialize_state_story: &InitializeStateStory, summary: String) {
        let expected: Option<(i32, i32, i64, i32)> =
            serde_json::from_str(&summary).expect("nullable state summary tuple");
        let expected = expected
            .map(|(state, leader, offset, complete)| (state, leader, Offset(offset), complete));
        check!(
            initialize_state_story
                .summary
                .get()
                .expect("summary observed")
                == expected
        );
    }
    #[rstest_bdd_macros::then("initialization appends exactly these complete snapshot records")]
    fn initialize_snapshots(
        initialize_state_story: &InitializeStateStory,
        #[datatable] datatable: Rows<InitializeSnapshot>,
    ) {
        let expected: Vec<_> = datatable
            .into_iter()
            .map(|row| {
                Logged::Snapshot(ShareSnapshotValue {
                    snapshot_epoch: row.snapshot_epoch,
                    state_epoch: row.state_epoch,
                    leader_epoch: row.leader_epoch,
                    start_offset: Offset(row.start_offset),
                    delivery_complete_count: row.delivery_complete_count,
                    create_timestamp: row.create_timestamp,
                    write_timestamp: row.write_timestamp,
                    state_batches: serde_json::from_str::<Vec<(i64, i64, i8, i16)>>(&row.batches)
                        .expect("state batch tuples")
                        .into_iter()
                        .map(|(first, last, delivery_state, delivery_count)| StateBatch {
                            first_offset: Offset(first),
                            last_offset: Offset(last),
                            delivery_state,
                            delivery_count,
                        })
                        .collect(),
                })
            })
            .collect();
        check!(
            initialize_state_story
                .appended
                .as_ref()
                .expect("appended records observed")
                == &expected
        );
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "Equal epoch and offset under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "Equal epoch with a new offset under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka1(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "An older state epoch under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka2(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "An uninitialized start offset on a new key under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka3(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A negative partition under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka4(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A negative state epoch under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka5(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A partition beyond the topic under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka6(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A topic absent from the image under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka7(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "An empty group id under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka8(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A resigned coordinator under Kafka4.3.1"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka9(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "Equal epoch and offset under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka10(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "Equal epoch with a new offset under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka11(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "An older state epoch under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka12(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "An uninitialized start offset on a new key under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka13(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A negative partition under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka14(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A negative state epoch under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka15(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A partition beyond the topic under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka16(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A topic absent from the image under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka17(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "An empty group id under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka18(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/initialize.feature",
        name = "A resigned coordinator under trunk"
    )]
    #[tokio::test]
    async fn initialize_state_answers_as_kafka19(initialize_state_story: InitializeStateStory) {
        drop(initialize_state_story);
    }

    /// An empty topic list gets a response with no results.
    #[tokio::test]
    async fn empty_topics_get_no_results() {
        let dir = tempfile::TempDir::new().expect("tempdir");
        let coordinator = super::super::test_support::coordinator(dir.path());
        let req = InitializeShareGroupStateRequest {
            group_id: "g".into(),
            ..Default::default()
        };
        let resp = initialize_state(&coordinator, &image_with_topic(TOPIC, 3), req).await;
        check!(resp == InitializeShareGroupStateResponse::default());
    }
}
