//! `DeleteShareGroupState` (`api_key=86`). The handler tombstones the durable
//! share state for each `(group, topic, partition)` that has state and drops
//! the in-memory entry. The handler gates on local leadership of the target
//! `__share_group_state` partition.

use {
    crate::share_coordinator::coordinator::ShareCoordinator,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        delete_share_group_state_request::DeleteShareGroupStateRequest,
        delete_share_group_state_response::{
            DeleteShareGroupStateResponse, DeleteStateResult, PartitionResult,
        },
    },
};

krabka_macros::share_state_handler!(
    DeleteShareGroupStateRequest,
    DeleteShareGroupStateResponse,
    DeleteStateResult,
    PartitionResult,
    delete_state
);

/// Deletes every partition of `req`, as Kafka's
/// `ShareCoordinatorService.deleteState` does.
///
/// An empty group id, an empty topic list, or a topic with no partitions gets
/// a response with no results.
async fn delete_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: DeleteShareGroupStateRequest,
) -> DeleteShareGroupStateResponse {
    if req.group_id.is_empty()
        || super::empty_partition_data(&req.topics, |topic| topic.partitions.is_empty())
    {
        return DeleteShareGroupStateResponse::default();
    }
    let results =
        krabka_macros::share_state_results!(req, DeleteStateResult, |group_id, topic_id, pd| {
            let result = coordinator
                .delete(image, group_id, topic_id, pd.partition)
                .await;
            let verdict = super::operation_result(result, "delete");
            PartitionResult {
                partition: pd.partition,
                error_code: verdict.code,
                error_message: verdict.message,
                ..Default::default()
            }
        });

    DeleteShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_coordinator::{
            coordinator::test_support::{Logged, image_with_topic, logged_records, logged_since},
            handlers::test_support::{
                StateFixture, StateResponseRow, coordinator, group_name_literal, retain_leadership,
                state_response_rows,
            },
        },
        assert2::check,
        krabka_log::Offset,
        krabka_protocol::{
            owned::delete_share_group_state_request::{DeleteStateData, PartitionData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([31; 16]);

    fn request(
        group_id: &str,
        topic_id: uuid::Uuid,
        partitions: &[i32],
    ) -> DeleteShareGroupStateRequest {
        krabka_macros::share_state_request! {
            DeleteShareGroupStateRequest, DeleteStateData;
            group [group_id];
            topic [ProtoUuid(*topic_id.as_bytes())];
            partitions [partitions.iter().map(|&partition| {
            PartitionData { partition, ..Default::default() }
            }).collect()];
        }
    }

    #[derive(Default)]
    struct DeleteStateStory {
        state: Option<StateFixture>,
        directory: Option<tempfile::TempDir>,
        observed: Option<DeleteShareGroupStateResponse>,
        appended: Option<Vec<Logged>>,
        seeded_key_kept: Option<bool>,
    }
    impl DeleteStateStory {
        fn state(&self) -> &StateFixture { self.state.as_ref().expect("initialized state") }
    }

    #[rstest::fixture]
    fn delete_state_story() -> DeleteStateStory {
        // The request, appended records and seeded-key observations share one real log.
        DeleteStateStory::default()
    }
    #[rstest_bdd_macros::given(
        "a led Share coordinator has topic 1f1f1f1f-1f1f-1f1f-1f1f-1f1f1f1f1f1f with 2 partitions and group g partition 0 at state epoch 1 start 0"
    )]
    async fn seeded_deletion_state(delete_state_story: &mut DeleteStateStory) {
        delete_state_story.directory = Some(tempfile::TempDir::new().expect("state logs"));
        delete_state_story.state = Some(StateFixture {
            coordinator: coordinator(delete_state_story.directory.as_ref().expect("logs").path()),
            image: image_with_topic(TOPIC, 2),
        });
        delete_state_story
            .state()
            .coordinator
            .lead_all_partitions_for_test()
            .await;
        delete_state_story
            .state()
            .coordinator
            .initialize(
                &delete_state_story.state().image,
                "g",
                TOPIC,
                0,
                1,
                Offset(0),
            )
            .await
            .expect("seed share state");
    }
    #[rstest_bdd_macros::given("the DeleteShareGroupState coordinator leadership is {led}")]
    async fn deletion_leadership(delete_state_story: &DeleteStateStory, led: bool) {
        retain_leadership(&delete_state_story.state().coordinator, led).await;
    }
    #[rstest_bdd_macros::when(
        "DeleteShareGroupState requests group {group} topic {topic} partitions {partitions} and observes its appended records and seeded key"
    )]
    async fn issue_state_deletion(
        delete_state_story: &mut DeleteStateStory,
        group: String,
        topic: String,
        partitions: String,
    ) {
        let group = group_name_literal(group);
        let topic = topic.parse::<uuid::Uuid>().expect("topic UUID");
        let partitions: Vec<i32> = serde_json::from_str(&partitions).expect("partition list");
        let state_partition = delete_state_story
            .state()
            .coordinator
            .state_partition_for("g", &topic, partitions.first().copied().unwrap_or(0))
            .unwrap();
        let before = logged_records(&delete_state_story.state().coordinator, state_partition).len();
        delete_state_story.observed = Some(
            delete_state(
                &delete_state_story.state().coordinator,
                &delete_state_story.state().image,
                request(&group, topic, &partitions),
            )
            .await,
        );
        delete_state_story.appended = Some(logged_since(
            &delete_state_story.state().coordinator,
            state_partition,
            before,
        ));
        delete_state_story.seeded_key_kept = Some(
            delete_state_story
                .state()
                .coordinator
                .state_for_test("g", TOPIC, 0)
                .await
                .is_some(),
        );
    }
    #[rstest_bdd_macros::then(
        "the complete DeleteShareGroupState response has these rows and default tags"
    )]
    fn deletion_response(
        delete_state_story: &DeleteStateStory,
        #[datatable] datatable: rstest_bdd::datatable::Rows<StateResponseRow>,
    ) {
        let expected: DeleteShareGroupStateResponse = state_response_rows(datatable);
        check!(delete_state_story.observed.as_ref().expect("RPC completed") == &expected);
    }
    #[rstest_bdd_macros::then(
        "deletion appends exactly {records} and keeps seeded partition 0 state {kept}"
    )]
    fn deletion_storage_effect(delete_state_story: &DeleteStateStory, records: String, kept: bool) {
        let expected: Vec<String> = serde_json::from_str(&records).expect("record kinds");
        let expected: Vec<_> = expected
            .into_iter()
            .map(|kind| match kind.as_str() {
                "tombstone" => Logged::Tombstone,
                unknown => panic!("unknown state record {unknown}"),
            })
            .collect();
        check!(
            delete_state_story
                .appended
                .as_ref()
                .expect("appended records observed")
                == &expected
        );
        check!(delete_state_story.seeded_key_kept == Some(kept));
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "An initialized key is tombstoned"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "A missing key writes no tombstone"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka1(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "A negative partition is refused"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka2(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "A partition beyond the topic is unknown"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka3(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "A topic absent from the image is unknown"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka4(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "An empty group returns no rows"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka5(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "An empty partition list returns no rows"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka6(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/delete.feature",
        name = "A resigned coordinator refuses deletion"
    )]
    #[tokio::test]
    async fn delete_state_answers_as_kafka7(delete_state_story: DeleteStateStory) {
        drop(delete_state_story);
    }
}
