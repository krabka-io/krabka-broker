//! `ReadShareGroupStateSummary` (`api_key=87`).
//!
//! This handler returns the small summary for each
//! `(group, topic, partition)` without the full state-batch list, as Kafka's
//! `ShareCoordinatorService.readStateSummary` does. The summary holds the
//! state epoch, the leader epoch, the start offset, and the delivery-complete
//! count. An empty group id, an empty topic list or a topic with no
//! partitions gets an empty response. A partition this broker does not lead
//! returns `NOT_COORDINATOR`, one that still loads
//! `COORDINATOR_LOAD_IN_PROGRESS`, a negative partition `INVALID_REQUEST`, and
//! a topic partition the metadata image lacks `UNKNOWN_TOPIC_OR_PARTITION`.
//! A key this broker leads but does not know returns Kafka's uninitialized
//! summary: start offset and delivery-complete count `-1`, epochs `0`.

use {
    crate::share_coordinator::coordinator::{
        ShareCoordinator, ShareStateError, UNINITIALIZED_START_OFFSET,
    },
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        read_share_group_state_summary_request::ReadShareGroupStateSummaryRequest,
        read_share_group_state_summary_response::{
            PartitionResult, ReadShareGroupStateSummaryResponse, ReadStateSummaryResult,
        },
    },
};

krabka_macros::share_state_handler!(
    ReadShareGroupStateSummaryRequest,
    ReadShareGroupStateSummaryResponse,
    ReadStateSummaryResult,
    PartitionResult,
    read_summaries
);

/// Serves every partition of `req`, as Kafka's
/// `ShareCoordinatorService.readStateSummary` does.
async fn read_summaries(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: ReadShareGroupStateSummaryRequest,
) -> ReadShareGroupStateSummaryResponse {
    if req.group_id.is_empty()
        || super::empty_partition_data(&req.topics, |topic| topic.partitions.is_empty())
    {
        return ReadShareGroupStateSummaryResponse::default();
    }
    let results = krabka_macros::share_state_results!(
        req,
        ReadStateSummaryResult,
        |group_id, topic_id, pd| {
            match coordinator
                .read_summary_checked(image, group_id, topic_id, pd.partition)
                .await
            {
                Ok(Some((state_epoch, leader_epoch, start_offset, delivery_complete_count))) => {
                    PartitionResult {
                        partition: pd.partition,
                        state_epoch,
                        leader_epoch,
                        start_offset: start_offset.0,
                        delivery_complete_count,
                        ..Default::default()
                    }
                }
                // Kafka's `PartitionFactory` sentinels for a key with no state.
                Ok(None) => PartitionResult {
                    partition: pd.partition,
                    state_epoch: 0,
                    leader_epoch: 0,
                    start_offset: UNINITIALIZED_START_OFFSET,
                    delivery_complete_count: UNINITIALIZED_DELIVERY_COMPLETE_COUNT,
                    ..Default::default()
                },
                // Kafka's `toErrorResponseData`: the code and the message, and
                // the schema default for every other field.
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error_message(error)),
                    ..Default::default()
                },
            }
        }
    );

    ReadShareGroupStateSummaryResponse {
        results,
        ..Default::default()
    }
}

/// Kafka's `PartitionFactory.UNINITIALIZED_DELIVERY_COMPLETE_COUNT`.
const UNINITIALIZED_DELIVERY_COMPLETE_COUNT: i32 = -1;

/// The row message: a refusal's own message, or the operation error that
/// `readStateSummary` prefixes with `Unable to read share group state summary: `.
fn error_message(error: ShareStateError) -> String {
    match error {
        ShareStateError::Refused { message, .. } => message.to_owned(),
        ShareStateError::Operation { message, .. } => {
            format!("Unable to read share group state summary: {message}")
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            PartitionResult, ReadShareGroupStateSummaryRequest, ReadShareGroupStateSummaryResponse,
            read_summaries,
        },
        crate::share_coordinator::handlers::test_support::{
            StateFixture, StateReply, group_name_literal, initialized_state, retain_leadership,
            state_response,
        },
        krabka_protocol::{
            owned::read_share_group_state_summary_request::{PartitionData, ReadStateSummaryData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([41; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([41; 16]);

    fn request(group_id: &str, partitions: &[i32]) -> ReadShareGroupStateSummaryRequest {
        krabka_macros::share_state_request! {
            ReadShareGroupStateSummaryRequest, ReadStateSummaryData;
            group [group_id];
            topic [WIRE_TOPIC];
            partitions [partitions.iter().map(|&partition| {
            PartitionData { partition, ..Default::default() }
            }).collect()];
        }
    }

    fn response(partitions: Vec<PartitionResult>) -> ReadShareGroupStateSummaryResponse {
        state_response(WIRE_TOPIC, partitions)
    }

    #[derive(Default)]
    struct SummaryStory {
        state: Option<StateFixture>,
        directory: Option<tempfile::TempDir>,
        observed: Option<ReadShareGroupStateSummaryResponse>,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct SummaryStoryRequest {
        partition: i32,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct SummaryStoryExpected {
        partition: i32,
        code: i16,
        message: String,
        state_epoch: i32,
        start_offset: i64,
        leader_epoch: i32,
        delivery_complete_count: i32,
    }
    #[rstest::fixture]
    fn summary_story() -> SummaryStory {
        // Each RPC observes independently seeded persisted state and local leadership.
        SummaryStory::default()
    }
    #[rstest_bdd_macros::given(
        "ReadShareGroupStateSummary reads topic 29292929-2929-2929-2929-292929292929 with 2 partitions and share-group partition 0 at state epoch 17 leader epoch 3 start 101 acknowledged batch 101..105 count 3 completed 9"
    )]
    async fn seed_summary_story(summary_story: &mut SummaryStory) {
        summary_story.directory = Some(tempfile::TempDir::new().expect("state logs"));
        summary_story.state = Some(
            initialized_state()
                .log_dir(summary_story.directory.as_ref().expect("state logs").path())
                .topic(TOPIC)
                .partitions(2)
                .partition(0)
                .stored_data(true)
                .call()
                .await,
        );
    }
    #[rstest_bdd_macros::given("the ReadShareGroupStateSummary coordinator leadership is {led}")]
    async fn leadership_summary_story(summary_story: &SummaryStory, led: bool) {
        retain_leadership(
            &summary_story
                .state
                .as_ref()
                .expect("seeded state")
                .coordinator,
            led,
        )
        .await;
    }
    #[rstest_bdd_macros::when(
        "ReadShareGroupStateSummary requests group {group} and these partitions"
    )]
    async fn request_summary_story(
        summary_story: &mut SummaryStory,
        group: String,
        #[datatable] datatable: rstest_bdd::datatable::Rows<SummaryStoryRequest>,
    ) {
        let group = group_name_literal(group);
        let requested: Vec<_> = datatable.into_iter().map(|row| row.partition).collect();
        summary_story.observed = Some(
            read_summaries(
                &summary_story
                    .state
                    .as_ref()
                    .expect("seeded state")
                    .coordinator,
                &summary_story.state.as_ref().expect("seeded state").image,
                request(&group, &requested),
            )
            .await,
        );
    }
    #[rstest_bdd_macros::when(
        "ReadShareGroupStateSummary requests group {group} with no topic rows"
    )]
    async fn empty_topics_summary_story(summary_story: &mut SummaryStory, group: String) {
        let group = group_name_literal(group);
        summary_story.observed = Some(
            read_summaries(
                &summary_story
                    .state
                    .as_ref()
                    .expect("seeded state")
                    .coordinator,
                &summary_story.state.as_ref().expect("seeded state").image,
                ReadShareGroupStateSummaryRequest {
                    group_id: group.into(),
                    ..Default::default()
                },
            )
            .await,
        );
    }
    #[rstest_bdd_macros::then(
        "the complete ReadShareGroupStateSummary response has these rows and default tags"
    )]
    fn response_summary_story(
        summary_story: &SummaryStory,
        #[datatable] datatable: rstest_bdd::datatable::Rows<SummaryStoryExpected>,
    ) {
        let expected: Vec<_> = datatable
            .into_iter()
            .map(|row| {
                let message = (row.message != "null").then_some(row.message);
                let mut expected = ReadShareGroupStateSummaryResponse::error_row(
                    row.partition,
                    row.code,
                    message.as_deref(),
                );
                expected.state_epoch = row.state_epoch;
                expected.start_offset = row.start_offset;
                expected.leader_epoch = row.leader_epoch;
                expected.delivery_complete_count = row.delivery_complete_count;
                expected
            })
            .collect();
        let expected = if expected.is_empty() {
            ReadShareGroupStateSummaryResponse::default()
        } else {
            response(expected)
        };
        assert2::check!(summary_story.observed.as_ref().expect("read RPC completed") == &expected);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "Persisted state is returned"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "An uninitialized key has the Kafka empty summary"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_1(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "A resigned coordinator refuses reads"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_2(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "A negative partition is refused"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_3(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "An out of range partition is unknown"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_4(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "An empty group returns no rows"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_5(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "An empty partition list returns no rows"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_6(summary_story: SummaryStory) { drop(summary_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read_summary.feature",
        name = "An empty topic list returns no rows"
    )]
    #[tokio::test]
    async fn read_summary_answers_as_kafka_7(summary_story: SummaryStory) { drop(summary_story); }
}
