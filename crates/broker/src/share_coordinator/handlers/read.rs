//! `ReadShareGroupState` (`api_key=84`). The handler returns the durable
//! delivery state for each `(group, topic, partition)`: the start offset and
//! the state batches. A partition this broker does not lead returns
//! per-partition `NOT_COORDINATOR`, and a partition that still loads returns
//! `COORDINATOR_LOAD_IN_PROGRESS`. A key with no state returns
//! `INVALID_REQUEST`. A request leader epoch above the stored one is persisted
//! before the answer, so the older share-partition leader is fenced.

use {
    crate::share_coordinator::coordinator::ShareCoordinator,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        read_share_group_state_request::ReadShareGroupStateRequest,
        read_share_group_state_response::{
            PartitionResult, ReadShareGroupStateResponse, ReadStateResult, StateBatch,
        },
    },
};

krabka_macros::share_state_handler!(
    ReadShareGroupStateRequest,
    ReadShareGroupStateResponse,
    ReadStateResult,
    PartitionResult,
    read_state
);

/// Serves every partition of `req`, as Kafka's
/// `ShareCoordinatorService.readState` does.
///
/// An empty topic list, a topic with no partitions, or an empty group id gets
/// a response with no results.
async fn read_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: ReadShareGroupStateRequest,
) -> ReadShareGroupStateResponse {
    if super::empty_partition_data(&req.topics, |topic| topic.partitions.is_empty())
        || req.group_id.is_empty()
    {
        return ReadShareGroupStateResponse::default();
    }
    let results =
        krabka_macros::share_state_results!(req, ReadStateResult, |group_id, topic_id, pd| {
            match coordinator
                .read(image, group_id, topic_id, pd.partition, pd.leader_epoch)
                .await
            {
                Ok(st) => PartitionResult {
                    partition: pd.partition,
                    state_epoch: st.state_epoch,
                    start_offset: st.start_offset.0,
                    state_batches: st.state_batches.iter().map(StateBatch::from).collect(),
                    ..Default::default()
                },
                // Kafka's `toErrorResponseData`: the code and the message,
                // and the default for every other field.
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error.row_message("read")),
                    ..Default::default()
                },
            }
        });

    ReadShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            PartitionResult, ReadShareGroupStateRequest, ReadShareGroupStateResponse, StateBatch,
            read_state,
        },
        crate::share_coordinator::handlers::test_support::{
            StateFixture, StateReply, group_name_literal, initialized_state, retain_leadership,
            state_response,
        },
        krabka_protocol::{
            owned::read_share_group_state_request::{PartitionData, ReadStateData},
            primitives::uuid::Uuid as ProtoUuid,
        },
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([33; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([33; 16]);

    struct ReadPartition {
        partition: i32,
        leader_epoch: i32,
    }

    fn request(group_id: &str, partitions: &[ReadPartition]) -> ReadShareGroupStateRequest {
        krabka_macros::share_state_request! {
            ReadShareGroupStateRequest, ReadStateData;
            group [group_id];
            topic [WIRE_TOPIC];
            partitions [partitions.iter().map(|row| {
            PartitionData { partition: row.partition, leader_epoch: row.leader_epoch, ..Default::default() }
            }).collect()];
        }
    }

    fn response(partitions: Vec<PartitionResult>) -> ReadShareGroupStateResponse {
        state_response(WIRE_TOPIC, partitions)
    }

    #[derive(Default)]
    struct ReadStory {
        state: Option<StateFixture>,
        directory: Option<tempfile::TempDir>,
        observed: Option<ReadShareGroupStateResponse>,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ReadStoryRequest {
        partition: i32,
        leader_epoch: i32,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct ReadStoryExpected {
        partition: i32,
        code: i16,
        message: String,
        state_epoch: i32,
        start_offset: i64,
        batches: String,
    }
    #[rstest::fixture]
    fn read_story() -> ReadStory {
        // Each RPC observes independently seeded persisted state and local leadership.
        ReadStory::default()
    }
    #[rstest_bdd_macros::given(
        "ReadShareGroupState reads topic 21212121-2121-2121-2121-212121212121 with 8 partitions and share-group partition 4 at state epoch 17 leader epoch 3 start 101 acknowledged batch 101..105 count 3 completed 9"
    )]
    async fn seed_read_story(read_story: &mut ReadStory) {
        read_story.directory = Some(tempfile::TempDir::new().expect("state logs"));
        read_story.state = Some(
            initialized_state()
                .log_dir(read_story.directory.as_ref().expect("state logs").path())
                .topic(TOPIC)
                .partitions(8)
                .partition(4)
                .stored_data(true)
                .call()
                .await,
        );
    }
    #[rstest_bdd_macros::given("the ReadShareGroupState coordinator leadership is {led}")]
    async fn leadership_read_story(read_story: &ReadStory, led: bool) {
        retain_leadership(
            &read_story.state.as_ref().expect("seeded state").coordinator,
            led,
        )
        .await;
    }
    #[rstest_bdd_macros::when("ReadShareGroupState requests group {group} and these partitions")]
    async fn request_read_story(
        read_story: &mut ReadStory,
        group: String,
        #[datatable] datatable: rstest_bdd::datatable::Rows<ReadStoryRequest>,
    ) {
        let group = group_name_literal(group);
        let requested: Vec<_> = datatable
            .into_iter()
            .map(|row| ReadPartition {
                partition: row.partition,
                leader_epoch: row.leader_epoch,
            })
            .collect();
        read_story.observed = Some(
            read_state(
                &read_story.state.as_ref().expect("seeded state").coordinator,
                &read_story.state.as_ref().expect("seeded state").image,
                request(&group, &requested),
            )
            .await,
        );
    }
    #[rstest_bdd_macros::when("ReadShareGroupState requests group {group} with no topic rows")]
    async fn empty_topics_read_story(read_story: &mut ReadStory, group: String) {
        let group = group_name_literal(group);
        read_story.observed = Some(
            read_state(
                &read_story.state.as_ref().expect("seeded state").coordinator,
                &read_story.state.as_ref().expect("seeded state").image,
                ReadShareGroupStateRequest {
                    group_id: group.into(),
                    ..Default::default()
                },
            )
            .await,
        );
    }
    #[rstest_bdd_macros::then(
        "the complete ReadShareGroupState response has these rows and default tags"
    )]
    fn response_read_story(
        read_story: &ReadStory,
        #[datatable] datatable: rstest_bdd::datatable::Rows<ReadStoryExpected>,
    ) {
        let expected: Vec<_> = datatable
            .into_iter()
            .map(|row| {
                let message = (row.message != "null").then_some(row.message);
                let mut expected = ReadShareGroupStateResponse::error_row(
                    row.partition,
                    row.code,
                    message.as_deref(),
                );
                expected.state_epoch = row.state_epoch;
                expected.start_offset = row.start_offset;
                expected.state_batches =
                    serde_json::from_str::<Vec<(i64, i64, i8, i16)>>(&row.batches)
                        .expect("batch tuples")
                        .into_iter()
                        .map(
                            |(first_offset, last_offset, delivery_state, delivery_count)| {
                                StateBatch {
                                    first_offset,
                                    last_offset,
                                    delivery_state,
                                    delivery_count,
                                    ..Default::default()
                                }
                            },
                        )
                        .collect();
                expected
            })
            .collect();
        let expected = if expected.is_empty() {
            ReadShareGroupStateResponse::default()
        } else {
            response(expected)
        };
        assert2::check!(read_story.observed.as_ref().expect("read RPC completed") == &expected);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "Persisted state is returned"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka(read_story: ReadStory) { drop(read_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "An uninitialized key is refused"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka_1(read_story: ReadStory) { drop(read_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "An old leader epoch is fenced"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka_2(read_story: ReadStory) { drop(read_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "A resigned coordinator refuses reads"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka_3(read_story: ReadStory) { drop(read_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "An empty group returns no rows"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka_4(read_story: ReadStory) { drop(read_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "An empty partition list returns no rows"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka_5(read_story: ReadStory) { drop(read_story); }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/read.feature",
        name = "An empty topic list returns no rows"
    )]
    #[tokio::test]
    async fn read_state_answers_as_kafka_6(read_story: ReadStory) { drop(read_story); }
}
