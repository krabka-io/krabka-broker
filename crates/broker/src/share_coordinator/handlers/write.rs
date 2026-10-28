//! `WriteShareGroupState` (`api_key=85`). The handler applies a delivery-state
//! delta for each `(group, topic, partition)`. The delta advances the start
//! offset, upserts state batches, and sets the delivery-complete count. It
//! keeps the stored state epoch and leader epoch. A key with no state returns
//! `INVALID_REQUEST`, and epoch fencing returns the per-partition error code.

use {
    crate::{
        codes,
        share_coordinator::{
            coordinator::{ShareCoordinator, ShareWrite},
            persistence::StateBatch,
        },
    },
    krabka_log::Offset,
    krabka_metadata::MetadataImage,
    krabka_protocol::owned::{
        write_share_group_state_request::WriteShareGroupStateRequest,
        write_share_group_state_response::{
            PartitionResult, WriteShareGroupStateResponse, WriteStateResult,
        },
    },
};

krabka_macros::share_state_handler!(
    WriteShareGroupStateRequest,
    WriteShareGroupStateResponse,
    WriteStateResult,
    PartitionResult,
    write_state
);

/// Applies every partition of `req`, as Kafka's
/// `ShareCoordinatorService.writeState` does.
///
/// An empty topic list, a topic with no partitions, or an empty group id gets
/// a response with no results.
async fn write_state(
    coordinator: &ShareCoordinator,
    image: &MetadataImage,
    req: WriteShareGroupStateRequest,
) -> WriteShareGroupStateResponse {
    if super::empty_partition_data(&req.topics, |topic| topic.partitions.is_empty())
        || req.group_id.is_empty()
    {
        return WriteShareGroupStateResponse::default();
    }
    let results =
        krabka_macros::share_state_results!(req, WriteStateResult, |group_id, topic_id, pd| {
            let request = ShareWrite {
                state_epoch: pd.state_epoch,
                leader_epoch: pd.leader_epoch,
                start_offset: Offset(pd.start_offset),
                delivery_complete_count: pd.delivery_complete_count,
                batches: pd.state_batches.iter().map(StateBatch::from).collect(),
            };
            let result = coordinator
                .write(image, group_id, topic_id, pd.partition, request)
                .await;
            match result {
                Ok(()) => PartitionResult {
                    partition: pd.partition,
                    error_code: codes::NONE,
                    error_message: None,
                    ..Default::default()
                },
                Err(error) => PartitionResult {
                    partition: pd.partition,
                    error_code: error.code(),
                    error_message: Some(error.row_message("write")),
                    ..Default::default()
                },
            }
        });

    WriteShareGroupStateResponse {
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::share_coordinator::handlers::test_support::{
            StateFixture, group_name_literal, initialized_state, retain_leadership,
        },
        assert2::check,
        krabka_protocol::{
            UnknownTaggedFields,
            owned::write_share_group_state_request::{
                PartitionData, StateBatch as WireStateBatch, WriteStateData,
            },
            primitives::uuid::Uuid as ProtoUuid,
        },
        rstest_bdd::datatable::Rows,
    };

    const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([51; 16]);
    const WIRE_TOPIC: ProtoUuid = ProtoUuid([51; 16]);

    /// A partition of a request, at a state epoch.
    struct PartitionAt {
        partition: i32,
        state_epoch: i32,
    }

    fn request(group_id: &str, partitions: &[PartitionAt]) -> WriteShareGroupStateRequest {
        WriteShareGroupStateRequest {
            group_id: group_id.into(),
            topics: vec![WriteStateData {
                topic_id: WIRE_TOPIC,
                partitions: partitions
                    .iter()
                    .map(
                        |&PartitionAt {
                             partition,
                             state_epoch,
                         }| PartitionData {
                            partition,
                            state_epoch,
                            leader_epoch: 3,
                            start_offset: 101,
                            delivery_complete_count: 9,
                            state_batches: vec![WireStateBatch {
                                first_offset: 101,
                                last_offset: 105,
                                delivery_state: 2,
                                delivery_count: 3,
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                    )
                    .collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn row(partition: i32, error_code: i16, message: Option<&str>) -> PartitionResult {
        PartitionResult {
            partition,
            error_code,
            error_message: message.map(str::to_owned),
            unknown_tagged_fields: UnknownTaggedFields(vec![]),
        }
    }

    use crate::share_coordinator::handlers::test_support::state_response;

    fn response(partitions: Vec<PartitionResult>) -> WriteShareGroupStateResponse {
        state_response(WIRE_TOPIC, partitions)
    }

    #[derive(Default)]
    struct WriteStateStory {
        state: Option<StateFixture>,
        directory: Option<tempfile::TempDir>,
        observed: Option<WriteShareGroupStateResponse>,
        summary: rstest_bdd::Slot<Option<(i32, i32, Offset, i32)>>,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct WriteStateInput {
        partition: i32,
        state_epoch: i32,
    }
    #[derive(rstest_bdd_macros::DataTableRow)]
    struct WriteStateExpected {
        partition: i32,
        code: i16,
        message: String,
    }
    #[rstest::fixture]
    fn write_state_story() -> WriteStateStory {
        // State and RPC observations are isolated for each write request.
        WriteStateStory::default()
    }
    #[rstest_bdd_macros::given(
        "a led Share coordinator has topic 33333333-3333-3333-3333-333333333333 with 8 partitions and share-group partition 4 at state epoch 17 start 90 leader epoch 0 completed 0"
    )]
    async fn initialized_write_state(write_state_story: &mut WriteStateStory) {
        write_state_story.directory = Some(tempfile::TempDir::new().expect("share state logs"));
        write_state_story.state = Some(
            initialized_state()
                .log_dir(write_state_story.directory.as_ref().expect("logs").path())
                .topic(TOPIC)
                .partitions(8)
                .partition(4)
                .call()
                .await,
        );
    }
    #[rstest_bdd_macros::given("the WriteShareGroupState coordinator leadership is {led}")]
    async fn write_state_leadership(write_state_story: &WriteStateStory, led: bool) {
        retain_leadership(
            &write_state_story
                .state
                .as_ref()
                .expect("initialized state")
                .coordinator,
            led,
        )
        .await;
    }
    #[rstest_bdd_macros::when(
        "WriteShareGroupState writes group {group} leader epoch 3 start 101 completed 9 acknowledged batch 101..105 count 3 to these partitions and observes stored partition 4"
    )]
    async fn issue_state_write(
        write_state_story: &mut WriteStateStory,
        group: String,
        #[datatable] datatable: Rows<WriteStateInput>,
    ) {
        let group = group_name_literal(group);
        let partitions: Vec<_> = datatable
            .into_iter()
            .map(|row| PartitionAt {
                partition: row.partition,
                state_epoch: row.state_epoch,
            })
            .collect();
        write_state_story.observed = Some(
            write_state(
                &write_state_story
                    .state
                    .as_ref()
                    .expect("initialized state")
                    .coordinator,
                &write_state_story
                    .state
                    .as_ref()
                    .expect("initialized state")
                    .image,
                request(&group, &partitions),
            )
            .await,
        );
        write_state_story.summary.set(
            write_state_story
                .state
                .as_ref()
                .expect("initialized state")
                .coordinator
                .read_summary("share-group", TOPIC, 4)
                .await
                .ok()
                .flatten(),
        );
    }
    #[rstest_bdd_macros::then(
        "the complete WriteShareGroupState response has these rows and default tags"
    )]
    fn state_write_response(
        write_state_story: &WriteStateStory,
        #[datatable] datatable: Rows<WriteStateExpected>,
    ) {
        let expected: Vec<_> = datatable
            .into_iter()
            .map(|expected| {
                row(
                    expected.partition,
                    expected.code,
                    if expected.message == "null" {
                        None
                    } else {
                        Some(expected.message.as_str())
                    },
                )
            })
            .collect();
        let expected = if expected.is_empty() {
            WriteShareGroupStateResponse::default()
        } else {
            response(expected)
        };
        check!(
            write_state_story
                .observed
                .as_ref()
                .expect("write completed")
                == &expected
        );
    }
    #[rstest_bdd_macros::then("the stored Share write summary is {summary}")]
    fn state_write_summary(write_state_story: &WriteStateStory, summary: String) {
        let expected: Option<(i32, i32, i64, i32)> =
            serde_json::from_str(&summary).expect("nullable summary tuple");
        let expected = expected
            .map(|(state, leader, offset, complete)| (state, leader, Offset(offset), complete));
        check!(
            write_state_story
                .summary
                .get()
                .expect("stored summary observed")
                == expected
        );
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/write.feature",
        name = "A matching state epoch applies the delivery delta"
    )]
    #[tokio::test]
    async fn write_state_answers_as_kafka(write_state_story: WriteStateStory) {
        drop(write_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/write.feature",
        name = "An uninitialized key refuses the write"
    )]
    #[tokio::test]
    async fn write_state_answers_as_kafka1(write_state_story: WriteStateStory) {
        drop(write_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/write.feature",
        name = "An old state epoch is fenced"
    )]
    #[tokio::test]
    async fn write_state_answers_as_kafka2(write_state_story: WriteStateStory) {
        drop(write_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/write.feature",
        name = "A resigned coordinator refuses the write"
    )]
    #[tokio::test]
    async fn write_state_answers_as_kafka3(write_state_story: WriteStateStory) {
        drop(write_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/write.feature",
        name = "An empty group returns no rows"
    )]
    #[tokio::test]
    async fn write_state_answers_as_kafka4(write_state_story: WriteStateStory) {
        drop(write_state_story);
    }

    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/share_coordinator/handlers/write.feature",
        name = "An empty partition list returns no rows"
    )]
    #[tokio::test]
    async fn write_state_answers_as_kafka5(write_state_story: WriteStateStory) {
        drop(write_state_story);
    }
}
