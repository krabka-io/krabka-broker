//! Appending the offset tombstones to the group's `__consumer_offsets`
//! partition.
//!
//! Deleting a committed offset is a write, not a metadata edit. Kafka runs it
//! as a `CoordinatorRuntime` write operation, which appends as the partition
//! leader and completes once the high watermark covers the records. The group
//! actor does the same through the coordinator's offsets log, and decides in
//! the same turn which keys need a tombstone at all: Kafka writes one only for
//! an offset the group holds. The error mapping for a write that fails lives
//! here.

use {
    crate::{
        codes,
        coordinator::unified::actor::{GroupActorHandle, GroupActorMessage},
    },
    tokio::sync::oneshot,
};

/// Ask the group actor behind `group_handle` to tombstone those of `keys` it
/// holds an offset for, and return once the tombstones are committed.
///
/// # Errors
///
/// Returns the top-level code that Kafka's `handleOperationException` answers
/// for the failed write: `NOT_COORDINATOR` when this broker does not lead the
/// partition or stops leading it first, and `COORDINATOR_NOT_AVAILABLE` when
/// the write does not commit in time. An actor that stopped after the group
/// check, before it could write, also answers `COORDINATOR_NOT_AVAILABLE`:
/// its offsets are still in the log, so the client retries rather than read
/// `NONE` for a delete that did not happen.
pub(super) async fn delete_held_offsets(
    group_handle: &GroupActorHandle,
    keys: Vec<(String, i32)>,
) -> Result<(), i16> {
    let (reply, rx) = oneshot::channel();
    if group_handle
        .tx
        .send(GroupActorMessage::DeleteOffsets { keys, reply })
        .await
        .is_err()
    {
        return Err(codes::COORDINATOR_NOT_AVAILABLE);
    }
    rx.await
        .unwrap_or(Err(codes::COORDINATOR_NOT_AVAILABLE))
        .map_err(operation_error_code)
}

/// Kafka's `CoordinatorOperationExceptionHelper.handleOperationException`: the
/// top-level code a failed coordinator write answers with.
fn operation_error_code(code: i16) -> i16 {
    match code {
        codes::MESSAGE_TOO_LARGE | codes::RECORD_LIST_TOO_LARGE => codes::UNKNOWN_SERVER_ERROR,
        other => codes::coordinator_operation_code(other),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{actor::GroupKindTag, test_support::make_coord_with_log},
        assert2::check,
    };

    /// An actor that stopped between the group check and the write has
    /// written nothing, so the delete must not answer `NONE`.
    #[tokio::test]
    async fn a_stopped_actor_answers_coordinator_not_available() {
        let (coordinator, log) = make_coord_with_log();
        let handle = coordinator.get_or_create_group("g", GroupKindTag::Classic);
        let (stopped, done) = oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::Shutdown(stopped))
            .await
            .expect("the actor is running");
        done.await.expect("the actor stopped");

        let got = delete_held_offsets(&handle, vec![("t".to_owned(), 0)]).await;

        check!(got == Err(codes::COORDINATOR_NOT_AVAILABLE));
        check!(log.batches().await.is_empty());
    }

    #[derive(Default)]
    struct OffsetDeleteWriteScenario {
        write_error: i16,
        operation_error: Option<i16>,
    }
    #[rstest::fixture]
    fn offset_delete_write() -> OffsetDeleteWriteScenario {
        let operation_error = None;
        OffsetDeleteWriteScenario {
            operation_error,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "writing an offset deletion tombstone fails with Kafka error {write_error}"
    )]
    fn failed_tombstone_write(
        offset_delete_write: &mut OffsetDeleteWriteScenario,
        write_error: i16,
    ) {
        offset_delete_write.write_error = write_error;
    }
    #[rstest_bdd_macros::when(
        "the offset deletion maps the write failure to its operation response"
    )]
    fn map_tombstone_failure(offset_delete_write: &mut OffsetDeleteWriteScenario) {
        offset_delete_write.operation_error =
            Some(operation_error_code(offset_delete_write.write_error));
    }
    #[rstest_bdd_macros::then("the offset deletion response error is {expected}")]
    fn check_tombstone_failure(offset_delete_write: &OffsetDeleteWriteScenario, expected: i16) {
        check!(offset_delete_write.operation_error.unwrap() == expected);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_offset_delete_write",fixtures=[offset_delete_write:OffsetDeleteWriteScenario]);
}
