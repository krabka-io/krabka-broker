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
/// An actor that stopped holds no offset, so nothing is written for it.
///
/// # Errors
///
/// Returns the top-level code that Kafka's `handleOperationException` answers
/// for the failed write: `NOT_COORDINATOR` when this broker does not lead the
/// partition or stops leading it first, and `COORDINATOR_NOT_AVAILABLE` when
/// the write does not commit in time.
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
        return Ok(());
    }
    rx.await.unwrap_or(Ok(())).map_err(operation_error_code)
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
    use {super::*, assert2::check};

    #[yare::parameterized(
        network_exception = { codes::NETWORK_EXCEPTION, codes::COORDINATOR_LOAD_IN_PROGRESS },
        unknown_topic = { codes::UNKNOWN_TOPIC_OR_PARTITION, codes::COORDINATOR_NOT_AVAILABLE },
        corrupt_message = { codes::CORRUPT_MESSAGE, codes::CORRUPT_MESSAGE },
        not_enough_replicas = { codes::NOT_ENOUGH_REPLICAS, codes::COORDINATOR_NOT_AVAILABLE },
        request_timed_out = { codes::REQUEST_TIMED_OUT, codes::COORDINATOR_NOT_AVAILABLE },
        not_leader = { codes::NOT_LEADER_OR_FOLLOWER, codes::NOT_COORDINATOR },
        kafka_storage_error = { codes::KAFKA_STORAGE_ERROR, codes::NOT_COORDINATOR },
        message_too_large = { codes::MESSAGE_TOO_LARGE, codes::UNKNOWN_SERVER_ERROR },
        record_list_too_large = { codes::RECORD_LIST_TOO_LARGE, codes::UNKNOWN_SERVER_ERROR },
        unknown_server_error = { codes::UNKNOWN_SERVER_ERROR, codes::UNKNOWN_SERVER_ERROR },
    )]
    fn write_errors_map_as_kafka_handle_operation_exception(write: i16, want: i16) {
        check!(operation_error_code(write) == want, "{write}");
    }
}
