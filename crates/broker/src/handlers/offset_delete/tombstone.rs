//! Appending the offset tombstones to the group's `__consumer_offsets`
//! partition.
//!
//! Deleting a committed offset is a write, not a metadata edit: the handler
//! hands a batch of null-valued records to the local partition writer and
//! waits for its acknowledgement. The error mapping for a writer that is gone,
//! that fails, or that drops the acknowledgement lives here with it.

use krabka_protocol::records::RecordBatch;
use tokio::sync::oneshot;

use crate::{
    broker::Broker,
    codes,
    coordinator::bootstrap::OFFSETS_TOPIC,
    partition::{ProduceData, ProduceJob, WriterMessage},
};

pub(super) async fn append_tombstones(
    broker: &Broker,
    offsets_partition: i32,
    batch: RecordBatch,
) -> Result<(), i16> {
    let Some(part_handle) = broker
        .partitions
        .get(OFFSETS_TOPIC, krabka_ids::PartitionIndex(offsets_partition))
    else {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    };
    let (ack_tx, ack_rx) = oneshot::channel();
    if part_handle
        .writer_tx
        .send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(batch),
            ack: ack_tx,
            producer_check: None,
        }))
        .await
        .is_err()
    {
        return Err(codes::UNKNOWN_SERVER_ERROR);
    }
    match ack_rx.await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => {
            tracing::error!(error = %e, "OffsetDelete writer returned error");
            Err(operation_error_code(codes::from_broker_error(&e)))
        }
        Err(e) => {
            tracing::error!(error = %e, "OffsetDelete writer ack dropped");
            Err(codes::UNKNOWN_SERVER_ERROR)
        }
    }
}

/// Kafka's `CoordinatorOperationExceptionHelper.handleOperationException`: the
/// top-level code a failed coordinator write answers with.
fn operation_error_code(code: i16) -> i16 {
    match code {
        codes::NETWORK_EXCEPTION => codes::COORDINATOR_LOAD_IN_PROGRESS,
        codes::UNKNOWN_TOPIC_OR_PARTITION
        | codes::NOT_ENOUGH_REPLICAS
        | codes::REQUEST_TIMED_OUT => codes::COORDINATOR_NOT_AVAILABLE,
        codes::NOT_LEADER_OR_FOLLOWER | codes::KAFKA_STORAGE_ERROR => codes::NOT_COORDINATOR,
        codes::MESSAGE_TOO_LARGE => codes::UNKNOWN_SERVER_ERROR,
        other => other,
    }
}

pub(super) fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_millis()),
    )
    .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn write_errors_map_as_kafka_handle_operation_exception() {
        for (write, want) in [
            (
                codes::NETWORK_EXCEPTION,
                codes::COORDINATOR_LOAD_IN_PROGRESS,
            ),
            (
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                codes::COORDINATOR_NOT_AVAILABLE,
            ),
            (codes::NOT_ENOUGH_REPLICAS, codes::COORDINATOR_NOT_AVAILABLE),
            (codes::REQUEST_TIMED_OUT, codes::COORDINATOR_NOT_AVAILABLE),
            (codes::NOT_LEADER_OR_FOLLOWER, codes::NOT_COORDINATOR),
            (codes::KAFKA_STORAGE_ERROR, codes::NOT_COORDINATOR),
            (codes::MESSAGE_TOO_LARGE, codes::UNKNOWN_SERVER_ERROR),
            (codes::UNKNOWN_SERVER_ERROR, codes::UNKNOWN_SERVER_ERROR),
            (codes::CORRUPT_MESSAGE, codes::CORRUPT_MESSAGE),
        ] {
            check!(operation_error_code(write) == want, "{write}");
        }
    }
}
