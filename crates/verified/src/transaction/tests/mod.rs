use super::*;

// Kafka `TransactionState` ids.
const ONGOING: i8 = 1;

const PREPARE_COMMIT: i8 = 2;

const PREPARE_ABORT: i8 = 3;

const COMPLETE_COMMIT: i8 = 4;

const COMPLETE_ABORT: i8 = 5;

fn marker(
    producer_epoch: i16,
    coordinator_epoch: i32,
    is_commit: bool,
    is_offsets_partition: bool,
) -> TransactionMarkerRequest {
    TransactionMarkerRequest {
        producer_id: 1,
        producer_epoch,
        coordinator_epoch,
        is_commit,
        is_offsets_partition,
    }
}

fn partition(
    producer_epoch: i16,
    coordinator_epoch: i32,
    has_pending_transaction: bool,
) -> TransactionMarkerPartitionState {
    TransactionMarkerPartitionState {
        producer_epoch,
        coordinator_epoch,
        has_pending_transaction,
    }
}

fn snapshot(pid: i64, epoch: i16, state: i8) -> TransactionSnapshot {
    TransactionSnapshot { pid, epoch, state }
}

mod pid_install_rejects_malformed_misplaced_and_colliding_records;

mod reaper_completion_requires_the_exact_prepared_snapshot;

mod idle_transaction_reaper_matches_kafka_timed_out_transactions;
