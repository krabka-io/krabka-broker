//! Grouped record-batch appends and the blocking-pool hop that runs them.
//!
//! Both the leader path, which lets the log stamp the base offset, and the
//! diskless path, which appends at an externally assigned base offset, share
//! the one-lock-per-group discipline, so they stay together in one module.

use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
};

use krabka_log::{Log, Offset};
use tokio::runtime::{Handle, RuntimeFlavor};

use super::storage::{lock_log, storage_failure_error};
use crate::partition::{AppendedBatch, ProduceData};

/// The writer's answer for a batch the log both placed and, when the partition
/// asks for it, stamped.
fn appended((base_offset, log_append_time_ms): (Offset, Option<i64>)) -> AppendedBatch {
    AppendedBatch {
        base_offset,
        log_append_time_ms,
    }
}

/// The writer's answer for an append at a caller-assigned offset.
///
/// These appends carry no log-append stamp. Kafka stamps under
/// `validateAndAssignOffsets`, which is the offset-assigning leader append
/// alone: a follower stores the leader's already-stamped bytes, and re-stamping
/// them would give one offset two different encodings across replicas.
fn at_offset(base_offset: Offset) -> AppendedBatch {
    AppendedBatch {
        base_offset,
        log_append_time_ms: None,
    }
}

/// Append a whole group of produce jobs under a single lock acquisition.
///
/// The function returns the per-job results, a base offset or an error, in
/// input order. It also returns the log-end offset after the append, for the
/// group's HW recompute, and the log's producer entry for every control batch
/// that appended, for the produce-path tracker's mirror.
///
/// The mirror entries are read here, under the one lock this function already
/// holds, rather than by a second, later lock acquisition. A `std::sync::
/// Mutex` lock taken directly on the calling async task blocks its worker
/// thread for as long as whichever task holds the lock, and does not yield it
/// back to the runtime the way an uncontended `.await` would. This function
/// itself already runs off the async worker pool (see
/// [`run_produce_append_batch`]), so a second, separate acquisition later, on
/// the worker pool, could block a worker thread behind a concurrent
/// `spawn_blocking` log operation such as a diskless flush's trim, starving
/// whatever else that thread was due to run -- including, on a freshly
/// promoted leader catching up on replicated markers across many partitions
/// at once, this broker's own heartbeat-sending task, past the controller's
/// liveness timeout.
///
/// Verbatim jobs go straight to `append_verbatim`. The function recompresses
/// owned jobs to the topic's configured codec, which it reads once under the
/// same lock. Control jobs skip that rewrite, because Kafka never compresses a
/// control batch that arrived uncompressed. Sequential appends stamp
/// sequential base offsets, so the function keeps the order across the group.
fn append_produce_batch(
    log: &Mutex<Log>,
    datas: Vec<ProduceData>,
) -> (
    Vec<Result<AppendedBatch, crate::error::BrokerError>>,
    Offset,
    Vec<krabka_log::ProducerSnapshotEntry>,
) {
    let mut guard = lock_log(log);
    let target = guard.config_snapshot().compression_type;
    let mut results = Vec::with_capacity(datas.len());
    let mut control_entries = Vec::new();
    for data in datas {
        let control_producer = data.control_producer_id();
        let r = match data {
            ProduceData::Verbatim(batch) => guard
                .append_verbatim(&batch)
                .map(appended)
                .map_err(crate::error::BrokerError::from),
            ProduceData::Owned(mut batch) => {
                if let Some(target) = target
                    && batch.attributes.compression() != target
                {
                    batch.attributes = batch.attributes.with_compression(target);
                }
                guard
                    .append(&mut batch)
                    .map(appended)
                    .map_err(crate::error::BrokerError::from)
            }
            ProduceData::OwnedControl(mut batch) => guard
                .append(&mut batch)
                .map(appended)
                .map_err(crate::error::BrokerError::from),
            ProduceData::OwnedCommitMarker {
                mut batch,
                commit_stamp,
            } => guard
                .append_with_commit_stamp(&mut batch, commit_stamp)
                .map(|base_offset| AppendedBatch {
                    base_offset,
                    log_append_time_ms: None,
                })
                .map_err(crate::error::BrokerError::from),
        };
        if r.is_ok()
            && let Some(producer_id) = control_producer
            && let Some(entry) = guard.producer_state_entry(producer_id)
        {
            control_entries.push(entry);
        }
        results.push(r);
    }
    // Read the post-append LEO once under the same lock so the HW recompute
    // reflects the whole group.
    let leo = guard.log_end_offset();
    (results, leo, control_entries)
}

fn append_produce_batch_at(
    log: &Mutex<Log>,
    base: Offset,
    datas: Vec<ProduceData>,
) -> (
    Vec<Result<AppendedBatch, crate::error::BrokerError>>,
    Offset,
    Vec<krabka_log::ProducerSnapshotEntry>,
) {
    let mut guard = lock_log(log);
    let target = guard.config_snapshot().compression_type;
    let mut next = base;
    let mut results = Vec::with_capacity(datas.len());
    let mut control_entries = Vec::new();
    for data in datas {
        let count = i64::from(data.record_count());
        let control_producer = data.control_producer_id();
        let result = match data {
            ProduceData::Verbatim(batch) => guard
                .append_verbatim_at(&batch, next)
                .map(at_offset)
                .map_err(crate::error::BrokerError::from),
            ProduceData::Owned(mut batch) => {
                if let Some(target) = target
                    && batch.attributes.compression() != target
                {
                    batch.attributes = batch.attributes.with_compression(target);
                }
                guard
                    .append_at(&mut batch, next)
                    .map(|()| at_offset(next))
                    .map_err(crate::error::BrokerError::from)
            }
            ProduceData::OwnedControl(mut batch) => guard
                .append_at(&mut batch, next)
                .map(|()| at_offset(next))
                .map_err(crate::error::BrokerError::from),
            ProduceData::OwnedCommitMarker {
                mut batch,
                commit_stamp,
            } => guard
                .append_at_with_commit_stamp(&mut batch, next, commit_stamp)
                .map(|()| at_offset(next))
                .map_err(crate::error::BrokerError::from),
        };
        if result.is_ok()
            && let Some(producer_id) = control_producer
            && let Some(entry) = guard.producer_state_entry(producer_id)
        {
            control_entries.push(entry);
        }
        next = Offset(next.0 + count);
        guard.reconcile_next_offset(next);
        results.push(result);
    }
    let leo = guard.log_end_offset();
    (results, leo, control_entries)
}

/// Run [`append_produce_batch`] away from normal async polling.
///
/// On the broker's multi-thread runtime, `block_in_place` avoids the per-batch
/// `spawn_blocking` scheduling hop. Tokio can still hand the worker's other
/// tasks to a replacement thread. Current-thread test runtimes keep the
/// `spawn_blocking` fallback because `block_in_place` is illegal there. The
/// writer loop is still the single serializer for this partition, so the append
/// order does not change.
pub(crate) async fn run_produce_append_batch(
    log: Arc<Mutex<Log>>,
    datas: Vec<ProduceData>,
) -> Result<
    (
        Vec<Result<AppendedBatch, crate::error::BrokerError>>,
        Offset,
        Vec<krabka_log::ProducerSnapshotEntry>,
    ),
    crate::error::BrokerError,
> {
    match Handle::current().runtime_flavor() {
        RuntimeFlavor::MultiThread => catch_unwind(AssertUnwindSafe(|| {
            tokio::task::block_in_place(move || append_produce_batch(&log, datas))
        }))
        .map_err(|_| storage_failure_error("append task panicked", "block_in_place panic")),
        _ => tokio::task::spawn_blocking(move || append_produce_batch(&log, datas))
            .await
            .map_err(|join_err| storage_failure_error("append task panicked", &join_err)),
    }
}

pub(crate) async fn run_produce_append_batch_at(
    log: Arc<Mutex<Log>>,
    base: Offset,
    datas: Vec<ProduceData>,
) -> Result<
    (
        Vec<Result<AppendedBatch, crate::error::BrokerError>>,
        Offset,
        Vec<krabka_log::ProducerSnapshotEntry>,
    ),
    crate::error::BrokerError,
> {
    match Handle::current().runtime_flavor() {
        RuntimeFlavor::MultiThread => catch_unwind(AssertUnwindSafe(|| {
            tokio::task::block_in_place(move || append_produce_batch_at(&log, base, datas))
        }))
        .map_err(|_| storage_failure_error("append task panicked", "block_in_place panic")),
        _ => tokio::task::spawn_blocking(move || append_produce_batch_at(&log, base, datas))
            .await
            .map_err(|join_err| storage_failure_error("append task panicked", &join_err)),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_compression::CompressionType;
    use krabka_log::{LogConfig, LogIo};
    use tempfile::tempdir;

    use super::*;
    use crate::partition_writer::test_support::sample_batch;

    #[derive(Debug)]
    struct FailNthWrite {
        next: std::sync::atomic::AtomicUsize,
        fail_at: usize,
    }

    impl LogIo for FailNthWrite {
        fn write(&self, file: &std::fs::File, buf: &[u8]) -> std::io::Result<usize> {
            use std::io::Write;

            let call = self.next.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if call == self.fail_at {
                Err(std::io::ErrorKind::StorageFull.into())
            } else {
                (&*file).write(buf)
            }
        }
    }

    #[test]
    fn grouped_produce_surfaces_nth_log_write_failure_without_advancing_leo() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        log.test_set_io(Arc::new(FailNthWrite {
            next: std::sync::atomic::AtomicUsize::new(0),
            fail_at: 2,
        }));
        let log = Mutex::new(log);

        let (results, leo, _) = append_produce_batch(
            &log,
            vec![
                ProduceData::Owned(sample_batch(1)),
                ProduceData::Owned(sample_batch(1)),
            ],
        );

        assert!(results[0].as_ref().unwrap().base_offset == Offset(0));
        assert!(matches!(
            &results[1],
            Err(crate::error::BrokerError::Log(krabka_log::LogError::Io(error)))
                if error.kind() == std::io::ErrorKind::StorageFull
        ));
        assert!(leo == Offset(1));
        assert!(log.lock().unwrap().log_end_offset() == Offset(1));
    }

    #[test]
    fn diskless_group_reanchors_after_partial_append_failure() {
        let dir = tempdir().expect("tempdir");
        let mut log = Log::open(dir.path(), LogConfig::default()).expect("open log");
        log.test_set_io(Arc::new(FailNthWrite {
            next: std::sync::atomic::AtomicUsize::new(0),
            fail_at: 2,
        }));
        let log = Mutex::new(log);

        let (results, leo, _) = append_produce_batch_at(
            &log,
            Offset(0),
            vec![
                ProduceData::Owned(sample_batch(1)),
                ProduceData::Owned(sample_batch(1)),
            ],
        );

        assert!(results[0].as_ref().unwrap().base_offset == Offset(0));
        assert!(matches!(
            &results[1],
            Err(crate::error::BrokerError::Log(krabka_log::LogError::Io(error)))
                if error.kind() == std::io::ErrorKind::StorageFull
        ));
        assert!(leo == Offset(1));

        let (results, leo, _) =
            append_produce_batch_at(&log, Offset(2), vec![ProduceData::Owned(sample_batch(1))]);

        assert!(results[0].as_ref().unwrap().base_offset == Offset(2));
        assert!(leo == Offset(3));
    }

    #[test]
    fn append_owned_batch_recompresses_to_configured_log_codec() {
        let dir = tempdir().expect("tempdir");
        let log = Mutex::new(
            Log::open(
                dir.path(),
                LogConfig {
                    compression_type: Some(CompressionType::Lz4),
                    ..LogConfig::default()
                },
            )
            .expect("open log"),
        );

        let original = sample_batch(2);
        assert!(original.attributes.compression() == CompressionType::None);

        let (results, leo, _) = append_produce_batch(&log, vec![ProduceData::Owned(original)]);
        assert!(results.len() == 1);
        let assigned = results.into_iter().next().unwrap().expect("append ok");
        assert!(assigned.base_offset == 0);
        assert!(leo == 2);

        let read = log
            .lock()
            .unwrap()
            .read(Offset(0), krabka_units::mebibytes(10))
            .unwrap();
        assert!(read.batches.len() == 1);
        check!(read.batches[0].attributes.compression() == CompressionType::Lz4);
        check!(read.batches[0].records.len() == 2);
    }

    /// A control batch that arrives uncompressed stays uncompressed, whatever
    /// the topic's `compression.type` says. Kafka never compresses one, and a
    /// control batch holds one small record, so the rewrite would both diverge
    /// from Kafka and buy nothing.
    #[test]
    fn append_control_batch_keeps_its_own_compression() {
        let dir = tempdir().expect("tempdir");
        let log = Mutex::new(
            Log::open(
                dir.path(),
                LogConfig {
                    compression_type: Some(CompressionType::Lz4),
                    ..LogConfig::default()
                },
            )
            .expect("open log"),
        );

        let mut marker = sample_batch(1);
        marker.attributes = marker.attributes.with_control(true);
        marker.producer_id = 7;
        marker.producer_epoch = 3;
        assert!(marker.attributes.compression() == CompressionType::None);

        let (results, _, control_entries) =
            append_produce_batch(&log, vec![ProduceData::OwnedControl(marker)]);
        let assigned = results.into_iter().next().unwrap().expect("append ok");
        assert!(assigned.base_offset == 0);

        // The mirror entry comes from this same lock acquisition, not a
        // second one: this is what keeps a produce-path mirror from ever
        // blocking a worker thread behind a concurrent blocking-pool log
        // operation.
        assert!(control_entries.len() == 1);
        check!(control_entries[0].producer_id == krabka_log::ProducerId(7));
        check!(control_entries[0].producer_epoch == 3);

        let read = log
            .lock()
            .unwrap()
            .read(Offset(0), krabka_units::mebibytes(10))
            .unwrap();
        assert!(read.batches.len() == 1);
        check!(read.batches[0].attributes.compression() == CompressionType::None);
        check!(read.batches[0].attributes.is_control_batch());
    }

    /// The follower path makes the same promise as the leader path. A
    /// replicated control batch must land byte-for-byte as the leader wrote
    /// it, or the two logs diverge.
    #[test]
    fn append_control_batch_at_offset_keeps_its_own_compression() {
        let dir = tempdir().expect("tempdir");
        let log = Mutex::new(
            Log::open(
                dir.path(),
                LogConfig {
                    compression_type: Some(CompressionType::Lz4),
                    ..LogConfig::default()
                },
            )
            .expect("open log"),
        );

        let mut marker = sample_batch(1);
        marker.attributes = marker.attributes.with_control(true);
        marker.producer_id = 7;
        marker.producer_epoch = 3;

        let (results, _, control_entries) =
            append_produce_batch_at(&log, Offset(0), vec![ProduceData::OwnedControl(marker)]);
        assert!(
            results
                .into_iter()
                .next()
                .unwrap()
                .expect("append ok")
                .base_offset
                == 0
        );
        assert!(control_entries.len() == 1);
        check!(control_entries[0].producer_id == krabka_log::ProducerId(7));
        check!(control_entries[0].producer_epoch == 3);

        let read = log
            .lock()
            .unwrap()
            .read(Offset(0), krabka_units::mebibytes(10))
            .unwrap();
        check!(read.batches[0].attributes.compression() == CompressionType::None);
        check!(read.batches[0].attributes.is_control_batch());
    }
}
