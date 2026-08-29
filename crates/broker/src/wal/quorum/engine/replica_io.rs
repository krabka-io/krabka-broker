//! The blocking log operations that a WAL replica performs, and the bridge
//! that keeps them off an async worker thread.
//!
//! Appending a verbatim batch, fsyncing, and trimming all block. Each entry
//! point here picks `block_in_place` or `spawn_blocking` by runtime flavour,
//! so callers on the produce and flusher paths can stay `async`.

use krabka_ids::Offset;

use super::BatchBytes;
use crate::{error::BrokerError, wal::quorum::log_view::ShardLog};

pub(in crate::wal::quorum) async fn sync_replica(
    log: ShardLog,
    batches: &[BatchBytes],
) -> Result<(), BrokerError> {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(|| sync_replica_blocking(&log, batches))
    } else {
        let batches = batches.to_vec();
        tokio::task::spawn_blocking(move || sync_replica_blocking(&log, &batches))
            .await
            .map_err(|e| BrokerError::Replication(format!("wal replica task panicked: {e}")))?
    }
}

pub(super) async fn trim_log(log: ShardLog, new_start: Offset) -> Result<Offset, BrokerError> {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(|| log.lock().trim_to_offset(new_start).map_err(Into::into))
    } else {
        tokio::task::spawn_blocking(move || {
            log.lock().trim_to_offset(new_start).map_err(Into::into)
        })
        .await
        .map_err(|error| {
            crate::partition_writer::storage_failure_error("wal trim task panicked", error)
        })?
    }
}

pub(super) fn sync_replica_blocking(
    log: &ShardLog,
    batches: &[BatchBytes],
) -> Result<(), BrokerError> {
    let mut log = log.lock();
    for batch in batches {
        let end = log.log_end_offset();
        if end <= batch.base_offset {
            log.append_verbatim_at(&batch.verbatim, batch.base_offset)?;
        } else if end < batch.last_offset + 1 {
            return Err(BrokerError::Replication(format!(
                "wal replica overlaps batch {}..{} at leo {end}",
                batch.base_offset.0,
                batch.last_offset.0 + 1
            )));
        }
    }
    log.sync()?;
    Ok(())
}
