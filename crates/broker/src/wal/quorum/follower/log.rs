//! The follower's own replica log: where it lives under a log directory, and
//! the trim, reset, and append operations that move it forward. Every mutation
//! is fsynced and then recorded in the durable-offset checkpoint, so the log
//! never advertises more than this broker has on disk.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use bytes::{Bytes, BytesMut};
use krabka_ids::Offset;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::RecordsPayload;
use krabka_raft::NodeId;

use super::{
    Config,
    checkpoint::{DURABLE_OFFSET_FILE, DurableRange, recover_durable_offset, write_durable_offset},
};
use crate::wal::quorum::{
    engine::{split_batches, sync_replica},
    log_view::ShardLog,
    registry::ShardId,
    shard_dir,
};

#[derive(Debug)]
pub(super) struct FollowerLog {
    pub(super) log: ShardLog,
    durable_offset_path: PathBuf,
}

impl FollowerLog {
    pub(super) fn open(config: &Config) -> Result<Self, crate::BrokerError> {
        let dir = config
            .log_dirs
            .iter()
            .map(|root| voter_dir(root, &config.topic, config.shard, config.node_id))
            .find(|candidate| candidate.exists())
            .map_or_else(
                || {
                    let partition_dir = crate::log_dir::place_partition_dir(
                        &config.log_dirs,
                        &config.topic,
                        config.shard.partition.0,
                    );
                    partition_dir
                        .parent()
                        .map(|root| voter_dir(root, &config.topic, config.shard, config.node_id))
                        .ok_or_else(|| {
                            crate::BrokerError::Replication("WAL log dir has no parent".into())
                        })
                },
                Ok,
            )?;
        Self::open_at(dir, &config.storage)
    }

    pub(super) fn open_at(dir: PathBuf, storage: &LogConfig) -> Result<Self, crate::BrokerError> {
        let mut log_config = storage.clone();
        log_config.validate_on_open = true;
        let durable_offset_path = dir.join(DURABLE_OFFSET_FILE);
        let mut log = Log::open(dir, log_config)?;
        recover_durable_offset(&mut log, &durable_offset_path)?;
        Ok(Self {
            log: ShardLog::new(Arc::new(std::sync::Mutex::new(log))),
            durable_offset_path,
        })
    }

    #[cfg(test)]
    pub(super) fn for_log(log: Log) -> Self {
        let durable_offset_path = log.dir().join(DURABLE_OFFSET_FILE);
        write_durable_offset(
            &durable_offset_path,
            DurableRange {
                start: log.log_start_offset(),
                end: log.log_end_offset(),
            },
        )
        .unwrap();
        Self {
            log: ShardLog::new(Arc::new(std::sync::Mutex::new(log))),
            durable_offset_path,
        }
    }

    pub(super) fn end_offset(&self) -> Offset {
        self.log.lock().log_end_offset()
    }

    pub(super) fn start_offset(&self) -> Offset {
        self.log.lock().log_start_offset()
    }

    pub(super) async fn trim_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        if offset <= self.start_offset() {
            return Ok(());
        }
        let log = self.log.clone();
        let durable_offset_path = self.durable_offset_path.clone();
        run_blocking(move || {
            let mut log = log.lock();
            log.trim_to_offset(offset)?;
            log.sync()?;
            write_durable_offset(
                &durable_offset_path,
                DurableRange {
                    start: log.log_start_offset(),
                    end: log.log_end_offset(),
                },
            )?;
            Ok(())
        })
        .await
    }

    pub(super) async fn reset_to(&self, offset: Offset) -> Result<(), crate::BrokerError> {
        let log = self.log.clone();
        let durable_offset_path = self.durable_offset_path.clone();
        run_blocking(move || {
            let mut log = log.lock();
            log.reset_to(offset)?;
            log.sync()?;
            write_durable_offset(
                &durable_offset_path,
                DurableRange {
                    start: offset,
                    end: offset,
                },
            )?;
            Ok(())
        })
        .await
    }

    pub(super) async fn append(
        &self,
        requested: Offset,
        leader_end: Offset,
        records: Option<RecordsPayload>,
    ) -> Result<Offset, crate::BrokerError> {
        if self.end_offset() != requested {
            return Err(crate::BrokerError::Replication(format!(
                "WAL follower moved from requested offset {} to {}",
                requested.0,
                self.end_offset().0
            )));
        }
        let Some(records) = records else {
            return Ok(requested);
        };
        let mut encoded = BytesMut::with_capacity(records.payload_len());
        records.encode_to(&mut encoded).map_err(|error| {
            crate::BrokerError::Replication(format!("encode WAL fetch: {error}"))
        })?;
        let bytes: Bytes = encoded.freeze();
        let batches = split_batches(&bytes)?;
        let mut expected = requested;
        for batch in &batches {
            if batch.base_offset != expected {
                return Err(crate::BrokerError::Replication(format!(
                    "WAL fetch is not contiguous at {}, got {}",
                    expected.0, batch.base_offset.0
                )));
            }
            expected = Offset(batch.last_offset.0.checked_add(1).ok_or_else(|| {
                crate::BrokerError::Replication("WAL fetch offset overflow".into())
            })?);
        }
        if expected.cmp(&leader_end).is_gt() {
            return Err(crate::BrokerError::Replication(format!(
                "WAL fetch ends at {}, beyond leader LEO {}",
                expected.0, leader_end.0
            )));
        }
        sync_replica(self.log.clone(), &batches).await?;
        let actual = self.end_offset();
        if actual != expected {
            return Err(crate::BrokerError::Replication(format!(
                "WAL follower ended at {}, expected {}",
                actual.0, expected.0
            )));
        }
        let durable_offset_path = self.durable_offset_path.clone();
        let start = self.start_offset();
        run_blocking(move || {
            write_durable_offset(&durable_offset_path, DurableRange { start, end: actual })?;
            Ok(())
        })
        .await?;
        Ok(actual)
    }
}

pub(super) fn voter_dir(root: &Path, topic: &str, shard: ShardId, node_id: NodeId) -> PathBuf {
    shard_dir(root, topic, Some(shard.topic_id), shard.partition)
        .join(format!("voter-{}", node_id.0))
}

async fn run_blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, crate::BrokerError> + Send + 'static,
) -> Result<T, crate::BrokerError> {
    if tokio::runtime::Handle::current().runtime_flavor()
        == tokio::runtime::RuntimeFlavor::MultiThread
    {
        tokio::task::block_in_place(operation)
    } else {
        tokio::task::spawn_blocking(operation)
            .await
            .map_err(|error| {
                crate::partition_writer::storage_failure_error(
                    "WAL follower storage task panicked",
                    error,
                )
            })?
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::*;

    #[tokio::test]
    async fn follower_appends_and_syncs_a_contiguous_fetch() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let batch = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let end = follower
            .append(Offset(0), Offset(1), Some(RecordsPayload::V2(vec![batch])))
            .await
            .unwrap();

        assert2::assert!((end) == (Offset(1)));
        drop(follower);
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!((reopened.log_end_offset()) == (Offset(1)));
    }

    #[tokio::test]
    async fn follower_rejects_a_gap_before_writing() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let batch = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let error = follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(vec![batch])))
            .await
            .unwrap_err();

        assert!(error.to_string().contains("not contiguous"));
        assert2::assert!((follower.end_offset()) == (Offset(0)));
    }

    #[tokio::test]
    async fn follower_accepts_a_partial_fetch_and_rejects_a_leader_overrun() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let first = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };

        let end = follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(vec![first])))
            .await
            .unwrap();

        assert!(end == Offset(1));
        let beyond_leader = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        let error = follower
            .append(
                Offset(1),
                Offset(1),
                Some(RecordsPayload::V2(vec![beyond_leader])),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("beyond leader LEO"));
        assert!(follower.end_offset() == Offset(1));
    }

    #[tokio::test]
    async fn follower_reset_persists_the_leader_log_start() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());

        follower.reset_to(Offset(7)).await.unwrap();

        assert2::assert!((follower.end_offset()) == (Offset(7)));
        drop(follower);
        let reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        assert2::assert!((reopened.log_start_offset()) == (Offset(7)));
        assert2::assert!((reopened.log_end_offset()) == (Offset(7)));
    }

    #[tokio::test]
    async fn follower_trim_persists_the_leader_log_start() {
        let dir = tempfile::tempdir().unwrap();
        let follower = FollowerLog::for_log(Log::open(dir.path(), LogConfig::default()).unwrap());
        let batches = (0..2)
            .map(|base_offset| RecordBatch {
                base_offset,
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .collect();
        follower
            .append(Offset(0), Offset(2), Some(RecordsPayload::V2(batches)))
            .await
            .unwrap();

        follower.trim_to(Offset(1)).await.unwrap();

        assert2::assert!((follower.start_offset()) == (Offset(1)));
        assert2::assert!((follower.end_offset()) == (Offset(2)));
        drop(follower);
        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        recover_durable_offset(&mut reopened, &dir.path().join(DURABLE_OFFSET_FILE)).unwrap();
        assert2::assert!((reopened.log_start_offset()) == (Offset(1)));
        assert2::assert!((reopened.log_end_offset()) == (Offset(2)));
    }
}
