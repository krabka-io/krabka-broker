//! In-process WAL quorum engine.
//!
//! This file is the module root. It holds the replica set, the shard engine
//! state, and the local-replica durability path. Each child holds one concern:
//! `batches` decodes a raw WAL byte range, `replica_io` runs the blocking log
//! operations off the async worker threads, `recovery` picks and enforces the
//! durable prefix a shard opens on, and `distributed` keeps the diskless
//! voter-set quorum.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
};

use bytes::Bytes;
use krabka_ids::Offset;
use krabka_kraft_core::{LogView as _, NodeId};
use krabka_log::{Log, VerbatimBatch};
use krabka_units::{ByteSize, convert::ByteSizeExt as _};
use tokio::sync::Notify;

mod batches;
mod distributed;
mod recovery;
mod replica_io;

pub(super) use self::{
    batches::{read_batches_exact, read_log_batches_exact, split_batches},
    replica_io::sync_replica,
};
use self::{
    recovery::{bootstrap_durable_prefix, recover_durable_prefix},
    replica_io::trim_log,
};
use crate::{error::BrokerError, wal::quorum::log_view::ShardLog};

/// A single durable member of a WAL quorum.
#[derive(Debug)]
pub(crate) struct WalReplica {
    pub(super) id: NodeId,
    log: ShardLog,
    alive: AtomicBool,
}

impl WalReplica {
    #[must_use]
    pub(crate) fn new(id: NodeId, log: Arc<Mutex<Log>>) -> Self {
        Self {
            id,
            log: ShardLog::new(log),
            alive: AtomicBool::new(true),
        }
    }

    #[must_use]
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
}

/// Drives the durable quorum frontier of a WAL shard.
#[derive(Debug)]
pub(crate) struct WalShardEngine {
    replicas: Vec<WalReplica>,
    expected_voters: usize,
    durable_watermark: AtomicI64,
    local_durable: AtomicI64,
    distributed_required: AtomicBool,
    distributed: Mutex<Option<DistributedQuorum>>,
    durable_advanced: Notify,
}

#[derive(Debug)]
struct DistributedQuorum {
    me: NodeId,
    voters: Vec<NodeId>,
    durable_offsets: HashMap<NodeId, Offset>,
}

pub(super) fn strict_majority(voter_count: usize) -> usize {
    voter_count.div_euclid(2).saturating_add(1)
}

/// One response from the leader-side WAL fetch path.
#[derive(Debug)]
pub(crate) struct WalFetchData {
    pub(crate) high_watermark: Offset,
    pub(crate) log_end_offset: Offset,
    pub(crate) log_start_offset: Offset,
    pub(crate) records: Bytes,
    pub(crate) offset_out_of_range: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum OpenMode {
    BootstrapFrom(NodeId),
    Recover,
    Distributed,
}

impl WalShardEngine {
    pub(crate) fn new(replicas: Vec<WalReplica>, mode: OpenMode) -> Result<Self, BrokerError> {
        if replicas.is_empty() {
            return Err(BrokerError::Replication(
                "wal quorum must contain at least one replica".into(),
            ));
        }
        let expected_voters = replicas.len();
        let durable_watermark = match mode {
            OpenMode::BootstrapFrom(source) => bootstrap_durable_prefix(&replicas, source)?,
            OpenMode::Recover => {
                recover_durable_prefix(&replicas, strict_majority(expected_voters))?
            }
            OpenMode::Distributed => replicas[0].log.lock().log_start_offset(),
        };
        Ok(Self {
            replicas,
            expected_voters,
            durable_watermark: AtomicI64::new(durable_watermark.0),
            local_durable: AtomicI64::new(durable_watermark.0),
            distributed_required: AtomicBool::new(false),
            distributed: Mutex::new(None),
            durable_advanced: Notify::new(),
        })
    }

    pub(crate) fn new_distributed(
        source: Arc<Mutex<Log>>,
        expected_voters: usize,
    ) -> Result<Self, BrokerError> {
        if expected_voters == 0 {
            return Err(BrokerError::Replication(
                "diskless WAL voter count must be positive".into(),
            ));
        }
        if expected_voters.is_multiple_of(2) {
            return Err(BrokerError::Replication(
                "diskless WAL voter count must be odd".into(),
            ));
        }
        let local_durable = {
            let mut log = source
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            log.sync()?;
            log.log_end_offset()
        };
        let mut engine = Self::new(
            vec![WalReplica::new(NodeId(0), source)],
            OpenMode::Distributed,
        )?;
        engine.expected_voters = expected_voters;
        engine
            .local_durable
            .store(local_durable.0, Ordering::Release);
        Ok(engine)
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn for_logs(logs: std::collections::BTreeMap<NodeId, Arc<Mutex<Log>>>) -> Self {
        let replicas = logs
            .into_iter()
            .map(|(id, log)| WalReplica::new(id, log))
            .collect();
        Self::new(replicas, OpenMode::Recover).expect("test WAL quorum recovers")
    }

    #[must_use]
    pub(crate) fn durable_watermark(&self) -> Offset {
        Offset(self.durable_watermark.load(Ordering::Acquire))
    }

    pub(crate) async fn wait_for_durable_advance(&self, after: Offset) -> Offset {
        loop {
            let advanced = self.durable_advanced.notified();
            let current = self.durable_watermark();
            if current.cmp(&after).is_gt() {
                return current;
            }
            advanced.await;
        }
    }

    #[cfg(test)]
    pub(crate) fn set_replica_alive(&self, id: NodeId, alive: bool) {
        if let Some(replica) = self.replicas.iter().find(|replica| replica.id == id) {
            replica.alive.store(alive, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn replica_end_offsets(&self) -> Vec<Offset> {
        self.replicas.iter().map(replica_end_offset).collect()
    }

    #[cfg(test)]
    pub(crate) fn replica_start_offsets(&self) -> Vec<Offset> {
        self.replicas
            .iter()
            .map(|replica| replica.log.lock().log_start_offset())
            .collect()
    }

    pub(crate) async fn replicate_and_sync(
        &self,
        source: &Arc<Mutex<Log>>,
        target: Offset,
    ) -> Result<Offset, BrokerError> {
        let committed = self.durable_watermark();
        if target <= committed {
            return Ok(committed);
        }
        let source = ShardLog::new(source.clone());
        let source_end = Offset(source.end_offset());
        if target > source_end {
            return Err(BrokerError::Replication(format!(
                "wal source ends at {}, before requested durable offset {}",
                source_end.0, target.0
            )));
        }

        if self.distributed_required.load(Ordering::Acquire) {
            let configured = self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_some();
            if !configured {
                return Err(BrokerError::Replication(
                    "diskless WAL broker placement is not available".into(),
                ));
            }
            sync_replica(source.clone(), &[]).await?;
            self.local_durable.fetch_max(target.0, Ordering::AcqRel);
            let me = self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .as_ref()
                .map(|quorum| quorum.me)
                .ok_or_else(|| {
                    BrokerError::Replication("diskless WAL broker placement disappeared".into())
                })?;
            let log_start = source.lock().log_start_offset();
            self.record_durable_offset(me, target, log_start, source_end);
            loop {
                if self.durable_watermark() >= target {
                    return Ok(self.durable_watermark());
                }
                let advanced = self.durable_advanced.notified();
                if self.durable_watermark() >= target {
                    return Ok(self.durable_watermark());
                }
                if self
                    .distributed
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_none()
                {
                    return Err(BrokerError::Replication(
                        "diskless WAL broker placement disappeared".into(),
                    ));
                }
                advanced.await;
            }
        }

        let mut synced = 0usize;
        for replica in &self.replicas {
            if !replica.alive.load(Ordering::Acquire) {
                continue;
            }
            let replica_end = replica_end_offset(replica);
            let Ok(batches) = read_batches_exact(&source, replica_end.min(target), target) else {
                continue;
            };
            if sync_replica(replica.log.clone(), &batches).await.is_ok() {
                synced += 1;
            }
        }
        let required = strict_majority(self.expected_voters);
        if synced < required {
            return Err(BrokerError::Replication(format!(
                "wal quorum has {synced} synced replicas, needs {required}"
            )));
        }
        self.durable_watermark.store(target.0, Ordering::Release);
        Ok(target)
    }

    pub(crate) fn serve_fetch(
        &self,
        fetch_offset: Offset,
        max_size: ByteSize,
    ) -> Result<WalFetchData, BrokerError> {
        let replica = self
            .replicas
            .iter()
            .find(|replica| replica.alive.load(Ordering::Acquire))
            .ok_or_else(|| {
                BrokerError::Replication("wal quorum has no live fetch replica".into())
            })?;
        let log = replica.log.lock();
        let log_start_offset = log.log_start_offset();
        let log_end_offset = log.log_end_offset();
        let offset_out_of_range = fetch_offset < log_start_offset || fetch_offset > log_end_offset;
        let records = if offset_out_of_range
            || fetch_offset == log_end_offset
            || max_size == ByteSize::ZERO
        {
            Bytes::new()
        } else {
            // A WAL follower must receive the leader's uncommitted tail and
            // fsync it before that follower can acknowledge the range. Limiting
            // this read to the current high watermark creates a deadlock: no
            // follower can fetch the bytes needed to advance the watermark.
            log.read_raw(fetch_offset, log_end_offset, max_size)?.bytes
        };
        Ok(WalFetchData {
            high_watermark: self.durable_watermark(),
            log_end_offset,
            log_start_offset,
            records,
            offset_out_of_range,
        })
    }

    pub(crate) async fn trim_to_offset(
        &self,
        source: &Arc<Mutex<Log>>,
        new_start: Offset,
    ) -> Result<Offset, BrokerError> {
        let source_replica = self
            .replicas
            .first()
            .ok_or_else(|| BrokerError::Replication("wal quorum has no source replica".into()))?;
        if !source_replica.log.shares_log(source) {
            return Err(BrokerError::Replication(
                "wal quorum source is not its first replica".into(),
            ));
        }
        if self.distributed_required.load(Ordering::Acquire) {
            if self
                .distributed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_none()
            {
                return Err(BrokerError::Replication(
                    "diskless WAL broker placement is not available".into(),
                ));
            }
            return trim_log(ShardLog::new(source.clone()), new_start).await;
        }
        // Trim replica copies before the partition source. If one copy fails,
        // the source remains available and a later flusher tick can retry.
        for replica in &self.replicas[1..] {
            trim_log(replica.log.clone(), new_start).await?;
        }
        trim_log(ShardLog::new(source.clone()), new_start).await
    }
}

fn replica_end_offset(replica: &WalReplica) -> Offset {
    Offset(replica.log.end_offset())
}

#[derive(Debug, Clone)]
pub(super) struct BatchBytes {
    pub(super) base_offset: Offset,
    pub(super) last_offset: Offset,
    pub(super) verbatim: VerbatimBatch,
}
