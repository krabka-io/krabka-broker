//! Command messages for a partition's single writer task, with the payload and
//! outcome types they carry. They live apart from the partition handle because
//! a request handler builds one without holding that handle.

use {
    crate::error::BrokerError,
    bytes::Bytes,
    krabka_compression::CompressionType,
    krabka_log::{LeaderEpoch, Log, LogConfig, Offset, ProducerId, VerbatimBatch},
    krabka_protocol::records::RecordBatch,
    std::{
        path::PathBuf,
        sync::{Arc, Mutex},
    },
    tokio::sync::oneshot,
};

/// The records to append for a single produce job. This is either the
/// producer's verbatim wire bytes (zero-copy passthrough fast path) or a fully
/// owned, decoded [`RecordBatch`]. The broker takes the fallback path when
/// passthrough is unsafe: recompression, legacy up-conversion, control
/// batches, and similar cases.
///
/// The `Owned` arm is a complete fallback, so the whole verbatim
/// passthrough feature is easy to revert. An "always construct `Owned`" rule
/// restores the previous behavior.
#[derive(Debug)]
pub enum ProduceData {
    /// Append the producer's exact wire bytes, and patch only `base_offset`
    /// and `partition_leader_epoch`. No decode, re-encode, recompress, or CRC.
    Verbatim(VerbatimBatch),
    /// Decode and re-encode the owned batch on append (the original path).
    /// The writer mutates `base_offset` before append.
    Owned(RecordBatch),
    /// An owned client batch together with the bytes the produce size gates
    /// already encoded it into.
    ///
    /// The writer appends `encoded` as it would a verbatim batch when the
    /// codec and compression level in force under the log lock are the ones
    /// `encoded` was made with, and otherwise falls back to [`Self::Owned`]
    /// and encodes `batch` afresh. Either way the stored bytes are the same.
    /// See [`Self::resolve_for_append`].
    OwnedEncoded {
        batch: RecordBatch,
        /// Boxed so the variant stays no larger than [`Self::Owned`] beside
        /// a pointer, and a writer job's size does not grow with it.
        encoded: Box<EncodedBatch>,
    },
    /// An internally built control batch, such as a transaction ABORT marker
    /// or a barrier marker.
    ///
    /// The writer appends it without the compression rewrite that
    /// [`Self::Owned`] gets. Kafka never compresses a control batch that
    /// arrived uncompressed, and a control batch holds one small record, so
    /// the rewrite would both diverge from Kafka and buy nothing.
    OwnedControl(RecordBatch),
    /// An internally built COMMIT marker plus the cross-domain coordinator's
    /// commit stamp. The stamp is written only to `.stampindex`; it never
    /// enters the Kafka batch bytes.
    OwnedCommitMarker {
        batch: RecordBatch,
        commit_stamp: u64,
    },
}

/// An owned batch encoded ahead of its append, with the codec and the
/// compression level the encode used.
///
/// The produce size gates have to encode an owned batch to measure it, since
/// only an encode can say how large a compressed batch is. This keeps what
/// they encoded, so the second gate and the append need not encode it again.
/// The bytes hold whatever `base_offset` and `partition_leader_epoch` the batch
/// carried when it was encoded; both sit outside the CRC region, and the
/// verbatim append patches both.
#[derive(Debug, Clone)]
pub struct EncodedBatch {
    codec: CompressionType,
    level: Option<i32>,
    bytes: Bytes,
}

impl EncodedBatch {
    /// Bytes that `batch.encode_with_compression_level(level)` writes for
    /// `batch` as it is, under its own codec.
    pub(crate) fn encode(
        batch: &RecordBatch,
        level: Option<i32>,
    ) -> Result<Self, krabka_protocol::records::RecordsError> {
        let mut buf = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode_with_compression_level(&mut buf, level)?;
        // The buffer is sized for the uncompressed batch. A compressed encode
        // fills a fraction of it, and the bytes are held until the append, so
        // they are copied down to their length rather than keep the rest.
        let bytes = if buf.len() < buf.capacity() {
            Bytes::copy_from_slice(&buf)
        } else {
            buf.freeze()
        };
        Ok(Self {
            codec: batch.attributes.compression(),
            level,
            bytes,
        })
    }

    /// Whether these bytes are what encoding under `codec` at `level` writes.
    pub(crate) fn encoded_with(&self, codec: CompressionType, level: Option<i32>) -> bool {
        self.codec == codec && self.level == level
    }

    /// The encoded length.
    pub(crate) fn len(&self) -> usize { self.bytes.len() }

    /// The encoded bytes, for tests that tell a reused encode from a new one.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> &Bytes { &self.bytes }
}

/// `bytes`, the wire encoding of `batch`, with the header fields the log's
/// verbatim append reads copied from `batch` rather than parsed back out.
pub(crate) fn verbatim_of(batch: &RecordBatch, bytes: Bytes) -> VerbatimBatch {
    VerbatimBatch {
        bytes,
        last_offset_delta: batch.last_offset_delta,
        max_timestamp: batch.max_timestamp,
        leader_epoch: LeaderEpoch(batch.partition_leader_epoch),
        producer_id: ProducerId(batch.producer_id),
        producer_epoch: batch.producer_epoch,
        base_sequence: batch.base_sequence,
        is_transactional: batch.attributes.is_transactional(),
    }
}

/// `batch` with its codec set to the topic's `compression.type`, when that
/// names one. `None` is `compression.type=producer`.
fn with_target_compression(batch: &mut RecordBatch, target: Option<CompressionType>) {
    if let Some(target) = target
        && batch.attributes.compression() != target
    {
        batch.attributes = batch.attributes.with_compression(target);
    }
}

impl ProduceData {
    /// The job the writer appends under `config`, the log's config read under
    /// the append's own lock.
    ///
    /// An owned client batch takes the partition's configured compression.
    /// Control batches and verbatim producer bytes keep their original shape.
    ///
    /// An [`Self::OwnedEncoded`] job whose bytes were encoded under the codec
    /// and level `config` now asks for becomes a [`Self::Verbatim`] job over
    /// those bytes: the log then stamps the base offset, the leader epoch and,
    /// on a `LogAppendTime` partition, the log-append time into them exactly
    /// as its owned append would before encoding, and stores the same bytes.
    /// A `compression.type` or level that changed since the gates encoded the
    /// batch drops the bytes and encodes the batch afresh under the new one.
    #[must_use]
    pub(crate) fn resolve_for_append(self, config: &LogConfig) -> Self {
        let target = config.compression_type;
        match self {
            Self::Owned(mut batch) => {
                with_target_compression(&mut batch, target);
                Self::Owned(batch)
            }
            Self::OwnedEncoded { mut batch, encoded } => {
                with_target_compression(&mut batch, target);
                let codec = batch.attributes.compression();
                if encoded.encoded_with(codec, config.compression_level(codec)) {
                    Self::Verbatim(verbatim_of(&batch, encoded.bytes))
                } else {
                    Self::Owned(batch)
                }
            }
            other
            @ (Self::Verbatim(_) | Self::OwnedControl(_) | Self::OwnedCommitMarker { .. }) => other,
        }
    }

    /// The producer ID of an internally built control batch, or `None` for a
    /// data batch.
    ///
    /// The writer reads the log's entry for this producer after the append,
    /// because a marker can change the producer epoch and sequence state that
    /// the produce path checks.
    #[must_use]
    pub(crate) fn control_producer_id(&self) -> Option<krabka_log::ProducerId> {
        match self {
            Self::Verbatim(_) | Self::Owned(_) | Self::OwnedEncoded { .. } => None,
            Self::OwnedControl(batch) | Self::OwnedCommitMarker { batch, .. } => {
                Some(krabka_log::ProducerId(batch.producer_id))
            }
        }
    }

    #[must_use]
    pub(crate) fn record_count(&self) -> u32 {
        match self {
            Self::Verbatim(batch) => u32::try_from(batch.last_offset_delta + 1)
                .expect("verbatim batch offset count is non-negative"),
            Self::Owned(batch) | Self::OwnedEncoded { batch, .. } => {
                u32::try_from(batch.last_offset_delta + 1)
                    .expect("owned batch offset count is non-negative")
            }
            Self::OwnedControl(batch) => u32::try_from(batch.last_offset_delta + 1)
                .expect("control batch offset count is non-negative"),
            Self::OwnedCommitMarker { batch, .. } => u32::try_from(batch.last_offset_delta + 1)
                .expect("commit marker offset count is non-negative"),
        }
    }

    /// The bytes this job appends and the newest timestamp it carries, which
    /// decide whether its append rolls the segment. An owned batch is encoded
    /// again on append, and a recompression can change its size, so its size
    /// here is an estimate. One the produce size gates already encoded is
    /// estimated the same way: the writer drops those bytes when the codec
    /// changed since, so their length can understate what it writes.
    pub(crate) fn append_footprint(&self) -> (usize, i64) {
        match self {
            Self::Verbatim(batch) => (batch.bytes.len(), batch.max_timestamp),
            Self::Owned(batch)
            | Self::OwnedEncoded { batch, .. }
            | Self::OwnedControl(batch)
            | Self::OwnedCommitMarker { batch, .. } => (batch.encoded_len(), batch.max_timestamp),
        }
    }
}

/// What the writer reports back for one appended batch.
///
/// This is Kafka's `LogAppendInfo` narrowed to the two values a produce
/// response takes from the append itself: where the batch landed and, on a
/// `message.timestamp.type=LogAppendTime` partition, the broker clock the log
/// stamped into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedBatch {
    /// The offset the log assigned to the batch's first record.
    pub base_offset: Offset,
    /// Kafka's `LogAppendInfo.logAppendTime`, which the produce response
    /// carries as `logAppendTimeMs`. `Some(ms)` only on a `LogAppendTime`
    /// partition; `None` under `CreateTime`, which the response reports as
    /// the `-1` Kafka reports.
    pub log_append_time_ms: Option<i64>,
}

/// Produce-path message sent from the Produce handler to the partition's
/// writer task. The writer assigns `base_offset`, overwrites whatever the
/// handler put there, and replies with the assigned value.
#[derive(Debug)]
pub struct ProduceJob {
    /// The records to append (verbatim passthrough or owned fallback).
    pub data: ProduceData,
    /// Oneshot that the writer uses to report a successful append, or failure,
    /// back to the handler.
    pub ack: oneshot::Sender<Result<AppendedBatch, BrokerError>>,
    /// The producer transaction check the log runs just before it appends a
    /// client batch. `None` for a batch the broker built itself, such as a
    /// marker, an offset commit or a tombstone.
    pub producer_check: Option<ProducerAppendCheck>,
}

/// A client batch as the log's transaction check sees it, and the KIP-890
/// verification guard the produce path obtained for it. See
/// [`krabka_log::Log::check_transactional_append`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerAppendCheck {
    pub batch: krabka_log::TransactionalBatch,
    pub guard: krabka_log::VerificationGuard,
}

/// All message kinds the partition's writer task accepts.
///
/// The writer task is single-consumer over a single `mpsc::Sender`. An enum
/// here keeps replication appends ordered with produce appends.
#[derive(Debug)]
pub enum WriterMessage {
    /// Acknowledge after every earlier writer command has completed. This
    /// orders coordinator recovery after an append whose reply was abandoned.
    Barrier { ack: oneshot::Sender<()> },
    /// Append a batch and assign `base_offset` from the log. The `Produce`
    /// handler sends this message.
    Produce(ProduceJob),
    /// Make the log prefix ending at `leo` durable before acknowledging.
    /// Diskless partitions sync their WAL; ordinary partitions fsync the log.
    SyncDurable {
        leo: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Append a batch at the caller-supplied offset, which the partition's
    /// leader already assigned. The per-(topic, partition) replicator on a
    /// follower broker sends this message.
    Replicate {
        batch: RecordBatch,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Append a leader's already-encoded data batch without decoding or
    /// recompressing it. Control batches cannot use this arm because their
    /// marker record is needed for transaction bookkeeping.
    ///
    /// One message carries every data batch of a fetch response row, in
    /// offset order, so a follower pays one writer round trip per partition
    /// per fetch rather than one per batch.
    ReplicateVerbatim {
        batches: Vec<ReplicatedBatch>,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Truncate the log so no records at offset `>= offset` remain. Used
    /// by the replicator's `OFFSET_OUT_OF_RANGE` recovery path.
    Truncate {
        offset: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Drop every segment and recreate the active segment at `new_base`.
    /// The replicator's `OFFSET_OUT_OF_RANGE` recovery path sends this when
    /// the follower has fallen behind the leader's `log_start`. The
    /// follower must move its own `log_start` *forward* past records it
    /// never saw, and `Truncate` cannot do that.
    ResetTo {
        new_base: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Recalculate idle timers after the log's configuration was swapped.
    ConfigChanged,
    /// Run one compaction pass. The writer actor serializes this with
    /// appends to preserve the single-writer invariant on `Log`.
    Compact {
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Run one local-retention pass: the time-based segment roll, then the
    /// `retention.ms` and `retention.bytes` eviction of sealed segments. The
    /// writer actor serializes this with appends for the same reason
    /// [`WriterMessage::Compact`] is serialized -- both mutate the log.
    Retain {
        ack: tokio::sync::oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Trim from the start of the log: drop sealed segments whose last
    /// offset is `< new_start`, advance `log_start_offset` if `new_start`
    /// falls inside the active segment. Returns the resulting
    /// `log_start_offset`. That value can be less than `new_start` when
    /// `new_start` falls between segment boundaries, which is Kafka
    /// semantics. The `DeleteRecords` handler sends this message.
    TrimToOffset {
        new_start: Offset,
        ack: tokio::sync::oneshot::Sender<Result<Offset, BrokerError>>,
    },
    /// Take the leader's global log start as this follower's own, as
    /// [`krabka_log::Log::follow_leader_log_start`] describes. Returns the
    /// resulting `log_start_offset`. The replicator sends this message.
    FollowLeaderLogStart {
        leader_start: Offset,
        ack: tokio::sync::oneshot::Sender<Result<Offset, BrokerError>>,
    },
    /// Test-only: shift the in-memory `log_start_offset` and do not
    /// physically truncate segments. This simulates retention-driven
    /// truncation for the `out_of_range_truncates_and_recovers`
    /// replication integration test.
    #[cfg(any(test, feature = "test-helpers"))]
    TestSetLogStart {
        new_start: Offset,
        ack: oneshot::Sender<Result<(), BrokerError>>,
    },
    /// Atomically swap the partition's `Log` to a future log that has
    /// fully caught up. Sent by the KIP-113 move task in
    /// `future_log.rs` once `future_log.LEO == current_log.LEO`. The
    /// writer re-checks the invariant under its own lock, then:
    /// 1. drops the current `Log`,
    /// 2. `fs::rename`s `future_path` → `target_partition_path`,
    /// 3. removes the source partition directory,
    /// 4. re-opens `Log` at `target_partition_path` and stores it,
    /// 5. updates `Partition.log_dir` to `target_log_dir`.
    ///
    /// If the future log fell behind during the request hop, returns
    /// `Ok(SwapOutcome::NotCaughtUp)` so the caller can loop once more.
    SwapFutureLog {
        target_log_dir: PathBuf,
        future_log: Arc<Mutex<Log>>,
        future_path: PathBuf,
        target_partition_path: PathBuf,
        ack: oneshot::Sender<Result<SwapOutcome, BrokerError>>,
    },
    /// Report and forget the lowest offset that a [`WriterMessage::Truncate`]
    /// or [`WriterMessage::ResetTo`] cut the log to since the last such
    /// report, or `None` when the log was not cut.
    ///
    /// The KIP-113 move task sends it before each catch-up pass and cuts its
    /// future log to the answer, so the moved copy never keeps records that
    /// the current log dropped. It is Kafka's
    /// `ReplicaFetcherThread.truncate`, which calls
    /// `replicaAlterLogDirsManager.markPartitionsForTruncation` after it
    /// truncates the current log. A [`WriterMessage::SwapFutureLog`] answers
    /// `NotCaughtUp` while a cut is unreported, whatever the log ends say.
    TakeFutureTruncation {
        ack: oneshot::Sender<Option<Offset>>,
    },
}

/// Result of a [`WriterMessage::SwapFutureLog`] handling cycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwapOutcome {
    /// The swap succeeded. The partition now serves from the
    /// target log dir, and the broker removed the source dir.
    Swapped,
    /// The future log was behind the current log when the writer
    /// re-checked. The caller should resume replication and retry.
    NotCaughtUp,
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn commit_marker_record_count_uses_batch_offset_span() {
        let data = ProduceData::OwnedCommitMarker {
            batch: krabka_protocol::records::RecordBatch {
                last_offset_delta: 3,
                ..krabka_protocol::records::RecordBatch::default()
            },
            commit_stamp: 99,
        };

        check!(data.record_count() == 4);
    }

    /// A compressed encode holds only its own bytes until the append, not the
    /// buffer sized for the uncompressed batch it was written into, and its job
    /// still counts the owned batch toward the append's footprint.
    #[test]
    fn an_encoded_batch_holds_its_bytes_and_sizes_its_job_as_the_owned_batch() {
        let value = Bytes::from(vec![b'x'; 64 * 1024]);
        let batch = RecordBatch {
            attributes: krabka_protocol::records::Attributes::default()
                .with_compression(CompressionType::Gzip),
            records: vec![krabka_protocol::records::Record {
                value: Some(value),
                ..Default::default()
            }],
            ..RecordBatch::default()
        };

        let encoded = EncodedBatch::encode(&batch, None).expect("encode");
        let len = encoded.len();
        // The writer drops the bytes if the topic's codec changed since the
        // gates, so a job is sized as the owned batch it may encode again,
        // not by the compressed bytes it may never write.
        let job = ProduceData::OwnedEncoded {
            batch: batch.clone(),
            encoded: Box::new(encoded.clone()),
        };
        check!(job.append_footprint() == (batch.encoded_len(), batch.max_timestamp));
        drop(job);
        check!(len < batch.encoded_len() / 10);
        let held = encoded
            .bytes
            .try_into_mut()
            .expect("the only handle on its bytes");
        check!(held.capacity() == len);
    }
}

/// One leader-encoded data batch a follower appends, and the offset the
/// leader assigned it.
#[derive(Debug)]
pub struct ReplicatedBatch {
    /// The batch as the leader wrote it.
    pub batch: VerbatimBatch,
    /// The leader's base offset for the batch.
    pub base_offset: Offset,
}
