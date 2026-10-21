use {
    crate::{
        codes,
        coordinator::partitioner::partition_for_group,
        error::BrokerError,
        metadata_source::MetadataSource,
        partition::{
            Partition, ProduceData, ProduceJob, ProducerAppendCheck, Uncommitted, WriterMessage,
        },
        partition_registry::PartitionRegistry,
    },
    async_trait::async_trait,
    futures_util::future::BoxFuture,
    krabka_ids::{LeaderEpoch, Offset, PartitionIndex},
    krabka_metadata::{MetadataImage, NodeId},
    krabka_protocol::records::RecordBatch,
    std::{sync::Arc, time::Duration},
    tokio::sync::{oneshot, watch},
};

#[cfg(test)]
mod tests;

pub const OFFSETS_TOPIC: &str = "__consumer_offsets";

/// How long a group coordinator write may wait for the high watermark to
/// cover it: the default of Kafka's `offsets.commit.timeout.ms`, which
/// `GroupCoordinatorService` passes to every `scheduleWriteOperation`.
const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long one window of [`LeaderAppend::committed_whenever`] waits before
/// it checks again.
const UNBOUNDED_WAIT_WINDOW: Duration = Duration::from_hours(1);

#[async_trait]
pub trait OffsetsLog: Send + Sync + std::fmt::Debug {
    /// Append `batch` to the `__consumer_offsets` partition of `group_id`, and
    /// return once it is in this broker's log, with the write's commit still
    /// to come.
    ///
    /// This is the append half of a `CoordinatorRuntime` write: the runtime
    /// applies the write to the shard's state as soon as it is appended, and
    /// completes the operation only when the high watermark passes it. A
    /// caller that must not wait for the second half in the meantime awaits
    /// the [`PendingWrite`] elsewhere.
    ///
    /// # Errors
    ///
    /// The errors of [`append_as_leader`] for the production log.
    async fn append_pending(
        &self,
        group_id: &str,
        batch: RecordBatch,
    ) -> Result<PendingWrite, BrokerError>;

    /// Append `batch` and wait until the write is committed, or until its
    /// write timeout passes.
    ///
    /// # Errors
    ///
    /// The errors of [`append_pending`](Self::append_pending), and those of
    /// [`PendingWrite::committed_in_time`].
    async fn append(&self, group_id: &str, batch: RecordBatch) -> Result<(), BrokerError> {
        self.append_pending(group_id, batch)
            .await?
            .committed_in_time()
            .await
    }
}

/// A group coordinator write that is in the log of its `__consumer_offsets`
/// partition and that may not be committed yet.
///
/// Its two futures borrow nothing, so they may be awaited outside the turn
/// that appended the write.
#[derive(derive_more::Debug)]
pub struct PendingWrite {
    /// The `__consumer_offsets` partition the write is in.
    pub partition: i32,
    /// The offset after the write's last record. The high watermark covers
    /// the write once it reaches this offset.
    pub end_offset: i64,
    /// The leader epoch the write was appended under. Two writes of one epoch
    /// sit in the log in the order they were appended, so the commit of the
    /// later one proves the earlier one committed too.
    pub leader_epoch: i32,
    /// Completes when the high watermark covers the write while the epoch
    /// still holds, and fails with `NOT_COORDINATOR` when the term ends
    /// first. It has no deadline of its own: a write whose client already
    /// heard its timeout may still commit, and Kafka shows it once it does.
    #[debug(skip)]
    pub committed: BoxFuture<'static, Result<(), BrokerError>>,
    /// Completes when the write timeout passes: the default of Kafka's
    /// `offsets.commit.timeout.ms`, after which `CoordinatorRuntime` fails
    /// the operation and the client hears `COORDINATOR_NOT_AVAILABLE`.
    #[debug(skip)]
    pub deadline: BoxFuture<'static, ()>,
}

impl PendingWrite {
    /// Wait until the write is committed, or fail when its deadline passes
    /// first.
    ///
    /// # Errors
    ///
    /// The errors of [`committed`](Self::committed), and
    /// [`BrokerError::CoordinatorWriteUncommitted`] with
    /// `COORDINATOR_NOT_AVAILABLE` when the deadline passes first.
    pub async fn committed_in_time(self) -> Result<(), BrokerError> {
        tokio::select! {
            biased;
            result = self.committed => result,
            () = self.deadline => Err(uncommitted(self.partition, codes::COORDINATOR_NOT_AVAILABLE)),
        }
    }
}

/// Resolves the group-id's live `__consumer_offsets` partition at every
/// append. Partitions are registered by bootstrap after `GroupCoordinator`
/// construction, so the registry is intentionally consulted lazily.
///
/// An append completes only when the write is committed, as a write of
/// Kafka's `CoordinatorRuntime` does: this broker leads the partition when it
/// appends, and the high watermark covers the batch while this broker still
/// leads the partition at the same leader epoch. A group coordinator that
/// answered before that point could hand a member an epoch that the next
/// leader of the partition never sees.
#[derive(derive_more::Debug)]
pub(crate) struct ProductionOffsetsLog {
    #[debug(skip)]
    partitions: Arc<PartitionRegistry>,
    #[debug(skip)]
    controller: Arc<dyn MetadataSource>,
    node_id: NodeId,
}

impl ProductionOffsetsLog {
    #[must_use]
    pub(crate) fn new(
        partitions: Arc<PartitionRegistry>,
        controller: Arc<dyn MetadataSource>,
        node_id: NodeId,
    ) -> Self {
        Self {
            partitions,
            controller,
            node_id,
        }
    }
}

#[async_trait]
impl OffsetsLog for ProductionOffsetsLog {
    async fn append_pending(
        &self,
        group_id: &str,
        batch: RecordBatch,
    ) -> Result<PendingWrite, BrokerError> {
        let partition_id = partition_for_group(&self.controller.current_image(), group_id);
        let append = append_as_leader(
            (&self.partitions, &*self.controller, self.node_id),
            partition_id,
            batch,
            None,
        )
        .await?;
        Ok(PendingWrite {
            partition: partition_id,
            end_offset: append.end_offset.0,
            leader_epoch: append.term.epoch.get(),
            committed: Box::pin(append.committed_whenever()),
            deadline: Box::pin(tokio::time::sleep(WRITE_TIMEOUT)),
        })
    }
}

/// A group coordinator write that is in this broker's log of its
/// `__consumer_offsets` partition, and that may not be committed yet.
///
/// Kafka's `CoordinatorRuntime` applies a write to the shard's state when it
/// appends it, and completes the operation only once the high watermark
/// passes it. A caller that has state to apply at the append does so between
/// [`append_as_leader`] and [`Self::committed`].
#[derive(derive_more::Debug)]
pub(crate) struct LeaderAppend {
    /// The offset the writer assigned to the batch.
    pub(crate) base_offset: Offset,
    #[debug(skip)]
    partition: Arc<Partition>,
    #[debug(skip)]
    images: watch::Receiver<Arc<MetadataImage>>,
    term: LedTerm,
    end_offset: Offset,
}

impl LeaderAppend {
    /// Wait until the write is committed under the term it was appended in.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::CoordinatorWriteUncommitted`] with
    /// `NOT_COORDINATOR` when this broker stops leading the partition first,
    /// and with `COORDINATOR_NOT_AVAILABLE` when the write times out.
    pub(crate) async fn committed(mut self) -> Result<(), BrokerError> {
        await_committed(
            &self.partition,
            &mut self.images,
            self.term,
            self.end_offset,
            WRITE_TIMEOUT,
        )
        .await
    }

    /// Wait until the write is committed under the term it was appended in,
    /// however long that takes.
    ///
    /// The wait runs in [`UNBOUNDED_WAIT_WINDOW`] windows, so a partition
    /// that does not replicate logs its stall once per window.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::CoordinatorWriteUncommitted`] with
    /// `NOT_COORDINATOR` when this broker stops leading the partition first.
    pub(crate) async fn committed_whenever(mut self) -> Result<(), BrokerError> {
        loop {
            let window = await_committed(
                &self.partition,
                &mut self.images,
                self.term,
                self.end_offset,
                UNBOUNDED_WAIT_WINDOW,
            )
            .await;
            match window {
                Err(BrokerError::CoordinatorWriteUncommitted {
                    code: codes::COORDINATOR_NOT_AVAILABLE,
                    ..
                }) => {}
                outcome => return outcome,
            }
        }
    }
}

/// Append `batch` to `partition_id` of `__consumer_offsets` as its leader, as
/// Kafka's `CoordinatorPartitionWriter.append` does: only while this broker
/// leads the partition, and with the leader epoch stamped on the batch.
///
/// `(partitions, controller, node_id)` are the local partitions, the metadata
/// that names the leader, and this broker. `producer_check` is the
/// transactional check the log runs under its append lock, for a write that
/// carries a producer's transactional records.
///
/// # Errors
///
/// Returns [`BrokerError::CoordinatorWriteUncommitted`] with
/// `NOT_COORDINATOR` when another broker leads the partition,
/// [`BrokerError::PartitionWriterDied`] when the partition has no live writer
/// here, and the writer's own error when the log refuses the batch.
pub(crate) async fn append_as_leader(
    (partitions, controller, node_id): (&PartitionRegistry, &dyn MetadataSource, NodeId),
    partition_id: i32,
    mut batch: RecordBatch,
    producer_check: Option<ProducerAppendCheck>,
) -> Result<LeaderAppend, BrokerError> {
    // Subscribe before the leadership check, so the commit wait sees every
    // image after the one the check read.
    let mut images = controller.watch_image();
    let image = images.borrow_and_update().clone();
    // Kafka's `CoordinatorPartitionWriter` appends as the partition leader
    // only, and the runtime refuses an operation on a shard that it does not
    // own with `NOT_COORDINATOR`.
    let Some(epoch) = led_epoch(&image, partition_id, node_id) else {
        return Err(uncommitted(partition_id, codes::NOT_COORDINATOR));
    };
    let writer_died = || BrokerError::PartitionWriterDied {
        topic: OFFSETS_TOPIC.into(),
        partition: partition_id,
    };
    let Some(partition) = partitions.get(OFFSETS_TOPIC, PartitionIndex(partition_id)) else {
        return Err(writer_died());
    };
    // `UnifiedLog.appendAsLeader` stamps the leader epoch on the batch.
    // The partition writer does not stamp an owned batch.
    batch.partition_leader_epoch = epoch.get();
    let end_offset = i64::from(batch.last_offset_delta) + 1;
    let (ack_tx, ack_rx) = oneshot::channel();
    if partition
        .writer_tx
        .send(WriterMessage::Produce(ProduceJob {
            data: ProduceData::Owned(batch),
            ack: ack_tx,
            producer_check,
        }))
        .await
        .is_err()
    {
        return Err(writer_died());
    }
    let appended = match ack_rx.await {
        Ok(Ok(appended)) => appended,
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err(writer_died()),
    };
    Ok(LeaderAppend {
        base_offset: appended.base_offset,
        end_offset: Offset(appended.base_offset.0 + end_offset),
        partition,
        images,
        term: LedTerm {
            partition: partition_id,
            node_id,
            epoch,
        },
    })
}

/// The leader epoch at which `node_id` leads `partition` of
/// `__consumer_offsets` in `image`, or `None` when another broker leads it or
/// the image does not hold it.
fn led_epoch(image: &MetadataImage, partition: i32, node_id: NodeId) -> Option<LeaderEpoch> {
    image
        .partition(OFFSETS_TOPIC, partition)
        .filter(|record| record.leader == node_id)
        .map(|record| record.leader_epoch)
}

/// One leadership term of a `__consumer_offsets` partition on this broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LedTerm {
    partition: i32,
    node_id: NodeId,
    epoch: LeaderEpoch,
}

impl LedTerm {
    /// Whether `image` still names this broker leader at the same epoch.
    ///
    /// A new epoch fails the term even when it names this broker again. The
    /// image watch keeps only the newest image, so a leadership that moved
    /// away and came back between two reads could have truncated the write.
    fn holds_in(self, image: &MetadataImage) -> bool {
        led_epoch(image, self.partition, self.node_id) == Some(self.epoch)
    }
}

/// Wait until the high watermark of `partition` reaches `end_offset`, the
/// offset after the appended batch, while `term` holds.
///
/// This is the deferred completion of a `CoordinatorRuntime` write: the
/// runtime completes the operation when the high watermark passes the
/// write, fails it with `NOT_COORDINATOR` when the shard unloads because the
/// broker lost the leadership, and fails it with a timeout that
/// `CoordinatorOperationExceptionHelper` answers as `COORDINATOR_NOT_AVAILABLE`.
/// The high watermark alone is not enough: a former leader that follows the
/// new one can see its high watermark pass `end_offset` over records that
/// replaced the write. The term is the image's, see
/// [`Partition::await_committed_while`].
async fn await_committed(
    partition: &Partition,
    images: &mut watch::Receiver<Arc<MetadataImage>>,
    term: LedTerm,
    end_offset: Offset,
    timeout: Duration,
) -> Result<(), BrokerError> {
    partition
        .await_committed_while(
            end_offset,
            std::time::Instant::now() + timeout,
            Some(images),
            |_, image| image.is_some_and(|image| term.holds_in(image)),
        )
        .await
        .map_err(|failure| match failure {
            Uncommitted::TermEnded => uncommitted(term.partition, codes::NOT_COORDINATOR),
            Uncommitted::TimedOut => uncommitted(term.partition, codes::COORDINATOR_NOT_AVAILABLE),
        })
}

fn uncommitted(partition: i32, code: i16) -> BrokerError {
    BrokerError::CoordinatorWriteUncommitted { partition, code }
}

/// The error code of a group write that failed.
///
/// A write that is not committed carries the answer of Kafka's
/// `CoordinatorOperationExceptionHelper` for it: `NOT_COORDINATOR` after a
/// lost leadership, so that the member looks the coordinator up again, and
/// `COORDINATOR_NOT_AVAILABLE` after a timeout. Any other failure answers
/// `COORDINATOR_LOAD_IN_PROGRESS`: the member retries here, and a new actor
/// serves the retry from the last committed state of the group.
pub(crate) fn write_failure_code(error: &BrokerError) -> i16 {
    match error {
        BrokerError::CoordinatorWriteUncommitted { code, .. } => *code,
        _ => codes::COORDINATOR_LOAD_IN_PROGRESS,
    }
}

pub mod fake {
    use {
        super::{BrokerError, OFFSETS_TOPIC, OffsetsLog, PendingWrite, async_trait},
        crate::coordinator::unified::persistence_next_gen::NextGenKey,
        krabka_protocol::records::RecordBatch,
        std::sync::{Arc, PoisonError},
        tokio::sync::{Mutex, Notify},
    };

    /// Which classic metadata operation a test expects in the append log.
    #[derive(Clone, Copy)]
    pub enum ClassicMetadataRecord {
        Write,
        Tombstone,
    }

    #[derive(Debug, Default)]
    pub struct InMemoryOffsetsLog {
        pub appended: Mutex<Vec<RecordBatch>>,
        pub fail_next: std::sync::atomic::AtomicBool,
        /// The error the next append fails with, ahead of `fail_next`.
        pub fail_next_with: std::sync::Mutex<Option<BrokerError>>,
        /// The simulated high watermark, which every append moves past itself
        /// unless a test holds it back.
        commit_gate: Arc<CommitGate>,
    }

    /// The simulated replication of [`InMemoryOffsetsLog`]. Each appended
    /// batch takes one offset, so a batch's end offset is the log's length
    /// after it.
    #[derive(Debug, Default)]
    struct CommitGate {
        state: std::sync::Mutex<GateState>,
        changed: Notify,
    }

    #[derive(Debug, Default)]
    struct GateState {
        /// While `true`, an append does not move the high watermark.
        held: bool,
        log_end: i64,
        high_watermark: i64,
        /// The write timeout has passed for every write that ends at or below
        /// it.
        timed_out_through: i64,
    }

    impl CommitGate {
        fn update(&self, change: impl FnOnce(&mut GateState)) {
            change(&mut self.state.lock().unwrap_or_else(PoisonError::into_inner));
            self.changed.notify_waiters();
        }

        /// Waits until `reached` holds of the gate's state.
        async fn until(&self, reached: impl Fn(&GateState) -> bool) {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if reached(&self.state.lock().unwrap_or_else(PoisonError::into_inner)) {
                    return;
                }
                changed.await;
            }
        }
    }

    #[async_trait]
    impl OffsetsLog for InMemoryOffsetsLog {
        async fn append_pending(
            &self,
            _group_id: &str,
            batch: RecordBatch,
        ) -> Result<PendingWrite, BrokerError> {
            let failure = self
                .fail_next_with
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take();
            if let Some(error) = failure {
                return Err(error);
            }
            if self
                .fail_next
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                return Err(BrokerError::PartitionWriterDied {
                    topic: OFFSETS_TOPIC.into(),
                    partition: 0,
                });
            }
            let mut appended = self.appended.lock().await;
            appended.push(batch);
            let end_offset = i64::try_from(appended.len()).unwrap_or(i64::MAX);
            drop(appended);
            self.commit_gate.update(|state| {
                state.log_end = end_offset;
                if !state.held {
                    state.high_watermark = end_offset;
                }
            });
            let committed = Arc::clone(&self.commit_gate);
            let deadline = Arc::clone(&self.commit_gate);
            Ok(PendingWrite {
                partition: 0,
                end_offset,
                leader_epoch: 0,
                committed: Box::pin(async move {
                    committed
                        .until(|state| state.high_watermark >= end_offset)
                        .await;
                    Ok(())
                }),
                deadline: Box::pin(async move {
                    deadline
                        .until(|state| state.timed_out_through >= end_offset)
                        .await;
                }),
            })
        }
    }

    impl InMemoryOffsetsLog {
        /// Keep the high watermark where it is: later appends stay in the log
        /// uncommitted, as with a follower that stopped replicating.
        pub fn hold_commits(&self) { self.commit_gate.update(|state| state.held = true); }

        /// Move the high watermark to the end of the log and stop holding it.
        pub fn release_commits(&self) {
            self.commit_gate.update(|state| {
                state.held = false;
                state.high_watermark = state.log_end;
            });
        }

        /// Pass the write timeout of every write in the log, while the records
        /// stay in the log and may still commit.
        pub fn time_out_uncommitted(&self) {
            self.commit_gate
                .update(|state| state.timed_out_through = state.log_end);
        }

        /// Refuse the next append with the selected error, or the default failure.
        ///
        /// # Panics
        ///
        /// Panics if the failure mutex is poisoned.
        pub fn fail_next_append(&self, failure: Option<BrokerError>) {
            match failure {
                Some(error) => *self.fail_next_with.lock().expect("not poisoned") = Some(error),
                None => self
                    .fail_next
                    .store(true, std::sync::atomic::Ordering::SeqCst),
            }
        }

        pub async fn batches(&self) -> Vec<RecordBatch> { self.appended.lock().await.clone() }

        /// Snapshot the records grouped by their original append batch.
        pub async fn record_batches(&self) -> Vec<Vec<krabka_protocol::records::Record>> {
            self.batches()
                .await
                .into_iter()
                .map(|batch| batch.records)
                .collect()
        }

        /// Returns `true` if and only if an appended record tombstones the
        /// classic k2 `GroupMetadata` record for `group_id`. Such a record has
        /// a key whose leading `i16` version is `2`, for classic
        /// `GroupMetadata`, and a null value. Tests read it to assert that the
        /// upgrade flip removed the classic group record atomically.
        pub async fn has_classic_group_metadata_tombstone(&self, group_id: &str) -> bool {
            self.has_classic_group_metadata_record(group_id, ClassicMetadataRecord::Tombstone)
                .await
        }

        /// Match a classic metadata key and its expected value presence.
        pub async fn has_classic_group_metadata_record(
            &self,
            group_id: &str,
            operation: ClassicMetadataRecord,
        ) -> bool {
            use crate::coordinator::unified::persistence::{Key, parse_key};
            self.appended.lock().await.iter().any(|batch| {
                batch.records.iter().any(|record| {
                    let value_matches = match operation {
                        ClassicMetadataRecord::Write => record.value.is_some(),
                        ClassicMetadataRecord::Tombstone => record.value.is_none(),
                    };
                    value_matches && record.key.as_ref().is_some_and(|key| {
                        matches!(parse_key(key), Ok(Key::GroupMetadata { group_id: ref id }) if id == group_id)
                    })
                })
            })
        }

        /// Returns `true` if and only if an appended record tombstones the
        /// next-gen k3 `GroupMetadata` record for `group_id`. Such a record has
        /// a key whose leading `i16` version is `3`, for next-gen
        /// `GroupMetadata`, and a null value. Tests read it to assert that the
        /// downgrade flip removed the next-gen group record atomically.
        /// `parse_key` dispatches version 3 to the next-gen family.
        pub async fn has_next_gen_group_metadata_tombstone(&self, group_id: &str) -> bool {
            use crate::coordinator::unified::persistence_next_gen::NextGenKey;
            self.has_next_gen_tombstone(&NextGenKey::GroupMetadata {
                group_id: group_id.into(),
            })
            .await
        }

        /// Returns `true` if and only if an appended record tombstones the
        /// group-level next-gen k6 `TargetAssignmentMetadata` record for
        /// `group_id`. Such a record has a key whose leading `i16` version is
        /// `6`, and a null value.
        ///
        /// Tests read it to assert that the downgrade flip also drops the
        /// group-level target metadata. That metadata would otherwise survive
        /// log compaction and bring the group back as next-gen on replay.
        /// `parse_key` dispatches version 6 to the next-gen family.
        pub async fn has_next_gen_target_metadata_tombstone(&self, group_id: &str) -> bool {
            use crate::coordinator::unified::persistence_next_gen::NextGenKey;
            self.has_next_gen_tombstone(&NextGenKey::TargetAssignmentMetadata {
                group_id: group_id.into(),
            })
            .await
        }

        async fn has_next_gen_tombstone(&self, expected: &NextGenKey) -> bool {
            use crate::coordinator::unified::persistence::{Key, parse_key};
            self.appended
                .lock()
                .await
                .iter()
                .flat_map(|batch| &batch.records)
                .any(|record| {
                    record.value.is_none() && record.key.as_ref().is_some_and(|key| {
                    matches!(parse_key(key), Ok(Key::NextGen(found)) if &found == expected)
                })
                })
        }
    }
}
