//! Completion of local mutations by the single cache consumer.
//!
//! A mutation publishes its event and then waits until the consumer pump has
//! applied the record at the acknowledged offset. The pump can consume that
//! record before the acknowledgement returns, so the publisher cannot simply
//! register its waiter afterwards. Holding a lock across the publish would
//! close that race, but would stall the pump for every metadata partition for
//! as long as one produce takes.
//!
//! Instead, [`MutationWaiters`] counts the publishes in flight on each
//! metadata partition. While a partition has one, the pump keeps the outcome
//! of each record it applies there that no waiter claimed, and the publisher
//! takes its outcome from those once it knows its offset. When the last
//! publish on a partition registers, the kept outcomes for it are dropped,
//! because no one can claim them any more. The lock is never held across an
//! `.await`.

use {
    super::{TopicBasedRemoteLogMetadataManager, assignment::ReadGate},
    crate::{error::MetadataLogError, partitioning::metadata_partition_for},
    bytes::Bytes,
    krabka_remote_storage::{RemoteStorageError, TopicIdPartition},
    std::{
        collections::{HashMap, hash_map::Entry},
        sync::{Mutex, MutexGuard},
    },
    tokio::sync::oneshot,
};

type Outcome = Result<(), RemoteStorageError>;
type MutationReply = oneshot::Sender<Outcome>;

/// A published record, named by the metadata partition and offset it landed at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct MutationKey {
    pub partition: i32,
    pub offset: i64,
}

/// The local mutations waiting for the pump, shared by publishers, the pump,
/// and the assignment reconciler.
#[derive(Default)]
pub(super) struct MutationWaiters {
    state: Mutex<WaiterState>,
    /// Held by the pump while it applies a record; see [`Self::stall_pump`].
    #[cfg(test)]
    stall: tokio::sync::Mutex<()>,
}

/// The bookkeeping behind [`MutationWaiters`], always under its lock.
#[derive(Default)]
struct WaiterState {
    /// Registered publishers, by the offset their record landed at.
    waiters: HashMap<MutationKey, MutationReply>,
    /// Publishes started on each metadata partition whose offset is not yet
    /// registered. A partition with none has no entry.
    in_flight: HashMap<i32, usize>,
    /// Outcomes the pump applied on a partition with a publish in flight and
    /// no waiter for the record yet.
    early: HashMap<MutationKey, Outcome>,
    /// How many times each metadata partition has been unassigned.
    removals: HashMap<i32, u64>,
}

impl WaiterState {
    fn removals(&self, partition: i32) -> u64 {
        self.removals.get(&partition).copied().unwrap_or_default()
    }

    /// End one in-flight publish on `partition`.
    fn end_publish(&mut self, partition: i32) {
        let Entry::Occupied(mut count) = self.in_flight.entry(partition) else {
            return;
        };
        *count.get_mut() -= 1;
        if *count.get() == 0 {
            count.remove();
            self.early.retain(|key, _| key.partition != partition);
        }
    }
}

impl MutationWaiters {
    fn state(&self) -> MutexGuard<'_, WaiterState> {
        self.state.lock().expect("mutation waiters mutex poisoned")
    }

    /// Start a publish on `partition` if `ready` admits it, checked under the
    /// lock so an unassignment cannot slip between the check and the count.
    fn begin_publish(
        &self,
        partition: i32,
        ready: impl FnOnce() -> bool,
    ) -> Result<InFlightPublish<'_>, RemoteStorageError> {
        let mut state = self.state();
        if !ready() {
            return Err(RemoteStorageError::NotReady { partition });
        }
        *state.in_flight.entry(partition).or_default() += 1;
        Ok(InFlightPublish {
            waiters: Some(self),
            partition,
            removals: state.removals(partition),
        })
    }

    /// Apply the consumed record at `key` with `apply` and deliver the
    /// outcome to its waiter, or keep it while a publish on its partition is
    /// in flight.
    ///
    /// `apply` and the delivery run together under the lock.
    pub(super) fn complete(&self, key: MutationKey, apply: impl FnOnce() -> Outcome) {
        let mut state = self.state();
        let outcome = apply();
        if let Some(reply) = state.waiters.remove(&key) {
            let _ = reply.send(outcome);
        } else if state.in_flight.contains_key(&key.partition) {
            // The first application is the record's outcome; a replay after
            // reassignment does not replace it.
            state.early.entry(key).or_insert(outcome);
        }
    }

    /// Stop waiting for the record at `key`.
    fn forget(&self, key: MutationKey) { self.state().waiters.remove(&key); }

    /// Run `unassign` for `partition` under the lock, then fail its waiters
    /// with `NotReady`. A publish still in flight on it fails the same way,
    /// unless the pump already applied its record.
    pub(super) fn remove_partition(&self, partition: i32, unassign: impl FnOnce()) {
        let mut state = self.state();
        unassign();
        *state.removals.entry(partition).or_default() += 1;
        let removed = state
            .waiters
            .extract_if(|key, _| key.partition == partition)
            .collect::<Vec<_>>();
        for (_, reply) in removed {
            let _ = reply.send(Err(RemoteStorageError::NotReady { partition }));
        }
    }

    /// Stop the pump before it applies its next record, until the guard drops.
    /// The pump takes the same guard around each record it applies.
    #[cfg(test)]
    pub(super) async fn stall_pump(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.stall.lock().await
    }

    /// `true` when no mutation is registered, in flight, or holding an outcome.
    #[cfg(test)]
    pub(super) fn is_idle(&self) -> bool {
        let state = self.state();
        state.waiters.is_empty() && state.in_flight.is_empty() && state.early.is_empty()
    }

    /// The number of registered waiters.
    #[cfg(test)]
    pub(super) fn registered(&self) -> usize { self.state().waiters.len() }
}

/// A publish counted in flight on its metadata partition. Dropping it before
/// [`Self::register`], on a publish error or a cancelled wait, ends the count.
struct InFlightPublish<'a> {
    waiters: Option<&'a MutationWaiters>,
    partition: i32,
    /// The partition's removal count when the publish started.
    removals: u64,
}

impl InFlightPublish<'_> {
    /// Register the acknowledged `offset` and return the channel its outcome
    /// arrives on. An outcome the pump already applied is sent at once.
    fn register(mut self, offset: i64) -> oneshot::Receiver<Outcome> {
        let (send, receive) = oneshot::channel();
        let Some(waiters) = self.waiters.take() else {
            return receive;
        };
        let partition = self.partition;
        let key = MutationKey { partition, offset };
        let mut state = waiters.state();
        if let Some(outcome) = state.early.remove(&key) {
            let _ = send.send(outcome);
        } else if state.removals(partition) == self.removals {
            state.waiters.insert(key, send);
        } else {
            // The partition was unassigned during the publish, so the pump
            // may never consume this record.
            let _ = send.send(Err(RemoteStorageError::NotReady { partition }));
        }
        state.end_publish(partition);
        receive
    }
}

impl Drop for InFlightPublish<'_> {
    fn drop(&mut self) {
        if let Some(waiters) = self.waiters.take()
            && let Ok(mut state) = waiters.state.lock()
        {
            state.end_publish(self.partition);
        }
    }
}

/// A published record's offset and the channel that reports its outcome.
struct TrackedPublish {
    offset: i64,
    reply: oneshot::Receiver<Outcome>,
}

impl TopicBasedRemoteLogMetadataManager {
    pub(super) fn publish_and_wait(
        &self,
        tp: &TopicIdPartition,
        event: Bytes,
    ) -> Result<(), RemoteStorageError> {
        let partition = metadata_partition_for(tp, self.log.partition_count());
        self.runtime.block_on(async {
            let TrackedPublish { offset, reply } = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => return Err(MetadataLogError::Closed.into_storage()),
                published = self.publish_tracked(partition, event) => published?,
            };
            let result = tokio::select! {
                biased;
                () = self.shutdown.cancelled() => Err(MetadataLogError::Closed.into_storage()),
                result = reply => result.unwrap_or_else(|_| Err(MetadataLogError::Closed.into_storage())),
            };
            self.mutation_waiters
                .forget(MutationKey { partition, offset });
            result
        })
    }

    async fn publish_tracked(
        &self,
        partition: i32,
        event: Bytes,
    ) -> Result<TrackedPublish, RemoteStorageError> {
        let in_flight = self.mutation_waiters.begin_publish(partition, || {
            matches!(self.metadata_partition_gate(partition), ReadGate::Ready)
        })?;
        let offset = self
            .log
            .publish(partition, event)
            .await
            .map_err(MetadataLogError::into_storage)?;
        Ok(TrackedPublish {
            offset,
            reply: in_flight.register(offset),
        })
    }
}

#[cfg(test)]
mod tests;
