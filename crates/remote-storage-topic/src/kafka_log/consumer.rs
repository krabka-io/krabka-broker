//! Live consumer state for one [`MetadataEventLog::subscribe`] call.
//!
//! A subscription owns one cancellable manual-`Fetch` task per assigned
//! partition. Each task drives its own dedicated
//! [`krabka_client_core::Connection`], because the broker is serial per
//! connection and a long-`max_wait_ms` fetch on a shared socket would
//! head-of-line-block every other RPC. All tasks emit
//! [`MetadataEventRecord`]s into one shared queue, and the
//! [`AssignmentHandle`] this module implements adds and removes partitions
//! while the subscription is live.
//!
//! [`MetadataEventLog::subscribe`]: crate::log::MetadataEventLog::subscribe

use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};

use krabka_client_core::{ClientFrameMax, ConnectionDispatchQueueCapacity, ConnectionOptions};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;
use krabka_units::prelude::{ByteSize, Time, TimeExt as _};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, warn};

use crate::{
    kafka_log::config::MetadataEventQueueCapacity,
    log::{AssignmentHandle, MetadataEventRecord, PartitionStart},
};

/// Per-subscription live consumer: one cancellable fetch task per assigned
/// partition, and every task emits into the shared `tx`.
pub(super) struct ConsumerState {
    pub(super) bootstrap: String,
    pub(super) client_id: String,
    pub(super) security: Option<krabka_client_core::security::ClientSecurity>,
    pub(super) topic: String,
    pub(super) topic_id: WireUuid,
    pub(super) tx: mpsc::Sender<MetadataEventRecord>,
    pub(super) fetch_max_wait: Time,
    pub(super) fetch_max_bytes: ByteSize,
    pub(super) fetch_retry_backoff: Time,
    pub(super) dispatch_queue_capacity: ConnectionDispatchQueueCapacity,
    pub(super) frame_max: ClientFrameMax,
    /// partition -> cancel token for its fetch task.
    pub(super) tasks: StdMutex<HashMap<i32, CancellationToken>>,
}

impl ConsumerState {
    pub(super) fn spawn_partition(self: &Arc<Self>, start: PartitionStart) {
        let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
        if tasks.contains_key(&start.partition) {
            return; // already assigned
        }
        let cancel = CancellationToken::new();
        tasks.insert(start.partition, cancel.clone());
        tokio::spawn(partition_fetch_loop(
            Arc::clone(self),
            start.partition,
            start.start_offset,
            cancel,
        ));
    }

    fn cancel_partition(&self, partition: i32) {
        if let Some(tok) = self
            .tasks
            .lock()
            .expect("metadata tasks mutex poisoned")
            .remove(&partition)
        {
            tok.cancel();
        }
    }

    pub(super) fn cancel_all(&self) {
        let mut tasks = self.tasks.lock().expect("metadata tasks mutex poisoned");
        for (_, tok) in tasks.drain() {
            tok.cancel();
        }
    }
}

pub(super) struct KafkaAssignmentHandle {
    pub(super) state: Arc<ConsumerState>,
}

impl AssignmentHandle for KafkaAssignmentHandle {
    fn add(&self, start: PartitionStart) {
        self.state.spawn_partition(start);
    }
    fn remove(&self, partition: i32) {
        self.state.cancel_partition(partition);
    }
    fn assigned(&self) -> Vec<i32> {
        let mut v: Vec<i32> = self
            .state
            .tasks
            .lock()
            .expect("metadata tasks mutex poisoned")
            .keys()
            .copied()
            .collect();
        v.sort_unstable();
        v
    }
}

pub(super) fn metadata_event_channel(
    capacity: MetadataEventQueueCapacity,
) -> (
    mpsc::Sender<MetadataEventRecord>,
    mpsc::Receiver<MetadataEventRecord>,
) {
    mpsc::channel(capacity.capacity())
}

/// Manual single-partition fetch loop over a dedicated connection.
///
/// A dedicated connection per partition keeps the metadata consumer off any
/// parkable or shared stream. The broker is serial per-connection, so a
/// long-`max_wait_ms` fetch must not head-of-line-block other RPCs.
// cargo-mutants: live-broker fetch loop over a real connection, not unit-testable
#[cfg_attr(test, mutants::skip)]
#[instrument(level = "debug", skip_all, fields(partition, start_offset))]
async fn partition_fetch_loop(
    state: Arc<ConsumerState>,
    partition: i32,
    start_offset: i64,
    cancel: CancellationToken,
) {
    use std::net::ToSocketAddrs;

    use krabka_client_core::{Connection, fetch_partition};

    // Dedicated connection for this partition's fetch loop. Resolve the
    // bootstrap address; on failure, warn and exit. The partition then
    // never advances past its resume offset, so the manager's readiness
    // gate keeps returning `NotReady` (retryable) for reads that hash
    // there until a later reconcile re-establishes the fetch loop.
    let Some(addr) = state
        .bootstrap
        .to_socket_addrs()
        .ok()
        .and_then(|mut a| a.next())
    else {
        warn!(bootstrap = %state.bootstrap, "metadata consumer: bad bootstrap addr");
        return;
    };
    let opts = ConnectionOptions {
        client_id: state.client_id.clone(),
        dispatch_queue_capacity: state.dispatch_queue_capacity,
        frame_max: state.frame_max,
        security: state.security.clone().map(Box::new),
        ..Default::default()
    };
    let conn = match Connection::connect_with_options(addr, opts).await {
        Ok(c) => c,
        Err(e) => {
            warn!(error = %e, partition, "metadata consumer: connect failed");
            return;
        }
    };

    let mut next_offset = start_offset.max(0);
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                conn.close();
                return;
            }
            res = fetch_partition(
                &conn,
                &state.topic,
                state.topic_id,
                partition,
                next_offset,
                state.fetch_max_wait,
                state.fetch_max_bytes,
            ) => {
                match res {
                    Ok(records) => {
                        for r in records {
                            // Re-check cancellation before every send: a
                            // remove() (for reassignment) that fires
                            // after fetch_partition resolved must not flush
                            // the rest of an already-fetched batch, or a
                            // task spawned on re-add from a new start_offset
                            // would double-deliver these same records.
                            if cancel.is_cancelled() {
                                conn.close();
                                return;
                            }
                            if r.offset < next_offset {
                                continue; // defensive: never go backwards
                            }
                            let payload = r.value.unwrap_or_default();
                            let record = MetadataEventRecord {
                                partition,
                                offset: r.offset,
                                payload,
                            };
                            next_offset = r.offset + 1;
                            if state.tx.send(record).await.is_err() {
                                conn.close();
                                return; // stream dropped
                            }
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, partition, "metadata consumer: fetch failed; retrying");
                        tokio::time::sleep(state.fetch_retry_backoff.to_std()).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::prelude::{mebibytes, millis};

    use super::*;
    use crate::kafka_log::config::METADATA_TOPIC;

    #[test]
    fn metadata_event_channel_uses_configured_capacity() {
        let (tx, _rx) = metadata_event_channel(MetadataEventQueueCapacity::new(2048).unwrap());
        check!(tx.max_capacity() == 2048);
    }

    #[tokio::test]
    async fn assignment_handle_tracks_spawned_partitions() {
        let (tx, _rx) = mpsc::channel::<MetadataEventRecord>(1);
        let state = Arc::new(ConsumerState {
            bootstrap: "invalid-bootstrap".into(),
            client_id: "test-consumer".into(),
            security: None,
            topic: METADATA_TOPIC.into(),
            topic_id: WireUuid::ZERO,
            tx,
            fetch_max_wait: millis(750),
            fetch_max_bytes: mebibytes(2),
            fetch_retry_backoff: millis(300),
            dispatch_queue_capacity: ConnectionDispatchQueueCapacity::default(),
            frame_max: ClientFrameMax::default(),
            tasks: StdMutex::new(HashMap::new()),
        });
        check!(state.fetch_max_wait == millis(750));
        check!(state.fetch_max_bytes == mebibytes(2));
        check!(state.fetch_retry_backoff == millis(300));
        let handle = KafkaAssignmentHandle {
            state: Arc::clone(&state),
        };

        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 7,
        });
        state.spawn_partition(PartitionStart {
            partition: 2,
            start_offset: 9,
        });
        handle.add(PartitionStart {
            partition: 0,
            start_offset: 0,
        });
        handle.remove(2);

        assert!(handle.assigned() == vec![0]);
        state.cancel_all();
    }
}
