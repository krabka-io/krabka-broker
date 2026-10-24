//! The broadcast-side filter that decides which live record reaches one
//! subscription's stream.
//!
//! A record passes only when its partition is currently assigned and its
//! offset is at or after that partition's cursor. A partition added mid-stream
//! is marked `via_inject`, so its passing records are re-routed into the
//! subscription's FIFO instead of being emitted here, which keeps them behind
//! the backlog that [`AssignmentHandle::add`] already queued.
//!
//! [`AssignmentHandle::add`]: crate::log::AssignmentHandle::add

use {
    super::{InProcessInner, SubscriptionState},
    crate::log::{MetadataEventRecord, MetadataEventStream},
    futures_util::{StreamExt, stream::unfold},
    std::collections::VecDeque,
    std::sync::Arc,
    tokio::sync::broadcast,
};

/// What [`filtered_broadcast`] does with a received live record.
enum Forward {
    /// Emit directly on the broadcast stream.
    Emit,
    /// Re-route through the `inject` FIFO for a partition added mid-stream.
    Inject,
    /// Not assigned, or below the cursor. Discard the record.
    Drop,
}

/// Forward a broadcast record only when its partition is currently
/// assigned and its offset is at/after the recorded live cursor for
/// that partition.
///
/// For a partition added mid-stream, which is the `via_inject` case, this
/// function re-routes a passing record into the `inject` FIFO instead of
/// emitting it here. The record then sits behind that partition's
/// already-injected backlog rather than races it through `stream::select`.
pub(super) fn filtered_broadcast(
    rx: broadcast::Receiver<MetadataEventRecord>,
    state: Arc<SubscriptionState>,
    inner: Arc<InProcessInner>,
) -> MetadataEventStream {
    unfold(
        (rx, state, inner, VecDeque::<MetadataEventRecord>::new()),
        |(mut rx, state, inner, mut pending)| async move {
            loop {
                if let Some(record) = pending.pop_front() {
                    let emit = {
                        let mut assigned = state.assigned.lock().expect("assigned mutex poisoned");
                        match assigned.get_mut(&record.partition) {
                            Some(cur) if record.offset >= cur.next => {
                                cur.next = record.offset.saturating_add(1);
                                true
                            }
                            _ => false,
                        }
                    };
                    if emit {
                        return Some((record, (rx, state, inner, pending)));
                    }
                    continue;
                }

                match rx.recv().await {
                    Ok(record) => {
                        let action = {
                            let mut assigned =
                                state.assigned.lock().expect("assigned mutex poisoned");
                            match assigned.get_mut(&record.partition) {
                                Some(cur) if record.offset >= cur.next => {
                                    cur.next = record.offset.saturating_add(1);
                                    if cur.via_inject {
                                        Forward::Inject
                                    } else {
                                        Forward::Emit
                                    }
                                }
                                _ => Forward::Drop,
                            }
                        };
                        match action {
                            Forward::Emit => return Some((record, (rx, state, inner, pending))),
                            Forward::Inject => {
                                // Queue behind backlog; if the receiver is gone
                                // the stream is being dropped anyway.
                                let _ = state.inject.send(record);
                            }
                            Forward::Drop => {}
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // The receiver has advanced past records that may not yet
                        // have been delivered. Replay from each assigned cursor
                        // under the same log lock used by append/add, so records
                        // are either in this retained range or arrive live after it.
                        let log = inner.log.lock().expect("metadata-log mutex poisoned");
                        let mut assigned = state.assigned.lock().expect("assigned mutex poisoned");
                        for (partition, cursor) in assigned.iter_mut() {
                            let Ok(index) = usize::try_from(*partition) else {
                                continue;
                            };
                            let Some(records) = log.get(index) else {
                                continue;
                            };
                            let begin = usize::try_from(cursor.next.max(0)).unwrap_or(usize::MAX);
                            let replay: Vec<_> = records.iter().skip(begin).cloned().collect();
                            if cursor.via_inject {
                                for record in replay {
                                    if state.inject.send(record.clone()).is_ok() {
                                        cursor.next = record.offset.saturating_add(1);
                                    }
                                }
                            } else {
                                pending.extend(replay);
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    )
    .boxed()
}
