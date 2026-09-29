//! The second phase of the dead-letter queue on the leader (KIP-1191): write
//! the records of an `Archiving` run to the queue, then archive the run.
//!
//! The first phase is a transition of the acquisition machine, and it is durable
//! before this module hears of it: [`SharePartitionLeaderManager::persist_if_dirty`]
//! hands over the runs that the machine has noted only once the coordinator has
//! stored them as `Archiving`. A leader that stops before the second phase
//! ends leaves those runs in the coordinator, and the next leader that loads
//! the partition resumes them, so a record is written at least once.
//!
//! The second phase is Kafka's `SharePartition.initiateDLQAndArchive`: it
//! archives the run whether or not the write worked, because a record that
//! cannot be dead-lettered must not hold the SPSO for ever. A failure is logged
//! as an error.

use std::sync::Arc;

use krabka_log::Offset;
use tokio::sync::Mutex;

use super::{LeaderKey, SharePartitionLeaderManager};
use crate::share_partition::{
    dlq::DlqRequest,
    group_settings::GroupShareSettings,
    state::{AcquisitionState, DlqCause, DlqRange},
};

impl SharePartitionLeaderManager {
    /// Starts the second phase for each of `ranges`, each in a task of its
    /// own, so a slow write does not hold up the others. It returns at once.
    pub(super) fn dispatch_dead_letters(
        &self,
        key: &LeaderKey,
        cell: &Arc<Mutex<AcquisitionState>>,
        ranges: Vec<DlqRange>,
    ) {
        if ranges.is_empty() {
            return;
        }
        let Some(manager) = self.me.upgrade() else {
            return;
        };
        for range in ranges {
            let manager = Arc::clone(&manager);
            let (key, cell) = (key.clone(), Arc::clone(cell));
            tokio::spawn(async move { manager.dead_letter(&key, &cell, range).await });
        }
    }

    /// Writes `range` to the queue, then archives it and makes that durable.
    async fn dead_letter(
        &self,
        (group, topic_id, partition): &LeaderKey,
        cell: &Arc<Mutex<AcquisitionState>>,
        range: DlqRange,
    ) {
        // The cause of a run that came back from the coordinator is not
        // stored, so it follows the group's delivery count limit.
        let limit =
            GroupShareSettings::resolve(&self.controller.current_image(), group, &self.config)
                .delivery_count_limit;
        let request = DlqRequest {
            group: group.clone(),
            topic_id: *topic_id,
            source_partition: *partition,
            first: range.first,
            last: range.last,
            delivery_count: range.delivery_count,
            cause: range
                .cause
                .unwrap_or_else(|| DlqCause::inferred(range.delivery_count, limit)),
        };
        let (Offset(first), Offset(last)) = (range.first, range.last);
        if let Err(error) = self.dlq.write(request).await {
            tracing::error!(
                group,
                %topic_id,
                partition,
                first,
                last,
                %error,
                "Failed to write to DLQ, proceeding to ARCHIVED regardless."
            );
        }
        let mut state = cell.lock().await;
        state.finish_archiving(range.first, range.last);
        // Best-effort: a failed write keeps the state dirty, so a later write
        // carries the archived run to the coordinator. The state stays in
        // memory until then, as in Kafka.
        let _ = self
            .persist_if_dirty(group, *topic_id, *partition, Some(cell), &mut state)
            .await;
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Instant};

    use assert2::assert;
    use krabka_log::Offset;

    use crate::{
        codes,
        share_partition::{
            dlq::{DlqError, DlqRequest, test_support::RecordingDlq},
            manager::test_support::{LOCK, manager_with_dlq},
            state::{AckType, AcquisitionState, DlqCause, RecordState},
        },
    };

    /// Records `0..records`, all acquired by `m1`, in a cell that has a
    /// dead-letter queue.
    async fn acquired_cell(
        mgr: &Arc<super::SharePartitionLeaderManager>,
        tid: uuid::Uuid,
        records: i64,
    ) -> Arc<tokio::sync::Mutex<AcquisitionState>> {
        let cell = mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));
        {
            let mut state = cell.lock().await;
            state.set_dlq_enabled(true);
            state.materialize(Offset(records), 100);
            let _ = state.acquire("m1", 100, Offset(i64::MAX), Instant::now(), LOCK, 5);
        }
        cell
    }

    /// Waits until `done` holds of the cell's states, or fails the test.
    async fn wait_for(
        cell: &Arc<tokio::sync::Mutex<AcquisitionState>>,
        done: impl Fn(&[(i64, RecordState)]) -> bool,
    ) -> Vec<(i64, RecordState)> {
        tokio::time::timeout(std::time::Duration::from_secs(60), async {
            loop {
                let states = cell.lock().await.record_states();
                if done(&states) {
                    return states;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the dead-letter write finished")
    }

    fn request(first: i64, last: i64, delivery_count: i16, cause: DlqCause) -> DlqRequest {
        DlqRequest {
            group: "g1".into(),
            topic_id: uuid::Uuid::from_bytes([61; 16]),
            source_partition: 0,
            first: Offset(first),
            last: Offset(last),
            delivery_count,
            cause,
        }
    }

    /// A write that cannot reach the coordinator keeps the run in `Archiving`
    /// in memory: the dead-letter write starts only after the run is durable,
    /// and a failed write of `Archiving` rolls the reject back.
    #[tokio::test(start_paused = true)]
    async fn the_write_starts_only_once_archiving_is_durable() {
        let dlq = Arc::new(RecordingDlq::default());
        let mgr = manager_with_dlq(dlq.clone());
        let tid = uuid::Uuid::from_bytes([61; 16]);
        let cell = acquired_cell(&mgr, tid, 2).await;

        // The test manager's persister cannot write, so the reject rolls back.
        let mut state = cell.lock().await;
        let code = mgr
            .apply_durably("g1", tid, 0, &cell, &mut state, |st| {
                st.acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
                    .err()
                    .unwrap_or(codes::NONE)
            })
            .await;
        let states = state.record_states();
        drop(state);
        tokio::task::yield_now().await;

        assert!(
            (code, states, dlq.requests())
                == (
                    codes::COORDINATOR_NOT_AVAILABLE,
                    vec![(0, RecordState::Acquired), (1, RecordState::Acquired)],
                    Vec::<DlqRequest>::new(),
                )
        );
    }

    /// Kafka's `initiateDLQAndArchive`: each run is written to the queue with
    /// its cause and delivery count, then archived and the SPSO moves on.
    #[tokio::test(start_paused = true)]
    async fn a_dispatched_run_is_written_then_archived() {
        let dlq = Arc::new(RecordingDlq::default());
        let mgr = manager_with_dlq(dlq.clone());
        let tid = uuid::Uuid::from_bytes([61; 16]);
        let cell = acquired_cell(&mgr, tid, 3).await;
        let key = ("g1".to_owned(), tid, 0);
        let ranges = {
            let mut state = cell.lock().await;
            state
                .acknowledge("m1", Offset(0), Offset(1), AckType::Reject, 5)
                .unwrap();
            state.take_pending_dlq()
        };

        mgr.dispatch_dead_letters(&key, &cell, ranges);
        let states = wait_for(&cell, |states| states.len() == 1).await;

        assert!(
            (states, dlq.requests(), cell.lock().await.start_offset)
                == (
                    vec![(2, RecordState::Acquired)],
                    vec![request(0, 1, 1, DlqCause::ClientReject)],
                    Offset(2),
                )
        );
    }

    /// A run that came back from the coordinator has no stored cause: it is
    /// dead-lettered for the delivery count when the count reached the limit,
    /// and for a reject when it did not. A failed write archives the run
    /// regardless.
    #[tokio::test(start_paused = true)]
    async fn a_failed_write_still_archives_and_a_restored_cause_is_inferred() {
        let dlq = Arc::new(RecordingDlq::failing(DlqError::Write("boom".into())));
        let mgr = manager_with_dlq(dlq.clone());
        let tid = uuid::Uuid::from_bytes([61; 16]);
        let cell = mgr.insert_for_test("g1", tid, 0, AcquisitionState::new(Offset(0)));
        let key = ("g1".to_owned(), tid, 0);
        let ranges = {
            let mut state = cell.lock().await;
            state.load_from(
                Offset(0),
                1,
                1,
                &[
                    crate::share_coordinator::persistence::StateBatch {
                        first_offset: Offset(0),
                        last_offset: Offset(0),
                        delivery_state: crate::share_partition::state::DS_ARCHIVING,
                        delivery_count: 5,
                    },
                    crate::share_coordinator::persistence::StateBatch {
                        first_offset: Offset(1),
                        last_offset: Offset(1),
                        delivery_state: crate::share_partition::state::DS_ARCHIVING,
                        delivery_count: 2,
                    },
                ],
            );
            state.take_pending_dlq()
        };

        mgr.dispatch_dead_letters(&key, &cell, ranges);
        let states = wait_for(&cell, <[_]>::is_empty).await;
        let mut written = dlq.requests();
        written.sort_by_key(|request| request.first.0);

        assert!(
            (states, written)
                == (
                    Vec::new(),
                    vec![
                        request(0, 0, 5, DlqCause::DeliveryCountExceeded),
                        request(1, 1, 2, DlqCause::ClientReject),
                    ],
                )
        );
    }
}
