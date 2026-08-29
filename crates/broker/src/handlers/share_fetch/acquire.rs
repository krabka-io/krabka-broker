//! The acquire passes that KIP-932 runs over the pending partitions: apply the
//! piggybacked acknowledgements, expire stale locks, materialize newly
//! produced records, and lock a batch of `Available` ones for this member.
//!
//! This is the stage that owns the per-partition
//! [`AcquisitionState`](crate::share_partition::state::AcquisitionState) locks,
//! the request-wide record budget, and the retry pass behind the long poll.

use std::{sync::Arc, time::Instant};

use krabka_log::Offset;

use super::{
    acknowledge::apply_one_ack,
    long_poll::long_poll,
    pending::PendingPartition,
    records::{control_batch_ranges, pending_activation_ranges, populate_acquired_response},
};
use crate::{broker::Broker, codes, error::BrokerError};

/// KFC-1: the most not-yet-due records an acquire pass leaves in one share
/// partition's window.
///
/// A deferred run is not in flight, so it does not spend
/// `share_group_max_inflight_records`, and materialization walks past it to
/// reach the due records behind it. That is the point of the whole path, and
/// it needs a second bound of its own: without one, a single far-future batch
/// at the head of the window pulls the rest of the log in behind it, and every
/// later pass re-walks all of it.
///
/// This should be the broker config `share_delivery_max_deferred_records`. It
/// is a constant here because the runtime settings it belongs beside live in
/// `file_config.rs` and in
/// [`ShareGroupConfig`](crate::coordinator::unified::share::config::ShareGroupConfig).
const SHARE_DELIVERY_MAX_DEFERRED_RECORDS: i64 = 1_000;

#[derive(Clone, Copy)]
pub(super) struct AcquireContext<'a> {
    pub(super) broker: &'a Broker,
    pub(super) manager: &'a Arc<crate::share_partition::manager::SharePartitionLeaderManager>,
    pub(super) group: &'a str,
    pub(super) member: &'a str,
    pub(super) max_records: i32,
    pub(super) max_bytes: i32,
    pub(super) is_renew_ack: bool,
    pub(super) config: &'a crate::coordinator::unified::share::config::ShareGroupConfig,
}

pub(super) async fn acquire_records(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    max_wait_ms: i32,
) -> Result<(), BrokerError> {
    let acquired = acquire_pass(context, pending, true).await?;
    if acquired == 0 && max_wait_ms > 0 {
        long_poll(context.broker, pending, max_wait_ms).await;
        acquire_pass(context, pending, false).await?;
    }
    Ok(())
}

/// Pulls freshly produced records into the acquisition window, unless the
/// schedule already holds back [`SHARE_DELIVERY_MAX_DEFERRED_RECORDS`] of them.
///
/// The previous pass's deferral still stands when this runs, which is why the
/// count means something here and would read zero after `promote_deferred`. It
/// is one pass out of date, and that only makes the bound conservative.
fn materialize_within_deferral_bound(
    state: &mut crate::share_partition::state::AcquisitionState,
    upper: Offset,
    max_inflight: i32,
) {
    if state.deferred_records() < SHARE_DELIVERY_MAX_DEFERRED_RECORDS {
        state.materialize(upper, max_inflight);
    }
}

fn remaining_record_budget(max_records: i32, acquired: i64) -> i32 {
    max_records
        .saturating_sub(i32::try_from(acquired).unwrap_or(i32::MAX))
        .max(0)
}

/// Runs one acquire pass over the pending partitions that this broker can
/// lead.
///
/// When `apply_acks` is true, this function applies the piggybacked
/// acknowledgement batches first, and sets `acknowledge_error_code`. When
/// `is_renew_ack` is set, those batches RENEW the acquisition lock instead of
/// acknowledging it, per KIP-932.
///
/// Under a `ReadCommitted` isolation level, this function clamps the
/// materialize and read window to the partition's last stable offset, so it
/// never acquires an uncommitted record. It returns the total number of
/// offsets that it acquired across all partitions in this pass.
///
/// On a KFC-1 scheduled topic it also re-derives which ranges of the window
/// are not due yet and marks them `Deferred`, so acquisition steps over them.
/// The derivation is thrown away and redone on each pass, so nothing outlives
/// the clock reading that produced it.
async fn acquire_pass(
    context: &AcquireContext<'_>,
    pending: &mut [PendingPartition],
    apply_acks: bool,
) -> Result<i64, BrokerError> {
    let &AcquireContext {
        broker,
        manager: mgr,
        group,
        member,
        max_records,
        max_bytes,
        is_renew_ack,
        config: cfg,
    } = context;
    let now = Instant::now();
    let read_committed = matches!(
        cfg.isolation_level,
        crate::coordinator::unified::share::config::ShareIsolationLevel::ReadCommitted
    );
    let mut total = 0_i64;

    for p in pending.iter_mut() {
        if !p.leadable {
            continue;
        }
        // Reset any prior pass's data for a clean re-acquire.
        p.out.records = None;
        p.out.acquired_records.clear();

        let cell = mgr.get_or_load(group, p.topic_id, p.partition_index).await;
        let mut st = cell.lock().await;

        // Apply piggybacked acknowledgements (first pass only). When the
        // request is a renew-ack, each batch RENEWs the lock on its range
        // rather than acknowledging it.
        if apply_acks && !p.ack_batches.is_empty() {
            let mut ack_err = codes::NONE;
            for (first, last, types) in &p.ack_batches {
                let res = if is_renew_ack {
                    st.renew(
                        member,
                        Offset(*first),
                        Offset(*last),
                        now,
                        cfg.record_lock_duration,
                    )
                } else {
                    apply_one_ack(&mut st, member, *first, *last, types, now)
                };
                if let Err(code) = res {
                    ack_err = code;
                }
            }
            p.out.acknowledge_error_code = ack_err;
        }

        if !p.fetchable {
            mgr.persist_if_dirty(group, p.topic_id, p.partition_index, &mut st)
                .await;
            continue;
        }

        // Expire stale locks, materialize freshly produced records, acquire.
        st.expire_locks(now);
        let part = p.topic_name.as_deref().and_then(|name| {
            broker
                .partitions
                .get(name, krabka_ids::PartitionIndex(p.partition_index))
        });
        let Some(part) = part else {
            // Lost the partition between the leadership check and here.
            p.out.error_code = codes::NOT_LEADER_OR_FOLLOWER;
            p.leadable = false;
            mgr.persist_if_dirty(group, p.topic_id, p.partition_index, &mut st)
                .await;
            continue;
        };
        let hwm = part.high_watermark().await;
        // Under read_committed, never surface records past the last stable
        // offset: clamp the materialize/read window to `min(lso, hwm)` so no
        // record from an OPEN transaction can be acquired.
        //
        // KFC-1 puts no delivery watermark here on purpose. That cap is what
        // holds a classic group to offset order, and a share group is exactly
        // the reader that does not need it: the window runs to the high
        // watermark and the deferral marks below hold the waiting records
        // back one range at a time.
        let upper = if read_committed {
            part.lso().min(hwm)
        } else {
            hwm
        };
        materialize_within_deferral_bound(&mut st, upper, cfg.max_inflight_records);
        // Transaction markers occupy log offsets but are broker metadata, not
        // user records. Archive them before acquisition so their encoded
        // coordinator epoch can never appear in a ShareFetch response.
        for (first, last) in control_batch_ranges(&part, st.start_offset, st.end_offset).await? {
            st.archive_internal(first, last);
        }
        // KFC-1: re-derive the deferral from the log and this partition's own
        // clock on every pass, exactly as the control-batch ranges above are.
        // Dropping it first is what keeps a batch that has since come due from
        // staying held back by an older clock reading.
        st.promote_deferred();
        let deferred = pending_activation_ranges(
            &part,
            st.start_offset,
            st.end_offset,
            part.delivery.now_ms(),
        )
        .await?;
        for (first, last) in deferred {
            st.defer_internal(first, last);
        }
        let remaining_records = remaining_record_budget(max_records, total);
        let acquired = if remaining_records > 0 {
            st.acquire(
                member,
                remaining_records,
                max_bytes,
                now,
                cfg.record_lock_duration,
                cfg.max_delivery_attempts,
            )
        } else {
            Vec::new()
        };

        if !acquired.is_empty() {
            total += populate_acquired_response(p, &part, &acquired, upper, max_bytes).await?;
        }

        p.out.error_code = codes::NONE;
        mgr.persist_if_dirty(group, p.topic_id, p.partition_index, &mut st)
            .await;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn materialization_stops_at_the_deferred_record_bound() {
        let bound = SHARE_DELIVERY_MAX_DEFERRED_RECORDS;
        let inflight = i32::try_from(bound).expect("the bound fits an inflight budget");
        for (deferred, expected_end) in [(bound - 1, bound + 9), (bound, bound)] {
            let mut state = crate::share_partition::state::AcquisitionState::new(Offset(0));
            state.materialize(Offset(deferred), inflight);
            state.defer_internal(Offset(0), Offset(deferred - 1));
            assert!(state.deferred_records() == deferred);

            materialize_within_deferral_bound(&mut state, Offset(deferred + 10), 10);

            assert!(
                state.end_offset == Offset(expected_end),
                "deferred={deferred}"
            );
        }
    }

    #[test]
    fn remaining_record_budget_is_request_wide_and_saturating() {
        check!(remaining_record_budget(500, 300) == 200);
        check!(remaining_record_budget(500, 500) == 0);
        check!(remaining_record_budget(500, i64::MAX) == 0);
    }
}
