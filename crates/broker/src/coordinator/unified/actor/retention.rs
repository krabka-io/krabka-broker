//! KIP-211 offset retention: the actor half of the sweep.
//!
//! `coordinator::retention` drives the cadence and decides which
//! groups this broker owns. This module answers the one question that needs
//! the group's own state, and it answers it inside the actor so a concurrent
//! join, commit, or leave cannot slip between the decision and the append.
//!
//! # What expires
//!
//! Nothing, while the group has a member. Kafka reaps an offset out from under
//! a live group only for a topic the group no longer subscribes to; krabka
//! keeps every offset of a live group instead, which is the safe side of that
//! difference and what an operator expects from
//! `offsets.retention.minutes`.
//!
//! For an empty group each offset expires on its own clock, per
//! [`OffsetEntry::is_expired`](crate::coordinator::unified::classic_state::OffsetEntry::is_expired):
//! the per-commit `retention_time_ms` a v2-v4 `OffsetCommit` asked for, or
//! `offsets.retention.minutes` measured from the base
//! [`group_empty_since_ms`] chooses — the moment the group emptied for a
//! classic group a consumer has joined, and the commit itself for a simple
//! group or a KIP-848 group, which is the split Kafka's
//! `offsetExpirationCondition` makes.
//!
//! # What the batch carries
//!
//! One tombstone for each expired offset, and — when the group keeps no
//! offset after the pass, including the group that never held one and has
//! been memberless for a whole sweep interval — the group's own tombstone in
//! the same batch,
//! so a reader of `__consumer_offsets` never sees a group record with no
//! offsets and no members hanging behind a partial write. The group record is
//! the classic k2 `GroupMetadata` for a classic group, and the next-gen k3
//! `GroupMetadata` plus k6 `TargetAssignmentMetadata` for a KIP-848 group.
//! Deleting the group stops the actor; the coordinator drops the registry
//! entry when it reads [`ReapOutcome::group_deleted`].

use tokio::sync::oneshot;

use crate::coordinator::unified::{
    GroupCoordinator, OffsetRecordBatchBuilder,
    group::{CoordinatorGroup, GroupKind},
    offsets_log::OffsetsLog,
    persistence::{Key, OffsetCommitValue, encode_key},
    persistence_next_gen::{NextGenKey, encode_key as encode_next_gen_key},
};

/// What one reap pass did to one group.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReapOutcome {
    /// The `(topic, partition)` offsets this pass tombstoned, sorted.
    pub reaped: Vec<(String, i32)>,
    /// `true` when the pass also tombstoned the group itself, which stops the
    /// actor and empties its registry entry.
    pub group_deleted: bool,
}

/// The `ReapExpiredOffsets` mailbox arm. Returns the actor's keep-running
/// flag: a group that tombstoned itself has no state left to serve.
pub(super) async fn handle_reap_message(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
    retention_ms: i64,
    empty_grace_ms: i64,
    reply: oneshot::Sender<ReapOutcome>,
) -> bool {
    let outcome = reap_expired_offsets(
        group,
        offsets_log,
        coordinator,
        now_ms,
        retention_ms,
        empty_grace_ms,
    )
    .await;
    let keep_running = !outcome.group_deleted;
    let _ = reply.send(outcome);
    keep_running
}

/// Tombstone every offset of this group that has fallen out of retention, and
/// the group with them when none is left.
///
/// A failed append leaves the in-memory offsets untouched and reports an empty
/// outcome, so the next sweep tries the same group again. That is the same
/// idempotent every-broker-sweeps shape the break-glass reaper uses: a
/// tombstone written twice is a no-op.
async fn reap_expired_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
    retention_ms: i64,
    empty_grace_ms: i64,
) -> ReapOutcome {
    // A live group keeps every offset, whatever its commit age.
    if group.has_members() {
        return ReapOutcome::default();
    }
    let empty_since_ms = group_empty_since_ms(group);
    let mut expired: Vec<(String, i32)> = group
        .committed_offsets
        .iter()
        .filter(|(_, entry)| entry.is_expired(now_ms, empty_since_ms, retention_ms))
        .map(|(key, _)| key.clone())
        .collect();
    // Once the sweep has taken the expired offsets the group keeps none, and
    // an empty group that keeps no offsets is a dead group. Kafka's
    // `GroupMetadataManager.cleanupGroupMetadata` transitions exactly that
    // group to `Dead` and appends its tombstone whether or not this pass
    // expired anything, which is why a group that never committed, or whose
    // last offset an `OffsetDelete` already removed, still goes. Verified on
    // `apache/kafka:4.3.1`: a group left memberless by
    // `kafka-consumer-groups --delete-offsets` disappears from `--list` at the
    // next `offsets.retention.check.interval.ms`.
    let delete_group = expired.len() == group.committed_offsets.len();
    if expired.is_empty() && !(delete_group && settled_empty(group, now_ms, empty_grace_ms)) {
        return ReapOutcome::default();
    }
    expired.sort_unstable();
    let batch = tombstone_batch(
        &group.group_id,
        &expired,
        delete_group.then_some(&group.kind),
        now_ms,
    );
    if let Err(error) = offsets_log.append(&group.group_id, batch).await {
        tracing::warn!(
            group_id = %group.group_id,
            %error,
            "offset-retention tombstone write failed; retrying on the next sweep",
        );
        return ReapOutcome::default();
    }
    for key in &expired {
        group.committed_offsets.remove(key);
    }
    if delete_group {
        coordinator.remove_cached_seed(&group.group_id);
    }
    tracing::info!(
        group_id = %group.group_id,
        offsets = expired.len(),
        group_deleted = delete_group,
        "reaped expired committed offsets",
    );
    ReapOutcome {
        reaped: expired,
        group_deleted: delete_group,
    }
}

/// `true` when this group has been memberless for a whole sweep interval.
///
/// The zero-offset deletion path needs it. Everything that puts a group in the
/// registry spawns the actor first and populates it a message later — an
/// `OffsetCommit` for an unknown id, the `WriteTxnMarkers` materialisation, a
/// `JoinGroup` — so a group that holds nothing right now may simply be one
/// whose first message has not landed yet. Kafka has no such window: its
/// coordinator creates and fills a group under one lock. Waiting one
/// `offsets.retention.check.interval.ms` gives every such caller its own
/// mailbox turn and still deletes the group on the pass after it truly went
/// idle, which is the timing `apache/kafka:4.3.1` shows.
///
/// A group whose offsets all aged out does not consult this: those offsets are
/// themselves proof that the group is older than its retention.
fn settled_empty(group: &CoordinatorGroup, now_ms: i64, empty_grace_ms: i64) -> bool {
    group
        .empty_since_ms
        .is_some_and(|since| now_ms >= since.saturating_add(empty_grace_ms))
}

/// The moment this group went empty, when that is the clock Kafka measures
/// its retention from, and `None` when Kafka measures from each commit
/// instead.
///
/// `ClassicGroup.offsetExpirationCondition` splits three ways. A classic group
/// that carries a protocol type — one some consumer has joined — measures from
/// `currentStateTimestamp`, the moment it went empty. A simple group, which
/// only ever committed offsets and so never took a protocol type, measures
/// from the commit; so does a KIP-848 group, per
/// `ConsumerGroup.offsetExpirationCondition`.
///
/// Only the first of the three has a group-empty moment that survives a
/// restart. A classic group writes the memberless k2 snapshot when its last
/// member leaves, and replay reads `current_state_timestamp_ms` back out of
/// it. The other two have nothing to read, so their
/// [`empty_since_ms`](CoordinatorGroup::empty_since_ms) is only the moment
/// this process first ran their actor — and measuring from that would hand
/// every dead group another full `offsets.retention.minutes` on every broker
/// restart, which is the leak this module exists to close.
fn group_empty_since_ms(group: &CoordinatorGroup) -> Option<i64> {
    match &group.kind {
        GroupKind::Classic(state) if state.protocol_type.is_some() => group.empty_since_ms,
        GroupKind::Classic(_) | GroupKind::Consumer(_) => None,
    }
}

/// One `__consumer_offsets` batch: an offset tombstone for each expired key,
/// then the group's own tombstones when `delete_group_of_kind` is set.
fn tombstone_batch(
    group_id: &str,
    expired: &[(String, i32)],
    delete_group_of_kind: Option<&GroupKind>,
    now_ms: i64,
) -> krabka_protocol::records::RecordBatch {
    let mut builder = OffsetRecordBatchBuilder::default();
    for (topic, partition) in expired {
        builder.push(
            OffsetCommitValue::encode_key(group_id, topic, *partition),
            None,
        );
    }
    match delete_group_of_kind {
        None => {}
        Some(GroupKind::Classic(_)) => builder.push(
            encode_key(&Key::GroupMetadata {
                group_id: group_id.into(),
            }),
            None,
        ),
        Some(GroupKind::Consumer(_)) => {
            builder.push(
                encode_next_gen_key(&NextGenKey::GroupMetadata {
                    group_id: group_id.into(),
                }),
                None,
            );
            builder.push(
                encode_next_gen_key(&NextGenKey::TargetAssignmentMetadata {
                    group_id: group_id.into(),
                }),
                None,
            );
        }
    }
    builder.finish(now_ms)
}
