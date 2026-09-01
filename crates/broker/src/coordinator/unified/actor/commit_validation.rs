//! Offset-commit fencing.
//!
//! `OffsetCommit` and `TxnOffsetCommit` fence identically, and both dispatch
//! on the group's LIVE protocol inside the actor, so the decision and the
//! request/reply wrapper that carries it live in one module.

use tokio::sync::oneshot;

use super::{ErrorCode, GroupActorHandle, GroupActorMessage};
use crate::{
    codes,
    coordinator::unified::{classic_ops, group::CoordinatorGroup},
};

pub(super) fn validate_commit_message(
    group: &CoordinatorGroup,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_or_epoch: i32,
) -> Result<(), ErrorCode> {
    if let Some(state) = group.as_consumer() {
        return state.validate_commit_decision(member_id, generation_or_epoch);
    }
    if let Some(state) = group.as_classic() {
        return classic_ops::validate_commit(
            state,
            member_id,
            group_instance_id,
            generation_or_epoch,
        )
        .map_or(Ok(()), Err);
    }
    Ok(())
}

/// Validates an offset commit, regular or transactional, against the group's
/// membership and generation (classic) or member epoch (KIP-848 next-gen).
///
/// It returns `Some(error_code)` when the commit must be rejected, and `None`
/// when the commit may proceed.
///
/// `OffsetCommit` and `TxnOffsetCommit` share this function so that the two
/// paths fence identically. KIP-447 requires transactional offset fencing to
/// be "consistent with normal offset fencing". For a simple consumer (empty
/// `member_id`, no `group_instance_id`) the classic path does nothing, so the
/// broker never fences a producer that supplies no group metadata.
///
/// Dispatch happens inside the actor on the LIVE `group.kind`, through the
/// single `ValidateCommit` message. It does not use the spawn-time
/// `handle.kind` hint, because a KIP-848 migration may have flipped the
/// protocol in place after spawn.
pub(crate) async fn validate_group_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
) -> Option<ErrorCode> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            group_instance_id: group_instance_id.map(str::to_string),
            generation_or_epoch,
            reply: tx,
        })
        .await
        .is_err()
    {
        return Some(codes::UNKNOWN_SERVER_ERROR);
    }
    match rx.await {
        Ok(Ok(())) => None,
        Ok(Err(code)) => Some(code),
        Err(_) => Some(codes::UNKNOWN_SERVER_ERROR),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::Offset;

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            GroupKindTag,
            test_support::{
                make_coordinator, make_coordinator_with_topic_policy, rpc, seed_classic_member,
            },
        },
        classic_state::OffsetEntry,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn classic_offset_validate_heartbeat_arms() {
        use krabka_protocol::owned::heartbeat_request::HeartbeatRequest;

        let (coord, _log) = make_coordinator();
        let handle = coord.get_or_create_classic("g");

        // UpdateCommitted then FetchOffsets round-trips on the kind-agnostic Group.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::UpdateCommitted {
                entries: vec![(
                    ("t".to_string(), 0),
                    OffsetEntry {
                        offset: Offset(42),
                        leader_epoch: 1,
                        metadata: String::new(),
                        commit_timestamp_ms: 0,
                    },
                )],
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply: tx })
            .await
            .unwrap();
        let committed = rx.await.unwrap().committed;
        assert!(committed.get(&("t".to_string(), 0)).unwrap().offset == 42);

        // Classic offset-commit validate: a simple consumer (no member/instance)
        // is allowed. `ValidateCommit` dispatches on the live (classic) kind.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ValidateCommit {
                member_id: String::new(),
                group_instance_id: None,
                generation_or_epoch: -1,
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap() == Ok(()));

        // Classic Heartbeat for an unknown member on an empty group.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ClassicHeartbeat {
                req: HeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "ghost".into(),
                    generation_id: 0,
                    ..Default::default()
                },
                reply: tx,
            })
            .await
            .unwrap();
        assert!(rx.await.unwrap() == codes::UNKNOWN_MEMBER_ID);

        // RemoveCommitted clears the entry.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::RemoveCommitted {
                keys: vec![("t".to_string(), 0)],
                reply: tx,
            })
            .await
            .unwrap();
        rx.await.unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::FetchOffsets { reply: tx })
            .await
            .unwrap();
        assert!(rx.await.unwrap().committed.is_empty());
    }

    /// Regression for the stale-`handle.kind` defect (KIP-848 live migration).
    /// The group is SPAWNED as a consumer group, because its first RPC was a
    /// native `ConsumerGroupHeartbeat`, so `handle.kind == Consumer`. It later
    /// hosts a classic member and then DOWNGRADES in place when the last
    /// native member leaves. The handle's spawn-time `kind` stays `Consumer`
    /// and is now stale.
    ///
    /// The defect was this: `offset_commit::validate` pre-dispatched on a
    /// per-handle kind mirror, so the broker could route a downgraded classic
    /// member's offset commit to the next-gen epoch path. `group.as_consumer()`
    /// is now `None`, so that path would reject with `UNKNOWN_MEMBER_ID`.
    /// With the single-source-of-truth fix, the one `ValidateCommit` message
    /// dispatches on the actor's LIVE `group.kind`, which is now classic.
    /// `classic_ops::validate_commit` then finds the re-expressed classic
    /// member and accepts the commit (`Ok(())`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawned_consumer_group_downgrade_allows_classic_offset_commit() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);

        // SPAWN the actor as a consumer group: the first RPC is a native
        // ConsumerGroupHeartbeat, so the handle's spawn-time `kind == Consumer`.
        let handle = coord.get_or_create_consumer("g");
        assert!(
            handle.kind == GroupKindTag::Consumer,
            "the group must be spawned consumer-kind"
        );

        let up = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");

        // A CLASSIC member joins the (consumer-kind) group as a hosted member.
        let join = rpc::classic_join(&handle, "m-classic", "t").await;
        assert!(join.error_code == codes::NONE);

        // The native consumer member leaves (member_epoch -1). It was the only
        // native member and a hosted classic member remains → DOWNGRADE in
        // place. The group is now live-Classic but the handle was spawned
        // Consumer (its `kind` field stays stale). `maybe_downgrade` runs inside
        // the Heartbeat handler AFTER the reply is sent, so we round-trip one
        // more message (the `classic_inspect` below) to be sure the in-place
        // flip has completed before validating.
        let leave = rpc::consumer_heartbeat(&handle, &native, -1, None).await;
        assert!(leave.error_code == codes::NONE);

        // The hosted classic member was re-expressed as a classic member. Read
        // the restored classic generation it must commit against. This
        // `ClassicInspect` round-trip is also the barrier that guarantees the
        // downgrade completed (only a classic-kind group answers it; the actor
        // processes it strictly after the leave's `maybe_downgrade`).
        let view = rpc::classic_inspect(&handle).await;
        // The handle's spawn-time `kind` is unchanged (and stale) — validation
        // must NOT consult it.
        assert!(
            handle.kind == GroupKindTag::Consumer,
            "spawn-time kind unchanged"
        );
        assert!(
            view.members.iter().any(|m| m.member_id == "m-classic"),
            "the hosted classic member must survive the downgrade"
        );
        let generation = view.generation_id;

        // Prove the fix at the routing boundary `offset_commit::validate` uses:
        // the single `ValidateCommit` message dispatches on the actor's LIVE
        // `group.kind` (now classic) and accepts the downgraded classic member's
        // commit (`Ok(())`). Pre-refactor, a handle-side mirror could route this
        // to the consumer epoch path and reject with `UNKNOWN_MEMBER_ID`.
        let (tx, rx) = tokio::sync::oneshot::channel();
        handle
            .tx
            .send(GroupActorMessage::ValidateCommit {
                member_id: "m-classic".into(),
                group_instance_id: None,
                generation_or_epoch: generation,
                reply: tx,
            })
            .await
            .unwrap();
        let result = rx.await.unwrap();
        assert!(
            result == Ok(()),
            "ValidateCommit must dispatch on the live (classic) kind and accept \
             the downgraded member (got {result:?})"
        );
    }

    /// Regression (user-requested): an UPGRADED group runs the consumer epoch
    /// fence on a native consumer member's commit. A classic group upgrades
    /// when a native consumer heartbeats in, so the handle's spawn-time `kind`
    /// is a stale `Classic`. `ValidateCommit` for that native member must
    /// dispatch on the LIVE consumer kind and apply the epoch fence. A STALE
    /// epoch, below the current one, gives `STALE_MEMBER_EPOCH`. A FENCED
    /// epoch, above the current one, gives `FENCED_MEMBER_EPOCH`. Before the
    /// refactor, a spawned-Classic upgraded group took the classic validate
    /// path and SKIPPED the epoch check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn upgraded_group_fences_stale_native_consumer_commit() {
        use crate::coordinator::unified::config::ConsumerGroupMigrationPolicy;
        let (coord, _log) =
            make_coordinator_with_topic_policy("t", 2, ConsumerGroupMigrationPolicy::Bidirectional);

        // SPAWN classic-kind via a seeded classic member, then UPGRADE by having
        // a native consumer heartbeat in. The handle's spawn-time `kind` stays
        // the stale `Classic`.
        let handle = seed_classic_member(&coord, "m1", "t", None);
        assert!(handle.kind == GroupKindTag::Classic);
        let up = rpc::consumer_heartbeat(&handle, "", 0, Some("t")).await;
        assert!(up.error_code == codes::NONE);
        let native = up.member_id.expect("native member id");
        let current_epoch = up.member_epoch;

        // The handle's spawn-time kind is the stale `Classic`; validation must
        // not consult it — it must run the consumer epoch fence.
        assert!(handle.kind == GroupKindTag::Classic);

        let cases = [
            // STALE epoch (< current) → STALE_MEMBER_EPOCH.
            (current_epoch - 1, Err(codes::STALE_MEMBER_EPOCH), "stale"),
            // FENCED epoch (> current) → FENCED_MEMBER_EPOCH.
            (current_epoch + 1, Err(codes::FENCED_MEMBER_EPOCH), "fenced"),
            // The current epoch is accepted.
            (current_epoch, Ok(()), "current"),
        ];
        for (epoch, want, label) in cases {
            let got = rpc::validate_commit(&handle, &native, epoch).await;
            assert!(
                got == want,
                "an upgraded group must run the consumer epoch fence ({label}, epoch {epoch}); got {got:?}"
            );
        }
    }
}
