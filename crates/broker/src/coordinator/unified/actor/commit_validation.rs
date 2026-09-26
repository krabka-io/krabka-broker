//! Offset-commit fencing.
//!
//! `OffsetCommit` and `TxnOffsetCommit` both dispatch on the group's LIVE
//! protocol inside the actor, so the decisions and the request/reply wrappers
//! that carry them live in one module. The two requests fence by different
//! rules, as Kafka's `validateOffsetCommit` does with its `isTransactional`
//! flag, and [`CommitFence`] says which rule a `ValidateCommit` runs.

use tokio::sync::oneshot;

use super::{ErrorCode, GroupActorHandle, GroupActorMessage};
use crate::{
    codes,
    coordinator::unified::{classic_ops, consumer_state::GroupState, group::CoordinatorGroup},
};

/// The first `OffsetCommit` version that a member of the consumer protocol
/// (KIP-848) may use. Kafka's `ConsumerGroup.validateOffsetCommit` answers
/// `UNSUPPORTED_VERSION` below it.
const FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION: i16 = 9;

/// The request whose rule a `ValidateCommit` runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitFence {
    /// `OffsetCommit` at this api version: Kafka's `validateOffsetCommit`
    /// with `isTransactional = false`.
    Offset { api_version: i16 },
    /// `TxnOffsetCommit`.
    Transactional,
}

pub(super) fn validate_commit_message(
    group: &mut CoordinatorGroup,
    member_id: &str,
    group_instance_id: Option<&str>,
    generation_or_epoch: i32,
    fence: CommitFence,
) -> Result<(), ErrorCode> {
    match fence {
        CommitFence::Offset { api_version } => {
            if let Some(state) = group.as_consumer() {
                return validate_consumer_offset_commit(
                    state,
                    member_id,
                    generation_or_epoch,
                    api_version,
                );
            }
            if let Some(state) = group.as_classic_mut() {
                classic_ops::validate_offset_commit(
                    state,
                    member_id,
                    group_instance_id,
                    generation_or_epoch,
                )?;
                classic_ops::refresh_committer_session(state, member_id);
            }
            Ok(())
        }
        CommitFence::Transactional => {
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
    }
}

/// Kafka's `ConsumerGroup.validateOffsetCommit` for an `OffsetCommit`.
///
/// A negative epoch commits on a group with no members: that is the admin
/// client or a consumer that does not use group management. Otherwise the
/// member must exist, a member of the consumer protocol must use `OffsetCommit`
/// v9 or later, and the epoch must be the member's epoch. A newer epoch
/// answers `STALE_MEMBER_EPOCH`, or `ILLEGAL_GENERATION` for a member of the
/// classic protocol.
///
/// An older epoch answers the same codes. Kafka (KIP-1251) accepts an older
/// epoch for a partition assigned to the member at or before that epoch; the
/// group does not track the epoch at which each partition was assigned, so it
/// cannot tell those partitions apart and refuses them all, as Kafka did
/// before KIP-1251.
fn validate_consumer_offset_commit(
    state: &GroupState,
    member_id: &str,
    member_epoch: i32,
    api_version: i16,
) -> Result<(), ErrorCode> {
    if member_epoch < 0 && state.members.is_empty() {
        return Ok(());
    }
    let member = state
        .members
        .get(member_id)
        .ok_or(codes::UNKNOWN_MEMBER_ID)?;
    let classic = member.is_classic();
    if !classic && api_version < FIRST_CONSUMER_PROTOCOL_COMMIT_VERSION {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    if member_epoch == member.member_epoch {
        Ok(())
    } else if classic {
        Err(codes::ILLEGAL_GENERATION)
    } else {
        Err(codes::STALE_MEMBER_EPOCH)
    }
}

/// Sends one `ValidateCommit` to the group's actor and waits for the answer.
///
/// Dispatch happens inside the actor on the LIVE `group.kind`. It does not use
/// the spawn-time `handle.kind` hint, because a KIP-848 migration may have
/// flipped the protocol in place after spawn.
async fn send_validate_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
    fence: CommitFence,
) -> Option<ErrorCode> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(GroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            group_instance_id: group_instance_id.map(str::to_string),
            generation_or_epoch,
            fence,
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

/// Validates a `TxnOffsetCommit` against the group's membership and
/// generation (classic) or member epoch (KIP-848 next-gen).
///
/// It returns `Some(error_code)` when the commit must be rejected, and `None`
/// when the commit may proceed. For a simple consumer (empty `member_id`, no
/// `group_instance_id`) the classic path does nothing, so the broker never
/// fences a producer that supplies no group metadata.
pub(crate) async fn validate_group_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
) -> Option<ErrorCode> {
    send_validate_commit(
        handle,
        member_id,
        generation_or_epoch,
        group_instance_id,
        CommitFence::Transactional,
    )
    .await
}

/// Validates an `OffsetCommit` at `api_version` against the group's live
/// protocol, by Kafka's `ClassicGroup.validateOffsetCommit` or
/// `ConsumerGroup.validateOffsetCommit`.
///
/// It returns `Some(error_code)` when the commit must be rejected, and `None`
/// when the commit may proceed. A commit that passes refreshes the session of
/// a classic member while the group is `Stable` or `PreparingRebalance`.
pub(crate) async fn validate_offset_commit(
    handle: &GroupActorHandle,
    member_id: &str,
    generation_or_epoch: i32,
    group_instance_id: Option<&str>,
    api_version: i16,
) -> Option<ErrorCode> {
    send_validate_commit(
        handle,
        member_id,
        generation_or_epoch,
        group_instance_id,
        CommitFence::Offset { api_version },
    )
    .await
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
        consumer_state::{ClassicMemberFacade, test_support::member as consumer_member},
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
                        expire_timestamp_ms: None,
                        topic_id: None,
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
                fence: CommitFence::Offset { api_version: 9 },
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
                fence: CommitFence::Offset { api_version: 9 },
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
    /// dispatch on the LIVE consumer kind and apply the epoch fence. Before the
    /// refactor, a spawned-Classic upgraded group took the classic validate
    /// path and SKIPPED the epoch check.
    ///
    /// `OffsetCommit` answers `STALE_MEMBER_EPOCH` on either side of the
    /// member epoch, as Kafka's `ConsumerGroup.validateOffsetCommit` does.
    /// `TxnOffsetCommit` keeps its own rule.
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

        let offset = CommitFence::Offset { api_version: 9 };
        let cases = [
            (offset, current_epoch - 1, Err(codes::STALE_MEMBER_EPOCH)),
            (offset, current_epoch + 1, Err(codes::STALE_MEMBER_EPOCH)),
            (offset, current_epoch, Ok(())),
            (
                CommitFence::Offset { api_version: 8 },
                current_epoch,
                Err(codes::UNSUPPORTED_VERSION),
            ),
            (
                CommitFence::Transactional,
                current_epoch - 1,
                Err(codes::STALE_MEMBER_EPOCH),
            ),
            (
                CommitFence::Transactional,
                current_epoch + 1,
                Err(codes::FENCED_MEMBER_EPOCH),
            ),
            (CommitFence::Transactional, current_epoch, Ok(())),
        ];
        let mut actual = Vec::new();
        for (fence, epoch, _) in cases {
            let got = rpc::validate_commit(&handle, &native, epoch, fence).await;
            actual.push((fence, epoch, got));
        }
        assert!(actual == cases);
    }

    /// One row of the consumer-group `OffsetCommit` table: the group's members
    /// as `(member id, member epoch, classic protocol)`, then the request's
    /// member id, epoch and api version, and the result.
    type ConsumerCase = (
        &'static [(&'static str, i32, bool)],
        &'static str,
        i32,
        i16,
        Result<(), i16>,
    );

    /// Kafka's `ConsumerGroup.validateOffsetCommit` for `OffsetCommit`, row by
    /// row.
    #[test]
    fn consumer_group_offset_commit_follows_kafka_rule() {
        const NATIVE: &[(&str, i32, bool)] = &[("native", 5, false)];
        const CLASSIC: &[(&str, i32, bool)] = &[("classic", 5, true)];
        let cases: [ConsumerCase; 12] = [
            // The admin client commits on a group with no members.
            (&[], "", -1, 9, Ok(())),
            (&[], "", -1, 2, Ok(())),
            // ... and not on a group with members.
            (NATIVE, "", -1, 9, Err(codes::UNKNOWN_MEMBER_ID)),
            // A member id the group does not hold.
            (&[], "ghost", 1, 9, Err(codes::UNKNOWN_MEMBER_ID)),
            (NATIVE, "ghost", 5, 9, Err(codes::UNKNOWN_MEMBER_ID)),
            // A member of the consumer protocol.
            (NATIVE, "native", 5, 9, Ok(())),
            (NATIVE, "native", 6, 9, Err(codes::STALE_MEMBER_EPOCH)),
            (NATIVE, "native", 4, 9, Err(codes::STALE_MEMBER_EPOCH)),
            (NATIVE, "native", 5, 8, Err(codes::UNSUPPORTED_VERSION)),
            // A member of the classic protocol.
            (CLASSIC, "classic", 5, 2, Ok(())),
            (CLASSIC, "classic", 6, 9, Err(codes::ILLEGAL_GENERATION)),
            (CLASSIC, "classic", 4, 9, Err(codes::ILLEGAL_GENERATION)),
        ];
        let mut actual = Vec::new();
        for (members, member_id, epoch, api_version, _) in cases {
            let mut state = GroupState::new("g");
            for &(id, member_epoch, classic) in members {
                let mut member = consumer_member(id);
                member.member_epoch = member_epoch;
                member.classic = classic.then(|| ClassicMemberFacade {
                    generation_id: member_epoch,
                    supported_protocols: Vec::new(),
                    session_timeout: std::time::Duration::from_secs(45),
                    last_synced_assignment: bytes::Bytes::new(),
                    awaiting_sync: false,
                });
                state.members.insert(id.to_string(), member);
            }
            let got = validate_consumer_offset_commit(&state, member_id, epoch, api_version);
            actual.push((members, member_id, epoch, api_version, got));
        }
        assert!(actual == cases);
    }
}
