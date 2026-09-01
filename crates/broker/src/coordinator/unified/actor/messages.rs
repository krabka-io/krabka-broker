//! The actor mailbox vocabulary: the [`GroupActorMessage`] command enum and
//! the structured results the parking classic RPCs reply with, kept together
//! because every handler module speaks this one protocol.

use bytes::Bytes;
use krabka_protocol::owned::{
    consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
    consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
    heartbeat_request::HeartbeatRequest, join_group_request::JoinGroupRequest,
    leave_group_request::LeaveGroupRequest, leave_group_response::MemberResponse,
    sync_group_request::SyncGroupRequest,
};
use tokio::sync::oneshot;

use super::{ClassicView, DescribeView, ErrorCode};
use crate::{
    codes,
    coordinator::{
        DeleteGroupError, GroupSnapshot,
        unified::{
            GroupSeed,
            classic_state::OffsetEntry,
            group::{CoordinatorGroup, GroupOffsets},
        },
    },
};

#[derive(Debug)]
pub enum GroupActorMessage {
    // ── next-gen consumer protocol (non-parking) ──
    Heartbeat {
        request: ConsumerGroupHeartbeatRequest,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<ConsumerGroupHeartbeatResponse>,
    },
    /// Validate an `OffsetCommit` against the group's LIVE protocol. The actor
    /// dispatches on `group.kind`. Next-gen checks `member_epoch`. Classic
    /// checks member, instance, and generation. `Ok(())` allows the commit and
    /// `Err(code)` rejects it.
    ValidateCommit {
        member_id: String,
        group_instance_id: Option<String>,
        /// The request's `generation_id_or_member_epoch` field. The actor
        /// reads it as the consumer `member_epoch` or as the classic
        /// generation, depending on the live kind.
        generation_or_epoch: i32,
        reply: oneshot::Sender<Result<(), ErrorCode>>,
    },
    Describe {
        reply: oneshot::Sender<DescribeView>,
    },

    // ── classic protocol (parking) ──
    ClassicJoin {
        req: JoinGroupRequest,
        version: i16,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<JoinResult>,
    },
    ClassicSync {
        req: SyncGroupRequest,
        reply: oneshot::Sender<SyncResult>,
    },
    ClassicHeartbeat {
        req: HeartbeatRequest,
        reply: oneshot::Sender<ErrorCode>,
    },
    ClassicLeave {
        req: LeaveGroupRequest,
        version: i16,
        reply: oneshot::Sender<LeaveResult>,
    },
    /// Atomically verify that a classic group is empty and append its k2
    /// tombstone. A successful delete stops the actor.
    ClassicDelete {
        reply: oneshot::Sender<Result<(), DeleteGroupError>>,
    },
    /// Read-only classic snapshot for the admin/offset-delete handlers.
    ClassicInspect {
        reply: oneshot::Sender<ClassicView>,
    },
    /// Kind-agnostic admin snapshot for the classic `ListGroups` and
    /// `DescribeGroups` path. It projects the LIVE group into a
    /// `GroupSnapshot`, whether that group is classic or consumer, and
    /// including a hosted-classic group after migration. A migrated group
    /// therefore reports coherently whatever the handle's spawn-time `kind`
    /// hint holds.
    InspectAny {
        reply: oneshot::Sender<GroupSnapshot>,
    },

    // ── committed offsets (protocol-agnostic; on `Group.committed_offsets`) ──
    UpdateCommitted {
        entries: Vec<((String, i32), OffsetEntry)>,
        reply: oneshot::Sender<()>,
    },
    /// Read the group's stable offsets and its unresolved transactional keys
    /// in one turn, for `OffsetFetch`.
    FetchOffsets {
        reply: oneshot::Sender<GroupOffsets>,
    },
    RemoveCommitted {
        keys: Vec<(String, i32)>,
        reply: oneshot::Sender<()>,
    },

    // ── in-flight transactional offsets (KIP-447) ──
    /// Record that `producer_id`'s open transaction has durably written
    /// offset commits for `keys` at offsets-log position `written_at`. Until
    /// its marker arrives, an `OffsetFetch` with `require_stable = true`
    /// answers `UNSTABLE_OFFSET_COMMIT` for them.
    ///
    /// `written_at` orders the mark against the producer's markers, which the
    /// sender cannot do for itself: it marks after its append is durable, so a
    /// marker for the very transaction it is marking can be resolved here in
    /// between.
    AddPendingTxnOffsets {
        producer_id: i64,
        written_at: i64,
        keys: Vec<(String, i32)>,
        reply: oneshot::Sender<()>,
    },
    /// Resolve `producer_id`'s transaction, whose marker is at offsets-log
    /// position `resolved_through`: publish `committed` and drop the
    /// producer's pending marks in the same turn. An abort marker sends an
    /// empty `committed`, which leaves the group's stable offsets as they
    /// were. Doing both in one message is what stops a `require_stable` fetch
    /// from seeing a partition that is neither pending nor yet updated.
    ResolveTxnOffsets {
        producer_id: i64,
        resolved_through: i64,
        committed: Vec<((String, i32), OffsetEntry)>,
        reply: oneshot::Sender<()>,
    },

    // ── bootstrap / lifecycle ──
    Seed(GroupSeed),
    /// Replace this (classic) actor's whole `Group` with replayed state.
    ClassicSeed(Box<CoordinatorGroup>),
    Shutdown(oneshot::Sender<()>),

    /// Test-only: flip the live `Group` to a fresh empty consumer group in
    /// place. This exercises the tick's dispatch on the live `group.kind`.
    #[cfg(test)]
    TestForceConsumerKind,
}

/// Structured `JoinGroup` result for the handler, which encodes it for the
/// wire version. It mirrors the fields of `JoinGroupResponse`.
#[derive(Debug, Default, Clone)]
pub struct JoinResult {
    pub error_code: ErrorCode,
    pub generation_id: i32,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinResultMember>,
}

#[derive(Debug, Clone)]
pub struct JoinResultMember {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub metadata: Bytes,
}

/// Structured `SyncGroup` result for the handler.
#[derive(Debug, Default, Clone)]
pub struct SyncResult {
    pub error_code: ErrorCode,
    pub assignment: Bytes,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
}

/// Structured `LeaveGroup` result. Versions 0–2 use the top-level error;
/// versions 3+ use the per-member results.
#[derive(Debug, Default)]
pub struct LeaveResult {
    pub error_code: ErrorCode,
    pub members: Vec<MemberResponse>,
}

pub(super) fn classic_leave_result(
    version: i16,
    result: Result<Vec<MemberResponse>, ErrorCode>,
) -> LeaveResult {
    match result {
        Ok(members) => match version {
            ..=2 => LeaveResult {
                error_code: members
                    .first()
                    .map_or(codes::NONE, |member| member.error_code),
                members,
            },
            _ => LeaveResult {
                error_code: codes::NONE,
                members,
            },
        },
        Err(error_code) => LeaveResult {
            error_code,
            members: Vec::new(),
        },
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn classic_leave_result_uses_the_wire_versions_error_shape() {
        let legacy = classic_leave_result(
            2,
            Ok(vec![MemberResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            }]),
        );
        check!(legacy.error_code == codes::UNKNOWN_MEMBER_ID);
        check!(legacy.members[0].error_code == codes::UNKNOWN_MEMBER_ID);

        let batched = classic_leave_result(
            3,
            Ok(vec![MemberResponse {
                error_code: codes::UNKNOWN_MEMBER_ID,
                ..Default::default()
            }]),
        );
        check!(batched.error_code == codes::NONE);
        check!(batched.members[0].error_code == codes::UNKNOWN_MEMBER_ID);

        let failed = classic_leave_result(3, Err(codes::COORDINATOR_LOAD_IN_PROGRESS));
        check!(failed.error_code == codes::COORDINATOR_LOAD_IN_PROGRESS);
        check!(failed.members.is_empty());
    }
}
