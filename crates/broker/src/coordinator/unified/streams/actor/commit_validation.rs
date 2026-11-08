//! The streams group's half of `OffsetCommit`, `TxnOffsetCommit`,
//! `OffsetFetch` and `OffsetDelete`: the round trips that ask the group's
//! actor, and the membership checks the actor runs, as Kafka's `StreamsGroup`
//! validates them.

use {
    super::{StreamsGroupActorHandle, StreamsGroupActorMessage},
    crate::{
        codes,
        coordinator::unified::{
            actor::{CommitFence, SubscribedTopics},
            commit_validation_reply,
            streams::{
                persistence::StreamsGroupTopologyValue,
                state::{StreamsGroupState, StreamsGroupStatePhase, StreamsMemberState},
            },
        },
        task_util::{AskError, ask},
    },
    std::{
        cmp::Ordering,
        collections::{BTreeMap, HashSet},
    },
};

/// Validates a `TxnOffsetCommit` of `partitions`, each a `(topic name,
/// partition)`, against a streams group's membership by sending a message to
/// its actor, as [`validate_offset_commit`] does with
/// [`CommitFence::Transactional`].
///
/// It returns `Some(error_code)` to reject the commit, and `None` to allow it.
///
/// The shared `validate_commit` knows only about the classic and
/// consumer `GroupActorHandle`. A streams-group consumer keeps its membership
/// in the streams actor, not a classic one, so this function must validate it
/// instead. Otherwise the broker fences the commit against an empty classic
/// actor and rejects it with `UNKNOWN_MEMBER_ID`.
pub(crate) async fn validate_streams_group_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    group_instance_id: Option<&str>,
    member_epoch: i32,
    partitions: Vec<(String, i32)>,
) -> Option<i16> {
    send_validate_commit(
        handle,
        CommitIdentity {
            member_id,
            group_instance_id,
            member_epoch,
            fence: CommitFence::Transactional,
        },
        partitions,
    )
    .await
}

/// Validates an `OffsetCommit` at `api_version` of `partitions`, each a
/// `(topic name, partition)`, against a streams group's membership by sending
/// a message to its actor, as [`validate_offset_commit`] does with
/// [`CommitFence::Offset`].
///
/// It returns `Some(error_code)` to reject the commit, and `None` to allow it.
pub(crate) async fn validate_streams_group_offset_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    api_version: i16,
    partitions: Vec<(String, i32)>,
) -> Option<i16> {
    send_validate_commit(
        handle,
        CommitIdentity {
            member_id,
            group_instance_id: None,
            member_epoch,
            fence: CommitFence::Offset { api_version },
        },
        partitions,
    )
    .await
}

/// Who a commit says it comes from, and which API sent it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct CommitIdentity<'a> {
    pub member_id: &'a str,
    pub group_instance_id: Option<&'a str>,
    pub member_epoch: i32,
    pub fence: CommitFence,
}

async fn send_validate_commit(
    handle: &StreamsGroupActorHandle,
    identity: CommitIdentity<'_>,
    partitions: Vec<(String, i32)>,
) -> Option<i16> {
    let asked = ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::ValidateCommit {
            member_id: identity.member_id.to_owned(),
            group_instance_id: identity.group_instance_id.map(str::to_owned),
            member_epoch: identity.member_epoch,
            fence: identity.fence,
            partitions,
            reply,
        }
    })
    .await;
    validation_answer(&asked)
}

/// The answer of a commit or fetch check that `asked` the actor. A closed
/// mailbox answers `UNKNOWN_SERVER_ERROR`; a reply, or a reply the actor
/// dropped, answers as [`commit_validation_reply`] does.
fn validation_answer(asked: &Result<Result<(), i16>, AskError>) -> Option<i16> {
    match asked {
        Err(AskError::Closed) => Some(codes::UNKNOWN_SERVER_ERROR),
        reply => commit_validation_reply(reply),
    }
}

/// Validates the v9+ member fields of an `OffsetFetch` against a streams
/// group's membership by sending a message to its actor, as
/// [`validate_offset_fetch`] does.
///
/// It returns `Some(error_code)` to refuse the fetch, and `None` to allow it.
///
/// A streams group keeps its members in the streams actor, while its
/// committed offsets live in the offsets-home actor, which knows no streams
/// member. Without this check a stale or unknown member would read the
/// group's offsets.
pub(crate) async fn validate_streams_group_offset_fetch(
    handle: &StreamsGroupActorHandle,
    member_id: Option<&str>,
    member_epoch: i32,
) -> Option<i16> {
    let asked = ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::ValidateFetch {
            member_id: member_id.map(str::to_owned),
            member_epoch,
            reply,
        }
    })
    .await;
    validation_answer(&asked)
}

/// Kafka's `StreamsGroup.validateOffsetFetch`.
///
/// A fetch with no member id and a negative epoch is the admin client or a
/// client that sends neither, and it is allowed. Otherwise the member must
/// exist (`UNKNOWN_MEMBER_ID`) and the epoch must be its member epoch
/// (`STALE_MEMBER_EPOCH`, for an older epoch as for a newer one).
///
/// # Errors
///
/// Returns the group error code of a refused fetch.
pub(crate) fn validate_offset_fetch(
    state: &StreamsGroupState,
    member_id: Option<&str>,
    member_epoch: i32,
) -> Result<(), i16> {
    if member_id.is_none() && member_epoch < 0 {
        return Ok(());
    }
    let member = member_id
        .and_then(|id| state.members.get(id))
        .ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if member.member_epoch == member_epoch {
        Ok(())
    } else {
        Err(codes::STALE_MEMBER_EPOCH)
    }
}

/// Asks a streams group which topics `OffsetDelete` must leave alone, as
/// Kafka's `OffsetMetadataManager.deleteOffsets` asks
/// `StreamsGroup.isSubscribedToTopic`. `StreamsGroup.validateOffsetDelete`
/// accepts every group.
///
/// # Errors
///
/// `GROUP_ID_NOT_FOUND` when the actor has stopped, as the classic and
/// consumer paths answer for theirs.
pub(crate) async fn streams_offset_delete_guard(
    handle: &StreamsGroupActorHandle,
) -> Result<SubscribedTopics, i16> {
    ask(&handle.tx, |reply| {
        StreamsGroupActorMessage::OffsetDeleteGuard { reply }
    })
    .await
    .map_err(|_| codes::GROUP_ID_NOT_FOUND)
}

/// Kafka's `StreamsGroup.isSubscribedToTopic`, for every topic at once.
///
/// An empty group subscribes to nothing, so that its offsets can be deleted
/// and expire. Otherwise the group subscribes to the source and repartition
/// source topics of its stored topology (`StreamsTopology.sourceTopicMap`),
/// and to nothing before it has one.
pub(crate) fn subscribed_topics(
    state: &StreamsGroupState,
    topology: Option<&StreamsGroupTopologyValue>,
) -> SubscribedTopics {
    let topics = match topology {
        Some(topology) if state.phase != StreamsGroupStatePhase::Empty => topology
            .subtopologies
            .iter()
            .flat_map(|subtopology| {
                subtopology.source_topics.iter().cloned().chain(
                    subtopology
                        .repartition_source_topics
                        .iter()
                        .map(|topic| topic.name.clone()),
                )
            })
            .collect(),
        _ => HashSet::new(),
    };
    SubscribedTopics::Named(topics)
}

/// The first `OffsetCommit` version that a member of the streams protocol may
/// use.
const FIRST_STREAMS_PROTOCOL_COMMIT_VERSION: i16 = 9;

/// Kafka's `StreamsGroup.validateOffsetCommit`.
///
/// A negative epoch commits on a group with no members: that is the admin
/// client or a consumer that does not use group management. A
/// `TxnOffsetCommit` with no member id, the unknown generation and no group
/// instance id carries no member to check. Otherwise the member must exist,
/// an `OffsetCommit` must be v9 or later, and the member's own epoch commits
/// every partition; a newer epoch is `STALE_MEMBER_EPOCH`.
///
/// An older epoch goes through Kafka's `createAssignmentEpochValidator`
/// (KIP-1251) over `partitions`, each a `(topic name, partition)`: see
/// [`assignment_epoch_error`].
///
/// # Errors
///
/// Returns the error code of a refused commit, which refuses every partition
/// of it.
pub(crate) fn validate_offset_commit(
    state: &StreamsGroupState,
    topology: Option<&StreamsGroupTopologyValue>,
    identity: CommitIdentity<'_>,
    partitions: &[(String, i32)],
) -> Result<(), i16> {
    let CommitIdentity {
        member_id,
        group_instance_id,
        member_epoch,
        fence,
    } = identity;
    if member_epoch < 0 && state.members.is_empty() {
        return Ok(());
    }
    if fence == CommitFence::Transactional
        && member_epoch == -1
        && member_id.is_empty()
        && group_instance_id.is_none()
    {
        return Ok(());
    }
    let member = state
        .members
        .get(member_id)
        .ok_or(codes::UNKNOWN_MEMBER_ID)?;
    if let CommitFence::Offset { api_version } = fence
        && api_version < FIRST_STREAMS_PROTOCOL_COMMIT_VERSION
    {
        return Err(codes::UNSUPPORTED_VERSION);
    }
    match member_epoch.cmp(&member.member_epoch) {
        Ordering::Equal => Ok(()),
        Ordering::Greater => Err(codes::STALE_MEMBER_EPOCH),
        Ordering::Less => {
            assignment_epoch_error(member, topology, member_epoch, partitions).map_or(Ok(()), Err)
        }
    }
}

/// Kafka 4.3.1's `StreamsGroup.createAssignmentEpochValidator` (KIP-1251),
/// which lets a member commit at an epoch older than its own.
///
/// The group must hold a topology. Each partition's topic must be a source or
/// repartition source topic of one of its subtopologies
/// (`StreamsTopology.sourceTopicMap`), and the partition must be an active task
/// of that subtopology which the member holds, assigned or pending
/// revocation. The epoch at which the member was assigned that task, the
/// `AssignmentEpochs` of its current assignment record, must not be newer than
/// `member_epoch`. A failure of any of these is `STALE_MEMBER_EPOCH`, and it
/// refuses the whole commit.
///
/// Kafka's `sourceTopicMap` is a `HashMap` that each subtopology overwrites in
/// turn, so a topic that two subtopologies read maps to the last one written.
/// This function takes the last subtopology in the topology's order. A
/// topology that Kafka accepts never has such a topic.
fn assignment_epoch_error(
    member: &StreamsMemberState,
    topology: Option<&StreamsGroupTopologyValue>,
    member_epoch: i32,
    partitions: &[(String, i32)],
) -> Option<i16> {
    let Some(topology) = topology else {
        return Some(codes::STALE_MEMBER_EPOCH);
    };
    let holds = |tasks: &BTreeMap<String, Vec<i32>>, subtopology: &str, partition: i32| {
        tasks
            .get(subtopology)
            .is_some_and(|partitions| partitions.contains(&partition))
    };
    let refused = partitions.iter().any(|(topic, partition)| {
        let Some(subtopology) = topology.subtopologies.iter().rev().find(|subtopology| {
            subtopology.source_topics.contains(topic)
                || subtopology
                    .repartition_source_topics
                    .iter()
                    .any(|source| &source.name == topic)
        }) else {
            return true;
        };
        let id = subtopology.subtopology_id.as_str();
        if !holds(&member.active, id, *partition)
            && !holds(&member.active_pending_revocation, id, *partition)
        {
            return true;
        }
        let assigned_at = member.active_task_epochs(id, &[*partition]);
        assigned_at
            .first()
            .is_none_or(|&epoch| member_epoch < epoch)
    });
    refused.then_some(codes::STALE_MEMBER_EPOCH)
}
