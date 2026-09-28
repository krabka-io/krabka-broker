//! KIP-1071 streams-group coordinator actor: a per-group tokio task that drives
//! the heartbeat epoch exchange, reconciliation, and persistence.
//!
//! This actor mirrors the overall shape of the KIP-932 share-group actor
//! ([`super::super::share::actor`]): a `tokio::select!` loop over an mpsc
//! message channel plus a `heartbeat_interval` session tick, the
//! `Pending*Records` → `RecordBatch` → `OffsetsLog::append` flush, and a
//! last-known-good cache hand-off through
//! `GroupCoordinator::update_streams_cache`.
//!
//! Two things differ. This actor assigns *tasks* `(subtopology, partition)`
//! across the active, standby, and warmup roles instead of topic partitions.
//! It also reconciles against a full `MetadataImage` through the
//! [`MetadataSource`], which resolves the topology and creates internal
//! topics, instead of the consumer `MetadataProvider`.
//!
//! Reconciliation needs a connected [`MetadataSource`]. The pure-coordinator
//! unit tests have no source, so the group stays `NotReady` with empty
//! assignments. Members there still mint a `member_id` and advance their
//! epoch, but the actor assigns no tasks.

use std::{collections::BTreeMap, sync::Arc};

use krabka_protocol::owned::{
    streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

mod heartbeat;
mod reconciliation;
mod records;
mod request;
pub(crate) mod response;

#[cfg(test)]
mod streams_group_model;

#[cfg(test)]
mod tests;

use self::{
    heartbeat::{handle_heartbeat, handle_session_tick},
    reconciliation::reconcile,
    records::{apply_seed, flush_pending, snapshot_pending_after_change},
    response::build_describe,
};
use super::{
    config::StreamsGroupConfig,
    persistence::{StreamsGroupPartitionMetadataValue, StreamsGroupTopologyValue},
    state::{self, StreamsGroupState},
};
use crate::{
    codes,
    coordinator::unified::{actor::CommitFence, offsets_log::OffsetsLog},
    metadata_source::MetadataSource,
};

/// Messages accepted by a [`StreamsGroupActorHandle`].
#[derive(Debug)]
pub enum StreamsGroupActorMessage {
    Heartbeat {
        request: Box<StreamsGroupHeartbeatRequest>,
        /// The request's API version. The `MISSING_CLIENT_TAGS` status goes
        /// out only at version 1 and above.
        version: i16,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<StreamsHeartbeatResult>,
    },
    Describe {
        reply: oneshot::Sender<StreamsDescribeView>,
    },
    /// Validates an `OffsetCommit` or `TxnOffsetCommit` against the streams
    /// group's membership, as Kafka's `StreamsGroup.validateOffsetCommit`
    /// does. `Ok(())` allows the commit, and `Err(code)` rejects it.
    ValidateCommit {
        member_id: String,
        /// The request's `generation_id_or_member_epoch` field, interpreted as
        /// the streams `member_epoch`.
        member_epoch: i32,
        fence: CommitFence,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    Seed(super::super::StreamsGroupSeed),
    Shutdown(oneshot::Sender<()>),
}

/// Kafka's `StreamsGroupHeartbeatResult`: the response, and the internal
/// topics that the handler must create through `CreateTopics`.
#[derive(Debug, Default)]
pub struct StreamsHeartbeatResult {
    pub response: StreamsGroupHeartbeatResponse,
    pub creatable_topics: Vec<super::topology::InternalTopicSpec>,
}

/// Read-only projection of [`StreamsGroupState`] for the
/// `StreamsGroupDescribe` handler.
#[derive(Debug, Clone)]
pub struct StreamsDescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub topology_epoch: i32,
    pub group_state: String,
    /// The topology that the members sent: the subtopologies and their
    /// topics. It is `None` only before any topology is initialized.
    pub topology: Option<StreamsGroupTopologyValue>,
    /// The topology as the last configuration sized it, when it is ready:
    /// Kafka describes this one, with the decided partition count of every
    /// internal topic.
    pub configured_topology: Option<super::topology::ConfiguredTopology>,
    /// The members, by member id.
    pub members: Vec<StreamsDescribeMember>,
}

/// One member of a [`StreamsDescribeView`].
#[derive(Debug, Clone, Default)]
pub struct StreamsDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub topology_epoch: i32,
    pub process_id: String,
    pub user_endpoint: Option<(String, u16)>,
    pub client_tags: Vec<(String, String)>,
    /// The task offsets that the member last reported, by
    /// `(subtopology, partition)`.
    pub task_offsets: BTreeMap<(String, i32), i64>,
    /// The task end offsets that the member last reported.
    pub task_end_offsets: BTreeMap<(String, i32), i64>,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
    /// The member's target assignment.
    pub target_active: BTreeMap<String, Vec<i32>>,
    pub target_standby: BTreeMap<String, Vec<i32>>,
    pub target_warmup: BTreeMap<String, Vec<i32>>,
}

#[derive(Debug)]
pub struct StreamsGroupActorHandle {
    pub tx: mpsc::Sender<StreamsGroupActorMessage>,
    _task: JoinHandle<()>,
}

impl StreamsGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<StreamsGroupConfig>,
        offsets_log: Arc<dyn OffsetsLog>,
        metadata_source: Option<Arc<dyn MetadataSource>>,
        coordinator: Arc<super::super::GroupCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.actor_mailbox_capacity);
        let task = tokio::spawn(actor_loop(
            group_id,
            config,
            offsets_log,
            metadata_source,
            coordinator,
            rx,
        ));
        Self { tx, _task: task }
    }
}

/// Validates a `TxnOffsetCommit` against a streams group's membership by
/// sending a message to its actor, as [`validate_offset_commit`] does with
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
    member_epoch: i32,
) -> Option<i16> {
    send_validate_commit(handle, member_id, member_epoch, CommitFence::Transactional).await
}

/// Validates an `OffsetCommit` at `api_version` against a streams group's
/// membership by sending a message to its actor, as
/// [`validate_offset_commit`] does with [`CommitFence::Offset`].
///
/// It returns `Some(error_code)` to reject the commit, and `None` to allow it.
pub(crate) async fn validate_streams_group_offset_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    api_version: i16,
) -> Option<i16> {
    send_validate_commit(
        handle,
        member_id,
        member_epoch,
        CommitFence::Offset { api_version },
    )
    .await
}

async fn send_validate_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
    fence: CommitFence,
) -> Option<i16> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(StreamsGroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            member_epoch,
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

/// The first `OffsetCommit` version that a member of the streams protocol may
/// use.
const FIRST_STREAMS_PROTOCOL_COMMIT_VERSION: i16 = 9;

/// Kafka's `StreamsGroup.validateOffsetCommit`.
///
/// A negative epoch commits on a group with no members: that is the admin
/// client or a consumer that does not use group management. A
/// `TxnOffsetCommit` with no member id and the unknown generation carries no
/// member to check. Otherwise the member must exist, an `OffsetCommit` must be
/// v9 or later, and the epoch must be the member's epoch; a newer epoch is
/// `STALE_MEMBER_EPOCH`.
///
/// Kafka accepts an older epoch for a partition whose task the member was
/// assigned at or before that epoch. The group does not keep the epoch at
/// which each task was assigned, so an older epoch is `STALE_MEMBER_EPOCH`
/// for every partition. `TxnOffsetCommit` passes no group instance id here, so
/// the transactional skip does not check it.
///
/// # Errors
///
/// Returns the error code of a refused commit.
pub(crate) fn validate_offset_commit(
    state: &StreamsGroupState,
    member_id: &str,
    member_epoch: i32,
    fence: CommitFence,
) -> Result<(), i16> {
    if member_epoch < 0 && state.members.is_empty() {
        return Ok(());
    }
    if fence == CommitFence::Transactional && member_epoch == -1 && member_id.is_empty() {
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
    if member_epoch == member.member_epoch {
        Ok(())
    } else {
        Err(codes::STALE_MEMBER_EPOCH)
    }
}

/// The actor's full mutable state.
///
/// It holds the in-memory state machine, the in-flight
/// `StreamsGroupTopologyValue`, and the last-derived partition metadata. The
/// actor keeps the resolved topology for persistence and reconcile, because
/// [`StreamsGroupState`] tracks only its presence and epoch.
#[derive(Clone)]
struct ActorState {
    state: StreamsGroupState,
    /// The full stored topology. It sits beside `state.topology`, which
    /// carries only the epoch. It is `None` until the first member supplies a
    /// topology.
    topology: Option<StreamsGroupTopologyValue>,
    /// Partition metadata from the most recent reconcile. The actor persists
    /// it as the group's `StreamsGroupPartitionMetadataValue`.
    partition_metadata: Option<StreamsGroupPartitionMetadataValue>,
    /// Kafka's `StreamsGroup.metadataHash`: the hash of the required topics in
    /// the image that the most recent reconcile configured the topology
    /// against. A heartbeat that sees another hash reconciles again.
    metadata_hash: i64,
    /// The internal topics that the topology needs and the metadata image
    /// does not hold. Every heartbeat answer carries them, as Kafka's
    /// `StreamsGroupHeartbeatResult.creatableTopics` does, and `KafkaApis`
    /// sends them to the controller as a `CreateTopics` request.
    creatable_topics: Vec<super::topology::InternalTopicSpec>,
    /// Set when a reconcile installed a new target. The next record batch then
    /// carries the target and current assignment of every member, because the
    /// new target changed all of them.
    target_changed: bool,
    /// Whether the topology was configured against the metadata image since
    /// the actor started. A seeded actor has not, so its first heartbeat
    /// configures the topology again, as Kafka does when the configured
    /// topology of a loaded group is empty.
    configured: bool,
    /// Kafka's `StreamsGroup.configuredTopology`: the topology as the most
    /// recent configuration against the metadata image sized it. It is
    /// `None` until a configuration succeeds.
    configured_topology: Option<super::topology::ConfiguredTopology>,
    /// When Kafka's initial rebalance delay of the group ends: set when the
    /// first member joins an empty group, and cleared when the delayed
    /// assignment runs.
    initial_rebalance_deadline: Option<tokio::time::Instant>,
    /// When the last target assignment was computed, for Kafka's assignment
    /// interval. `None` until one is computed.
    assignment_timestamp: Option<tokio::time::Instant>,
}

impl ActorState {
    fn new(group_id: String) -> Self {
        Self {
            state: StreamsGroupState::new(group_id),
            topology: None,
            partition_metadata: None,
            metadata_hash: 0,
            creatable_topics: Vec::new(),
            target_changed: false,
            configured: false,
            configured_topology: None,
            initial_rebalance_deadline: None,
            assignment_timestamp: None,
        }
    }
}

async fn actor_loop(
    group_id: String,
    default_config: Arc<StreamsGroupConfig>,
    offsets_log: Arc<dyn OffsetsLog>,
    metadata_source: Option<Arc<dyn MetadataSource>>,
    coordinator: Arc<super::super::GroupCoordinator>,
    mut rx: mpsc::Receiver<StreamsGroupActorMessage>,
) {
    let mut config = resolve_group_config(&default_config, metadata_source.as_ref(), &group_id);
    let mut metadata_rx = metadata_source.as_ref().map(|source| source.watch_image());
    let mut actor = ActorState::new(group_id);
    let mut tick = tokio::time::interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                let refused = match handle_message(
                    &mut actor,
                    &config,
                    &*offsets_log,
                    metadata_source.as_ref(),
                    &coordinator,
                    msg,
                )
                .await
                {
                    Step::Continue => continue,
                    Step::Stop => break,
                    Step::RefusedJoin(refused) => refused,
                };
                // Kafka writes no record for a heartbeat that its coordinator
                // refuses, so such a heartbeat never creates the group. With
                // nothing queued behind it, the actor of a group that holds
                // nothing closes its mailbox, answers what raced in, and
                // leaves no group behind.
                if !rx.is_empty() {
                    refused.send();
                    continue;
                }
                rx.close();
                while let Some(msg) = rx.recv().await {
                    if let Step::RefusedJoin(other) = handle_message(
                        &mut actor,
                        &config,
                        &*offsets_log,
                        metadata_source.as_ref(),
                        &coordinator,
                        msg,
                    )
                    .await
                    {
                        other.send();
                    }
                }
                if actor.holds_nothing() {
                    forget_abandoned_group(&coordinator, &actor.state.group_id);
                }
                refused.send();
                break;
            }
            _ = tick.tick() => {
                if handle_session_tick(&mut actor, &config, &*offsets_log, metadata_source.as_ref(), &coordinator).await.is_err() {
                    break;
                }
            }
            () = wait_for_initial_rebalance_delay(actor.initial_rebalance_deadline) => {
                // Kafka's `computeDelayedTargetAssignment`.
                actor.initial_rebalance_deadline = None;
                if actor.state.members.is_empty() || !actor.assignment_pending() {
                    continue;
                }
                reconcile(&mut actor, &config, metadata_source.as_ref());
                let pending = snapshot_pending_after_change(&mut actor, &[]);
                if flush_pending(&actor, pending, &*offsets_log, &coordinator, chrono_now_ms())
                    .await
                    .is_err()
                {
                    break;
                }
            }
            () = wait_for_rebalance_deadline(actor.state.next_rebalance_deadline()) => {
                if handle_session_tick(&mut actor, &config, &*offsets_log, metadata_source.as_ref(), &coordinator).await.is_err() {
                    break;
                }
            }
            image = wait_for_metadata_change(&mut metadata_rx) => {
                let Some(image) = image else {
                    metadata_rx = None;
                    continue;
                };
                let next = resolve_group_config_from_image(
                    &default_config,
                    &image,
                    &actor.state.group_id,
                );
                if next != config {
                    config = next;
                    tick = tokio::time::interval(config.heartbeat_interval);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    actor.state.dirty = true;
                    reconcile(&mut actor, &config, metadata_source.as_ref());
                    let pending = snapshot_pending_after_change(&mut actor, &[]);
                    if flush_pending(
                        &actor,
                        pending,
                        &*offsets_log,
                        &coordinator,
                        chrono_now_ms(),
                    )
                    .await
                    .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }
}

/// What the actor loop does after one message.
enum Step {
    Continue,
    Stop,
    /// A heartbeat that the coordinator refused, for a group that holds
    /// nothing. The loop sends the answer once it decided whether the group
    /// goes away.
    RefusedJoin(Box<DeferredReply>),
}

/// A heartbeat answer that the loop sends later.
struct DeferredReply {
    reply: oneshot::Sender<StreamsHeartbeatResult>,
    response: StreamsGroupHeartbeatResponse,
}

impl DeferredReply {
    fn send(self) {
        let _ = self.reply.send(StreamsHeartbeatResult {
            response: self.response,
            creatable_topics: Vec::new(),
        });
    }
}

impl ActorState {
    /// Whether the target assignment is behind the group epoch.
    fn assignment_pending(&self) -> bool {
        self.state.target.epoch < self.state.group_epoch
    }

    /// The configured topology, when it is ready for an assignment.
    fn ready_topology(&self) -> Option<&super::topology::ConfiguredTopology> {
        self.configured_topology
            .as_ref()
            .filter(|configured| configured.is_ready())
    }

    /// Whether the group holds nothing that a record wrote: its initial group
    /// epoch, no member and no topology. Such a group exists only because a
    /// heartbeat reached its actor.
    fn holds_nothing(&self) -> bool {
        self.state.group_epoch == state::INITIAL_EPOCH
            && self.state.members.is_empty()
            && self.topology.is_none()
    }
}

/// Drops the registry entry and the `Streams` type lock of a group whose
/// actor closed without writing a record. A newer actor for the same id, whose
/// mailbox is open, keeps both.
fn forget_abandoned_group(coordinator: &super::super::GroupCoordinator, group_id: &str) {
    coordinator
        .streams_groups
        .remove_if(group_id, |_, handle| handle.tx.is_closed());
    coordinator
        .group_types
        .remove_if(group_id, |_, group_type| {
            *group_type == super::super::GroupType::Streams
                && !coordinator.streams_groups.contains_key(group_id)
        });
}

/// Handles one mailbox message.
async fn handle_message(
    actor: &mut ActorState,
    config: &StreamsGroupConfig,
    offsets_log: &dyn OffsetsLog,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    coordinator: &Arc<super::super::GroupCoordinator>,
    msg: StreamsGroupActorMessage,
) -> Step {
    match msg {
        StreamsGroupActorMessage::Heartbeat {
            request,
            version,
            client_id,
            client_host,
            reply,
        } => {
            match handle_heartbeat(
                actor,
                config,
                offsets_log,
                metadata_source,
                coordinator,
                &request,
                super::super::ClientIdentity {
                    id: &client_id,
                    host: &client_host,
                },
            )
            .await
            {
                Ok(response) if response.error_code != codes::NONE && actor.holds_nothing() => {
                    return Step::RefusedJoin(Box::new(DeferredReply { reply, response }));
                }
                Ok(mut response) => {
                    response::add_missing_client_tags(
                        &mut response,
                        &actor.state,
                        config,
                        &request,
                        version,
                    );
                    // Kafka answers the internal topics to create only with a
                    // response that the group accepted.
                    let creatable_topics = if response.error_code == codes::NONE {
                        actor.creatable_topics.clone()
                    } else {
                        Vec::new()
                    };
                    let _ = reply.send(StreamsHeartbeatResult {
                        response,
                        creatable_topics,
                    });
                }
                Err(e) => {
                    tracing::warn!(
                        group_id = %actor.state.group_id,
                        error = %e,
                        "streams-group actor exiting after log-write failure",
                    );
                    let _ = reply.send(StreamsHeartbeatResult {
                        response: response::error_resp(codes::COORDINATOR_LOAD_IN_PROGRESS, None),
                        creatable_topics: Vec::new(),
                    });
                    return Step::Stop;
                }
            }
        }
        StreamsGroupActorMessage::Describe { reply } => {
            let _ = reply.send(build_describe(
                &actor.state,
                actor.topology.as_ref(),
                actor.ready_topology(),
            ));
        }
        StreamsGroupActorMessage::ValidateCommit {
            member_id,
            member_epoch,
            fence,
            reply,
        } => {
            let _ = reply.send(validate_offset_commit(
                &actor.state,
                &member_id,
                member_epoch,
                fence,
            ));
        }
        StreamsGroupActorMessage::Seed(seed) => {
            apply_seed(actor, seed);
        }
        StreamsGroupActorMessage::Shutdown(reply) => {
            let _ = reply.send(());
            return Step::Stop;
        }
    }
    Step::Continue
}

fn resolve_group_config(
    defaults: &StreamsGroupConfig,
    metadata_source: Option<&Arc<dyn MetadataSource>>,
    group_id: &str,
) -> StreamsGroupConfig {
    metadata_source.map_or_else(
        || defaults.clone(),
        |source| resolve_group_config_from_image(defaults, &source.current_image(), group_id),
    )
}

fn resolve_group_config_from_image(
    defaults: &StreamsGroupConfig,
    image: &krabka_metadata::MetadataImage,
    group_id: &str,
) -> StreamsGroupConfig {
    let Some(overrides) = image.group_config(group_id) else {
        return defaults.clone();
    };
    match defaults.with_group_overrides(overrides) {
        Ok(config) => config,
        Err(error) => {
            tracing::error!(group_id, %error, "ignoring invalid persisted streams group config");
            defaults.clone()
        }
    }
}

/// Sleeps until `deadline`, or for ever when no rebalance timeout is armed.
async fn wait_for_rebalance_deadline(deadline: Option<std::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => std::future::pending().await,
    }
}

/// Sleeps until the initial rebalance delay ends, or for ever when none runs.
async fn wait_for_initial_rebalance_delay(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn wait_for_metadata_change(
    rx: &mut Option<tokio::sync::watch::Receiver<Arc<krabka_metadata::MetadataImage>>>,
) -> Option<Arc<krabka_metadata::MetadataImage>> {
    match rx {
        Some(rx) => {
            rx.changed().await.ok()?;
            Some(rx.borrow_and_update().clone())
        }
        None => std::future::pending().await,
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}
