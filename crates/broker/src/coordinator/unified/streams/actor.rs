//! KIP-1071 streams-group coordinator actor: a per-group tokio task that drives
//! the heartbeat epoch exchange, reconciliation, and persistence.
//!
//! This actor mirrors the overall shape of the KIP-932 share-group actor
//! ([`share::actor`]): a `tokio::select!` loop over an mpsc message channel
//! plus a `heartbeat_interval` session tick, the
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
//! Reconciliation needs the coordinator's [`MetadataSource`]. The actor reads
//! it from the coordinator when it wakes, not when it starts: the
//! `__consumer_offsets` replay spawns the actors of the loaded groups before
//! the broker connects the source. The pure-coordinator unit tests have no
//! source, so the group stays `NotReady` with empty assignments. Members there
//! still mint a `member_id` and advance their epoch, but the actor assigns no
//! tasks.
//!
//! [`share::actor`]: crate::coordinator::unified::share::actor

#[cfg(test)]
use crate::task_util::spawn_dropping_mailbox;
use {
    crate::time_util::sleep_until_opt,
    krabka_metadata::NodeId,
    krabka_protocol::owned::{
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
    std::{collections::BTreeMap, future, sync::Arc, time::Instant},
    tokio::{
        sync::{mpsc, oneshot, watch::Receiver},
        task::JoinHandle,
        time::{self, Interval, MissedTickBehavior, interval},
    },
};

pub(crate) mod commit_validation;
mod description;
mod heartbeat;
mod reconciliation;
mod records;
mod request;
pub(crate) mod response;

#[cfg(test)]
mod streams_group_model;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod test_support;

pub use self::description::{DescriptionPush, PushAnswer};
use {
    self::{
        commit_validation::{
            CommitIdentity, subscribed_topics, validate_offset_commit, validate_offset_fetch,
        },
        heartbeat::{handle_heartbeat, handle_session_tick},
        records::{apply_seed, reconcile_and_flush},
        response::build_describe,
    },
    super::{
        config::{StreamsGroupConfig, resolve_broker_and_group_config},
        description::{DescriptionEpochs, DescriptionRuntime, TopologyDescription},
        migration::DowngradeOutcome,
        persistence::StreamsGroupTopologyValue,
        state::{self, StreamsGroupState},
        topology::{ConfiguredTopology, InternalTopicSpec},
    },
    crate::{
        codes,
        coordinator::{
            DeleteGroupError,
            unified::{
                ClientIdentity, GroupCoordinator, GroupType, StreamsGroupSeed,
                actor::{CommitFence, ReapOutcome, SubscribedTopics},
                offsets_log::{OffsetsLog, write_failure_code},
            },
        },
        error::BrokerError,
        metadata_source::MetadataSource,
        task_util::spawn_mailbox,
    },
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
        /// The request's `group_instance_id`. Only a `TxnOffsetCommit` reads
        /// it: one that names an instance id is checked even with no member
        /// id and the unknown generation.
        group_instance_id: Option<String>,
        /// The request's `generation_id_or_member_epoch` field, interpreted as
        /// the streams `member_epoch`.
        member_epoch: i32,
        fence: CommitFence,
        /// The `(topic name, partition)` of every partition the commit
        /// writes, which an older epoch is checked against one by one.
        partitions: Vec<(String, i32)>,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    /// Validates the v9+ member fields of an `OffsetFetch` against the streams
    /// group's membership, as Kafka's `StreamsGroup.validateOffsetFetch`
    /// does. `Ok(())` allows the fetch, and `Err(code)` is the group error.
    ValidateFetch {
        member_id: Option<String>,
        member_epoch: i32,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    /// Answers the topics whose offsets `OffsetDelete` must not remove, as
    /// Kafka's `StreamsGroup.isSubscribedToTopic` decides them.
    OffsetDeleteGuard {
        reply: oneshot::Sender<SubscribedTopics>,
    },
    /// KIP-1331: a member's `StreamsGroupTopologyDescriptionUpdate`, past the
    /// handler's protocol gate, group `Read` grant and request checks.
    PushDescription {
        push: Box<DescriptionPush>,
        reply: oneshot::Sender<PushAnswer>,
    },
    Seed(StreamsGroupSeed),
    /// Retention runs under the Streams membership actor, never its offset home.
    ReapExpiredOffsets {
        now_ms: i64,
        retention_ms: i64,
        reply: oneshot::Sender<ReapOutcome>,
    },
    Delete(oneshot::Sender<Result<(), DeleteGroupError>>),
    Convert {
        member_id: String,
        now_ms: i64,
        reply: oneshot::Sender<Result<DowngradeOutcome, BrokerError>>,
    },
    DeleteDescription(oneshot::Sender<Result<bool, BrokerError>>),
    PrepareConversion(oneshot::Sender<Result<bool, BrokerError>>),
    Shutdown(oneshot::Sender<()>),
}

/// Kafka's `StreamsGroupHeartbeatResult`: the response, and the internal
/// topics that the handler must create through `CreateTopics`.
#[derive(Debug, Default)]
pub struct StreamsHeartbeatResult {
    pub response: StreamsGroupHeartbeatResponse,
    pub creatable_topics: Vec<InternalTopicSpec>,
}

/// Read-only projection of [`StreamsGroupState`] for the
/// `StreamsGroupDescribe` handler.
#[derive(Debug, Clone)]
pub struct StreamsDescribeView {
    pub assignor_name: String,
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
    pub configured_topology: Option<ConfiguredTopology>,
    /// The members, by member id.
    pub members: Vec<StreamsDescribeMember>,
    /// KIP-1331: the topology description that the plugin holds for the
    /// group's current topology epoch.
    pub topology_description: Option<TopologyDescription>,
    /// The backend failed while reading an otherwise reliably stored epoch.
    pub topology_description_error: bool,
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
    owner: Arc<()>,
    _task: JoinHandle<()>,
}

impl StreamsGroupActorHandle {
    pub fn spawn(
        group_id: String,
        config: Arc<StreamsGroupConfig>,
        coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        let owner = Arc::new(());
        let actor_owner = Arc::clone(&owner);
        let (tx, task) = spawn_mailbox(config.actor_mailbox_capacity, |rx| {
            actor_loop(group_id, config, coordinator, actor_owner, rx)
        });
        Self {
            tx,
            owner,
            _task: task,
        }
    }

    /// A stand-in actor that takes every message and drops it unanswered, as
    /// an actor does when it stops with requests still in its mailbox.
    #[cfg(test)]
    pub(crate) fn dropping_replies() -> Self {
        let (tx, task) = spawn_dropping_mailbox();
        Self {
            tx,
            owner: Arc::new(()),
            _task: task,
        }
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
    /// Kafka's `StreamsGroup.metadataHash`: the hash of the required topics in
    /// the image that the most recent reconcile configured the topology
    /// against. A heartbeat that sees another hash reconciles again.
    metadata_hash: i64,
    /// Kafka's `StreamsGroup.validatedTopologyEpoch`: the epoch of the
    /// topology that the group last found configured and ready in the
    /// metadata image when it bumped its epoch, or -1 when it was not ready.
    /// A new group holds 0, as Kafka's `TimelineInteger` starts. A heartbeat
    /// that validates another epoch bumps the group epoch.
    validated_topology_epoch: i32,
    /// Kafka's `StreamsGroup.lastAssignmentConfigs`: the assignment
    /// configuration of the last group epoch bump. A heartbeat that sees
    /// another configuration bumps the group epoch.
    last_assignment_configs: BTreeMap<String, String>,
    /// The internal topics that the topology needs and the metadata image
    /// does not hold. Every heartbeat answer carries them, as Kafka's
    /// `StreamsGroupHeartbeatResult.creatableTopics` does, and `KafkaApis`
    /// sends them to the controller as a `CreateTopics` request.
    creatable_topics: Vec<InternalTopicSpec>,
    /// The members whose target the last target assignment changed, set when
    /// a reconcile installed a target and taken by the records of the
    /// transition that installed it.
    target_changed: Option<Vec<String>>,
    /// Whether the topology was configured against the metadata image since
    /// the actor started. A seeded actor has not, so its first heartbeat
    /// configures the topology again, as Kafka does when the configured
    /// topology of a loaded group is empty.
    configured: bool,
    /// Kafka's `StreamsGroup.configuredTopology`: the topology as the most
    /// recent configuration against the metadata image sized it. It is
    /// `None` until a configuration succeeds.
    configured_topology: Option<ConfiguredTopology>,
    /// When Kafka's initial rebalance delay of the group ends: set when the
    /// first member joins an empty group, and cleared when the delayed
    /// assignment runs.
    initial_rebalance_deadline: Option<time::Instant>,
    /// Kafka's `StreamsGroup.assignmentTimestamp`: the wall-clock time in
    /// milliseconds at which the last target assignment calculation
    /// finished, or 0 when there is no previous assignment or its time is
    /// unknown. It is the `AssignmentTimestamp` of the group's target
    /// assignment metadata record, and Kafka's assignment interval runs from
    /// it.
    assignment_timestamp_ms: i64,
    /// KIP-1331: what the topology description plugin holds for the group.
    /// A group metadata record carries it only in trunk mode: see
    /// [`Self::trunk_records`].
    description_epochs: DescriptionEpochs,
    /// Whether the group writes Kafka trunk's KIP-1331 tags 2 and 3 of its
    /// group metadata record: whether
    /// `group.streams.topology.description.plugin.class` is set. Kafka trunk
    /// moves the description epochs away from -1, and so writes the tags,
    /// only through its plugin paths; without a plugin the group writes the
    /// record as Kafka 4.3.1 does, whatever epochs a replayed record held.
    trunk_records: bool,
    /// Shared with replacement actors, but not persisted across broker restarts.
    description_runtime: Arc<DescriptionRuntime>,
}

impl ActorState {
    /// Apply a published broker value or group override and restart its
    /// session tick only on change.
    fn refresh_configuration(
        &mut self,
        defaults: &StreamsGroupConfig,
        (image, node): (&krabka_metadata::MetadataImage, Option<NodeId>),
        config: &mut StreamsGroupConfig,
        tick: &mut Interval,
    ) {
        let next = resolve_broker_and_group_config(defaults, image, node, &self.state.group_id);
        if next != *config {
            *config = next;
            self.trunk_records = config.description_plugin_configured();
            *tick = session_tick(config);
        }
    }

    fn new(group_id: String) -> Self {
        Self {
            state: StreamsGroupState::new(group_id),
            topology: None,
            metadata_hash: 0,
            validated_topology_epoch: 0,
            last_assignment_configs: BTreeMap::new(),
            creatable_topics: Vec::new(),
            target_changed: None,
            configured: false,
            configured_topology: None,
            initial_rebalance_deadline: None,
            assignment_timestamp_ms: 0,
            description_epochs: DescriptionEpochs::default(),
            trunk_records: false,
            description_runtime: Arc::default(),
        }
    }
}

/// What woke the actor loop.
enum Wake {
    /// A mailbox message.
    Message(Box<StreamsGroupActorMessage>),
    /// The session tick, or an armed rebalance timeout that fired.
    SessionCheck,
    /// Kafka's initial rebalance delay ended.
    InitialDelayEnded,
    /// A new metadata image, or `None` once the source closed its watch.
    Image(Option<Arc<krabka_metadata::MetadataImage>>),
}

/// The coordinator's metadata source and its image watch, once the actor
/// holds them.
#[derive(Default)]
struct MetadataLink {
    source: Option<Arc<dyn MetadataSource>>,
    images: Option<Receiver<Arc<krabka_metadata::MetadataImage>>>,
}

impl MetadataLink {
    /// Takes the coordinator's metadata source when the link has none, and
    /// returns the current image of the source that it took.
    ///
    /// The broker connects the source after the `__consumer_offsets` replay
    /// has spawned the actors of the loaded groups, so an actor can start
    /// without one. Kafka's coordinator gives every loaded group the metadata
    /// image, so the loop takes the source before it handles each wake-up.
    fn attach(
        &mut self,
        coordinator: &GroupCoordinator,
    ) -> Option<Arc<krabka_metadata::MetadataImage>> {
        if self.source.is_some() {
            return None;
        }
        let source = coordinator.metadata_source()?;
        self.images = Some(source.watch_image());
        let image = source.current_image();
        self.source = Some(source);
        Some(image)
    }
}

/// The session tick of `config`: the actor looks for expired sessions once
/// per heartbeat interval.
fn session_tick(config: &StreamsGroupConfig) -> Interval {
    let mut tick = interval(config.heartbeat_interval);
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    tick
}

async fn actor_loop(
    group_id: String,
    default_config: Arc<StreamsGroupConfig>,
    coordinator: Arc<GroupCoordinator>,
    owner: Arc<()>,
    mut rx: mpsc::Receiver<StreamsGroupActorMessage>,
) {
    let node = coordinator.node_id();
    let mut metadata = MetadataLink::default();
    let mut config = metadata.attach(&coordinator).map_or_else(
        || (*default_config).clone(),
        |image| resolve_broker_and_group_config(&default_config, &image, node, &group_id),
    );
    let mut actor = ActorState::new(group_id);
    // A stale Streams lookup after conversion must not recreate plugin state.
    if coordinator
        .group_type(&actor.state.group_id)
        .is_none_or(|kind| kind == GroupType::Streams)
    {
        actor.description_runtime = Arc::clone(
            coordinator
                .streams_description_runtime
                .entry(actor.state.group_id.clone())
                .or_default()
                .value(),
        );
    }
    actor.trunk_records = config.description_plugin_configured();
    let mut tick = session_tick(&config);
    loop {
        let wake = tokio::select! {
            msg = rx.recv() => match msg {
                Some(msg) => Wake::Message(Box::new(msg)),
                // Every handle is gone.
                None => break,
            },
            _ = tick.tick() => Wake::SessionCheck,
            () = sleep_until_opt(actor.initial_rebalance_deadline) => {
                Wake::InitialDelayEnded
            }
            () = sleep_until_opt(actor.state.next_rebalance_deadline()) => {
                Wake::SessionCheck
            }
            image = wait_for_metadata_change(&mut metadata.images) => Wake::Image(image),
        };
        if let Some(image) = metadata.attach(&coordinator) {
            actor.refresh_configuration(&default_config, (&image, node), &mut config, &mut tick);
        }
        let turn = StreamsTurn {
            config: &config,
            metadata_source: metadata.source.as_ref(),
            coordinator: &coordinator,
        };
        match wake {
            Wake::Message(msg) => {
                let refused = match handle_message(&mut actor, turn, *msg).await {
                    Step::Continue => continue,
                    Step::Stop => break,
                    Step::RefusedJoin(refused) => refused,
                };
                // Kafka writes no record for a heartbeat that its coordinator
                // refuses, so such a heartbeat never creates the group.
                if !rx.is_empty() {
                    refused.send();
                    continue;
                }
                close_refused_group(&mut actor, turn, &owner, &mut rx, *refused).await;
                break;
            }
            Wake::SessionCheck => {
                if let Err(error) = handle_session_tick(&mut actor, turn).await {
                    exit_on_write_failure(&actor.state.group_id, &error);
                    break;
                }
            }
            Wake::InitialDelayEnded => {
                // Kafka's `computeDelayedTargetAssignment`.
                actor.initial_rebalance_deadline = None;
                if actor.state.members.is_empty() || !actor.assignment_pending() {
                    continue;
                }
                if let Err(error) = reconcile_and_flush(&mut actor, turn).await {
                    if matches!(error, BrokerError::StreamsTaskAssignment { .. }) {
                        tracing::warn!(%error, "Streams delayed assignment failed; retaining prior state");
                    } else {
                        exit_on_write_failure(&actor.state.group_id, &error);
                        break;
                    }
                }
            }
            Wake::Image(image) => {
                let Some(image) = image else {
                    metadata.images = None;
                    continue;
                };
                // Kafka reads a group's configuration when it needs it. A
                // changed assignment configuration bumps the group epoch at
                // the next heartbeat, which compares it with the
                // `LastAssignmentConfigs` of the last bump.
                actor.refresh_configuration(
                    &default_config,
                    (&image, node),
                    &mut config,
                    &mut tick,
                );
            }
        }
    }
}

/// Ends the actor of a group that refused a heartbeat with nothing queued
/// behind it: closes the mailbox, answers what raced in, leaves no group
/// behind when it still holds nothing, and then sends the `refused` answer.
async fn close_refused_group(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
    owner: &Arc<()>,
    rx: &mut mpsc::Receiver<StreamsGroupActorMessage>,
    refused: DeferredReply,
) {
    rx.close();
    while let Some(msg) = rx.recv().await {
        match handle_message(actor, turn, msg).await {
            Step::Continue => {}
            Step::RefusedJoin(other) => other.send(),
            // A failed write, or a shutdown, ends the drain as it ends the
            // loop. A failed write left changes in memory that never reached
            // the log, and Kafka's `CoordinatorRuntime` reverts them to the
            // last committed snapshot, so the group is judged by what its
            // writes persisted.
            Step::Stop => {
                *actor = persisted_state(turn.coordinator, &actor.state.group_id);
                break;
            }
        }
    }
    if actor.holds_nothing() {
        forget_abandoned_group(turn.coordinator, actor, owner);
    }
    refused.send();
}

/// What one wake-up of the actor loop hands to the paths that write records:
/// the group's configuration, its metadata source, and the coordinator that
/// owns the offsets log and the cache.
#[derive(Clone, Copy)]
struct StreamsTurn<'a> {
    config: &'a StreamsGroupConfig,
    metadata_source: Option<&'a Arc<dyn MetadataSource>>,
    coordinator: &'a GroupCoordinator,
}

impl<'a> StreamsTurn<'a> {
    /// The `__consumer_offsets` log that the turn writes to.
    fn offsets_log(self) -> &'a dyn OffsetsLog { &*self.coordinator.offsets_log }
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
    fn assignment_pending(&self) -> bool { self.state.target.epoch < self.state.group_epoch }

    /// The configured topology, when it is ready for an assignment.
    fn ready_topology(&self) -> Option<&ConfiguredTopology> {
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

/// The state of `group_id` as its last successful write left it: the seed
/// that every flush caches, or an empty group when nothing was written.
fn persisted_state(coordinator: &GroupCoordinator, group_id: &str) -> ActorState {
    let mut actor = ActorState::new(group_id.to_owned());
    actor.description_runtime = Arc::clone(
        coordinator
            .streams_description_runtime
            .entry(group_id.to_owned())
            .or_default()
            .value(),
    );
    if let Some(seed) = coordinator.cached_streams_seed(group_id) {
        apply_seed(&mut actor, seed);
    }
    actor
}

/// Drops the registry entry and the `Streams` type lock of a group whose
/// actor closed without writing a record. A newer actor for the same id, whose
/// mailbox is open, keeps both.
fn forget_abandoned_group(coordinator: &GroupCoordinator, actor: &ActorState, owner: &Arc<()>) {
    use dashmap::mapref::entry::Entry;
    let group_id = &actor.state.group_id;
    if let Entry::Occupied(entry) = coordinator.streams_groups.entry(group_id.clone())
        && entry.get().tx.is_closed()
        && Arc::ptr_eq(&entry.get().owner, owner)
    {
        // Keep replacement spawning blocked while retiring the old runtime.
        description::retire_runtime(actor, coordinator);
        coordinator
            .group_types
            .remove_if(group_id, |_, group_type| *group_type == GroupType::Streams);
        entry.remove();
    }
}

/// Handles one mailbox message.
async fn handle_message(
    actor: &mut ActorState,
    turn: StreamsTurn<'_>,
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
            // A conversion can finish between wire lookup and handle acquisition.
            // Do not let a stale Streams lookup overwrite classic ownership.
            if turn
                .coordinator
                .group_type(&actor.state.group_id)
                .is_some_and(|kind| kind != GroupType::Streams)
            {
                return Step::RefusedJoin(Box::new(DeferredReply {
                    reply,
                    response: response::error_resp(codes::GROUP_ID_NOT_FOUND, None),
                }));
            }
            match handle_heartbeat(actor, turn, &request, ClientIdentity {
                id: &client_id,
                host: &client_host,
            })
            .await
            {
                Ok(response) if response.error_code != codes::NONE && actor.holds_nothing() => {
                    return Step::RefusedJoin(Box::new(DeferredReply { reply, response }));
                }
                Ok(mut response) => {
                    response::add_missing_client_tags(
                        &mut response,
                        &actor.state,
                        turn.config,
                        &request,
                        version,
                    );
                    description::maybe_request_description(
                        actor,
                        turn.config,
                        &request,
                        version,
                        &mut response,
                        Instant::now(),
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
                    let code = exit_on_write_failure(&actor.state.group_id, &e);
                    let _ = reply.send(StreamsHeartbeatResult {
                        response: response::error_resp(code, None),
                        creatable_topics: Vec::new(),
                    });
                    return Step::Stop;
                }
            }
        }
        StreamsGroupActorMessage::Describe { reply } => {
            let mut view = build_describe(
                &actor.state,
                actor.topology.as_ref(),
                actor.ready_topology(),
            );
            match description::stored_description(actor, turn).await {
                Ok(description) => view.topology_description = description,
                Err(error) => {
                    tracing::warn!(
                        group_id = %actor.state.group_id,
                        ?error,
                        "streams-group topology description unavailable",
                    );
                    view.topology_description_error = true;
                }
            }
            turn.config
                .selected_task_assignor_name()
                .clone_into(&mut view.assignor_name);
            let _ = reply.send(view);
        }
        StreamsGroupActorMessage::PushDescription { push, reply } => {
            match description::handle_push(actor, turn, &push).await {
                Ok(answer) => {
                    let _ = reply.send(answer);
                }
                Err(e) => {
                    let code = exit_on_write_failure(&actor.state.group_id, &e);
                    let _ = reply.send((code, None));
                    return Step::Stop;
                }
            }
        }
        StreamsGroupActorMessage::ValidateCommit {
            member_id,
            group_instance_id,
            member_epoch,
            fence,
            partitions,
            reply,
        } => {
            let _ = reply.send(validate_offset_commit(
                &actor.state,
                actor.topology.as_ref(),
                CommitIdentity {
                    member_id: &member_id,
                    group_instance_id: group_instance_id.as_deref(),
                    member_epoch,
                    fence,
                },
                &partitions,
            ));
        }
        StreamsGroupActorMessage::OffsetDeleteGuard { reply } => {
            let _ = reply.send(subscribed_topics(&actor.state, actor.topology.as_ref()));
        }
        StreamsGroupActorMessage::ValidateFetch {
            member_id,
            member_epoch,
            reply,
        } => {
            let _ = reply.send(validate_offset_fetch(
                &actor.state,
                member_id.as_deref(),
                member_epoch,
            ));
        }
        StreamsGroupActorMessage::Seed(seed) => {
            apply_seed(actor, seed);
        }
        StreamsGroupActorMessage::Delete(reply) => {
            let result = description::delete(actor, turn).await;
            let stop = result.is_ok() || matches!(result, Err(DeleteGroupError::Internal));
            return reply_and_step(reply, result, stop);
        }
        StreamsGroupActorMessage::Convert {
            member_id,
            now_ms,
            reply,
        } => {
            let result = description::convert(actor, turn, &member_id, now_ms).await;
            let stop = matches!(
                &result,
                Ok(DowngradeOutcome::Converted | DowngradeOutcome::Removed)
            ) || result.is_err();
            return reply_and_step(reply, result, stop);
        }
        StreamsGroupActorMessage::PrepareConversion(reply) => {
            let result = description::prepare_conversion(actor, turn).await;
            let failed = result.is_err();
            return reply_and_step(reply, result, failed);
        }
        StreamsGroupActorMessage::ReapExpiredOffsets {
            now_ms,
            retention_ms,
            reply,
        } => match description::reap(actor, turn, now_ms, retention_ms).await {
            Ok(outcome) => {
                let stop = outcome.group_deleted;
                return reply_and_step(reply, outcome, stop);
            }
            Err(error) => {
                exit_on_write_failure(&actor.state.group_id, &error);
                return reply_and_step(reply, ReapOutcome::default(), true);
            }
        },
        StreamsGroupActorMessage::DeleteDescription(reply) => {
            let result = description::cleanup(actor, turn, true).await;
            let failed = result.is_err();
            return reply_and_step(reply, result, failed);
        }
        StreamsGroupActorMessage::Shutdown(reply) => {
            return reply_and_step(reply, (), true);
        }
    }
    Step::Continue
}

/// Sends `result` on `reply`, and ends the actor when the turn `stops` it.
fn reply_and_step<T>(reply: oneshot::Sender<T>, result: T, stops: bool) -> Step {
    let _ = reply.send(result);
    if stops { Step::Stop } else { Step::Continue }
}

/// Logs that the actor of `group_id` exits after `error`, a failed log write,
/// and returns the error code that answers the request whose write failed.
fn exit_on_write_failure(group_id: &str, error: &BrokerError) -> i16 {
    tracing::warn!(
        group_id = %group_id,
        error = %error,
        "streams-group actor exiting after log-write failure",
    );
    write_failure_code(error)
}

async fn wait_for_metadata_change(
    rx: &mut Option<Receiver<Arc<krabka_metadata::MetadataImage>>>,
) -> Option<Arc<krabka_metadata::MetadataImage>> {
    match rx {
        Some(rx) => {
            rx.changed().await.ok()?;
            Some(rx.borrow_and_update().clone())
        }
        None => future::pending().await,
    }
}

use crate::time_util::now_ms as chrono_now_ms;
