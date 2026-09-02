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
mod response;

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
    state::StreamsGroupState,
};
use crate::{
    codes,
    coordinator::unified::{offsets_log::OffsetsLog, validate_member_epoch},
    metadata_source::MetadataSource,
};

/// Messages accepted by a [`StreamsGroupActorHandle`].
#[derive(Debug)]
pub enum StreamsGroupActorMessage {
    Heartbeat {
        request: Box<StreamsGroupHeartbeatRequest>,
        client_id: String,
        client_host: String,
        reply: oneshot::Sender<StreamsGroupHeartbeatResponse>,
    },
    Describe {
        reply: oneshot::Sender<StreamsDescribeView>,
    },
    /// Validates an `OffsetCommit` or `TxnOffsetCommit` against the streams
    /// group's membership. KIP-1071 fences by `member_epoch`, as a KIP-848
    /// consumer group does. `Ok(())` allows the commit, and `Err(code)`
    /// rejects it. The actor does not fence a simple-consumer commit, which
    /// has an empty `member_id` and `member_epoch == -1`. This mirrors the
    /// consumer-group `ValidateCommit`.
    ValidateCommit {
        member_id: String,
        /// The request's `generation_id_or_member_epoch` field, interpreted as
        /// the streams `member_epoch`.
        member_epoch: i32,
        reply: oneshot::Sender<Result<(), i16>>,
    },
    Seed(super::super::StreamsGroupSeed),
    Shutdown(oneshot::Sender<()>),
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
    /// The group's resolved topology: the subtopologies and their topics.
    ///
    /// The real JVM `DescribeStreamsGroupsHandler` rejects a describe response
    /// with no topology, so this field must hold a value once a member has
    /// supplied one. It is `None` only before any topology is initialized.
    pub topology: Option<StreamsGroupTopologyValue>,
    pub members: Vec<StreamsDescribeMember>,
}

#[derive(Debug, Clone)]
pub struct StreamsDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub process_id: String,
    pub active: BTreeMap<String, Vec<i32>>,
    pub standby: BTreeMap<String, Vec<i32>>,
    pub warmup: BTreeMap<String, Vec<i32>>,
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

/// Validates an `OffsetCommit` or `TxnOffsetCommit` against a streams group's
/// membership by sending a message to its actor.
///
/// It returns `Some(error_code)` to reject the commit, and `None` to allow it.
/// Per KIP-447, a streams group fences offset commits by `member_epoch`, the
/// request's `generation_id_or_member_epoch`, exactly as a KIP-848 consumer
/// group does.
///
/// The shared `validate_group_commit` knows only about the classic and
/// consumer `GroupActorHandle`. A streams-group consumer keeps its membership
/// in the streams actor, not a classic one, so this function must validate it
/// instead. Otherwise the broker fences the commit against an empty classic
/// actor and rejects it with `UNKNOWN_MEMBER_ID`.
pub(crate) async fn validate_streams_group_commit(
    handle: &StreamsGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
) -> Option<i16> {
    let (tx, rx) = oneshot::channel();
    if handle
        .tx
        .send(StreamsGroupActorMessage::ValidateCommit {
            member_id: member_id.to_string(),
            member_epoch,
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
}

impl ActorState {
    fn new(group_id: String) -> Self {
        Self {
            state: StreamsGroupState::new(group_id),
            topology: None,
            partition_metadata: None,
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
                match msg {
                    StreamsGroupActorMessage::Heartbeat { request, client_id, client_host, reply } => {
                        match handle_heartbeat(
                            &mut actor,
                            &config,
                            &*offsets_log,
                            metadata_source.as_ref(),
                            &coordinator,
                            &request,
                            super::super::ClientIdentity {
                                id: &client_id,
                                host: &client_host,
                            },
                        )
                        .await
                        {
                            Ok(resp) => {
                                let _ = reply.send(resp);
                            }
                            Err(e) => {
                                tracing::warn!(
                                    group_id = %actor.state.group_id,
                                    error = %e,
                                    "streams-group actor exiting after log-write failure",
                                );
                                let _ = reply.send(StreamsGroupHeartbeatResponse {
                                    error_code: codes::COORDINATOR_LOAD_IN_PROGRESS,
                                    ..Default::default()
                                });
                                break;
                            }
                        }
                    }
                    StreamsGroupActorMessage::Describe { reply } => {
                        let _ = reply.send(build_describe(&actor.state, actor.topology.as_ref()));
                    }
                    StreamsGroupActorMessage::ValidateCommit { member_id, member_epoch, reply } => {
                        // KIP-447 fencing for a streams group: member_epoch must
                        // match the member's current epoch, mirroring the KIP-848
                        // consumer-group check. A simple-consumer commit (empty
                        // member_id, member_epoch == -1) is not fenced.
                        let result: Result<(), i16> = if member_id.is_empty() {
                            Ok(())
                        } else {
                            validate_member_epoch(
                                actor.state.members.get(&member_id).map(|m| m.member_epoch),
                                member_epoch,
                            )
                            .map(|_| ())
                        };
                        let _ = reply.send(result);
                    }
                    StreamsGroupActorMessage::Seed(seed) => {
                        apply_seed(&mut actor, seed);
                    }
                    StreamsGroupActorMessage::Shutdown(reply) => {
                        let _ = reply.send(());
                        break;
                    }
                }
            }
            _ = tick.tick() => {
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
                    reconcile(&mut actor, &config, metadata_source.as_ref()).await;
                    let pending = snapshot_pending_after_change(&actor, &[]);
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
