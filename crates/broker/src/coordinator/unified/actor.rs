//! Per-group tokio actor.
//!
//! The actor owns one unified `Group`: either the classic 5-state machine or
//! the next-gen epoch machine. Next-gen heartbeats are non-parking mpsc
//! messages with `oneshot` replies.
//!
//! Classic `JoinGroup` and `SyncGroup` parking becomes a park/wake message
//! protocol. The actor holds the reply `oneshot::Sender` in a parked registry
//! and resolves it at the rebalance boundary: the rebalance-deadline timer, an
//! all-members-joined early-complete, or the leader's `SyncGroup`.
//!
//! This file is the module root. It holds the actor's identity — the handle,
//! the mailbox loop, and the shared services and constants — while each RPC
//! path lives in its own submodule.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

mod classic_join;
mod classic_leave;
mod classic_sync;
mod commit_validation;
mod dispatch;
mod downgrade;
mod heartbeat;
mod member_state;
mod messages;
mod pending_records;
mod persistence;
mod seed;
mod tick;
mod views;
mod waiters;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

pub(crate) use self::{
    commit_validation::validate_group_commit,
    pending_records::PendingRecords,
    persistence::{classic_group_metadata_record, full_pending_records},
};
use self::{
    dispatch::handle_actor_message, tick::handle_actor_tick, waiters::complete_classic_rebalance,
};
pub use self::{
    messages::{GroupActorMessage, JoinResult, JoinResultMember, LeaveResult, SyncResult},
    views::{ClassicMemberView, ClassicView, DescribeMember, DescribeView},
};
use crate::coordinator::unified::{
    GroupCoordinator, config::NextGenConfig, group::CoordinatorGroup, offsets_log::OffsetsLog,
    reconciler::ReconcileInput,
};

/// A Kafka wire `error_code` value, as carried in response `error_code`
/// fields. The values live in [`crate::codes`].
pub type ErrorCode = i16;

/// Fallback session timeout (30 s, in ms) for a persisted or requested classic
/// `session_timeout_ms` that the target type cannot represent.
const FALLBACK_SESSION_TIMEOUT_MS: u64 = 30_000;

/// [`FALLBACK_SESSION_TIMEOUT_MS`] as the persisted/wire `i32` field.
const FALLBACK_SESSION_TIMEOUT_MS_I32: i32 = 30_000;

/// Fallback rebalance timeout (60 s, in ms) for a persisted or requested
/// `rebalance_timeout_ms` that the target type cannot represent.
const FALLBACK_REBALANCE_TIMEOUT_MS: u64 = 60_000;

/// [`FALLBACK_REBALANCE_TIMEOUT_MS`] as the persisted/wire `i32` field.
const FALLBACK_REBALANCE_TIMEOUT_MS_I32: i32 = 60_000;

/// Fallback `heartbeat_interval_ms` of 5 s, the KIP-848 default heartbeat
/// interval. The actor reports it when the configured interval overflows the
/// wire `i32`.
const FALLBACK_HEARTBEAT_INTERVAL_MS: i32 = 5_000;

/// Which protocol an actor's `Group` speaks. This value is fixed at spawn. The
/// handle exposes it so that the coordinator can route or reject
/// cross-protocol RPCs, and filter admin views, without a message to the
/// actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupKindTag {
    Classic,
    Consumer,
}

#[derive(Debug)]
pub struct GroupActorHandle {
    pub tx: mpsc::Sender<GroupActorMessage>,
    /// Spawn-time protocol hint, fixed for the actor's lifetime. A KIP-848
    /// live migration can flip the group's kind in place after spawn, which
    /// leaves this field stale. The code therefore reads it ONLY for
    /// spawn-time wiring (the initial `CoordinatorGroup::new_classic` or
    /// `new_consumer`) and for replay assertions. Every routing and validation
    /// decision dispatches on the actor's LIVE `group.kind` inside the actor,
    /// never on this field.
    pub kind: GroupKindTag,
    _task: JoinHandle<()>,
}

impl GroupActorHandle {
    pub fn spawn(
        group_id: String,
        kind: GroupKindTag,
        config: Arc<NextGenConfig>,
        metadata_provider: Arc<dyn MetadataProvider>,
        offsets_log: Arc<dyn OffsetsLog>,
        coordinator: Arc<GroupCoordinator>,
    ) -> Self {
        let (tx, rx) = mpsc::channel(config.actor_mailbox_capacity);
        let task = tokio::spawn(actor_loop(
            group_id,
            kind,
            config,
            metadata_provider,
            offsets_log,
            coordinator,
            rx,
        ));
        Self {
            tx,
            kind,
            _task: task,
        }
    }
}

pub trait MetadataProvider: Send + Sync + std::fmt::Debug {
    fn snapshot(&self) -> ReconcileInput;
}

/// Parked classic-protocol waiters for one group.
#[derive(Default)]
struct ParkedWaiters {
    /// Parked `JoinGroup` handlers, keyed by `member_id`, holding the reply
    /// sender.
    joiners: HashMap<String, oneshot::Sender<JoinResult>>,
    /// Parked `SyncGroup` followers, keyed by `member_id`, holding the reply
    /// sender.
    followers: HashMap<String, oneshot::Sender<SyncResult>>,
}

#[derive(Clone, Copy)]
struct ActorServices<'a> {
    config: &'a NextGenConfig,
    metadata: &'a dyn MetadataProvider,
    offsets_log: &'a dyn OffsetsLog,
    coordinator: &'a GroupCoordinator,
}

async fn actor_loop(
    group_id: String,
    kind: GroupKindTag,
    config: Arc<NextGenConfig>,
    metadata: Arc<dyn MetadataProvider>,
    offsets_log: Arc<dyn OffsetsLog>,
    coordinator: Arc<GroupCoordinator>,
    mut rx: mpsc::Receiver<GroupActorMessage>,
) {
    let mut group = match kind {
        GroupKindTag::Classic => CoordinatorGroup::new_classic(group_id),
        GroupKindTag::Consumer => CoordinatorGroup::new_consumer(group_id),
    };
    let mut parked = ParkedWaiters::default();
    // A single configured session-expiry tick, kind-agnostic. The tick arm
    // dispatches on the live `group.kind`, so the cadence must not depend on
    // the spawn-time kind. Expiry is a
    // `last_seen`-vs-`session_timeout` comparison, so its cadence only changes
    // how often we check, never the outcome.
    //
    // Driven through the injected `AsyncSleeper` (production: real time; tests:
    // a controlled mock timeline). A zero-duration first sleep reproduces
    // `tokio::time::interval`'s immediate t=0 tick; each subsequent sleep is
    // re-armed to the configured interval only after the tick body runs
    // (`MissedTickBehavior::Delay` semantics — a slow tick never bursts). The
    // future is held across loop iterations so an inbound-message stream never
    // resets the tick schedule (matching the persistent `Interval`).
    let sleeper = config.sleeper.clone();
    let mut tick = sleeper.sleep_for_async(Duration::ZERO);
    loop {
        let deadline = classic_deadline(&group);
        tokio::select! {
            msg = rx.recv() => {
                let Some(msg) = msg else { break };
                let services = ActorServices {
                    config: &config,
                    metadata: &*metadata,
                    offsets_log: &*offsets_log,
                    coordinator: &coordinator,
                };
                if !handle_actor_message(&mut group, &mut parked, services, msg).await {
                    break;
                }
            }
            () = &mut tick => {
                let services = ActorServices {
                    config: &config,
                    metadata: &*metadata,
                    offsets_log: &*offsets_log,
                    coordinator: &coordinator,
                };
                if !handle_actor_tick(&mut group, &mut parked, services).await {
                    break;
                }
                tick = sleeper.sleep_for_async(config.session_expiry_tick);
            }
            () = opt_sleep(deadline) => {
                // Classic rebalance deadline fired: complete with whoever is here.
                if let Some(state) = group.as_classic_mut() {
                    complete_classic_rebalance(
                        state,
                        &mut parked.joiners,
                        &mut parked.followers,
                    );
                }
            }
        }
    }
}

/// The classic rebalance-completion deadline, if a rebalance is open.
fn classic_deadline(group: &CoordinatorGroup) -> Option<Instant> {
    group.as_classic().and_then(|s| s.rebalance_deadline)
}

/// A future that resolves at `deadline`, or never if `None`.
async fn opt_sleep(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d.into()).await,
        None => std::future::pending::<()>().await,
    }
}

fn chrono_now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(0))
}

// `reconciler_model` drives the real heartbeat step, so these two are
// re-exported for it alone.
#[cfg(test)]
pub(crate) use self::heartbeat::{HeartbeatStep, step_heartbeat};

#[cfg(test)]
#[path = "reconciler_model.rs"]
mod reconciler_model;

/// Compositional model: the KIP-848 reconciliation engine, composed with a
/// modeled offset-commit fencing and fetch layer. It covers consumer delivery
/// correctness through rebalances.
#[cfg(test)]
#[path = "consumer_group_composition_model.rs"]
mod consumer_group_composition_model;
