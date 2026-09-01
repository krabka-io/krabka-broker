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
mod retention;
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
    retention::ReapOutcome,
    views::{ClassicMemberView, ClassicView, DescribeMember, DescribeView},
};
use crate::{
    coordinator::unified::{
        GroupCoordinator, config::NextGenConfig, group::CoordinatorGroup, offsets_log::OffsetsLog,
        reconciler::ReconcileInput,
    },
    time_util,
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

/// Names this actor's session-expiry cadence in the timer-failure logs that
/// [`time_util::arm`] and [`time_util::fired`] emit, so an operator can tell
/// which loop lost its ticker.
const TICK_TASK: &str = "consumer group actor";

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
    // Driven through the injected `Timer` (production: real time; tests: a
    // controlled manual timeline). A zero-duration first deadline reproduces
    // `tokio::time::interval`'s immediate t=0 tick; each subsequent deadline is
    // armed to the configured interval only after the tick body runs
    // (`MissedTickBehavior::Delay` semantics — a slow tick never bursts). The
    // future is held across loop iterations so an inbound-message stream never
    // resets the tick schedule (matching the persistent `Interval`). It owns
    // its registration outright instead of borrowing the timer, so nothing
    // has to be cloned out of `config` to keep it alive across the arms.
    let Some(mut tick) = time_util::arm(&*config.timer, Duration::ZERO, TICK_TASK) else {
        // No ticker, no actor. Take the same exit the loop body takes below,
        // so the offset-retention clock is stamped once before we go away.
        group.observe_membership(chrono_now_ms());
        return;
    };
    // A zero-duration deadline is already due, so `Timer` hands back a future
    // that is ready on its first poll. `tokio::select!` polls its branches in a
    // random order, so without this yield the t=0 tick wins roughly half the
    // races against a mailbox that spawn already filled -- and a replayed group
    // whose `last_seen` values predate this actor is then swept before it
    // answers a single request. Yielding once lets those queued messages land
    // first, which is the ordering this loop has always had: the sleeper it
    // used before returned a `tokio::time::sleep`, and even a zero-duration one
    // goes through the runtime and polls `Pending` once.
    tokio::task::yield_now().await;
    loop {
        let deadline = classic_deadline(&group);
        let keep_running = tokio::select! {
            msg = rx.recv() => match msg {
                None => false,
                Some(msg) => {
                    let services = ActorServices {
                        config: &config,
                        metadata: &*metadata,
                        offsets_log: &*offsets_log,
                        coordinator: &coordinator,
                    };
                    handle_actor_message(&mut group, &mut parked, services, msg).await
                }
            },
            outcome = &mut tick => {
                // A ticker that failed, or that cannot be armed again, takes
                // this actor's session-expiry sweep with it. Report it as
                // "stop" rather than returning outright: the loop tail still
                // has to stamp `observe_membership` and break cleanly.
                if time_util::fired(outcome, TICK_TASK) {
                    let services = ActorServices {
                        config: &config,
                        metadata: &*metadata,
                        offsets_log: &*offsets_log,
                        coordinator: &coordinator,
                    };
                    let keep_running =
                        handle_actor_tick(&mut group, &mut parked, services).await;
                    match time_util::arm(&*config.timer, config.session_expiry_tick, TICK_TASK) {
                        Some(next) => {
                            tick = next;
                            keep_running
                        }
                        None => false,
                    }
                } else {
                    false
                }
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
                true
            }
        };
        // One place maintains `empty_since_ms`, after whatever the turn did to
        // membership. Every join, leave, eviction, seed, and in-place kind flip
        // therefore keeps the offset-retention clock honest without knowing it
        // exists.
        group.observe_membership(chrono_now_ms());
        if !keep_running {
            break;
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

/// The wall-clock reading this actor subtree stamps records and deadlines
/// with, in milliseconds since the Unix epoch. It reads `std::time`, not
/// chrono, which the name predates.
///
/// This is deliberately **not** [`crate::time_util::now_ms`], which is
/// otherwise the same function. The two disagree on one arm: a duration that
/// overflows `i64` milliseconds saturates to `i64::MAX` there and to `0` here.
/// Collapsing this into the shared helper would therefore change what the
/// offset-retention clock reads, so it stays separate until someone decides
/// which answer that clock wants.
///
/// That arm needs a system clock set roughly 292 million years ahead to reach,
/// and the two answers fail in opposite directions: `0` dates a group to the
/// epoch, so its offsets expire at once, while `i64::MAX` dates it to now, so
/// they never expire. The share-group actor keeps its own copy of this same
/// function, with the same divergence.
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
