//! The single owning engine task: its `select!` loop over the command mpsc and
//! the election, fetch and heartbeat timers, the dispatch of each core
//! [`Action`] it produces, and the timer reconciliation that follows a role
//! change.

use krabka_ids::Offset;
use krabka_units::prelude::TimeExt as _;
use tokio::{
    sync::mpsc,
    time::{Duration, Instant},
};

use super::{
    Engine,
    offsets::is_single_voter_majority,
    timing::{
        election_timeout_ms, election_timer_starts_election, following_leader_for_role,
        heartbeat_period, instant_from_clock_base, should_fail_waiters_on_leadership_change,
    },
};
use crate::{
    error::RaftError,
    kraft::{
        action::{Action, TimerKind},
        event::Event,
        role::Role,
        transport::{Command, TimerTick, api_key, wire},
        types::{NodeId, SimInstant},
    },
};

impl Engine {
    /// The event loop. `select!`s the command mpsc against the election/fetch
    /// timers and the leader heartbeat interval, turning each into core input
    /// and executing the resulting [`Action`]s. Single-threaded over all
    /// consensus state, so no locking is needed inside; peer sends are
    /// fire-and-forget (see the module docs).
    pub async fn run(mut self, mut cmd_rx: mpsc::Receiver<Command>) {
        // A dynamically formatted controller starts as an observer with no
        // known leader.  It still has one or more discovery voters from the
        // bootstrap configuration, so poll one immediately.  The Fetch reply
        // redirects us to the current leader and begins normal replication.
        // Without this kick, observers never arm either election or fetch
        // timers and therefore cannot reach the auto-join workflow.
        if let Some(peer) = self.discovery_peer() {
            self.send_fetch(peer);
            self.arm_fetch_timer();
        }

        // Heartbeat ticks the whole time; the loop only acts on it while leader.
        let hb_period = heartbeat_period(self.election_timeout, self.heartbeat_interval);
        let mut heartbeat = tokio::time::interval(hb_period.to_std());
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            // Build the timer futures fresh each turn from the current deadlines.
            let election_sleep = sleep_until_opt(self.election_at);
            let fetch_sleep = sleep_until_opt(self.fetch_at);
            tokio::pin!(election_sleep);
            tokio::pin!(fetch_sleep);

            tokio::select! {
                cmd = cmd_rx.recv() => {
                    match cmd {
                        None | Some(Command::Shutdown) => break,
                        Some(c) => self.on_command(c),
                    }
                }
                () = &mut election_sleep => {
                    self.election_at = None;
                    self.on_timer(TimerTick::Election);
                }
                () = &mut fetch_sleep => {
                    self.fetch_at = None;
                    self.on_timer(TimerTick::Fetch);
                }
                _ = heartbeat.tick() => {
                    self.on_timer(TimerTick::Heartbeat);
                }
            }
            self.retry_pending_downgrade_snapshot();
        }
        // Fail any parked submitters so callers don't hang on shutdown.
        for w in self.commit_waiters.drain(..) {
            let _ = w.reply.send(Err(RaftError::Shutdown));
        }
    }

    /// Logical "now" for the core, derived from the monotonic clock base.
    pub fn now(&self) -> SimInstant {
        let ms = Instant::now()
            .saturating_duration_since(self.clock_base)
            .as_millis();
        SimInstant(u64::try_from(ms).unwrap_or(u64::MAX))
    }

    pub fn on_command(&mut self, cmd: Command) {
        match cmd {
            Command::Shutdown => {}
            Command::Event(event) => self.on_event(event),
            Command::FetchResponse { from, body } => self.on_fetch_response(from, &body),
            Command::FetchSnapshotResponse { from, body } => {
                self.on_fetch_snapshot_response(from, &body);
            }
            Command::Inbound(inbound) => self.on_inbound(inbound),
            Command::Timer(tick) => self.on_timer(tick),
            Command::SubmitChange { records, reply } => self.on_submit_change(&records, reply),
            Command::Reconfigure { change, reply } => self.on_reconfigure(change, reply),
            Command::TriggerSnapshot { reply } => {
                let _ = reply.send(self.do_trigger_snapshot());
            }
            Command::QuorumStateSnapshot { reply } => {
                let _ = reply.send(self.quorum_state_snapshot());
            }
            Command::MetadataFetch {
                fetch_offset,
                max_size,
                reply,
            } => {
                let _ = reply.send(self.metadata_fetch_slice(fetch_offset, max_size));
            }
            #[cfg(test)]
            Command::TestAppendAndCommit { records, reply } => {
                let off = self.test_append_and_commit(&records);
                let _ = reply.send(off);
            }
        }
    }

    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(node = self.me.0, epoch = self.core.quorum_state().leader_epoch, role = self.core.role().name())
    )]
    pub fn on_event(&mut self, event: Event) {
        let now = self.now();
        let prev_role = self.core.role().name();
        let actions = self.core.on_event(event, &self.log, now);
        self.execute(actions);
        self.reconcile_timers(prev_role);
        self.publish_leader();
    }

    /// Map a timer tick to liveness behavior.
    pub fn on_timer(&mut self, tick: TimerTick) {
        match tick {
            TimerTick::Election => {
                // The election timer is only armed for voters not currently
                // leading. Firing it starts an election.
                if election_timer_starts_election(
                    self.core.is_voter(),
                    self.core.role().is_leader(),
                ) {
                    self.on_event(Event::ElectionTimeout);
                }
            }
            TimerTick::Fetch => {
                // A fetch-timer expiry while we still believe in a reachable
                // leader RE-POLLS rather than electing; only a sustained loss
                // (the configured consecutive-miss limit) feeds FetchTimeout.
                let leader = self.following_leader();
                if let Some(leader_id) = leader {
                    self.fetch_misses += 1;
                    if self.fetch_misses >= self.controller_fetch_miss_limit.get() {
                        self.fetch_misses = 0;
                        self.on_event(Event::FetchTimeout);
                    } else {
                        // Re-poll the leader and re-arm the fetch timer.
                        self.send_fetch(leader_id);
                        self.arm_fetch_timer();
                    }
                } else if self.core.is_voter() {
                    // No leader to poll but the fetch watchdog fired: elect.
                    self.on_event(Event::FetchTimeout);
                } else if let Some(peer) = self.discovery_peer() {
                    // An observer has no election timeout. Retry discovery
                    // until a bootstrap voter redirects it to the leader.
                    self.send_fetch(peer);
                    self.arm_fetch_timer();
                }
            }
            TimerTick::Heartbeat => {
                if self.core.role().is_leader() {
                    let epoch = self.core.quorum_state().leader_epoch;
                    self.broadcast_begin_quorum_epoch(epoch);
                }
            }
        }
    }

    /// The leader id we are actively following (Follower / attached Observer),
    /// if any.
    pub fn following_leader(&self) -> Option<NodeId> {
        following_leader_for_role(self.core.role())
    }

    /// Pick a configured voter for observer leader discovery.
    pub fn discovery_peer(&self) -> Option<NodeId> {
        if self.core.is_voter() || self.following_leader().is_some() {
            return None;
        }
        self.core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .find(|id| *id != self.me)
            .or_else(|| self.peers.discovery_peers().into_iter().next())
    }

    /// Execute a batch of [`Action`]s, dispatching peer sends fire-and-forget.
    pub fn execute(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => {
                    self.broadcast_vote(epoch, pre_vote);
                }
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.broadcast_begin_quorum_epoch(epoch);
                }
                Action::SendEndQuorumEpoch { epoch } => {
                    self.broadcast_end_quorum_epoch(epoch);
                }
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute only the local (non-network, non-reply) actions in `actions`.
    pub fn execute_local_only(&mut self, actions: Vec<Action>) {
        for action in actions {
            match action {
                Action::SendVoteRequest { epoch, pre_vote } => self.broadcast_vote(epoch, pre_vote),
                Action::SendBeginQuorumEpoch { epoch } => {
                    self.broadcast_begin_quorum_epoch(epoch);
                }
                Action::SendEndQuorumEpoch { epoch } => self.broadcast_end_quorum_epoch(epoch),
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                Action::ReplyVote { .. } => {}
                other => self.execute_one_local(other),
            }
        }
    }

    /// Execute a single non-network [`Action`] synchronously.
    pub fn execute_one_local(&mut self, action: Action) {
        match action {
            Action::AppendLeaderChange { epoch } => {
                if let Err(e) = self.append_leader_change(epoch) {
                    tracing::error!(?e, "kraft: append leader-change failed");
                } else if is_single_voter_majority(self.core.quorum_state().majority()) {
                    // There is no follower Fetch acknowledgement to drive HWM
                    // recomputation in a one-voter quorum. The local append is
                    // already a majority, so commit the current-epoch barrier.
                    self.advance_and_apply(self.log.log_end_offset());
                }
            }
            Action::AdvanceHighWatermark(n) => {
                // `n` is the core's raw i64 HWM target; wrap into the log domain.
                self.advance_and_apply(Offset(n));
            }
            Action::TruncateTo(point) => {
                // `point.offset` is the core's raw i64 divergence point.
                if let Err(e) = self.log.truncate_to(Offset(point.offset)) {
                    tracing::error!(?e, "kraft: truncate failed");
                } else {
                    self.restore_control_state_after_truncation(point.offset);
                }
            }
            Action::PersistQuorumState => {
                if let Err(e) = self.persist_quorum_state() {
                    tracing::error!(?e, "kraft: persist quorum-state failed");
                }
            }
            Action::ResetTimer { kind, deadline } => match kind {
                TimerKind::Election => self.election_at = Some(self.deadline_instant(deadline)),
                TimerKind::Fetch => self.fetch_at = Some(self.deadline_instant(deadline)),
            },
            Action::TransitionedTo(_name) => {}
            Action::SendVoteRequest { .. }
            | Action::SendBeginQuorumEpoch { .. }
            | Action::SendEndQuorumEpoch { .. }
            | Action::SendFetch { .. }
            | Action::ReplyVote { .. } => {
                debug_assert!(false, "network/reply action routed to local executor");
            }
        }
    }

    /// After processing an event, cancel timers irrelevant to the new role and
    /// arm the ones it needs that the core did not explicitly reset.
    pub fn reconcile_timers(&mut self, _prev_role: &'static str) {
        match self.core.role() {
            Role::Leader { .. } => {
                // A leader never elects on a timer and never fetch-watchdogs.
                self.election_at = None;
                self.fetch_at = None;
                self.fetch_misses = 0;
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                // A follower has no election timer; the fetch watchdog (armed by
                // the core's ResetTimer/Fetch) covers liveness.
                self.election_at = None;
            }
            Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Unattached { .. }
            | Role::Voted { .. } => {
                // Mid-election: no leader to fetch from, election timer governs.
                self.fetch_at = None;
                self.fetch_misses = 0;
            }
            Role::Resigned => {}
        }
        self.fail_waiters_on_leadership_loss();
    }

    /// Detect a transition away from leadership — Leader → non-Leader, or a
    /// leader-epoch bump while we still nominally lead — and fail every parked
    /// `submit_change` waiter with `NotLeader` so the caller's future resolves
    /// promptly instead of hanging until shutdown (FIX 1). Records appended at
    /// our old epoch can no longer commit once we step down (a new leader may
    /// truncate them), so the parked waiters are unresolvable and must error.
    pub fn fail_waiters_on_leadership_loss(&mut self) {
        let is_leader = self.core.role().is_leader();
        let epoch = self.core.quorum_state().leader_epoch;
        let lost_leadership = should_fail_waiters_on_leadership_change(
            self.was_leader,
            is_leader,
            self.held_epoch,
            epoch,
        );
        if lost_leadership && !self.commit_waiters.is_empty() {
            let current_leader = self.core.quorum_state().leader_id;
            for w in self.commit_waiters.drain(..) {
                let _ = w.reply.send(Err(RaftError::NotLeader { current_leader }));
            }
        }
        if lost_leadership
            && let Some(mut pending) = self.pending_reconfig.take()
            && let Some(reply) = pending.reply.take()
        {
            let _ = reply.send(Err(RaftError::NotLeader {
                current_leader: self.core.quorum_state().leader_id,
            }));
        }
        self.was_leader = is_leader;
        self.held_epoch = epoch;
    }

    /// Arm the fetch timer one election-timeout out from now (re-poll cadence).
    pub fn arm_fetch_timer(&mut self) {
        self.fetch_at = Some(
            Instant::now() + Duration::from_millis(election_timeout_ms(self.election_timeout)),
        );
    }

    /// Convert a core [`SimInstant`] deadline into a `tokio::time::Instant`.
    pub fn deadline_instant(&self, deadline: SimInstant) -> Instant {
        instant_from_clock_base(self.clock_base, deadline)
    }

    pub fn publish_leader(&self) {
        let leader = self.core.quorum_state().leader_id;
        if *self.leader_tx.borrow() != leader {
            let _ = self.leader_tx.send(leader);
        }
        // Republish the structured consensus snapshot for the handle's
        // synchronous `quorum_state()` (DescribeQuorum). `send_replace` keeps
        // the watch's stored value current even with no active receiver.
        let snapshot = self.quorum_state_snapshot();
        self.quorum_tx.send_replace(snapshot);
    }
}

/// Decode a non-Fetch peer response body into the matching `Receive*Response`
/// event. `peer` is the responder, used to fill `from`. Returns `None` for
/// `Ack` (Begin/End acks produce no core event), `Fetch` (handled by the
/// dedicated [`Engine::on_fetch_response`] path, which must touch the log before
/// the core sees the event), and undecodable bodies.
pub fn response_to_event(peer: NodeId, api_key: i16, body: &[u8]) -> Option<Event> {
    match api_key {
        self::api_key::VOTE => match wire::PeerResponse::decode_vote(body)? {
            wire::PeerResponse::Vote { epoch, granted } => Some(Event::ReceiveVoteResponse {
                from: peer,
                epoch,
                vote_granted: granted,
            }),
            _ => None,
        },
        // Begin/End acks produce no core event; Fetch is handled by the
        // dedicated `FetchResponse` command path before reaching here.
        _ => None,
    }
}

/// `Some` sleep future for an armed deadline; a never-ready future otherwise so
/// `select!` ignores the disarmed timer.
pub async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}
