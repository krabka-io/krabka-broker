//! The single owning engine task: its `select!` loop over the command mpsc and
//! the election, fetch, check-quorum and heartbeat timers, the dispatch of each
//! core [`Action`] it produces, and the timer reconciliation that follows a
//! role change.

use {
    super::{
        Engine, METADATA_LOG_CLEAN_INTERVAL,
        graceful_shutdown::graceful_shutdown_window,
        timing::{
            election_timer_starts_election, following_leader_for_role, heartbeat_period,
            instant_from_clock_base, should_fail_waiters_on_leadership_change, whole_millis,
        },
    },
    crate::{
        error::RaftError,
        event_queue::Arrival,
        kraft::{
            action::{Action, TimerKind},
            event::Event,
            role::Role,
            transport::{Command, QueuedCommand, TimerTick, api_key, wire},
            types::{NodeId, SimInstant},
        },
    },
    krabka_ids::Offset,
    krabka_units::prelude::TimeExt as _,
    std::sync::Arc,
    tokio::{
        sync::mpsc,
        time::{Duration, Instant},
    },
};

/// What woke one turn of the loop.
enum Wake {
    /// A command arrived on the mpsc.
    Command {
        command: Command,
        arrival: Arrival,
    },
    Election,
    Fetch,
    CheckQuorum,
    Heartbeat,
    /// A parked fetch's wait ran out; the turn's end answers it.
    ParkedFetch,
    Noop,
    Clean,
    SnapshotComplete(Result<super::snapshotting::SnapshotJobCompletion, tokio::task::JoinError>),
}

impl Engine {
    /// The event loop. `select!`s the command mpsc against the election/fetch
    /// timers and the leader heartbeat interval, turning each into core input
    /// and executing the resulting [`Action`]s. Single-threaded over all
    /// consensus state, so no locking is needed inside; peer sends are
    /// fire-and-forget (see the module docs).
    pub async fn run(mut self, mut cmd_rx: mpsc::Receiver<QueuedCommand>) {
        // A dynamically formatted controller starts as an observer with no
        // known leader.  It still has one or more discovery voters from the
        // bootstrap configuration, so poll one immediately.  The Fetch reply
        // redirects us to the current leader and begins normal replication.
        // Without this kick, observers never arm either election or fetch
        // timers and therefore cannot reach the auto-join workflow.
        self.send_discovery_fetch();

        // Heartbeat ticks the whole time; the loop only acts on it while leader.
        let hb_period = heartbeat_period(self.timeouts.fetch, self.heartbeat_interval);
        let mut heartbeat = tokio::time::interval(hb_period.to_std());
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        let mut faulted = false;
        // Set by a graceful `Shutdown`: when the engine stops waiting for the
        // quorum to elect a successor.
        let mut shutdown_at: Option<Instant> = None;
        loop {
            // Build the timer futures fresh each turn from the current deadlines.
            let election_sleep = sleep_until_opt(self.election_at);
            let fetch_sleep = sleep_until_opt(self.fetch_at);
            let check_quorum_sleep = sleep_until_opt(self.check_quorum_at);
            let parked_fetch_sleep = sleep_until_opt(self.next_parked_fetch_deadline());
            let noop_sleep = sleep_until_opt(self.noop_at);
            let clean_sleep = tokio::time::sleep_until(self.clean_at);
            let shutdown_sleep = sleep_until_opt(shutdown_at);
            let snapshot_complete = async {
                match self.snapshot_job.as_mut() {
                    Some(job) => job.await,
                    None => std::future::pending().await,
                }
            };
            tokio::pin!(election_sleep);
            tokio::pin!(fetch_sleep);
            tokio::pin!(check_quorum_sleep);
            tokio::pin!(parked_fetch_sleep);
            tokio::pin!(noop_sleep);
            tokio::pin!(clean_sleep);
            tokio::pin!(shutdown_sleep);
            tokio::pin!(snapshot_complete);

            // A shutting-down replica starts no election of its own: Kafka's
            // poll loop keeps a resigned or unattached voter where it is, and
            // lets a candidate finish only the election it already runs.
            let shutting_down = shutdown_at.is_some();
            let wake = tokio::select! {
                queued = cmd_rx.recv() => match queued {
                    None | Some(QueuedCommand { command: Command::Halt, .. }) => break,
                    Some(QueuedCommand { command, enqueued_at }) => {
                        if matches!(command, Command::Shutdown) {
                            shutdown_at.get_or_insert_with(|| {
                                Instant::now() + graceful_shutdown_window(self.timeouts.fetch)
                            });
                        }
                        Wake::Command {
                            arrival: Arrival {
                                enqueued_at,
                                caller_gone: command.caller_gone(),
                            },
                            command,
                        }
                    }
                },
                () = &mut election_sleep => Wake::Election,
                () = &mut fetch_sleep => Wake::Fetch,
                () = &mut check_quorum_sleep => Wake::CheckQuorum,
                () = &mut shutdown_sleep => {
                    tracing::warn!(
                        node = self.me.0,
                        role = self.core.role().name(),
                        "kraft: graceful shutdown timed out before a successor was elected"
                    );
                    break;
                }
                _ = heartbeat.tick() => Wake::Heartbeat,
                // A parked fetch whose wait ran out is answered below.
                () = &mut parked_fetch_sleep => Wake::ParkedFetch,
                () = &mut noop_sleep => Wake::Noop,
                () = &mut clean_sleep => Wake::Clean,
                result = &mut snapshot_complete => Wake::SnapshotComplete(result),
            };
            self.run_turn(wake, shutting_down);
            // A write to the metadata log directory that failed this turn
            // ends the controller (KIP-858), and so does a failed controller
            // activation. The engine stops taking part in the quorum at once,
            // rather than serve from a log it can no longer extend or that it
            // could not seed.
            if self.publish_fault() {
                faulted = true;
                break;
            }
            // Whatever this turn did may have grown the log, moved the high
            // watermark or ended the leadership a parked fetch waits on.
            self.complete_parked_fetches(Instant::now());
            self.retry_pending_downgrade_snapshot();
            // A quiet log still needs to snapshot on the time cap: an apply
            // that advances the HWM already retriggers this check, but an
            // idle voter only ticks the loop on its timers.
            self.maybe_snapshot();
            // The event is done, unless a write parked its timer in a commit
            // waiter.
            self.turn = None;
            self.event_queue.finish_turn();
            // A graceful shutdown resigns a leadership this replica holds, or
            // wins while it waits, and ends once a successor is known.
            if shutdown_at.is_some() && self.step_graceful_shutdown() {
                break;
            }
        }
        if faulted {
            // The engine takes part in nothing more, but it keeps its watch
            // channels open, and it answers no caller, until the controller
            // shuts it down. The controller publishes the fault as fatal
            // first. So a reader that sees the image close, or a submit fail,
            // also sees the fatal fault and not a bare shutdown.
            let mut held = Vec::new();
            while let Some(command) = cmd_rx.recv().await {
                if matches!(command.command, Command::Shutdown | Command::Halt) {
                    break;
                }
                held.push(command);
            }
        }
        // Fail any parked submitters so callers don't hang on shutdown.
        for w in self.commit_waiters.drain(..) {
            let _ = w.reply.send(Err(RaftError::Shutdown));
        }
    }

    /// Run the event the loop woke for, metered as one Kafka controller
    /// event: a command with its queue time, a timer without one.
    /// Run one turn for `wake`. While `shutting_down`, the election, fetch and
    /// check-quorum timers clear without acting.
    fn run_turn(&mut self, wake: Wake, shutting_down: bool) {
        let arrival = match &wake {
            Wake::Command { arrival, .. } => Some(*arrival),
            _ => None,
        };
        self.turn = Some(self.event_queue.start(arrival));
        match wake {
            Wake::Command { command, .. } => self.on_command(command),
            Wake::Election => {
                self.election_at = None;
                if !shutting_down {
                    self.on_timer(TimerTick::Election);
                }
            }
            Wake::Fetch => {
                self.fetch_at = None;
                if !shutting_down {
                    self.on_timer(TimerTick::Fetch);
                }
            }
            Wake::CheckQuorum => {
                self.check_quorum_at = None;
                if !shutting_down {
                    self.on_timer(TimerTick::CheckQuorum);
                }
            }
            Wake::Heartbeat => self.on_timer(TimerTick::Heartbeat),
            Wake::ParkedFetch => {}
            Wake::Noop => self.on_noop_timer(),
            Wake::Clean => {
                self.clean_at = Instant::now() + METADATA_LOG_CLEAN_INTERVAL;
                if let Err(error) = self.maybe_clean() {
                    tracing::error!(?error, "kraft: metadata log cleaning failed");
                }
            }
            Wake::SnapshotComplete(result) => self.finish_snapshot_job(result),
        }
    }

    /// Publish the fault that stops the engine: the failure of the metadata
    /// log directory when a write to it returned an I/O error, or else the
    /// failure of a controller activation, or else a committed record this
    /// node could not replay. Returns `true` once a fault is published.
    pub fn publish_fault(&mut self) -> bool {
        let fault = if let Some(failure) = self.log.failure() {
            format!(
                "the metadata log directory {} has failed: {failure}",
                self.data_dir
                    .parent()
                    .unwrap_or(self.data_dir.as_path())
                    .display()
            )
        } else if let Some(fault) = &self.activation_fault {
            fault.clone()
        } else if let Some(fault) = &self.replay_fault {
            fault.to_string()
        } else {
            return false;
        };
        if self.fault_tx.borrow().is_none() {
            tracing::error!(%fault, "kraft: stopping the controller");
            self.fault_tx.send_replace(Some(fault));
        }
        true
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
            Command::Shutdown | Command::Halt => {}
            Command::Event(event) => self.on_event(event),
            Command::FetchResponse { from, body } => self.on_fetch_response(from, &body),
            Command::VoteResponse { from, body } => self.on_vote_response(from, &body),
            Command::FetchSnapshotResponse { from, body } => {
                self.on_fetch_snapshot_response(from, &body);
            }
            Command::Inbound(inbound) => self.on_inbound(inbound),
            Command::Timer(tick) => self.on_timer(tick),
            Command::SubmitChange {
                records,
                expected_image,
                reply,
            } => {
                self.on_submit_change_if_image(&records, expected_image.as_ref(), reply);
            }
            Command::SubmitDelegationTokenMutations { mutations, reply } => {
                self.on_submit_delegation_token_mutations(&mutations, reply);
            }
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
                replica,
                reply,
            } => {
                if let Some(replica) = replica {
                    self.record_metadata_fetch(replica, fetch_offset);
                }
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
                // A voter elects on the first expiry, as Kafka's
                // `KafkaRaftClient.pollFollowerAsVoter` moves to Prospective
                // once `FollowerState.hasFetchTimeoutExpired`. Only a
                // successful Fetch re-arms the timer, so an expiry already
                // means a whole fetch timeout without one. An observer has no
                // election to start: it re-polls the leader it follows, and
                // only the configured run of consecutive misses sends it back
                // to discovery.
                if self.core.is_voter() {
                    self.fetch_misses = 0;
                    self.on_event(Event::FetchTimeout);
                } else if let Some(leader_id) = self.following_leader() {
                    self.fetch_misses += 1;
                    if self.fetch_misses >= self.controller_fetch_miss_limit.get() {
                        self.fetch_misses = 0;
                        self.on_event(Event::FetchTimeout);
                    } else {
                        // Re-poll the leader and re-arm the fetch timer.
                        self.send_fetch(leader_id);
                        self.arm_fetch_timer();
                    }
                } else {
                    // An observer has no election timeout. Retry discovery
                    // until a bootstrap voter redirects it to the leader.
                    self.send_discovery_fetch();
                }
            }
            TimerTick::CheckQuorum => {
                // Only a leader runs this watchdog. The core re-arms it from
                // every Fetch/FetchSnapshot that completes a majority, so
                // reaching the deadline means the quorum has gone quiet.
                if self.core.role().is_leader() {
                    self.on_event(Event::CheckQuorumTimeout);
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
    pub fn following_leader(&self) -> Option<NodeId> { following_leader_for_role(self.core.role()) }

    /// Pick the peer for the next observer leader discovery Fetch: the voters
    /// other than this node, or the bootstrap servers when it knows no voter,
    /// taken in turn by the count in `discovery_attempts`.
    ///
    /// A discovery Fetch to a peer that is down, or that knows no leader,
    /// attaches to nothing, so the next one goes to the next peer. Kafka's
    /// `RequestManager.findReadyBootstrapServer` moves on the same way. A
    /// controller lists its own address in `controller.quorum.bootstrap.servers`
    /// as every other node does, and a discovery Fetch can go to that address.
    pub fn discovery_peer(&self) -> Option<NodeId> {
        if self.core.is_voter() || self.following_leader().is_some() {
            return None;
        }
        let voters: Vec<NodeId> = self
            .core
            .quorum_state()
            .voters
            .ids()
            .into_iter()
            .filter(|id| *id != self.me)
            .collect();
        let peers = if voters.is_empty() {
            self.peers.discovery_peers()
        } else {
            voters
        };
        peers
            .get(self.discovery_attempts % peers.len().max(1))
            .copied()
    }

    /// Send the next observer leader discovery Fetch, if this node is a
    /// leaderless observer with a peer to ask, and arm the fetch timer that
    /// retries it.
    pub(super) fn send_discovery_fetch(&mut self) {
        if let Some(peer) = self.discovery_peer() {
            self.discovery_attempts = self.discovery_attempts.wrapping_add(1);
            self.send_fetch(peer);
            self.arm_fetch_timer();
        }
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
                Action::SendEndQuorumEpoch {
                    epoch,
                    preferred_successors,
                } => {
                    self.broadcast_end_quorum_epoch(epoch, &preferred_successors);
                }
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                // The Fetch handler reads it off the actions and sends it in
                // the response. It changes nothing on this node.
                Action::ReplyDivergingEpoch(_) => {}
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
                Action::SendEndQuorumEpoch {
                    epoch,
                    preferred_successors,
                } => self.broadcast_end_quorum_epoch(epoch, &preferred_successors),
                Action::SendFetch { leader_id } => {
                    self.send_fetch(leader_id);
                    self.fetch_misses = 0;
                }
                Action::ReplyVote { .. } | Action::ReplyDivergingEpoch(_) => {}
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
                    return;
                }
                // Kafka's controller activation: the first write of the
                // epoch seeds a log that holds no `metadata.version`.
                self.complete_activation();
                // There is no follower Fetch acknowledgement to drive HWM
                // recomputation in a one-voter quorum. The local append is
                // already a majority, so commit the current-epoch barrier.
                self.advance_if_single_voter();
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
                    self.restore_control_state_after_truncation(self.log.log_end_offset().0);
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
                TimerKind::CheckQuorum => {
                    self.check_quorum_at = Some(self.deadline_instant(deadline));
                }
            },
            Action::TransitionedTo(_name) => {}
            Action::SendVoteRequest { .. }
            | Action::SendBeginQuorumEpoch { .. }
            | Action::SendEndQuorumEpoch { .. }
            | Action::SendFetch { .. }
            | Action::ReplyVote { .. }
            | Action::ReplyDivergingEpoch(_) => {
                assert2::assert!(false, "network/reply action routed to local executor");
            }
        }
    }

    /// After processing an event, cancel timers irrelevant to the new role and
    /// arm the ones it needs that the core did not explicitly reset.
    pub fn reconcile_timers(&mut self, _prev_role: &'static str) {
        match self.core.role() {
            Role::Leader { .. } => {
                // A leader never elects on a timer and never fetch-watchdogs.
                // Its liveness is the check-quorum deadline, which the core
                // arms on promotion and re-arms from majority contact.
                self.election_at = None;
                self.fetch_at = None;
                self.fetch_misses = 0;
            }
            Role::Follower { .. } | Role::Observer { .. } => {
                // A follower has no election timer; the fetch watchdog (armed by
                // the core's ResetTimer/Fetch) covers liveness.
                self.election_at = None;
                self.check_quorum_at = None;
            }
            Role::Prospective { .. }
            | Role::Candidate { .. }
            | Role::Unattached { .. }
            | Role::Voted { .. } => {
                // Mid-election: no leader to fetch from, election timer governs.
                self.fetch_at = None;
                self.fetch_misses = 0;
                self.check_quorum_at = None;
            }
            Role::Resigned => {
                // Stepped down: no leadership left to watch and no leader to
                // fetch from. The election timer the core armed on the way in
                // is what carries this node into its next pre-vote round.
                self.fetch_at = None;
                self.fetch_misses = 0;
                self.check_quorum_at = None;
            }
        }
        self.reconcile_noop_timer();
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
        if self.was_leader != is_leader || self.held_epoch != epoch {
            // An admission image also identifies the authority that planned
            // the mutation. Invalidate it on role/epoch changes even when no
            // metadata records changed. This runs before the next command,
            // including while the new leader's control batch is uncommitted.
            // Preserve the published contents: self.image can include a
            // downgrade whose checkpoint has not yet made it publishable.
            self.image.republish();
        }
        let lost_leadership = should_fail_waiters_on_leadership_change(
            self.was_leader,
            is_leader,
            self.held_epoch,
            epoch,
        );
        if lost_leadership {
            // A later leadership starts with no observers, as a new
            // `LeaderState` does.
            self.observers.clear();
        }
        if lost_leadership && !self.commit_waiters.is_empty() {
            let current_leader = self.core.quorum_state().leader_id;
            for w in self.commit_waiters.drain(..) {
                let _ = w.reply.send(Err(RaftError::NotLeader { current_leader }));
            }
        }
        if lost_leadership {
            self.fail_pending_reconfiguration();
        }
        self.was_leader = is_leader;
        self.held_epoch = epoch;
    }

    /// Arm the fetch timer one fetch timeout out from now (re-poll cadence).
    pub fn arm_fetch_timer(&mut self) {
        self.fetch_at =
            Some(Instant::now() + Duration::from_millis(whole_millis(self.timeouts.fetch)));
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

/// `Some` sleep future for an armed deadline; a never-ready future otherwise so
/// `select!` ignores the disarmed timer.
pub async fn sleep_until_opt(deadline: Option<Instant>) {
    match deadline {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending::<()>().await,
    }
}
