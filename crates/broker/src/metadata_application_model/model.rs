//! The stateright [`Model`]: its actions, its transition function, its
//! properties, and the [`Quiescence`] split of its actions into faults and
//! steps.

use {
    super::{
        cluster::{Cluster, FetchOutcome, IsrRequest, ReplayFrom, fetch_outcome},
        state::{CONTROLLER, Event, Members, Node, NodeState, REPLICAS, State, View, Witnessed},
    },
    crate::{codes, model_check::Quiescence},
    krabka_metadata::MetadataRecord,
    stateright::{Model, Property},
};

/// The model and its bounds.
pub(super) struct MetadataApplication {
    pub(super) cluster: Cluster,
    /// Where a restart replay starts.
    pub(super) replay_from: ReplayFrom,
    /// Crash-restarts of the replicas, in total.
    pub(super) restarts: u8,
    /// The longest committed log the search follows.
    pub(super) max_log: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Action {
    /// Fault: the node is killed and started again. It rebuilds its image
    /// from its checkpoint and its committed log, and the controller runs the
    /// node's re-registration plan.
    CrashRestart(Node),
    /// The node's `KRaft` replica applies the next committed batch.
    Apply(Node),
    /// The node writes an interval checkpoint of its image.
    Checkpoint(Node),
    /// One reconcile pass of the replicator supervisor: it takes the image
    /// from the watch channel, installs the partition's leadership, and
    /// retargets the replica fetcher.
    Reconcile(Node),
    /// The node's replica fetcher fetches from the leader it follows.
    Fetch(Node),
    /// The leader's ISR maintenance sends `AlterPartition` to the controller.
    AlterPartition(Node),
}

/// The properties, by name.
pub(super) const EVERY_BROKER_CONVERGES: &str = "every_broker_installs_the_committed_target";
pub(super) const LEADER_CONVERGES: &str = "the_committed_leader_installs_the_committed_target";

impl MetadataApplication {
    /// Node 1 leads alone: its follower has fallen out of the ISR.
    const fn initial_view() -> View {
        View {
            leader: 1,
            leader_epoch: 0,
            isr: Members::of_const(1 << 1),
            partition_epoch: 0,
            recovering: false,
        }
    }

    /// Commit `records` as one batch. The controller encodes it against its
    /// image and applies it at once.
    fn append(&self, state: &mut State, records: &[MetadataRecord]) {
        let batch = self.cluster.commit(state.committed(), records);
        let controller = state.node_mut(CONTROLLER);
        controller.image = self.cluster.apply(controller.image, &batch);
        controller.applied += 1;
        state.log.push(batch);
    }

    /// The ISR the leader `node` proposes, or `None` when its ISR maintenance
    /// sends nothing.
    ///
    /// This restates `isr_maintenance::run` and `proposal_at`: only a leader
    /// whose installed target is its image's proposes; a follower that has
    /// caught up at the leader's epoch joins the proposed ISR
    /// (`isr_candidate_selected`); and a partition in `RECOVERING` sends its
    /// current ISR to report `RECOVERED` even when nothing changed. The model
    /// leaves out the lag-driven shrink.
    fn isr_request(state: &State, node: Node) -> Option<IsrRequest> {
        let leader = state.node(node);
        let view = leader.image;
        if view.leader != node || leader.installed != Some(view.target()) {
            return None;
        }
        let new_isr = Members::of(REPLICAS.into_iter().filter(|&other| {
            view.isr.contains(other) || {
                let follower = state.node(other);
                follower.caught_up && follower.fetcher == Some(view.target())
            }
        }));
        (new_isr != view.isr || view.recovering).then_some(IsrRequest {
            requester: node,
            new_isr,
            leader_epoch: view.leader_epoch,
            partition_epoch: view.partition_epoch,
        })
    }

    fn crash_restart(&self, state: &mut State, node: Node) {
        let replayed = {
            let restarting = state.node(node);
            self.cluster.replay(
                self.replay_from,
                restarting.checkpoint,
                &state.log,
                restarting.applied,
            )
        };
        if replayed.below_checkpoint {
            state.witnessed = state.witnessed.with(Event::ReplayAboveCheckpoint);
        }
        let restarting = state.node_mut(node);
        *restarting = NodeState {
            image: replayed.view,
            seen: None,
            installed: None,
            fetcher: None,
            caught_up: false,
            ..*restarting
        };
        Self::forget_followers(state, node);
        state.restarts += 1;
        let plan = self.cluster.restart_plan(state.committed(), node);
        if plan.unclean {
            state.witnessed = state.witnessed.with(Event::UncleanElection);
        }
        if !plan.records.is_empty() {
            self.append(state, &plan.records);
        }
    }

    /// `ReplicaState::reset_for_leader`: a leader's record of its followers
    /// does not survive a new target or a new process.
    fn forget_followers(state: &mut State, leader: Node) {
        for other in REPLICAS {
            let follower = state.node_mut(other);
            if follower
                .fetcher
                .is_some_and(|target| target.leader == leader)
            {
                follower.caught_up = false;
            }
        }
    }

    /// One `ReplicatorSupervisor::reconcile` pass over the image the watch
    /// channel holds. The channel keeps only the latest image, so a node that
    /// applied several batches since its last pass reconciles once, to the
    /// newest.
    ///
    /// `reconcile_local_partitions` comes first: a leader installs through
    /// `install_local_leadership`, a follower through
    /// `install_replication_target`. A leader that already installed a newer
    /// epoch keeps it (`promote_under_write_guard`). `reconcile_fetchers`
    /// comes after.
    fn reconcile(state: &mut State, node: Node) {
        let current = state.node(node);
        let view = current.image;
        let want = view.target();
        let keeps_newer = view.leader == node
            && current.installed.is_some_and(|installed| {
                installed.leader == node && installed.leader_epoch > view.leader_epoch
            });
        if current.installed != Some(want) && !keeps_newer {
            state.node_mut(node).installed = Some(want);
            Self::forget_followers(state, node);
        }
        let reconciled = state.node_mut(node);
        reconciled.seen = Some(view);
        let fetcher = (view.leader != node).then_some(want);
        if reconciled.fetcher != fetcher {
            reconciled.fetcher = fetcher;
            reconciled.caught_up = false;
        }
    }

    fn alter_partition(&self, state: &mut State, request: IsrRequest) {
        let (code, records) = self.cluster.alter_partition(state.committed(), request);
        if code == codes::NONE && !records.is_empty() {
            self.append(state, &records);
        }
    }

    /// Whether every replica has installed the committed target.
    fn every_broker_converged(state: &State) -> bool {
        let committed = state.committed().target();
        REPLICAS
            .into_iter()
            .all(|node| state.node(node).installed == Some(committed))
    }

    /// Whether the committed leader has installed the committed target.
    fn leader_converged(state: &State) -> bool {
        let committed = state.committed().target();
        state.node(committed.leader).installed == Some(committed)
    }
}

impl Model for MetadataApplication {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        let view = Self::initial_view();
        let mut nodes = [NodeState::new(view); 3];
        for node in REPLICAS {
            if node != view.leader {
                nodes[usize::from(node - 1)].fetcher = Some(view.target());
            }
        }
        vec![State {
            log: Vec::new(),
            nodes,
            restarts: 0,
            witnessed: Witnessed::default(),
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        for node in REPLICAS {
            let current = state.node(node);
            if state.restarts < self.restarts {
                actions.push(Action::CrashRestart(node));
            }
            if usize::from(current.applied) < state.log.len() {
                actions.push(Action::Apply(node));
            }
            if current.applied > current.checkpoint.offset {
                actions.push(Action::Checkpoint(node));
            }
            if current.seen != Some(current.image) {
                actions.push(Action::Reconcile(node));
            }
            if current.fetcher.is_some() {
                actions.push(Action::Fetch(node));
            }
            if Self::isr_request(state, node).is_some() {
                actions.push(Action::AlterPartition(node));
            }
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::CrashRestart(node) => self.crash_restart(&mut next, node),
            Action::Apply(node) => {
                let applying = state.node(node);
                let batch = state.log.get(usize::from(applying.applied))?;
                let image = self.cluster.apply(applying.image, batch);
                let applying = next.node_mut(node);
                applying.image = image;
                applying.applied += 1;
            }
            Action::Checkpoint(node) => {
                let checkpointing = next.node_mut(node);
                checkpointing.checkpoint.view = checkpointing.image;
                checkpointing.checkpoint.offset = checkpointing.applied;
            }
            Action::Reconcile(node) => Self::reconcile(&mut next, node),
            Action::Fetch(node) => {
                let target = state.node(node).fetcher?;
                let fetched = fetch_outcome(state.node(node), target, state.node(target.leader));
                if fetched == FetchOutcome::Appended {
                    next.node_mut(node).caught_up = true;
                }
            }
            Action::AlterPartition(node) => {
                let request = Self::isr_request(state, node)?;
                self.alter_partition(&mut next, request);
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(EVERY_BROKER_CONVERGES, |model: &Self, state: &State| {
                Self::every_broker_converged(state) || !model.quiescent(state)
            }),
            Property::always(LEADER_CONVERGES, |model: &Self, state: &State| {
                Self::leader_converged(state) || !model.quiescent(state)
            }),
            Property::sometimes("an_unclean_election_commits", |_, state: &State| {
                state.witnessed.has(Event::UncleanElection)
            }),
            Property::sometimes(
                "a_restart_replays_from_a_checkpoint_above_committed_batches",
                |_, state: &State| state.witnessed.has(Event::ReplayAboveCheckpoint),
            ),
            Property::sometimes(
                "the_cluster_converges_after_an_unclean_election",
                |model: &Self, state: &State| {
                    state.witnessed.has(Event::UncleanElection)
                        && Self::every_broker_converged(state)
                        && model.quiescent(state)
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &State) -> bool { state.log.len() <= self.max_log }
}

impl Quiescence for MetadataApplication {
    fn is_fault(&self, action: &Action) -> bool { matches!(action, Action::CrashRestart(_)) }
}
