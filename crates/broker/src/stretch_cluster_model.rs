//! Exhaustive stateright model of a three-site stretch cluster with a
//! data-bearing witness. The design is
//! [KFC-2](../../../docs/KFCs/KFC-2-witness-broker-stretch-cluster.md).
//!
//! A witness replicates the partition, it counts toward `min.insync.replicas`,
//! and it votes in `KRaft`. It serves no client, and it never takes partition
//! leadership. One replica for each of three sites, with
//! `min.insync.replicas=2`, then survives the loss of any single site with
//! `acks=all` intact. This model checks that claim over every reachable state
//! of a bounded cluster.
//!
//! # The model drives the real code
//!
//! The model adds no election logic of its own.
//!
//! - [`stretch_replicas`] builds the replica list, so the model inherits the
//!   real site spread and the real leadership pinning.
//! - [`failover_one`] takes every controller failover decision.
//! - [`select_new_leader_for_partition`] takes every KIP-460 preferred
//!   election.
//! - The durability arithmetic comes from the Creusot-proved kernels in
//!   [`krabka_verified::stretch`].
//!
//! Two behaviours have no pure production core to call, so the model states
//! them itself and names the code they mirror.
//!
//! - `SiteUp` and `SiteHeal` fold in the `isr_maintenance` catch-up. A replica
//!   that runs again and that reaches the leader rejoins the in-sync replica
//!   set, because an idle partition has nothing to catch up on.
//! - `ProduceAcksAll` mirrors the two `acks=all` gates of
//!   `handlers::produce`. An in-sync replica set under `min.insync.replicas`
//!   gives `NOT_ENOUGH_REPLICAS`. An in-sync replica that the leader cannot
//!   reach holds back the high watermark and gives
//!   `NOT_ENOUGH_REPLICAS_AFTER_APPEND`.
//!
//! # What the model checks
//!
//! Five `always` properties:
//!
//! 1. `leader_never_witness` — no reachable state has a witness leader.
//! 2. `single_site_loss_keeps_acks_all_writable` — with at most one site down
//!    or isolated, with the controller converged, and with every reachable
//!    replica caught up, an `acks=all` produce commits. This is the headline
//!    claim of the feature.
//! 3. `one_leader_per_epoch` — a leader change always carries a strictly
//!    greater leader epoch, so two leaders never share one epoch.
//! 4. `minority_never_commits` — a set of sites that holds fewer than a voter
//!    majority never commits a write.
//! 5. `preferred_site_keeps_leadership` — a preferred election puts the leader
//!    in the preferred site whenever that site holds an alive, in-sync,
//!    non-witness replica.
//!
//! Five `sometimes` properties keep a vacuous pass out of reach. The important
//! one is `witness_stays_in_isr_after_failover`: the witness is what keeps
//! `acks=all` writable after a site loss, so an election that skipped it must
//! still leave it in the in-sync replica set.
//!
//! # The controller quorum is not modelled
//!
//! The model lets the controller decide and commit in every state, including
//! states where `KRaft` holds no metadata quorum. This over-approximates the
//! reachable set. An `always` safety property that holds over a superset holds
//! over the real set too, so every result here is the stronger form of the
//! claim: it needs no assumption about metadata availability at all. The extra
//! states matter, because they are the states where the witness is the only
//! surviving in-sync member. Those are the states the witness rule exists for,
//! and a quorum gate would hide them.
//!
//! # Why the model stays at one replica per site
//!
//! A `replication.factor` above the site count puts two replicas in one site,
//! and `minority_never_commits` then reports a write that commits inside that
//! one site. That report is not an artefact of the missing quorum gate: the
//! in-sync replica set can shrink to those two while a real `KRaft` quorum
//! holds, because one site down plus one lagging replica is enough and the
//! quorum lives in the other two sites. The write is then acknowledged with
//! every copy in one site, and the loss of that site loses it while the
//! survivors still hold the majority that elects a leader which never saw it.
//!
//! Such a configuration is therefore rejected before a broker starts, and not
//! merely left out of the model.
//! [`krabka_verified::stretch::min_insync_is_site_loss_safe`] requires that no
//! single site hold `min.insync.replicas` replicas, which for four replicas
//! over three sites leaves no safe value at all. The model checks the profile
//! that validation admits, which is one replica per site.
//!
//! # RED witness
//!
//! [`legacy_elect`] reproduces the pre-witness controller, which took the first
//! alive in-sync member with no witness filter. A `#[should_panic]` test proves
//! that `leader_never_witness` fires against it. A second `#[should_panic]`
//! test drops `min.insync.replicas` to 1 and proves that
//! `minority_never_commits` fires. A model that cannot fail is not evidence.
//!
//! Memory safety: stateright BFS keeps every visited unique state resident, so
//! `within_boundary` + `target_state_count` + `timeout` fence each run. You
//! MUST run these models under the host memory watchdog while you tune the
//! bounds.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    time::Duration,
};

use assert2::assert;
use krabka_metadata::{LeaderEpoch, MetadataImage, MetadataRecord, PartitionRecord, TopicRecord};
use krabka_raft::NodeId;
use krabka_verified::stretch::{
    min_insync_is_site_loss_safe, quorum_survives_any_single_site_loss, site_loss_survivors,
};
use stateright::{Checker, Model, Property};
use uuid::Uuid;

use crate::{
    config_keys::RecoveryStrategy,
    heartbeat::controller_state::ControllerLivenessState,
    leader_election::{
        ElectionType, FailoverDecision, failover_one, select_new_leader_for_partition,
    },
    site_placement::{SiteBrokerView, stretch_replicas},
};

const MAX_STATES: usize = 200_000;
const MAX_DEPTH: usize = 60;
const CHECK_TIMEOUT: Duration = Duration::from_mins(2);

/// The one topic of the model. Every action works on partition 0 of it.
const TOPIC: &str = "stretch";

/// The largest count of sites that the model impairs at once. Two of three
/// sites is already a full loss of the metadata quorum, and a third impaired
/// site adds no new decision.
const MAX_IMPAIRED_SITES: usize = 2;

/// Run `future` to completion on a per-thread current-thread tokio runtime.
///
/// [`select_new_leader_for_partition`] is `async`, and the stateright BFS
/// runs on plain threads with no runtime. Each thread builds one runtime and
/// reuses it for every transition.
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    thread_local! {
        static RUNTIME: tokio::runtime::Runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("current-thread tokio runtime");
    }
    RUNTIME.with(|runtime| runtime.block_on(future))
}

/// The controller failover decision, as a function pointer. The model runs the
/// real [`failover_one`] under it. The RED witness runs [`legacy_elect`].
type ElectFn = fn(
    &PartitionRecord,
    NodeId,
    &HashSet<NodeId>,
    &HashSet<NodeId>,
    RecoveryStrategy,
    bool,
) -> FailoverDecision;

/// One site of the stretch cluster.
struct SiteConfig {
    /// The `broker.rack` value that names the site.
    name: &'static str,
    /// The count of `KRaft` voters that the site holds.
    voters: i64,
}

/// One broker of the model cluster: node id, site index, and witness role.
type BrokerSpec = (u64, usize, bool);

/// The outcome of one `acks=all` produce.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum WriteOutcome {
    /// The leader refused the write, or the high watermark never covered it.
    Rejected,
    /// Every in-sync replica took the record, and the high watermark advanced.
    Committed,
}

/// Bounded config for the stretch-cluster model.
struct StretchModel {
    /// The sites, in model index order. Index `k` is site `k`.
    sites: Vec<SiteConfig>,
    /// The brokers of the cluster, in node-id order.
    brokers: Vec<SiteBrokerView>,
    /// The site index of each broker.
    site_of: BTreeMap<NodeId, u8>,
    /// The replicas of partition 0, from the real [`stretch_replicas`]. Order
    /// is significant: `replicas[0]` is the preferred leader.
    replicas: Vec<NodeId>,
    /// The witness nodes among `replicas`.
    witnesses: HashSet<NodeId>,
    /// The site that holds partition leadership in a healthy cluster.
    preferred_site: u8,
    /// The topic's `min.insync.replicas`.
    min_insync: i32,
    /// The sum of `voters` over every site.
    total_voters: i64,
    /// The replica count that survives the loss of any one site, from the
    /// proved [`site_loss_survivors`].
    survivors: i64,
    /// `min.insync.replicas` keeps `acks=all` durable and writable through the
    /// loss of any one site, from the proved [`min_insync_is_site_loss_safe`].
    min_insync_safe: bool,
    /// The `KRaft` voter split keeps a majority through the loss of any one
    /// site, from the proved [`quorum_survives_any_single_site_loss`].
    quorum_tolerates_one_loss: bool,
    /// The largest count of sites that the model impairs at once.
    max_impaired: usize,
    /// The leader-epoch cap that bounds the search.
    max_epoch: i32,
    /// The controller failover decision under test.
    elect: ElectFn,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct StretchState {
    /// The sites that are powered off.
    down: BTreeSet<u8>,
    /// The sites that run, and that a network partition cut off from the rest.
    isolated: BTreeSet<u8>,
    leader: NodeId,
    /// The in-sync replica set, in replica order.
    isr: Vec<NodeId>,
    leader_epoch: i32,
    /// The outcome of the last `acks=all` produce.
    last_write: Option<WriteOutcome>,
    /// Sticky. A write committed inside a set of sites that holds no voter
    /// majority.
    commit_in_minority: bool,
    /// Sticky. A leader change reused a leader epoch.
    epoch_reused: bool,
    /// Sticky. A preferred election left the leader outside the preferred
    /// site while that site held an electable replica.
    preferred_pinning_broken: bool,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
enum StretchAction {
    /// The site loses power. Every broker of the site stops.
    SiteDown(u8),
    /// The site comes back. Its replicas catch up and rejoin the in-sync set.
    SiteUp(u8),
    /// A network partition cuts the site off from the other sites.
    SitePartition(u8),
    /// The network partition heals.
    SiteHeal(u8),
    /// The controller runs its failover decision for a broker it cannot reach.
    Failover(NodeId),
    /// An operator, or the KIP-460 auto-rebalance, asks for a preferred
    /// election on partition 0.
    PreferredElection,
    /// A producer sends one `acks=all` record to the partition leader.
    ProduceAcksAll,
}

// ============================ configuration ============================

impl StretchModel {
    /// Three sites with one broker each. Node 3 sits in the cheap third site
    /// and carries the witness role. `replication.factor=3` with
    /// `min.insync.replicas=2` is the supported stretch shape.
    fn three_sites(min_insync: i32, elect: ElectFn) -> Self {
        Self::new(
            vec![
                SiteConfig {
                    name: "east",
                    voters: 1,
                },
                SiteConfig {
                    name: "west",
                    voters: 1,
                },
                SiteConfig {
                    name: "witness",
                    voters: 1,
                },
            ],
            &[(1, 0, false), (2, 1, false), (3, 2, true)],
            3,
            min_insync,
            elect,
        )
    }

    fn new(
        sites: Vec<SiteConfig>,
        brokers: &[BrokerSpec],
        replication_factor: i16,
        min_insync: i32,
        elect: ElectFn,
    ) -> Self {
        let preferred_site = 0_u8;
        let views: Vec<SiteBrokerView> = brokers
            .iter()
            .map(|&(node_id, site, is_witness)| SiteBrokerView {
                node_id: NodeId(node_id),
                site: Some(sites[site].name.to_string()),
                is_witness,
            })
            .collect();
        let site_of: BTreeMap<NodeId, u8> = brokers
            .iter()
            .map(|&(node_id, site, _)| {
                (
                    NodeId(node_id),
                    u8::try_from(site).expect("site index fits in u8"),
                )
            })
            .collect();
        let witnesses: HashSet<NodeId> = brokers
            .iter()
            .filter(|&&(_, _, is_witness)| is_witness)
            .map(|&(node_id, _, _)| NodeId(node_id))
            .collect();

        // The real placement decides the replica list, including which site
        // holds `replicas[0]`.
        let placement = stretch_replicas(
            &views,
            1,
            replication_factor,
            Some(sites[preferred_site as usize].name),
        );
        assert!(
            placement.len() == 1,
            "stretch_replicas refused the model configuration"
        );
        let replicas = placement[0].clone();
        let preferred = *replicas.first().expect("a placed partition has replicas");
        assert!(
            !witnesses.contains(&preferred),
            "stretch_replicas put witness {preferred} first in {replicas:?}"
        );
        assert!(
            site_of[&preferred] == preferred_site,
            "stretch_replicas put {preferred} outside the preferred site in {replicas:?}"
        );

        let voters: Vec<i64> = sites.iter().map(|site| site.voters).collect();
        let total_voters = voters.iter().sum();
        let site_count = i64::try_from(sites.len()).expect("site count fits in i64");
        let rf = i64::from(replication_factor);
        Self {
            survivors: site_loss_survivors(rf, site_count),
            min_insync_safe: min_insync_is_site_loss_safe(rf, site_count, i64::from(min_insync)),
            quorum_tolerates_one_loss: quorum_survives_any_single_site_loss(&voters),
            max_impaired: MAX_IMPAIRED_SITES.min(sites.len() - 1),
            sites,
            brokers: views,
            site_of,
            replicas,
            witnesses,
            preferred_site,
            min_insync,
            total_voters,
            max_epoch: 6,
            elect,
        }
    }
}

// ============================ model helpers ============================

/// The count of sites that are down or isolated. The two sets are disjoint.
fn impaired(state: &StretchState) -> usize {
    state.down.len() + state.isolated.len()
}

/// `true` when sites `left` and `right` both run and can reach each other. An
/// isolated site reaches only itself.
fn same_component(state: &StretchState, left: u8, right: u8) -> bool {
    if state.down.contains(&left) || state.down.contains(&right) {
        return false;
    }
    left == right || !(state.isolated.contains(&left) || state.isolated.contains(&right))
}

/// Record a leader change that reused an epoch. A new leader must always carry
/// a strictly greater leader epoch, which is what makes an epoch name at most
/// one leader.
fn check_epoch(last: &StretchState, next: &mut StretchState) {
    let reused = next.leader_epoch < last.leader_epoch
        || (next.leader != last.leader && next.leader_epoch <= last.leader_epoch);
    if reused {
        next.epoch_reused = true;
    }
}

impl StretchModel {
    fn site_of(&self, node: NodeId) -> u8 {
        self.site_of[&node]
    }

    fn site_count(&self) -> u8 {
        u8::try_from(self.sites.len()).expect("site count fits in u8")
    }

    /// The alive set as the controller sees it. The controller reaches every
    /// broker of a site that runs and that no network partition cut off.
    fn alive(&self, state: &StretchState) -> HashSet<NodeId> {
        self.brokers
            .iter()
            .map(|broker| broker.node_id)
            .filter(|&node| {
                let site = self.site_of(node);
                !(state.down.contains(&site) || state.isolated.contains(&site))
            })
            .collect()
    }

    /// The sites of the network component that holds `site`. A down site holds
    /// no component. An isolated site is alone. Every other running site is in
    /// the one large component.
    fn component_of(&self, state: &StretchState, site: u8) -> BTreeSet<u8> {
        if state.down.contains(&site) {
            return BTreeSet::new();
        }
        if state.isolated.contains(&site) {
            return BTreeSet::from([site]);
        }
        (0..self.site_count())
            .filter(|k| !(state.down.contains(k) || state.isolated.contains(k)))
            .collect()
    }

    /// `true` when the network component that holds the leader also holds a
    /// strict majority of the `KRaft` voters.
    fn leader_holds_majority(&self, state: &StretchState) -> bool {
        let component = self.component_of(state, self.site_of(state.leader));
        let voters: i64 = component
            .iter()
            .map(|&site| self.sites[site as usize].voters)
            .sum();
        2 * voters > self.total_voters
    }

    /// The `PartitionRecord` that the metadata image holds in `state`.
    fn record(&self, state: &StretchState) -> PartitionRecord {
        PartitionRecord {
            topic: TOPIC.to_string(),
            partition: 0,
            leader: state.leader,
            replicas: self.replicas.clone(),
            isr: state.isr.clone(),
            leader_epoch: LeaderEpoch(state.leader_epoch),
            adding_replicas: vec![],
            removing_replicas: vec![],
            directories: vec![],
            partition_epoch: 0,
        }
    }

    /// A metadata image that holds the one topic and its one partition, for
    /// the real [`select_new_leader_for_partition`].
    fn image(&self, state: &StretchState) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: TOPIC.to_string(),
            topic_id: Uuid::nil(),
            partitions: 1,
            replication_factor: i16::try_from(self.replicas.len())
                .expect("replica count fits in i16"),
        }));
        image.apply(&MetadataRecord::V1Partition(self.record(state)));
        image
    }

    /// A liveness registry that holds a fresh heartbeat for every broker the
    /// controller reaches, for the real [`select_new_leader_for_partition`].
    fn liveness(&self, state: &StretchState) -> ControllerLivenessState {
        let liveness = ControllerLivenessState::new(krabka_units::secs(60));
        let alive = self.alive(state);
        block_on(async {
            // Node-id order, not hash order, so the registry is the same on
            // every visit to the state.
            for broker in &self.brokers {
                if alive.contains(&broker.node_id) {
                    liveness.record_heartbeat(broker.node_id.0).await;
                }
            }
        });
        liveness
    }

    /// Put the in-sync replica set back in replica order, which is the order
    /// the controller writes and the order a clean election reads.
    fn normalize_isr(&self, isr: &mut [NodeId]) {
        isr.sort_by_key(|node| {
            self.replicas
                .iter()
                .position(|replica| replica == node)
                .unwrap_or(usize::MAX)
        });
    }

    /// Fold the `isr_maintenance` catch-up into a site recovery. A replica
    /// that runs again and that reaches the leader has nothing to catch up on
    /// in an idle partition, so the controller puts it back in the in-sync
    /// replica set.
    fn rejoin_isr(&self, state: &mut StretchState, site: u8) {
        if !same_component(state, site, self.site_of(state.leader)) {
            return;
        }
        let returning: Vec<NodeId> = self
            .replicas
            .iter()
            .copied()
            .filter(|&replica| self.site_of(replica) == site && !state.isr.contains(&replica))
            .collect();
        state.isr.extend(returning);
        self.normalize_isr(&mut state.isr);
    }
}

// ============================ the acks=all write ============================

impl StretchModel {
    /// The outcome of one `acks=all` produce against the current leader. This
    /// mirrors the two gates of `handlers::produce`.
    fn produce_outcome(&self, state: &StretchState) -> WriteOutcome {
        let leader_site = self.site_of(state.leader);
        if state.down.contains(&leader_site) {
            // No leader runs, so the client gets NOT_LEADER_OR_FOLLOWER.
            return WriteOutcome::Rejected;
        }
        let isr_size = i32::try_from(state.isr.len()).expect("ISR size fits in i32");
        if isr_size < self.min_insync {
            // `validate_partition_gate`: NOT_ENOUGH_REPLICAS (19).
            return WriteOutcome::Rejected;
        }
        // The high watermark covers the append only after every in-sync
        // replica takes the record. A replica outside the leader's network
        // component never takes it, and the produce times out with
        // NOT_ENOUGH_REPLICAS_AFTER_APPEND (20).
        let component = self.component_of(state, leader_site);
        if state
            .isr
            .iter()
            .all(|&node| component.contains(&self.site_of(node)))
        {
            WriteOutcome::Committed
        } else {
            WriteOutcome::Rejected
        }
    }

    fn apply_produce(&self, state: &mut StretchState) {
        let outcome = self.produce_outcome(state);
        state.last_write = Some(outcome);
        if outcome == WriteOutcome::Committed && !self.leader_holds_majority(state) {
            state.commit_in_minority = true;
        }
    }
}

// ============================ controller actions ============================

impl StretchModel {
    /// `true` when the controller has no failover work left. Every broker it
    /// cannot reach gives a decision that changes nothing.
    fn converged(&self, state: &StretchState) -> bool {
        let alive = self.alive(state);
        let record = self.record(state);
        self.replicas.iter().all(|&replica| {
            alive.contains(&replica)
                || matches!(
                    (self.elect)(
                        &record,
                        replica,
                        &alive,
                        &self.witnesses,
                        RecoveryStrategy::None,
                        false,
                    ),
                    FailoverDecision::NoChange | FailoverDecision::Unavailable
                )
        })
    }

    /// Run the controller failover decision for `dead` and apply it.
    fn apply_failover(&self, last: &StretchState, dead: NodeId) -> Option<StretchState> {
        let alive = self.alive(last);
        if alive.contains(&dead) {
            return None;
        }
        let decision = (self.elect)(
            &self.record(last),
            dead,
            &alive,
            &self.witnesses,
            RecoveryStrategy::None,
            false,
        );
        let mut state = last.clone();
        match decision {
            FailoverDecision::Elect { leader, isr, .. } => {
                if last.leader_epoch >= self.max_epoch {
                    return None;
                }
                state.leader = leader;
                state.isr = isr;
                state.leader_epoch += 1;
            }
            FailoverDecision::ShrinkIsr { isr } => state.isr = isr,
            FailoverDecision::Recover(_)
            | FailoverDecision::Unavailable
            | FailoverDecision::NoChange => return None,
        }
        self.normalize_isr(&mut state.isr);
        check_epoch(last, &mut state);
        Some(state)
    }

    /// `true` when the preferred site holds a replica that can take
    /// leadership: alive, in the in-sync replica set, and not a witness.
    ///
    /// The model gives each site one broker, so that replica is `replicas[0]`,
    /// which is the one replica a Kafka preferred election considers. A site
    /// with a second broker would need this test to name `replicas[0]`
    /// directly.
    fn preferred_site_is_electable(&self, state: &StretchState) -> bool {
        let alive = self.alive(state);
        self.replicas.iter().any(|&replica| {
            self.site_of(replica) == self.preferred_site
                && !self.witnesses.contains(&replica)
                && alive.contains(&replica)
                && state.isr.contains(&replica)
        })
    }

    /// Run the real KIP-460 preferred election and apply its record.
    fn apply_preferred(&self, last: &StretchState) -> Option<StretchState> {
        let image = self.image(last);
        let liveness = self.liveness(last);
        let elected = block_on(select_new_leader_for_partition(
            &image,
            &liveness,
            &self.witnesses,
            TOPIC,
            0,
            ElectionType::Preferred,
        ));
        let mut state = last.clone();
        if let Ok(record) = elected {
            if last.leader_epoch >= self.max_epoch {
                return None;
            }
            state.leader = record.leader;
            state.isr = record.isr;
            state.leader_epoch = record.leader_epoch.0;
            self.normalize_isr(&mut state.isr);
        }
        // Leadership pinning: the preferred site keeps the leader whenever it
        // holds a replica that can take leadership.
        if self.preferred_site_is_electable(last)
            && self.site_of(state.leader) != self.preferred_site
        {
            state.preferred_pinning_broken = true;
        }
        check_epoch(last, &mut state);
        if state == *last {
            return None;
        }
        Some(state)
    }
}

// ============================ properties ============================

impl StretchModel {
    /// The precondition of the single-site-loss availability claim. The two
    /// proved kernels gate it, so a configuration that is not site-loss safe
    /// makes no availability claim at all.
    fn single_site_loss_holds(&self, state: &StretchState) -> bool {
        self.min_insync_safe
            && self.quorum_tolerates_one_loss
            && impaired(state) <= 1
            && self.converged(state)
            && self.reachable_replicas_in_isr(state)
    }

    /// `true` when every replica the controller reaches is in the in-sync
    /// replica set. This is the "replicas caught up" half of the claim.
    fn reachable_replicas_in_isr(&self, state: &StretchState) -> bool {
        let alive = self.alive(state);
        self.replicas
            .iter()
            .all(|replica| !alive.contains(replica) || state.isr.contains(replica))
    }
}

impl Model for StretchModel {
    type State = StretchState;
    type Action = StretchAction;

    fn init_states(&self) -> Vec<Self::State> {
        vec![StretchState {
            down: BTreeSet::new(),
            isolated: BTreeSet::new(),
            leader: self.replicas[0],
            isr: self.replicas.clone(),
            leader_epoch: 0,
            last_write: None,
            commit_in_minority: false,
            epoch_reused: false,
            preferred_pinning_broken: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        let room = impaired(state) < self.max_impaired;
        for site in 0..self.site_count() {
            if state.down.contains(&site) {
                actions.push(StretchAction::SiteUp(site));
                continue;
            }
            if state.isolated.contains(&site) {
                actions.push(StretchAction::SiteHeal(site));
                actions.push(StretchAction::SiteDown(site));
            } else if room {
                actions.push(StretchAction::SitePartition(site));
                actions.push(StretchAction::SiteDown(site));
            }
        }
        let alive = self.alive(state);
        for &replica in &self.replicas {
            if !alive.contains(&replica) {
                actions.push(StretchAction::Failover(replica));
            }
        }
        actions.push(StretchAction::PreferredElection);
        actions.push(StretchAction::ProduceAcksAll);
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        match action {
            StretchAction::SiteDown(site) => {
                let already_impaired = state.isolated.remove(&site);
                if state.down.contains(&site)
                    || (!already_impaired && impaired(last) >= self.max_impaired)
                {
                    return None;
                }
                state.down.insert(site);
            }
            StretchAction::SiteUp(site) => {
                if !state.down.remove(&site) {
                    return None;
                }
                self.rejoin_isr(&mut state, site);
            }
            StretchAction::SitePartition(site) => {
                if state.down.contains(&site)
                    || state.isolated.contains(&site)
                    || impaired(last) >= self.max_impaired
                {
                    return None;
                }
                state.isolated.insert(site);
            }
            StretchAction::SiteHeal(site) => {
                if !state.isolated.remove(&site) {
                    return None;
                }
                self.rejoin_isr(&mut state, site);
            }
            StretchAction::Failover(dead) => return self.apply_failover(last, dead),
            StretchAction::PreferredElection => return self.apply_preferred(last),
            StretchAction::ProduceAcksAll => self.apply_produce(&mut state),
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            // 1. A witness serves no client, so it never leads a partition.
            Property::always(
                "leader_never_witness",
                |model: &StretchModel, state: &StretchState| {
                    !model.witnesses.contains(&state.leader)
                },
            ),
            // 2. The headline claim: one site loss keeps `acks=all` writable.
            Property::always(
                "single_site_loss_keeps_acks_all_writable",
                |model: &StretchModel, state: &StretchState| {
                    if !model.single_site_loss_holds(state) {
                        return true;
                    }
                    let isr_size = i64::try_from(state.isr.len()).expect("ISR size fits in i64");
                    model.produce_outcome(state) == WriteOutcome::Committed
                        && isr_size >= model.survivors
                },
            ),
            // 3. A leader change always carries a greater epoch.
            Property::always(
                "one_leader_per_epoch",
                |_: &StretchModel, state: &StretchState| !state.epoch_reused,
            ),
            // 4. A minority of the voters never commits a write.
            Property::always(
                "minority_never_commits",
                |_: &StretchModel, state: &StretchState| !state.commit_in_minority,
            ),
            // 5. The preferred site keeps leadership while it can take it.
            Property::always(
                "preferred_site_keeps_leadership",
                |_: &StretchModel, state: &StretchState| !state.preferred_pinning_broken,
            ),
            // The witness is what keeps `acks=all` writable after a site loss,
            // so an election that skipped it must still leave it in the ISR.
            Property::sometimes(
                "witness_stays_in_isr_after_failover",
                |model: &StretchModel, state: &StretchState| {
                    state.leader_epoch > 0
                        && state.isr.iter().any(|node| model.witnesses.contains(node))
                },
            ),
            // Non-vacuity for property 2. The precondition of the headline
            // claim is reachable with a whole site lost.
            Property::sometimes(
                "single_site_loss_precondition_is_reachable",
                |model: &StretchModel, state: &StretchState| {
                    impaired(state) == 1 && model.single_site_loss_holds(state)
                },
            ),
            Property::sometimes(
                "one_site_loss_still_commits",
                |_: &StretchModel, state: &StretchState| {
                    impaired(state) == 1 && state.last_write == Some(WriteOutcome::Committed)
                },
            ),
            Property::sometimes(
                "two_site_loss_rejects_the_write",
                |_: &StretchModel, state: &StretchState| {
                    impaired(state) == 2 && state.last_write == Some(WriteOutcome::Rejected)
                },
            ),
            Property::sometimes(
                "preferred_election_returns_leadership",
                |model: &StretchModel, state: &StretchState| {
                    impaired(state) == 0
                        && state.leader_epoch > 0
                        && model.site_of(state.leader) == model.preferred_site
                },
            ),
        ]
    }

    fn within_boundary(&self, state: &Self::State) -> bool {
        state.leader_epoch <= self.max_epoch && impaired(state) <= self.max_impaired
    }
}

// ============================ runner ============================

fn run(model: StretchModel, label: &str) {
    let checker = model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .timeout(CHECK_TIMEOUT)
        .spawn_bfs()
        .join();
    eprintln!(
        "[{label}] unique_states={} generated={} max_depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert!(
        checker.max_depth() < MAX_DEPTH,
        "[{label}] hit depth cap {MAX_DEPTH}: depth-truncated, not exhaustive"
    );
    assert!(
        checker.state_count() < MAX_STATES,
        "[{label}] hit state cap {MAX_STATES}: truncated, not exhaustive"
    );
    checker.assert_properties();
}

#[test]
fn stretch_three_sites() {
    // The supported shape: one broker per site, RF=3, min.insync.replicas=2.
    run(
        StretchModel::three_sites(2, failover_one),
        "stretch_three_sites",
    );
}

// ============================ RED witness ============================

/// The pre-witness controller decision. It is the same shape as
/// [`failover_one`], and it takes the first alive in-sync member with no
/// witness filter. Leadership can then land on a node that serves no client.
fn legacy_elect(
    record: &PartitionRecord,
    dead: NodeId,
    alive: &HashSet<NodeId>,
    _witnesses: &HashSet<NodeId>,
    _strategy: RecoveryStrategy,
    _unclean_enabled: bool,
) -> FailoverDecision {
    let alive_isr: Vec<NodeId> = record
        .isr
        .iter()
        .filter(|node| **node != dead && alive.contains(node))
        .copied()
        .collect();
    if record.leader == dead {
        // BUG: no witness filter on the leader pick.
        return match alive_isr.first().copied() {
            Some(leader) => FailoverDecision::Elect {
                leader,
                isr: alive_isr,
                unclean: false,
            },
            None => FailoverDecision::Unavailable,
        };
    }
    if alive_isr.len() < record.isr.len() {
        return FailoverDecision::ShrinkIsr { isr: alive_isr };
    }
    FailoverDecision::NoChange
}

#[test]
#[should_panic(expected = "leader_never_witness")]
fn red_witness_unaware_election_elects_a_witness() {
    // Both data sites go down. The witness is then the only alive in-sync
    // member, and the pre-witness pick hands it leadership. The real
    // `failover_one` answers `Unavailable` there instead.
    run(
        StretchModel::three_sites(2, legacy_elect),
        "red_legacy_elect",
    );
}

#[test]
#[should_panic(expected = "minority_never_commits")]
fn red_min_insync_one_commits_in_a_minority() {
    // `min.insync.replicas=1` lets a lone surviving replica commit an
    // `acks=all` write while its site holds one voter of three. The value 2 is
    // what keeps every commit across two of the three sites, which is a voter
    // majority. This proves that `minority_never_commits` is a real gate and
    // not a tautology of the model.
    run(
        StretchModel::three_sites(1, failover_one),
        "red_min_insync_one",
    );
}
