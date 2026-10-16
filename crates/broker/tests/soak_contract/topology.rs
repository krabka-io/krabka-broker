//! Who leads what: the cluster as a fault resolves its target and as a
//! recovery is measured.
//!
//! The view is read two ways. Partition leaders, replicas and ISRs come from a
//! `Metadata` request pinned to one node, and the active controller from each
//! node's `krabka_broker_active_controller` gauge. Both reads tolerate a node
//! that is stopped or paused: a request to it times out, and the caller asks
//! another node.

use {
    crate::probe,
    krabka_bench_driver::soak::contract::Target,
    krabka_client_core::{Client, ProtocolRequest},
    krabka_protocol::owned::{
        metadata_request::MetadataRequest, metadata_response::MetadataResponse,
    },
    krabka_units::prelude::*,
    std::{
        collections::{BTreeMap, BTreeSet},
        future::Future,
        path::PathBuf,
        sync::Mutex,
        time::Duration,
    },
};

/// A topic partition.
pub(crate) type TopicPartition = (String, i32);

/// The gauge a node sets to 1 while it is the active controller.
const ACTIVE_CONTROLLER: &str = "krabka_broker_active_controller";

/// How long one probe of one node may take. A paused broker accepts the
/// connection and never answers, so every probe is bounded.
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// One partition as a node's metadata reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PartitionState {
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) leader: i32,
    pub(crate) replicas: Vec<i32>,
    pub(crate) isr: Vec<i32>,
}

/// The partitions of a `Metadata` response.
pub(crate) fn partition_states(response: &MetadataResponse) -> Vec<PartitionState> {
    response
        .topics
        .iter()
        .filter_map(|topic| topic.name.as_ref().map(|name| (name, topic)))
        .flat_map(|(name, topic)| {
            topic.partitions.iter().map(|partition| PartitionState {
                topic: name.clone(),
                partition: partition.partition_index,
                leader: partition.leader_id,
                replicas: partition.replica_nodes.clone(),
                isr: partition.isr_nodes.clone(),
            })
        })
        .collect()
}

/// The leader of one partition, when it has one.
pub(crate) fn leader_of(states: &[PartitionState], topic: &str, partition: i32) -> Option<i32> {
    states
        .iter()
        .find(|state| state.topic == topic && state.partition == partition)
        .map(|state| state.leader)
        .filter(|leader| *leader >= 0)
}

/// The partitions `node` leads that can fail over: those with a replica on
/// another node. A single-replica partition, such as the broker's own audit
/// topic, has nowhere to move.
pub(crate) fn led_by(states: &[PartitionState], node: i32) -> BTreeSet<TopicPartition> {
    states
        .iter()
        .filter(|state| {
            state.leader == node && state.replicas.iter().any(|replica| *replica != node)
        })
        .map(|state| (state.topic.clone(), state.partition))
        .collect()
}

/// The partitions of `led` that have no live leader other than `target`, with
/// the leader each reports (`-1` for none, `None` when it is not reported).
pub(crate) fn not_failed_over(
    states: &[PartitionState],
    led: &BTreeSet<TopicPartition>,
    target: i32,
) -> Vec<(TopicPartition, Option<i32>)> {
    led.iter()
        .filter_map(|(topic, partition)| {
            let reported = states
                .iter()
                .find(|state| state.topic == *topic && state.partition == *partition)
                .map(|state| state.leader);
            let moved = reported.is_some_and(|leader| leader >= 0 && leader != target);
            (!moved).then(|| ((topic.clone(), *partition), reported))
        })
        .collect()
}

/// Whether every partition of every topic in `topics` (name and partition
/// count) is reported with its full replica set in its ISR.
pub(crate) fn isr_complete(states: &[PartitionState], topics: &[(String, i32)]) -> bool {
    topics.iter().all(|(topic, partitions)| {
        (0..*partitions).all(|partition| {
            states
                .iter()
                .find(|state| state.topic == *topic && state.partition == partition)
                .is_some_and(|state| {
                    !state.replicas.is_empty()
                        && state
                            .replicas
                            .iter()
                            .all(|replica| state.isr.contains(replica))
                })
        })
    })
}

/// The two roles a fault target is named by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct Roles {
    pub(crate) controller: Option<i32>,
    /// The leader of the first path's partition 0.
    pub(crate) leader: Option<i32>,
}

/// The node `target` names among `nodes` (index order), given the roles.
pub(crate) fn resolve_target(target: Target, nodes: &[i32], roles: Roles) -> Result<i32, String> {
    match target {
        Target::Controller => roles
            .controller
            .ok_or_else(|| "no node reports itself the active controller".to_owned()),
        Target::Leader => roles
            .leader
            .ok_or_else(|| "the first path's partition 0 has no leader".to_owned()),
        Target::Follower => nodes
            .iter()
            .copied()
            .find(|node| Some(*node) != roles.controller && Some(*node) != roles.leader)
            .ok_or_else(|| format!("every node is the controller or the leader: {roles:?}")),
        Target::Broker(index) => usize::try_from(index)
            .ok()
            .and_then(|index| nodes.get(index).copied())
            .ok_or_else(|| format!("no broker at index {index}")),
        Target::Any => Err("`any` is resolved when the schedule expands, never here".to_owned()),
    }
}

/// What `node` was when a fault landed on it: `controller` when it was the
/// active controller, whatever else it was, then `leader` when it led the
/// first path's partition 0, else `follower`; `broker` when no node reported
/// itself the controller, so the roles were not known.
pub(crate) fn role_of(node: i32, roles: Roles) -> &'static str {
    if roles.controller.is_none() {
        "broker"
    } else if roles.controller == Some(node) {
        "controller"
    } else if roles.leader == Some(node) {
        "leader"
    } else {
        "follower"
    }
}

/// The node whose `/metrics` body reports it the active controller, among
/// `scrapes` of `(node, body)`.
pub(crate) fn active_controller(scrapes: &[(i32, String)]) -> Option<i32> {
    scrapes
        .iter()
        .find(|(_, body)| probe::sum_metric(body, ACTIVE_CONTROLLER) >= 1.0)
        .map(|(node, _)| *node)
}

/// One broker as the executor addresses it.
#[derive(Debug, Clone)]
pub(crate) struct Node {
    pub(crate) id: i32,
    pub(crate) container: String,
    pub(crate) bootstrap: String,
    pub(crate) metrics_url: String,
    /// The host side of the node's `/var/lib/krabka` mount.
    pub(crate) root: PathBuf,
}

/// The cluster's nodes, and a client pinned to each.
pub(crate) struct Nodes {
    members: Vec<Node>,
    http: reqwest::Client,
    /// A client per node that never refreshes metadata, so every request it
    /// sends goes to that node's own bootstrap address.
    clients: Mutex<BTreeMap<i32, Client>>,
}

impl Nodes {
    pub(crate) fn new(members: Vec<Node>) -> Self {
        Self {
            members,
            http: reqwest::Client::builder()
                .timeout(PROBE_TIMEOUT)
                .build()
                .expect("build an HTTP client"),
            clients: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn ids(&self) -> Vec<i32> { self.members.iter().map(|node| node.id).collect() }

    pub(crate) fn node(&self, id: i32) -> Option<&Node> {
        self.members.iter().find(|node| node.id == id)
    }

    async fn client(&self, id: i32) -> Result<Client, String> {
        if let Some(client) = self
            .clients
            .lock()
            .ok()
            .and_then(|clients| clients.get(&id).cloned())
        {
            return Ok(client);
        }
        let node = self.node(id).ok_or_else(|| format!("no node {id}"))?;
        let client = bounded(
            Client::builder()
                .bootstrap(node.bootstrap.clone())
                .client_id("soak-contract-probe")
                .request_timeout(secs(3))
                .build(),
        )
        .await?
        .map_err(|error| error.to_string())?;
        if let Ok(mut clients) = self.clients.lock() {
            clients.insert(id, client.clone());
        }
        Ok(client)
    }

    /// Send `request` to node `id` itself, bounded by the probe timeout. A
    /// failure drops the cached client, so the next request reconnects.
    pub(crate) async fn send<R>(&self, id: i32, request: R) -> Result<R::Response, String>
    where
        R: ProtocolRequest,
    {
        let client = self.client(id).await?;
        let outcome = bounded(client.send(request))
            .await
            .and_then(|sent| sent.map_err(|error| error.to_string()));
        if outcome.is_err()
            && let Ok(mut clients) = self.clients.lock()
        {
            clients.remove(&id);
        }
        outcome
    }

    /// Node `id`'s metadata for every topic.
    pub(crate) async fn metadata(&self, id: i32) -> Result<Vec<PartitionState>, String> {
        self.send(id, MetadataRequest {
            topics: None,
            ..Default::default()
        })
        .await
        .map(|response| partition_states(&response))
    }

    /// Metadata from the first node that answers, asking every node other
    /// than `avoid` before `avoid` itself.
    pub(crate) async fn metadata_from_any(
        &self,
        avoid: Option<i32>,
    ) -> Option<(i32, Vec<PartitionState>)> {
        let mut order = self.ids();
        order.sort_by_key(|id| Some(*id) == avoid);
        for id in order {
            if let Ok(states) = self.metadata(id).await {
                return Some((id, states));
            }
        }
        None
    }

    /// Node `id`'s `/metrics` body, when it answers.
    pub(crate) async fn scrape(&self, id: i32) -> Option<String> {
        let node = self.node(id)?;
        let response = self.http.get(&node.metrics_url).send().await.ok()?;
        response.text().await.ok()
    }

    /// The active controller, as the nodes other than `avoid` report it.
    pub(crate) async fn controller(&self, avoid: Option<i32>) -> Option<i32> {
        let mut scrapes = Vec::new();
        for id in self.ids().into_iter().filter(|id| Some(*id) != avoid) {
            if let Some(body) = self.scrape(id).await {
                scrapes.push((id, body));
            }
        }
        active_controller(&scrapes)
    }

    /// The roles a target is resolved against, with the first path's topic.
    pub(crate) async fn roles(&self, first_topic: &str) -> Roles {
        let controller = self.controller(None).await;
        let leader = self
            .metadata_from_any(None)
            .await
            .and_then(|(_, states)| leader_of(&states, first_topic, 0));
        Roles { controller, leader }
    }
}

/// `future`, or an error once the probe timeout passes.
async fn bounded<T>(future: impl Future<Output = T>) -> Result<T, String> {
    tokio::time::timeout(PROBE_TIMEOUT, future)
        .await
        .map_err(|_| format!("no answer within {PROBE_TIMEOUT:?}"))
}

#[cfg(test)]
mod tests {
    use {
        super::{
            PartitionState, Roles, active_controller, isr_complete, led_by, not_failed_over,
            resolve_target, role_of,
        },
        assert2::check,
        krabka_bench_driver::soak::contract::Target,
        std::collections::BTreeSet,
    };

    fn state(topic: &str, partition: i32, leader: i32, isr: &[i32]) -> PartitionState {
        PartitionState {
            topic: topic.to_owned(),
            partition,
            leader,
            replicas: vec![1, 2, 3],
            isr: isr.to_vec(),
        }
    }

    fn cluster() -> Vec<PartitionState> {
        vec![
            state("a", 0, 1, &[1, 2, 3]),
            state("a", 1, 2, &[2, 3, 1]),
            state("b", 0, 1, &[1, 3]),
            PartitionState {
                topic: "audit".to_owned(),
                partition: 0,
                leader: 1,
                replicas: vec![1],
                isr: vec![1],
            },
        ]
    }

    const NODES: [i32; 3] = [1, 2, 3];

    #[yare::parameterized(
        controller = { Target::Controller, Roles { controller: Some(3), leader: Some(1) }, Ok(3) },
        leader = { Target::Leader, Roles { controller: Some(3), leader: Some(1) }, Ok(1) },
        follower = { Target::Follower, Roles { controller: Some(3), leader: Some(1) }, Ok(2) },
        follower_when_one_node_is_both = { Target::Follower, Roles { controller: Some(1), leader: Some(1) }, Ok(2) },
        broker = { Target::Broker(2), Roles::default(), Ok(3) },
        broker_out_of_range = { Target::Broker(3), Roles::default(), Err(()) },
        no_controller = { Target::Controller, Roles::default(), Err(()) },
        unresolved_any = { Target::Any, Roles::default(), Err(()) },
    )]
    fn a_target_resolves_against_the_cluster_as_it_is(
        target: Target,
        roles: Roles,
        expected: Result<i32, ()>,
    ) {
        check!(resolve_target(target, &NODES, roles).map_err(|_| ()) == expected);
    }

    #[yare::parameterized(
        controller_wins_over_leader = { 1, Roles { controller: Some(1), leader: Some(1) }, "controller" },
        leader = { 2, Roles { controller: Some(1), leader: Some(2) }, "leader" },
        follower = { 3, Roles { controller: Some(1), leader: Some(2) }, "follower" },
        unknown = { 3, Roles { controller: None, leader: Some(2) }, "broker" },
    )]
    fn a_target_records_the_one_role_it_held(node: i32, roles: Roles, role: &str) {
        check!(role_of(node, roles) == role);
    }

    #[test]
    fn a_node_leads_the_partitions_it_is_leader_of_that_can_move() {
        let led: BTreeSet<_> = [("a".to_owned(), 0), ("b".to_owned(), 0)].into();
        check!(led_by(&cluster(), 1) == led);
        check!(led_by(&cluster(), 3).is_empty());
    }

    #[yare::parameterized(
        still_led_by_target = { &[(0, 1), (1, 2)], false },
        moved = { &[(0, 2), (1, 3)], true },
        leaderless = { &[(0, -1), (1, 3)], false },
    )]
    fn failover_needs_a_live_leader_elsewhere_for_every_partition(
        leaders: &[(i32, i32)],
        expected: bool,
    ) {
        let states: Vec<_> = leaders
            .iter()
            .map(|(partition, leader)| state("a", *partition, *leader, &[2, 3]))
            .collect();
        let led: BTreeSet<_> = [("a".to_owned(), 0), ("a".to_owned(), 1)].into();
        check!(not_failed_over(&states, &led, 1).is_empty() == expected);
    }

    #[test]
    fn nothing_led_has_nothing_to_fail_over() {
        check!(not_failed_over(&cluster(), &BTreeSet::new(), 1).is_empty());
    }

    #[yare::parameterized(
        complete = { &[("a", 2)], true },
        shrunk = { &[("a", 2), ("b", 1)], false },
        missing_partition = { &[("a", 3)], false },
        unknown_topic = { &[("c", 1)], false },
    )]
    fn the_isr_is_complete_only_when_every_partition_holds_every_replica(
        topics: &[(&str, i32)],
        expected: bool,
    ) {
        let topics: Vec<_> = topics
            .iter()
            .map(|(name, n)| ((*name).to_owned(), *n))
            .collect();
        check!(isr_complete(&cluster(), &topics) == expected);
    }

    #[test]
    fn the_controller_is_the_node_whose_gauge_reads_one() {
        let body = |value: u8| {
            format!(
                "# TYPE krabka_broker_active_controller gauge\nkrabka_broker_active_controller {value}\n"
            )
        };
        check!(active_controller(&[(1, body(0)), (2, body(1)), (3, body(0))]) == Some(2));
        check!(active_controller(&[(1, body(0))]) == None);
        check!(active_controller(&[]) == None);
    }
}
