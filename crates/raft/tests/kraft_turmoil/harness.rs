//! Real controllers and logs with Turmoil-owned hosts, sockets, and Tokio time. The registry is a test observation channel; peer messages only cross simulated TCP.

use {
    super::transport::{self, PORT, SimulatedPeers, hostname},
    krabka_metadata::MetadataRecord,
    krabka_raft::{
        MetadataLogConfig, RaftError, SubmitChangeResult,
        kraft::{KraftController, KraftOpenConfig, NodeId, QuorumStateSnapshot, QuorumTimeouts},
    },
    krabka_units::prelude::millis,
    std::{
        cell::{Ref, RefCell},
        collections::BTreeMap,
        future::Future,
        rc::Rc,
        sync::{Arc, Mutex},
        time::Duration,
    },
    tokio::time::{self, error::Elapsed},
    turmoil::{Builder, Sim, net::TcpListener},
    uuid::Uuid,
};

pub const VOTERS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
pub type Nodes = Arc<Mutex<BTreeMap<NodeId, KraftController>>>;
pub type Leader = (NodeId, u32);
pub type SubmissionResult = Result<SubmitChangeResult, RaftError>;
pub type TimedSubmissionResult = Result<SubmissionResult, Elapsed>;

#[derive(Clone)]
pub struct Submission {
    response: Rc<RefCell<Option<SubmissionResult>>>,
}

impl Submission {
    pub fn response(&self) -> Ref<'_, Option<SubmissionResult>> { self.response.borrow() }
}

krabka_macros::empty_endpoint_voters!(voter_set);
krabka_macros::topic_record_fixture!(single_partition_topic);

pub fn topic_record(name: &str, id: u128) -> MetadataRecord {
    MetadataRecord::V1Topic(single_partition_topic(name, Uuid::from_u128(id)))
}

pub fn controller(nodes: &Nodes, node: NodeId) -> KraftController {
    nodes
        .lock()
        .unwrap()
        .get(&node)
        .expect("voter is running")
        .clone()
}

pub struct Cluster {
    pub sim: Sim<'static>,
    pub nodes: Nodes,
    seed: u64,
    clients: usize,
    submissions: usize,
}

impl Cluster {
    pub fn new(seed: u64) -> Self {
        let mut sim = Builder::new()
            .rng_seed(seed)
            .enable_random_order()
            .simulation_duration(Duration::from_secs(60))
            .min_message_latency(Duration::from_millis(1))
            .max_message_latency(Duration::from_millis(10))
            .build();
        let nodes = Nodes::default();
        for (index, node) in VOTERS.into_iter().enumerate() {
            let nodes = Arc::clone(&nodes);
            // The host factory retains the same directory across crash/bounce.
            let directory = Arc::new(tempfile::tempdir().expect("voter log directory"));
            sim.host(hostname(node), move || {
                let nodes = Arc::clone(&nodes);
                let directory = Arc::clone(&directory);
                async move {
                    let listener = TcpListener::bind(("0.0.0.0", PORT)).await?;
                    let controller = KraftController::open(
                        directory.path().to_path_buf(),
                        KraftOpenConfig::builder()
                            .me(node)
                            .cluster_id(Uuid::from_u128(100))
                            .directory_id(Uuid::nil())
                            .bootstrap_voters(voter_set(&VOTERS))
                            .timeouts(QuorumTimeouts {
                                election: millis([150, 300, 450][index]),
                                fetch: millis(500),
                            })
                            .peers(Arc::new(SimulatedPeers))
                            .metadata_log(MetadataLogConfig {
                                max_idle_interval: millis(0),
                                ..MetadataLogConfig::default()
                            })
                            .build(),
                        |_| Ok(()),
                    )?;
                    nodes.lock().unwrap().insert(node, controller.clone());
                    transport::serve(listener, controller).await
                }
            });
        }
        Self {
            sim,
            nodes,
            seed,
            clients: 0,
            submissions: 0,
        }
    }

    pub fn run<T, F, Fut>(&mut self, operation: F) -> T
    where
        T: 'static,
        F: FnOnce(Nodes) -> Fut + 'static,
        Fut: Future<Output = T> + 'static,
    {
        let result = Rc::new(RefCell::new(None));
        let output = Rc::clone(&result);
        let nodes = Arc::clone(&self.nodes);
        self.clients += 1;
        self.sim
            .client(format!("driver-{}", self.clients), async move {
                *output.borrow_mut() = Some(operation(nodes).await);
                Ok(())
            });
        self.sim
            .run()
            .unwrap_or_else(|error| panic!("Turmoil seed {}: {error}", self.seed));
        result.take().expect("simulation client completed")
    }

    pub fn partition(&self, node: NodeId) {
        for peer in VOTERS.into_iter().filter(|peer| *peer != node) {
            self.sim.partition(hostname(node), hostname(peer));
        }
    }

    pub fn repair(&self, node: NodeId) {
        for peer in VOTERS.into_iter().filter(|peer| *peer != node) {
            self.sim.repair(hostname(node), hostname(peer));
        }
    }

    pub fn crash(&mut self, node: NodeId) {
        self.sim.crash(hostname(node));
        self.nodes.lock().unwrap().remove(&node);
    }

    pub fn start_submission(&mut self, node: NodeId, record: MetadataRecord) -> Submission {
        let response = Rc::new(RefCell::new(None));
        let output = Rc::clone(&response);
        let nodes = Arc::clone(&self.nodes);
        self.submissions += 1;
        // A completed Turmoil client stops ticking. A host keeps this request alive across observation phases, including network repair.
        self.sim
            .host(format!("submission-{}", self.submissions), move || {
                let output = Rc::clone(&output);
                let nodes = Arc::clone(&nodes);
                let record = record.clone();
                async move {
                    let response = controller(&nodes, node).submit_change(vec![record]).await;
                    *output.borrow_mut() = Some(response);
                    Ok(())
                }
            });
        Submission { response }
    }

    pub fn await_submission(&mut self, submission: &Submission) -> Result<(), Elapsed> {
        let submission = submission.clone();
        self.run(move |_| async move {
            time::timeout(Duration::from_secs(10), async {
                while submission.response().is_none() {
                    time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
        })
    }
}

pub fn snapshots(nodes: &Nodes, voters: &[NodeId]) -> Vec<QuorumStateSnapshot> {
    let nodes = nodes.lock().unwrap();
    voters
        .iter()
        .filter_map(|node| nodes.get(node).map(KraftController::quorum_snapshot))
        .collect()
}

pub async fn await_leader(nodes: &Nodes, voters: &[NodeId]) -> Result<Leader, Elapsed> {
    time::timeout(Duration::from_secs(10), async {
        loop {
            let states = snapshots(nodes, voters);
            if states.len() == voters.len()
                && let Some(first) = states.first()
                && let Some(leader) = first.leader_id
                && voters.contains(&leader)
                && states.iter().all(|state| {
                    state.leader_id == Some(leader) && state.leader_epoch == first.leader_epoch
                })
                && states.iter().filter(|state| state.is_leader).count() == 1
            {
                return (leader, first.leader_epoch);
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}

pub async fn submit(nodes: &Nodes, node: NodeId, name: &str, id: u128) -> TimedSubmissionResult {
    time::timeout(
        Duration::from_secs(10),
        controller(nodes, node).submit_change(vec![topic_record(name, id)]),
    )
    .await
}

pub async fn await_topic(nodes: &Nodes, voters: &[NodeId], name: &str) -> Result<(), Elapsed> {
    time::timeout(Duration::from_secs(10), async {
        loop {
            if voters.iter().all(|node| {
                nodes
                    .lock()
                    .unwrap()
                    .get(node)
                    .is_some_and(|controller| controller.current_image().topic(name).is_some())
            }) {
                return;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
}
