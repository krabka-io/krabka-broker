//! Seeded hardship tests for real async `KRaft` engines and on-disk logs. Every step stays synchronous so Turmoil drives its own runtimes outside the BDD runner's `block_on`.
#![cfg(test)]

use {
    self::harness::{
        Cluster, Leader, Submission, TimedSubmissionResult, VOTERS, await_leader, await_topic,
        controller, submit, topic_record,
    },
    krabka_bdd::{Given, ScenarioContext, TableRow, given, table, then, when},
    krabka_raft::{RaftError, SubmitChangeResult, kraft::NodeId},
    rstest::fixture,
    std::{collections::BTreeMap, time::Duration},
    tokio::time::{self, error::Elapsed},
    uuid::Uuid,
};

#[path = "kraft_turmoil/harness.rs"]
mod harness;
#[path = "kraft_turmoil/transport.rs"]
mod transport;

#[derive(ScenarioContext)]
struct QuorumScenario {
    #[introduced_by("Given three KRaft voters have elected a leader under Turmoil seed {seed}")]
    cluster: Given<Cluster>,
    #[introduced_by("Given three KRaft voters have elected a leader under Turmoil seed {seed}")]
    original: Given<Leader>,
    #[introduced_by("Given the majority has replaced the isolated leader")]
    replacement: Given<Result<Leader, Elapsed>>,
    #[introduced_by("When a leader receives a topic")]
    submission: Given<TimedSubmissionResult>,
    #[introduced_by("When a leader receives a topic")]
    propagation: Given<Result<(), Elapsed>>,
    #[introduced_by("When the isolated leader receives a topic")]
    isolated_submission: Given<Submission>,
    #[introduced_by("When the isolated leader receives a topic")]
    isolated_topic: Given<TopicIdentity>,
    #[introduced_by("When the minority resumes replication")]
    isolated_completion: Given<Result<(), Elapsed>>,
    #[introduced_by("When the crashed host restarts from its existing log directory")]
    restarted: Given<Result<(), Elapsed>>,
    #[introduced_by("When the minority resumes replication")]
    rejoined: Given<Result<Leader, Elapsed>>,
}

#[derive(Debug, Clone, PartialEq, Eq, TableRow)]
struct TopicIdentity {
    topic: String,
    identity: u128,
}

#[derive(Debug, PartialEq, Eq)]
struct VoterObservation {
    node: NodeId,
    leader: Option<NodeId>,
    epoch: u32,
    is_leader: bool,
    topics: BTreeMap<String, u128>,
    fault: Option<String>,
}

impl QuorumScenario {
    fn cluster(&self) -> &Cluster { self.cluster.get() }

    fn cluster_mut(&mut self) -> &mut Cluster { self.cluster.get_mut() }

    fn original(&self) -> Leader { *self.original.get() }

    fn replacement(&self) -> Leader {
        *self
            .replacement
            .get()
            .as_ref()
            .expect("majority elected a replacement")
    }

    fn seed_topic(&mut self, name: String, id: u128) {
        let (leader, _) = self.original();
        self.cluster_mut().run(move |nodes| async move {
            submit(&nodes, leader, &name, id)
                .await
                .expect("setup submission completed in simulated time")
                .expect("setup topic was committed");
            await_topic(&nodes, &VOTERS, &name)
                .await
                .expect("setup topic reached every voter");
        });
    }

    fn submit_and_observe(&mut self, leader: NodeId, voters: Vec<NodeId>, name: String, id: u128) {
        let (submission, propagation) = self.cluster_mut().run(move |nodes| async move {
            let submission = submit(&nodes, leader, &name, id).await;
            let propagation = await_topic(&nodes, &voters, &name).await;
            (submission, propagation)
        });
        self.submission.set(submission);
        self.propagation.set(propagation);
    }

    fn submit_isolated(&mut self, name: String, id: u128) {
        let leader = self.original().0;
        let submission = self
            .cluster_mut()
            .start_submission(leader, topic_record(&name, id));
        self.isolated_submission.set(submission);
        self.isolated_topic.set(TopicIdentity {
            topic: name,
            identity: id,
        });
        // Observe two seconds without cancelling the submitted request. Its host remains active through later repair.
        self.cluster_mut().run(|_| async {
            time::sleep(Duration::from_secs(2)).await;
        });
    }

    fn elect_replacement(&mut self) {
        let original = self.original().0;
        let replacement = self.cluster_mut().run(move |nodes| async move {
            let survivors: Vec<_> = VOTERS
                .into_iter()
                .filter(|node| *node != original)
                .collect();
            await_leader(&nodes, &survivors).await
        });
        self.replacement.set(replacement);
    }

    fn seed_minority_write(&mut self, name: String, id: u128) {
        self.seed_topic("orders".into(), 1);
        self.cluster().partition(self.original().0);
        self.submit_isolated(name, id);
        self.elect_replacement();
        self.replacement();
    }

    fn seed_majority_topic(&mut self, name: String, id: u128) {
        let original = self.original().0;
        let survivors = VOTERS
            .into_iter()
            .filter(|node| *node != original)
            .collect();
        self.submit_and_observe(self.replacement().0, survivors, name, id);
        self.submission
            .get()
            .as_ref()
            .expect("setup submission completed")
            .as_ref()
            .expect("replacement committed the setup topic");
        self.propagation
            .get()
            .as_ref()
            .expect("setup topic reached the majority");
    }

    fn seed_crashed_leader(&mut self) {
        self.seed_topic("orders".into(), 1);
        let original = self.original().0;
        self.cluster_mut().crash(original);
        self.elect_replacement();
        self.seed_majority_topic("payments".into(), 2);
        self.cluster().partition(original);
    }

    fn restart(&mut self) {
        let leader = self.original().0;
        self.cluster_mut().sim.bounce(transport::hostname(leader));
        let restarted = self.cluster_mut().run(move |nodes| async move {
            time::timeout(Duration::from_secs(10), async {
                while !nodes.lock().unwrap().contains_key(&leader) {
                    time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
        });
        self.restarted.set(restarted);
    }

    fn rejoin(&mut self, name: String) {
        self.cluster().repair(self.original().0);
        let rejoined = self.cluster_mut().run(move |nodes| async move {
            await_topic(&nodes, &VOTERS, &name).await?;
            await_leader(&nodes, &VOTERS).await
        });
        self.rejoined.set(rejoined);
        if self.isolated_submission.is_set() {
            let submission = self.isolated_submission.get().clone();
            let completed = self.cluster_mut().await_submission(&submission);
            self.isolated_completion.set(completed);
        }
    }
}

#[fixture]
fn quorum() -> QuorumScenario {
    QuorumScenario {
        ..QuorumScenario::default()
    }
}

#[given("three KRaft voters have elected a leader under Turmoil seed {seed}")]
fn elected_quorum(quorum: &mut QuorumScenario, seed: u64) {
    let mut cluster = Cluster::new(seed);
    let leader = cluster.run(|nodes| async move {
        await_leader(&nodes, &VOTERS)
            .await
            .expect("initial quorum elected a leader")
    });
    quorum.original.set(leader);
    quorum.cluster.set(cluster);
}

#[given("topic {name} with identity {id} is committed on every voter")]
fn committed_topic(quorum: &mut QuorumScenario, name: String, id: u128) {
    quorum.seed_topic(name, id);
}

#[given("the original leader host is disconnected from both followers")]
fn isolated_leader(quorum: &QuorumScenario) { quorum.cluster().partition(quorum.original().0); }

#[given(
    "the majority has replaced the leader that received isolated topic {name} with identity {id}"
)]
fn minority_context(quorum: &mut QuorumScenario, name: String, id: u128) {
    quorum.seed_minority_write(name, id);
}

#[given(
    "the majority has committed topic {name} with identity {id} beyond an isolated minority write"
)]
fn repaired_context(quorum: &mut QuorumScenario, name: String, id: u128) {
    quorum.seed_minority_write("isolated".into(), 99);
    quorum.seed_majority_topic(name, id);
}

#[given("a crashed leader is isolated from its replacement quorum")]
fn crashed_context(quorum: &mut QuorumScenario) { quorum.seed_crashed_leader(); }

#[given("the original leader has restarted without peer access")]
fn restarted_context(quorum: &mut QuorumScenario) {
    quorum.seed_crashed_leader();
    quorum.restart();
    quorum
        .restarted
        .get()
        .as_ref()
        .expect("setup host restarted");
}

#[when("a follower receives topic {name} with identity {id}")]
fn follower_submission(quorum: &mut QuorumScenario, name: String, id: u128) {
    let (leader, _) = quorum.original();
    let submission = quorum.cluster_mut().run(move |nodes| async move {
        let follower = VOTERS
            .into_iter()
            .find(|node| *node != leader)
            .expect("a follower");
        submit(&nodes, follower, &name, id).await
    });
    quorum.submission.set(submission);
}

#[when("the elected leader receives topic {name} with identity {id}")]
fn leader_submission(quorum: &mut QuorumScenario, name: String, id: u128) {
    quorum.submit_and_observe(quorum.original().0, VOTERS.to_vec(), name, id);
}

#[when("the isolated leader receives topic {name} with identity {id}")]
fn isolated_submission(quorum: &mut QuorumScenario, name: String, id: u128) {
    quorum.submit_isolated(name, id);
}

#[when("the replacement leader receives topic {name} with identity {id}")]
fn replacement_submission(quorum: &mut QuorumScenario, name: String, id: u128) {
    let original = quorum.original().0;
    let survivors = VOTERS
        .into_iter()
        .filter(|node| *node != original)
        .collect();
    quorum.submit_and_observe(quorum.replacement().0, survivors, name, id);
}

#[when("the minority resumes replication of topic {name}")]
fn reconnect(quorum: &mut QuorumScenario, name: String) { quorum.rejoin(name); }

#[when("the crashed host restarts from its existing log directory")]
fn restart_leader(quorum: &mut QuorumScenario) { quorum.restart(); }

#[then("the follower refuses the submission with the elected leader hint")]
fn follower_hint(quorum: &QuorumScenario) {
    assert2::assert!(let Ok(Err(RaftError::NotLeader { current_leader })) = quorum.submission.get());
    assert2::assert!(*current_leader == Some(quorum.original().0));
}

#[then("the leader acknowledges the submission")]
fn acknowledged_submission(quorum: &QuorumScenario) {
    assert2::assert!(let Ok(Ok(result)) = quorum.submission.get());
    assert2::assert!(*result == SubmitChangeResult::default());
    assert2::assert!(let Ok(()) = quorum.propagation.get());
}

#[then("every voter contains topic {name} with identity {id}")]
fn topic_applied(quorum: &QuorumScenario, name: String, id: u128) {
    let actual: Vec<_> = VOTERS
        .into_iter()
        .map(|node| {
            let controller = controller(&quorum.cluster().nodes, node);
            let identity = controller
                .current_image()
                .topic(&name)
                .map(|topic| topic.topic_id);
            let fault = controller.watch_fault().borrow().clone();
            (node, identity, fault)
        })
        .collect();
    let expected: Vec<_> = VOTERS
        .into_iter()
        .map(|node| (node, Some(Uuid::from_u128(id)), None::<String>))
        .collect();
    assert2::assert!(actual == expected);
}

#[then("no voter exposes topic {name}")]
fn topic_absent(quorum: &QuorumScenario, name: String) {
    let visible: Vec<_> = VOTERS
        .into_iter()
        .filter(|node| {
            controller(&quorum.cluster().nodes, *node)
                .current_image()
                .topic(&name)
                .is_some()
        })
        .collect();
    assert2::assert!(visible.is_empty());
}

#[then("the isolated write remains uncommitted")]
fn minority_uncommitted(quorum: &QuorumScenario) {
    let response = quorum.isolated_submission.get().response();
    let allowed = matches!(
        response.as_ref(),
        None | Some(Err(RaftError::NotLeader { .. }))
    );
    let controller = controller(&quorum.cluster().nodes, quorum.original().0);
    let visible = controller
        .current_image()
        .topic(&quorum.isolated_topic.get().topic)
        .map(|topic| topic.topic_id);
    let fault = controller.watch_fault().borrow().clone();
    assert2::assert!(
        (allowed, visible, fault) == (true, None, None),
        "isolated response: {response:?}"
    );
}

#[then("the isolated voter retains only this committed metadata")]
fn minority_metadata(quorum: &QuorumScenario, datatable: Vec<Vec<String>>) {
    let expected = expected_topics(&datatable);
    let observed = observe_voter(quorum, quorum.original().0);
    let response = quorum.isolated_submission.get().response();
    let allowed = matches!(
        response.as_ref(),
        None | Some(Err(RaftError::NotLeader { .. }))
    );
    assert2::assert!(
        (allowed, observed.topics, observed.fault) == (true, expected, None),
        "isolated response: {response:?}"
    );
}

#[then("the original isolated submission is refused after reconciliation")]
fn minority_refused(quorum: &QuorumScenario) {
    assert2::assert!(let Ok(()) = quorum.isolated_completion.get());
    let response = quorum.isolated_submission.get().response();
    assert2::assert!(let Some(Err(RaftError::NotLeader { .. })) = response.as_ref());
}

#[then("the restarted voter recovers exactly this metadata before contacting peers")]
fn local_recovery(quorum: &QuorumScenario, datatable: Vec<Vec<String>>) {
    assert2::assert!(let Ok(()) = quorum.restarted.get());
    let observed = observe_voter(quorum, quorum.original().0);
    assert2::assert!((observed.topics, observed.fault) == (expected_topics(&datatable), None));
}

#[then("all voters agree on this recovered metadata")]
fn recovered_quorum(quorum: &QuorumScenario, datatable: Vec<Vec<String>>) {
    assert2::assert!(let Ok(actual_leader) = quorum.rejoined.get());
    let replacement = quorum.replacement();
    let topics = expected_topics(&datatable);
    let actual: Vec<_> = VOTERS
        .into_iter()
        .map(|node| observe_voter(quorum, node))
        .collect();
    let expected: Vec<_> = VOTERS
        .into_iter()
        .map(|node| VoterObservation {
            node,
            leader: Some(replacement.0),
            epoch: replacement.1,
            is_leader: node == replacement.0,
            topics: topics.clone(),
            fault: None,
        })
        .collect();
    let original = quorum.original();
    assert2::assert!(
        (
            *actual_leader,
            replacement.0 != original.0,
            replacement.1 > original.1,
            actual
        ) == (replacement, true, true, expected)
    );
}

fn expected_topics(datatable: &[Vec<String>]) -> BTreeMap<String, u128> {
    table::rows::<TopicIdentity>(datatable)
        .expect("valid expected topic identities")
        .into_iter()
        .map(|row| (row.topic, row.identity))
        .collect()
}

fn observe_voter(quorum: &QuorumScenario, node: NodeId) -> VoterObservation {
    let controller = controller(&quorum.cluster().nodes, node);
    let snapshot = controller.quorum_snapshot();
    let topics = controller
        .current_image()
        .topics()
        .map(|topic| (topic.name.clone(), topic.topic_id.as_u128()))
        .collect();
    let fault = controller.watch_fault().borrow().clone();
    VoterObservation {
        node,
        leader: snapshot.leader_id,
        epoch: snapshot.leader_epoch,
        is_leader: snapshot.is_leader,
        topics,
        fault,
    }
}

krabka_bdd::scenarios!("tests/features/kraft_turmoil", fixtures = [quorum: QuorumScenario]);
