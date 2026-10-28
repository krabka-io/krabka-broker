//! An approved proposal outlives the controller that recorded it.
//!
//! The approver set is a per-node file value, so the design leans on the
//! proposal living in the metadata log for any node that becomes controller to
//! spend it. The case takes the controller away and asks the survivor.
//!
//! A `PLAINTEXT` listener has one principal, so the two approvals are written
//! straight into the log here. The wire path that produces them is proved on
//! the SASL broker elsewhere in the suite.

use {
    crate::{
        cluster::{index_of, plain_client, start_gated_cluster, wait_for_new_leader},
        principals::principal,
        proposals::{ACTION_DELETE_TOPIC, now_ms, open, stored},
        support::{self, shutdown_cluster},
        topics::{create_topic, delete_topic_as_admin_client},
    },
    assert2::check,
    krabka_broker::{BrokerConfig, BrokerHandle, NodeId, config::BackgroundUncleanRecovery},
    krabka_client_core::Client,
    krabka_metadata::{
        BreakGlassApproval as StoredApproval, BreakGlassProposalRecord, MetadataRecord,
    },
    krabka_protocol::{krabka::break_glass::DescribedBreakGlassProposal, primitives::uuid::Uuid},
    rstest::fixture,
    rstest_bdd_macros::{given, scenario, then, when},
    tempfile::TempDir,
};

#[derive(Default)]
struct DurableApproval {
    cluster: Vec<(BrokerHandle, BrokerConfig, TempDir)>,
    stopped_directory: Option<TempDir>,
    client: Option<Client>,
    after_client: Option<Client>,
    old_leader: Option<NodeId>,
    new_leader: Option<NodeId>,
    proposal: Option<Uuid>,
    before: Option<DescribedBreakGlassProposal>,
    after: Option<DescribedBreakGlassProposal>,
    deletion: Option<i16>,
    topic: String,
}

#[fixture]
fn durable() -> DurableApproval {
    // Start this scenario with fresh inputs and observations.

    DurableApproval {
        cluster: Vec::new(),
        stopped_directory: None,
        client: None,
        after_client: None,
        old_leader: None,
        new_leader: None,
        proposal: None,
        before: None,
        after: None,
        deletion: None,
        topic: String::new(),
    }
}

#[given("a three-controller cluster holds approvals by {users} for deleting RF3 topic {topic}")]
async fn replicated_approval(durable: &mut DurableApproval, users: String, topic: String) {
    let cluster = start_gated_cluster(3, BackgroundUncleanRecovery::Off).await;
    support::wait_for_all_brokers_registered(&cluster, 3).await;
    let leader = cluster[0].0.wait_until_controller_leader().await.unwrap();
    let leader_index = index_of(&cluster, leader);
    let follower_index = (0..cluster.len())
        .find(|index| *index != leader_index)
        .expect("a follower");
    let client = plain_client(&cluster[follower_index].1.listen_addr.to_string()).await;
    create_topic(&client, &topic, 3).await;
    let id = open(&client, ACTION_DELETE_TOPIC, &topic).await;
    let wanted = uuid::Uuid::from_bytes(id.0);
    cluster[leader_index]
        .0
        .wait_for_image(|image| image.break_glass_proposal(wanted).is_some())
        .await;
    let held = cluster[leader_index]
        .0
        .controller_image_for_test()
        .break_glass_proposal(wanted)
        .expect("proposal in image")
        .clone();
    let approvals: Vec<StoredApproval> = users.split(',').map(approval_by).collect();
    let count = approvals.len();
    cluster[leader_index]
        .0
        .submit_metadata_record_for_test(MetadataRecord::V1BreakGlassProposal(
            BreakGlassProposalRecord { approvals, ..held },
        ))
        .await
        .expect("approvals commit");
    for (handle, _, _) in &cluster {
        handle
            .wait_for_image(|image| {
                image
                    .break_glass_proposal(wanted)
                    .is_some_and(|proposal| proposal.approvals.len() == count)
            })
            .await;
    }
    durable.before = Some(stored(&client, id).await);
    durable.cluster = cluster;
    durable.client = Some(client);
    durable.old_leader = Some(leader);
    durable.proposal = Some(id);
    durable.topic = topic;
}

#[when("the controller that recorded the approvals shuts down and a survivor takes leadership")]
async fn replace_controller(durable: &mut DurableApproval) {
    let old = durable.old_leader.unwrap();
    let index = index_of(&durable.cluster, old);
    let (broker, _config, directory) = durable.cluster.remove(index);
    broker.shutdown().await;
    durable.stopped_directory = Some(directory);
    let elected = wait_for_new_leader(&durable.cluster[0].0, old).await;
    let index = index_of(&durable.cluster, elected);
    let client = plain_client(&durable.cluster[index].1.listen_addr.to_string()).await;
    durable.after = Some(stored(&client, durable.proposal.unwrap()).await);
    durable.after_client = Some(client);
    durable.new_leader = Some(elected);
}

#[then("a different controller holds the complete unchanged proposal and both approvals")]
fn unchanged(durable: &DurableApproval) {
    check!(durable.new_leader.unwrap() != durable.old_leader.unwrap());
    check!(durable.after == durable.before);
}

#[when("the admin client retries deletion of the approved topic against the new controller")]
async fn consume(durable: &mut DurableApproval) {
    durable.deletion = Some(
        delete_topic_as_admin_client(durable.after_client.as_ref().unwrap(), &durable.topic).await,
    );
}

#[then(
    "deletion returns error {error} and the new controller records a nonzero consumption timestamp"
)]
async fn consumed(durable: &DurableApproval, error: i16) {
    check!(durable.deletion == Some(error));
    let wanted = uuid::Uuid::from_bytes(durable.proposal.unwrap().0);
    durable.cluster[index_of(&durable.cluster, durable.new_leader.unwrap())]
        .0
        .wait_for_image(|image| {
            image
                .break_glass_proposal(wanted)
                .is_some_and(|proposal| proposal.consumed_at_ms != 0)
        })
        .await;
    check!(
        stored(
            durable.after_client.as_ref().unwrap(),
            durable.proposal.unwrap()
        )
        .await
        .consumed_at_ms
            != 0
    );
}

#[scenario(
    path = "tests/features/migrated/tests/break_glass/failover.feature",
    name = "an approved proposal survives a controller failover"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_approved_proposal_survives_a_controller_failover(durable: DurableApproval) {
    drop(durable.client);
    drop(durable.after_client);
    shutdown_cluster(durable.cluster).await;
}

/// One unsigned approval by `user`, in the metadata form.
fn approval_by(user: &str) -> StoredApproval {
    StoredApproval {
        principal: principal(user),
        approved_at_ms: now_ms(),
        key_id: String::new(),
        signature: Vec::new(),
    }
}
