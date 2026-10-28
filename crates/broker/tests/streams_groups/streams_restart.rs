//! A broker restart reloads a streams group from `__consumer_offsets`, and the
//! reloaded group goes on assigning its tasks.

use {
    crate::{
        streams_harness::{
            StreamsGroupScenario, active_partitions_for, connect, first_join, follow_up,
            streams_group, topology,
        },
        support::heartbeat_config,
    },
    assert2::assert,
    krabka_broker::{BootstrapMode, Broker},
    krabka_client_core::Client,
    krabka_protocol::owned::{
        common::streams_group_heartbeat_request::task_ids::TaskIds as ReqTaskIds,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
    },
    std::time::Duration,
    tokio::time,
};

const GROUP: &str = "streams-restart";
const TOPIC: &str = "restart-input";

/// A member as its client sees it: the epoch and the active partitions of
/// subtopology `0` that the coordinator last sent.
#[derive(Debug)]
struct Member {
    id: String,
    epoch: i32,
    active: Vec<i32>,
}

impl Member {
    /// Sends one heartbeat that reports the owned tasks, and takes the epoch
    /// and any new task list from the response.
    async fn heartbeat(&mut self, client: &Client) {
        let owned = if self.active.is_empty() {
            vec![]
        } else {
            vec![ReqTaskIds {
                subtopology_id: "0".into(),
                partitions: self.active.clone(),
                ..Default::default()
            }]
        };
        let resp = client
            .send(
                follow_up()
                    .group(GROUP)
                    .member_id(&self.id)
                    .epoch(self.epoch)
                    .maybe_active(Some(owned))
                    .call(),
            )
            .await
            .expect("heartbeat");
        assert!(resp.error_code == 0, "heartbeat error: {resp:?}");
        self.epoch = resp.member_epoch;
        if resp.active_tasks.is_some() {
            self.active = active_partitions_for(&resp, "0");
        }
    }
}

use {
    krabka_protocol::owned::streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    rstest::fixture,
    rstest_bdd_macros::{scenario, then, when},
};

#[derive(Default)]
struct StreamsRecovery {
    reloaded: Option<Member>,
    joining: Option<Member>,
    join_response: Option<StreamsGroupHeartbeatResponse>,
}

#[fixture]
fn streams_recovery() -> StreamsRecovery {
    // Members report their actual owned tasks during the post-restart handover.
    StreamsRecovery::default()
}

#[when("the streams broker restarts preserving the original member and assigned tasks")]
async fn restart_streams_assignment(
    streams_group: &mut StreamsGroupScenario,
    streams_recovery: &mut StreamsRecovery,
) {
    let joined = streams_group
        .heartbeat
        .as_ref()
        .expect("original member joined");
    streams_recovery.reloaded = Some(Member {
        id: streams_group.member.clone(),
        epoch: joined.member_epoch,
        active: active_partitions_for(joined, "0"),
    });
    streams_group
        .broker
        .take()
        .expect("first streams broker")
        .shutdown()
        .await;
    let mut config = heartbeat_config(
        streams_group
            .directory
            .as_ref()
            .expect("durable directory")
            .path(),
    );
    config.bootstrap_mode = BootstrapMode::Rejoin;
    let broker = Broker::start(config).await.unwrap();
    broker.wait_until_group_coordinator_ready().await;
    streams_group.client = Some(connect(&broker.listen_addr().to_string()).await);
    streams_group.broker = Some(broker);
}

#[when("a new streams process p2 joins after recovery")]
async fn join_recovered_streams_assignment(
    streams_group: &StreamsGroupScenario,
    streams_recovery: &mut StreamsRecovery,
) {
    let response = streams_group
        .client()
        .send(StreamsGroupHeartbeatRequest {
            process_id: Some("p2".into()),
            ..first_join(GROUP, topology(TOPIC, vec![]))
        })
        .await
        .expect("join after restart");
    streams_recovery.joining = Some(Member {
        id: response.member_id.clone(),
        epoch: response.member_epoch,
        active: active_partitions_for(&response, "0"),
    });
    streams_recovery.join_response = Some(response);
}

#[then("the recovered streams coordinator accepts the new process")]
fn recovered_streams_join(streams_recovery: &StreamsRecovery) {
    let response = streams_recovery
        .join_response
        .as_ref()
        .expect("join answered");
    assert!(response.error_code == 0, "{response:?}");
}

#[when("both streams members report owned tasks until the assignment handover converges")]
async fn hand_over_recovered_streams_tasks(
    streams_group: &StreamsGroupScenario,
    streams_recovery: &mut StreamsRecovery,
) {
    let reloaded = streams_recovery
        .reloaded
        .as_mut()
        .expect("recovered member");
    let joining = streams_recovery.joining.as_mut().expect("new member");
    for _ in 0..20 {
        if reloaded.active == [0] && joining.active == [1] {
            break;
        }
        reloaded.heartbeat(streams_group.client()).await;
        joining.heartbeat(streams_group.client()).await;
        time::sleep(Duration::from_millis(100)).await;
    }
}

#[then("the recovered member owns task zero and the joining process owns task one")]
fn recovered_streams_handover(streams_recovery: &StreamsRecovery) {
    assert!(
        (
            &streams_recovery
                .reloaded
                .as_ref()
                .expect("recovered member")
                .active,
            &streams_recovery
                .joining
                .as_ref()
                .expect("new member")
                .active
        ) == (&vec![0], &vec![1])
    );
}

#[scenario(
    path = "tests/features/migrated/tests/streams_groups/streams_restart.feature",
    name = "a reloaded group assigns tasks to a member that joins after a restart"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_reloaded_group_assigns_tasks_to_a_member_that_joins_after_a_restart(
    streams_group: StreamsGroupScenario,
    streams_recovery: StreamsRecovery,
) {
    let _ = streams_recovery;
    streams_group.shutdown().await;
    drop(streams_group);
}
