//! Fault/recovery histories through real membership actors and metadata writes.
//!
//! The scope is assignment callback failure, rollback and coordinator replay.
//! Replicated storage, transactions and arbitrary network partitions are not
//! modeled by the in-memory offsets-log fixture.
use {
    super::*,
    crate::{
        coordinator::unified::streams::assignor::test_support::Recoverable,
        model_check::check_model, task_util::ask,
    },
    assert2::assert,
    stateright::{Model, Property},
    std::sync::atomic::{AtomicBool, Ordering},
    tokio::runtime::{Builder, Runtime},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Action {
    Fail,
    Repair,
    JoinSecond,
    Heartbeat,
    LeaveSecond,
    Restart,
    StaleDelivery,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    trace: Vec<Action>,
    atomic: bool,
    progress: bool,
    retired: bool,
}
struct AssignmentModel {
    runtime: Runtime,
}
impl Model for AssignmentModel {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            trace: vec![],
            atomic: true,
            progress: true,
            retired: true,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.trace.len() < 4 {
            actions.extend([
                Action::Fail,
                Action::Repair,
                Action::JoinSecond,
                Action::Heartbeat,
                Action::LeaveSecond,
                Action::Restart,
                Action::StaleDelivery,
            ]);
        }
    }

    fn next_state(&self, previous: &State, action: Action) -> Option<State> {
        let mut trace = previous.trace.clone();
        trace.push(action);
        let mut result = self.runtime.block_on(replay(&trace));
        result.atomic &= previous.atomic;
        result.progress &= previous.progress;
        result.retired &= previous.retired;
        Some(result)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always(
                "failed_assignment_changes_neither_live_members_nor_durable_records",
                |_, state: &State| state.atomic,
            ),
            Property::always(
                "assignment_errors_are_reported_and_healthy_retry_progresses",
                |_, state: &State| state.progress,
            ),
            Property::always(
                "retired_coordinator_cannot_acknowledge_delayed_heartbeat",
                |_, state: &State| state.retired,
            ),
        ]
    }
}
fn configured(fail: &Arc<AtomicBool>) -> StreamsGroupConfig {
    let mut config = undelayed();
    config
        .task_assignors
        .register(Arc::new(Recoverable(Arc::clone(fail))))
        .expect("register");
    config
}
fn join(member: &str) -> StreamsGroupHeartbeatRequest {
    streams_join_request()
        .topology(one_subtopology().call())
        .member_id(member)
        .call()
}
async fn replay(trace: &[Action]) -> State {
    let fail = Arc::new(AtomicBool::new(false));
    let mut log = Arc::new(InMemoryOffsetsLog::default());
    let mut coordinator = coordinator_with_log(configured(&fail), Arc::clone(&log));
    coordinator.set_metadata_source(input_topic_source(1));
    let mut handle = coordinator.get_or_create_streams("g");
    let first = heartbeat_result_at(&handle, join("m1"), 1).await.response;
    assert!(first.error_code == codes::NONE);
    let mut retired_handle = None;
    let mut result = State {
        trace: trace.to_vec(),
        atomic: true,
        progress: true,
        retired: true,
    };
    for action in trace {
        match action {
            Action::Fail => fail.store(true, Ordering::SeqCst),
            Action::Repair => fail.store(false, Ordering::SeqCst),
            Action::Restart => {
                let seed = coordinator
                    .cached_streams_seed("g")
                    .expect("committed state");
                coordinator.shutdown_all().await;
                handle.tx.closed().await;
                retired_handle = Some(Arc::clone(&handle));
                log = Arc::new(InMemoryOffsetsLog::default());
                coordinator = coordinator_with_log(configured(&fail), Arc::clone(&log));
                coordinator.set_metadata_source(input_topic_source(1));
                coordinator.update_streams_cache("g", seed);
                handle = coordinator.get_or_create_streams("g");
            }
            Action::StaleDelivery => {
                if let Some(retired) = &retired_handle {
                    let response = ask(&retired.tx, |reply| StreamsGroupActorMessage::Heartbeat {
                        request: Box::new(join("delayed")),
                        version: 1,
                        client_id: "client".into(),
                        client_host: "host".into(),
                        reply,
                    })
                    .await;
                    result.retired &= response.is_err();
                }
            }
            Action::JoinSecond | Action::Heartbeat | Action::LeaveSecond => {
                let before_members = describe_member_ids(&handle).await;
                let second_exists = before_members.iter().any(|member| member == "m2");
                if (*action == Action::JoinSecond && second_exists)
                    || (*action == Action::LeaveSecond && !second_exists)
                {
                    continue;
                }
                let before = coordinator.cached_streams_seed("g");
                let batches = log.batches().await.len();
                let request = match action {
                    Action::JoinSecond => join("m2"),
                    Action::LeaveSecond => StreamsGroupHeartbeatRequest {
                        group_id: "g".into(),
                        member_id: "m2".into(),
                        member_epoch: -1,
                        ..Default::default()
                    },
                    _ => {
                        let view = describe(&handle).await;
                        let member = view
                            .members
                            .iter()
                            .find(|member| member.member_id == "m1")
                            .expect("first member");
                        StreamsGroupHeartbeatRequest {
                            group_id: "g".into(),
                            member_id: "m1".into(),
                            member_epoch: member.member_epoch,
                            ..Default::default()
                        }
                    }
                };
                let response = heartbeat_result_at(&handle, request, 1).await.response;
                let must_fail = *action == Action::JoinSecond && fail.load(Ordering::SeqCst);
                if must_fail {
                    result.progress &= response.error_code == codes::UNKNOWN_SERVER_ERROR;
                } else if !fail.load(Ordering::SeqCst) || *action == Action::LeaveSecond {
                    result.progress &= response.error_code == codes::NONE;
                } else {
                    result.progress &=
                        [codes::NONE, codes::UNKNOWN_SERVER_ERROR].contains(&response.error_code);
                }
                if must_fail || response.error_code == codes::UNKNOWN_SERVER_ERROR {
                    result.atomic &= coordinator.cached_streams_seed("g") == before
                        && log.batches().await.len() == batches
                        && describe_member_ids(&handle).await == before_members;
                }
                if *action == Action::JoinSecond && !must_fail {
                    result.progress &= response.error_code == codes::NONE
                        && describe_member_ids(&handle)
                            .await
                            .iter()
                            .any(|member| member == "m2");
                }
            }
        }
    }
    coordinator.shutdown_all().await;
    result
}
#[test]
fn task_assignor_registry_stateright_failure_recovery_and_retired_messages() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    // All 1+7+49+343+2401 histories; no cap is reached and no error prunes a path.
    check_model(
        AssignmentModel { runtime },
        "Streams assignment failure actors",
        (6, 10_000, 10_000),
        2801,
    );
}
