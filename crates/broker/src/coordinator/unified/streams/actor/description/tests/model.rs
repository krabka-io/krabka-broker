//! Exhaustive bounded schedules replayed against real actors, offsets reaping,
//! backend failure injection and durable Streams seeds.
//!
//! No symbolic substitute
//! for the implementation is used. The model does not simulate a network or
//! claim the Jepsen transaction/replication checks are covered.
use {
    super::*,
    crate::{coordinator::retention::sweep, model_check::check_model},
    stateright::{Model, Property},
    tokio::runtime::{Builder, Runtime},
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Action {
    Leave,
    FailDelete,
    Repair,
    SweepFresh,
    SweepExpired,
    Restart,
    StalePush,
    ExplicitDelete,
    Convert,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    trace: Vec<Action>,
    safe: bool,
}
struct CleanupModel {
    runtime: Runtime,
}
impl Model for CleanupModel {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            trace: vec![],
            safe: true,
        }]
    }

    fn actions(&self, state: &State, actions: &mut Vec<Action>) {
        if state.trace.len() < 4 {
            actions.extend([
                Action::Leave,
                Action::FailDelete,
                Action::Repair,
                Action::SweepFresh,
                Action::SweepExpired,
                Action::Restart,
                Action::StalePush,
                Action::ExplicitDelete,
                Action::Convert,
            ]);
        }
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut trace = state.trace.clone();
        trace.push(action);
        let safe = state.safe && self.runtime.block_on(replay(&trace));
        Some(State { trace, safe })
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always(
            "retention_storage_and_recovery_are_safe_and_progress",
            |_, state: &State| state.safe,
        )]
    }
}

async fn replay(trace: &[Action]) -> bool {
    let backend = Arc::new(NativeStore::default());
    let mut coordinator = native_coordinator(
        Arc::clone(&backend),
        Arc::new(InMemoryOffsetsLog::default()),
    );
    let mut handle = store_native_topology(&coordinator).await;
    commit_retention_offset(&coordinator).await;
    let mut retired = None;
    let mut member = true;
    let mut offsets = true;
    let mut deleted = false;
    let mut safe = true;
    for action in trace {
        if deleted {
            continue;
        }
        match action {
            Action::Leave => {
                if member {
                    leave_native_member(&handle).await;
                    member = false;
                }
            }
            Action::FailDelete => backend.fail_delete.store(true, Ordering::Relaxed),
            Action::Repair => backend.fail_delete.store(false, Ordering::Relaxed),
            Action::SweepFresh | Action::SweepExpired => {
                let expired = *action == Action::SweepExpired;
                let rows = sweep(
                    &coordinator,
                    |_| true,
                    if expired { 101 } else { 99 },
                    100,
                    0,
                )
                .await;
                let removed = rows.iter().any(|row| row.outcome.group_deleted);
                if member || (offsets && !expired) {
                    safe &= !removed && backend.value.lock().expect("lock").is_some();
                }
                if !member && expired {
                    offsets = false;
                }
                if !member && !offsets && !backend.fail_delete.load(Ordering::Relaxed) {
                    // Progress is checked on every healthy eligible attempt, including after
                    // failed deletion, a coordinator restart and a stale response.
                    safe &= removed && backend.value.lock().expect("lock").is_none();
                }
                if backend.fail_delete.load(Ordering::Relaxed) {
                    safe &= !removed;
                }
                deleted = removed;
            }
            Action::ExplicitDelete => {
                use crate::coordinator::DeleteGroupError;
                let result = coordinator.delete_group("app").await;
                if member {
                    safe &= result == Err(DeleteGroupError::NonEmpty);
                } else if backend.fail_delete.load(Ordering::Relaxed) {
                    safe &= result == Err(DeleteGroupError::TopologyDescription);
                } else {
                    safe &= result.is_ok();
                    safe &= coordinator.group_type("app").is_none();
                    safe &= coordinator.find("app").is_none();
                    safe &= backend.value.lock().expect("lock").is_none();
                    safe &= ask(&handle.tx, StreamsGroupActorMessage::Delete)
                        .await
                        .is_err();
                    deleted = true;
                }
            }
            Action::Convert => {
                use crate::coordinator::unified::{
                    GroupType, streams::migration::DowngradeOutcome,
                };
                let result = coordinator
                    .try_convert_streams_to_classic("app", "", 1)
                    .await
                    .expect("conversion");
                if member {
                    safe &= result == DowngradeOutcome::RejectLiveMembers;
                } else if result == DowngradeOutcome::Converted {
                    safe &= !backend.fail_delete.load(Ordering::Relaxed);
                    safe &= coordinator.group_type("app") == Some(GroupType::Classic);
                    safe &= backend.value.lock().expect("lock").is_none();
                    safe &= ask(&handle.tx, StreamsGroupActorMessage::Delete)
                        .await
                        .is_err();
                    deleted = true;
                } else {
                    safe &= result == DowngradeOutcome::RetryCleanup;
                }
            }
            Action::Restart => {
                let seed = coordinator
                    .cached_streams_seed("app")
                    .expect("durable seed");
                coordinator.shutdown_all().await;
                retired = Some(Arc::clone(&handle));
                coordinator = native_coordinator(
                    Arc::clone(&backend),
                    Arc::new(InMemoryOffsetsLog::default()),
                );
                coordinator.update_streams_cache("app", seed);
                handle = coordinator.get_or_create_streams("app");
                if offsets {
                    commit_retention_offset(&coordinator).await;
                }
                // An independent offsets actor must not authorize cleanup of live members.
                safe &= describe(&handle).await.members.is_empty() != member;
            }
            Action::StalePush => {
                let before = backend.value.lock().expect("lock").clone();
                if let Some(old_coordinator) = &retired {
                    // A delayed request bound to the retired coordinator must not
                    // mutate a shared backend after movement or deliver success.
                    let stale = ask(&old_coordinator.tx, |reply| {
                        StreamsGroupActorMessage::PushDescription {
                            push: Box::new(description_push("a", 0, 1)),
                            reply,
                        }
                    })
                    .await;
                    safe &= stale.is_err();
                }
                let result = push(&handle, description_push("a", -1, 1)).await;
                safe &= result.0 != codes::NONE;
                safe &= *backend.value.lock().expect("lock") == before;
            }
        }
    }
    coordinator.shutdown_all().await;
    safe
}

#[test]
fn streams_cleanup_stateright_actor_schedules() {
    let runtime = Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    // All 1 + 9 + 81 + 729 + 6561 schedules through four operations. The
    // cap is above the exhaustive reachable set, never a truncated success.
    check_model(
        CleanupModel { runtime },
        "streams cleanup actors",
        (6, 10_000, 10_000),
        7381,
    );
}
