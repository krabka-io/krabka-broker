//! Stateful incarnation/retry model through the real asynchronous deletion
//! handler, tombstone encoder and in-memory offsets log. The log is a test
//! double; disk/raft durability, actor ordering and recovery IO are not proved.

use {
    super::delete_topic_offsets,
    crate::{
        coordinator::{
            test_support::offset_entry,
            topic_deletion::DeletedTopic,
            unified::{
                classic_state::ClassicGroup,
                group::{CoordinatorGroup, GroupKind},
                offsets_log::fake::InMemoryOffsetsLog,
            },
        },
        model_check::check_model,
    },
    stateright::{Model, Property},
    std::{collections::HashMap, sync::atomic::Ordering},
    tokio::runtime::Builder,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Attempt {
    before: Option<u8>,
    pending: bool,
    deleted: u8,
    failure: bool,
    removed: usize,
    after: Option<u8>,
    pending_after: bool,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    // None = absent, 0 = legacy/no UUID, 1 and 2 = distinct incarnations.
    entry: Option<u8>,
    pending: bool,
    last: Option<Attempt>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Commit(u8),
    Pending,
    Delete(u8, bool),
}
struct DeletionModel;

fn key() -> (String, i32) { ("topic".to_owned(), 0) }

fn delete(state: &State, deleted: u8, failure: bool) -> State {
    let committed = state.entry.map(|id| {
        let mut entry = offset_entry(7);
        entry.topic_id = (id != 0).then(|| uuid::Uuid::from_u128(u128::from(id)));
        (key(), entry)
    });
    let mut group = CoordinatorGroup::seeded(
        "g",
        GroupKind::Classic(ClassicGroup::new("g")),
        committed.into_iter().collect::<HashMap<_, _>>(),
    );
    if state.pending {
        group.add_pending_txn_offsets(7, 10, [key()]);
    }
    let log = InMemoryOffsetsLog::default();
    log.fail_next.store(failure, Ordering::SeqCst);
    let topics = [DeletedTopic {
        name: "topic".to_owned(),
        topic_id: uuid::Uuid::from_u128(u128::from(deleted)),
    }];
    let runtime = Builder::new_current_thread().build().unwrap();
    let removed = runtime
        .block_on(delete_topic_offsets(&mut group, &log, &topics))
        .len();
    let offsets = group.offsets();
    let after = offsets.committed.get(&key()).map(|entry| {
        entry
            .topic_id
            .map_or(0, |id| u8::try_from(id.as_u128()).unwrap())
    });
    let pending_after = !offsets.pending_txn.is_empty();
    State {
        entry: after,
        pending: pending_after,
        last: Some(Attempt {
            before: state.entry,
            pending: state.pending,
            deleted,
            failure,
            removed,
            after,
            pending_after,
        }),
    }
}

impl Model for DeletionModel {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            entry: None,
            pending: false,
            last: None,
        }]
    }

    fn actions(&self, _: &State, actions: &mut Vec<Action>) {
        actions.extend([
            Action::Commit(0),
            Action::Commit(1),
            Action::Commit(2),
            Action::Pending,
            Action::Delete(1, false),
            Action::Delete(2, false),
            Action::Delete(1, true),
            Action::Delete(2, true),
        ]);
    }

    fn next_state(&self, state: &State, action: Action) -> Option<State> {
        let mut next = state.clone();
        match action {
            Action::Commit(id) => next.entry = Some(id),
            Action::Pending => next.pending = true,
            Action::Delete(id, fail) => next = delete(state, id, fail),
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("failed_append_is_atomic", |_, s: &State| {
                s.last.as_ref().is_none_or(|a| {
                    !a.failure
                        || (a.removed == 0 && a.before == a.after && a.pending == a.pending_after)
                })
            }),
            Property::always(
                "different_incarnation_survives_without_pending_name",
                |_, s: &State| {
                    s.last.as_ref().is_none_or(|a| {
                        a.pending
                            || a.before.is_none_or(|id| id == 0 || id == a.deleted)
                            || (a.removed == 0 && a.after == a.before)
                    })
                },
            ),
            Property::always("successful_deletion_clears_exact_key", |_, s: &State| {
                s.last.as_ref().is_none_or(|a| {
                    a.removed == 0 || (!a.failure && a.after.is_none() && !a.pending_after)
                })
            }),
            Property::sometimes("legacy_offset_removed", |_, s: &State| {
                s.last
                    .as_ref()
                    .is_some_and(|a| a.before == Some(0) && a.removed != 0)
            }),
            Property::sometimes("new_incarnation_retained", |_, s: &State| {
                s.last.as_ref().is_some_and(|a| {
                    a.before == Some(2)
                        && !a.pending
                        && a.deleted == 1
                        && !a.failure
                        && a.removed == 0
                })
            }),
            Property::sometimes("pending_offset_removed_by_name", |_, s: &State| {
                s.last.as_ref().is_some_and(|a| a.pending && a.removed != 0)
            }),
            Property::sometimes("append_failure_reached", |_, s: &State| {
                s.last
                    .as_ref()
                    .is_some_and(|a| a.failure && a.before.is_some())
            }),
        ]
    }
}

#[test]
fn topic_deletion_interleaves_incarnations_pending_offsets_and_append_failures() {
    check_model(
        DeletionModel,
        "topic_offset_deletion",
        (20, 100_000, 2000),
        210,
    );
}
