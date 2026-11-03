//! Exhaustive configuration/membership/task-universe changes.
//!
//! Every transition executes the real registry, override validation and
//! assignment validator.
use {
    super::{
        AssignorInput, AssignorMember,
        test_support::{Fail, registered_config},
    },
    crate::{coordinator::unified::streams::config::KEY_ASSIGNOR_NAME, model_check::check_model},
    stateright::{Model, Property},
    std::sync::Arc,
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    selected: u8,
    members: u8,
    partitions: u8,
    safe: bool,
}
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum Action {
    Select(u8),
    ToggleA,
    ToggleB,
    Partitions(u8),
}
struct Registry;
impl Model for Registry {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            selected: 0,
            members: 3,
            partitions: 1,
            safe: true,
        }]
    }

    fn actions(&self, _: &State, actions: &mut Vec<Action>) {
        actions.extend([Action::ToggleA, Action::ToggleB]);
        for value in 0..3 {
            actions.extend([Action::Select(value), Action::Partitions(value)]);
        }
    }

    fn next_state(&self, last: &State, action: Action) -> Option<State> {
        let mut state = last.clone();
        match action {
            Action::Select(value) => state.selected = value,
            Action::ToggleA => state.members ^= 1,
            Action::ToggleB => state.members ^= 2,
            Action::Partitions(value) => state.partitions = value,
        }
        let mut config = registered_config();
        config
            .task_assignors
            .register(Arc::new(Fail))
            .expect("register");
        let name = ["first", "second", "fail"][usize::from(state.selected)];
        let selected = config
            .with_group_overrides(&[(KEY_ASSIGNOR_NAME.into(), name.into())].into())
            .expect("selection");
        let members: Vec<_> = ["a", "b"]
            .iter()
            .enumerate()
            .filter(|(index, _)| state.members & (1 << index) != 0)
            .map(|(_, id)| AssignorMember {
                member_id: (*id).into(),
                ..Default::default()
            })
            .collect();
        let partitions: Vec<_> = (0..i32::from(state.partitions)).collect();
        let input = AssignorInput {
            tasks: [("s".into(), partitions.clone())].into(),
            ..Default::default()
        };
        let result = selected.assign_tasks(&members, &input);
        let expected_member = if state.selected == 0 { "a" } else { "b" };
        let should_succeed = state.selected < 2 && state.members & (1 << state.selected) != 0;
        state.safe &= selected.selected_task_assignor_name() == name;
        state.safe &= match result {
            Ok(assignment) => {
                should_succeed
                    && assignment.active
                        == [(expected_member.into(), [("s".into(), partitions)].into())].into()
                    && assignment.standby.is_empty()
            }
            Err(_) => !should_succeed,
        };
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![Property::always(
            "selection_executes_exact_implementation_or_rejects_invalid_target",
            |_, state: &State| state.safe,
        )]
    }
}
#[test]
fn native_registry_stateright_configuration_changes() {
    // Three selections, four membership sets, three task universes. All are
    // reachable and every transition executes production assignment logic.
    check_model(Registry, "Streams registry", (8, 1000, 1000), 36);
}
