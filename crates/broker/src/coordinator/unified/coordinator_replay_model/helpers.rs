use super::*;

pub(super) fn bit(member: u8) -> u8 {
    1 << member
}

pub(super) fn member_exists(state: &State, member: u8) -> bool {
    state.members & bit(member) != 0
}

pub(super) fn mutation(state: &State, kind: ReplayRecordKind, member: u8) -> ReplayMutation {
    replay_mutation(
        kind,
        Some(kind),
        state.group_epoch.is_some(),
        member_exists(state, member),
    )
}

pub(super) fn tombstone(state: &State, kind: ReplayRecordKind, member: u8) -> ReplayMutation {
    replay_mutation(
        kind,
        None,
        state.group_epoch.is_some(),
        member_exists(state, member),
    )
}

pub(super) fn coherent(state: &State) -> bool {
    if state.group_epoch.is_none() {
        return state.target_epoch == 0
            && state.members == 0
            && state.targets == 0
            && state.currents == [None, None]
            && state.metadata == 0;
    }
    state.targets & !state.members == 0
        && state.currents.iter().enumerate().all(|(member, epoch)| {
            epoch.is_none() || state.members & bit(u8::try_from(member).unwrap()) != 0
        })
        && state.group_epoch.is_some_and(|epoch| epoch >= 0)
        && state.target_epoch >= 0
        && state.currents.iter().flatten().all(|epoch| *epoch >= 0)
}
