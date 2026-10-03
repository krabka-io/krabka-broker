use super::*;

pub(super) fn check(model: SpoolModel) -> impl Checker<SpoolModel> {
    model
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join()
}
