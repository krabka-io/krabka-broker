//! The bounded shape of one checked run: the member-id pool, the
//! `group.instance.id` pool, and the logical-clock cap.
//!
//! The shape lives here rather than in the model state, so that the state stays
//! the part the checker enumerates and the bounds stay the part a test picks.

pub(super) struct ClassicModel {
    pub(super) members: Vec<&'static str>,
    pub(super) instances: Vec<&'static str>,
    pub(super) max_clock: i64,
}
