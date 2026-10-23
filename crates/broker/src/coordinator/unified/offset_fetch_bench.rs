//! Stable benchmark seam for the synchronous work of an offset-fetch actor turn.

use {
    super::group::{CoordinatorGroup, OffsetFetchMember, OffsetFetchSelection},
    crate::coordinator::unified::classic_state::OffsetEntry,
    krabka_log::Offset,
    std::collections::HashSet,
};

/// Pre-populated coordinator state used by the offset-fetch benchmark.
pub struct Fixture(CoordinatorGroup);

/// A selection prepared before the message reaches the actor.
pub struct Request(OffsetFetchSelection);

impl Request {
    #[must_use]
    pub fn new(requested: usize) -> Self {
        let keys = (0..requested)
            .map(|partition| {
                let partition = i32::try_from(partition).expect("benchmark partition fits i32");
                (format!("topic-{partition}"), partition)
            })
            .collect::<HashSet<_>>();
        Self(OffsetFetchSelection::Keys(keys))
    }
}

impl Fixture {
    #[must_use]
    pub fn new(stored: usize) -> Self {
        let mut group = CoordinatorGroup::new_classic("bench");
        for partition in 0..stored {
            let partition = i32::try_from(partition).expect("benchmark partition fits i32");
            let key = (format!("topic-{partition}"), partition);
            group.committed_offsets.insert(key.clone(), OffsetEntry {
                offset: Offset(i64::from(partition)),
                leader_epoch: 1,
                metadata: "metadata-owned-by-the-entry".repeat(2),
                commit_timestamp_ms: 0,
                expire_timestamp_ms: None,
                topic_id: None,
            });
            group.add_pending_txn_offsets(i64::from(partition), 1, [key]);
        }
        Self(group)
    }

    /// Run exactly the state projection performed synchronously by dispatch.
    #[must_use]
    pub fn actor_turn(&self, request: &Request) -> (usize, usize) {
        let offsets = self
            .0
            .offsets_for_member(
                &OffsetFetchMember {
                    member_id: None,
                    member_epoch: -1,
                },
                &request.0,
            )
            .expect("classic group accepts an admin fetch");
        (offsets.committed.len(), offsets.pending_txn.len())
    }

    /// The previous actor turn, which cloned all state before handler filtering.
    #[must_use]
    pub fn all_actor_turn(&self) -> (usize, usize) {
        let offsets = self.0.offsets();
        (offsets.committed.len(), offsets.pending_txn.len())
    }
}
