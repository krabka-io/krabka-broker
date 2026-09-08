//! KIP-932 share-group membership state machine.
//!
//! It mirrors the consumer next-gen `consumer_state::GroupState` without any
//! offset or revocation machinery. Share-group assignment is non-exclusive,
//! so members carry no assignment-ack state beyond a member epoch.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use krabka_protocol::primitives::uuid::Uuid;

use super::super::expired_member_ids;

/// One member of a share group.
#[derive(Debug, Clone)]
pub struct ShareMemberState {
    pub member_id: String,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    pub member_epoch: i32,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub last_seen: Instant,
}

impl ShareMemberState {
    /// Construct a freshly joining member at epoch 0 with no assignment.
    pub fn joining(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
        subs: HashSet<String>,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            rack_id: None,
            client_id: client_id.into(),
            client_host: client_host.into(),
            subscribed_topic_names: subs,
            member_epoch: 0,
            assigned_partitions: HashMap::new(),
            last_seen: Instant::now(),
        }
    }
}

/// The target assignment computed by the most recent reconcile.
#[derive(Debug, Clone)]
pub struct ShareTargetAssignment {
    pub epoch: i32,
    pub per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>,
}

/// Full membership state of a single share group.
#[derive(Debug, Clone)]
pub struct ShareGroupState {
    pub group_id: String,
    pub group_epoch: i32,
    pub members: HashMap<String, ShareMemberState>,
    pub target: ShareTargetAssignment,
    /// Set whenever membership or subscription changes, so the actor knows a
    /// reconcile is pending. It clears once the reconcile installs a target.
    pub dirty: bool,
    /// KIP-932: `(topic_id, partition)` share-states this
    /// group has already Initialized in the share-state persister. Seeded from
    /// the replayed `ShareGroupStatePartitionMetadata` (key v15) so a
    /// post-restart heartbeat does not re-Initialize. The lifecycle hook adds
    /// to this on each successful `SharePersister::initialize`.
    pub initialized: HashSet<(Uuid, i32)>,
    /// The topic name behind each topic id in [`Self::initialized`].
    ///
    /// KIP-932's `ShareGroupStatePartitionMetadata` carries `TopicName` beside
    /// `TopicId` on every entry, and the tooling that reads that record has no
    /// topic-id index of its own, so the name has to live as long as the entry
    /// does. It is learned from the metadata image when the lifecycle hook
    /// initializes a partition, and re-seeded from the replayed record after a
    /// restart, exactly as Kafka's `GroupMetadataManager` keeps
    /// `InitMapValue.name()`. An id with no name here is written as
    /// [`UNKNOWN_TOPIC_NAME`], the same fallback Kafka writes for an id its
    /// metadata image no longer holds.
    ///
    /// [`UNKNOWN_TOPIC_NAME`]: super::persistence::UNKNOWN_TOPIC_NAME
    pub topic_names: HashMap<Uuid, String>,
}

impl ShareGroupState {
    pub fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            group_epoch: 0,
            members: HashMap::new(),
            target: ShareTargetAssignment {
                epoch: 0,
                per_member: HashMap::new(),
            },
            dirty: false,
            initialized: HashSet::new(),
            topic_names: HashMap::new(),
        }
    }

    /// Drop the name of every topic that no longer has an initialized
    /// partition, so the map stays exactly the set of topics the next
    /// `ShareGroupStatePartitionMetadata` record will name.
    pub fn forget_unused_topic_names(&mut self) {
        let Self {
            initialized,
            topic_names,
            ..
        } = self;
        let live: HashSet<Uuid> = initialized.iter().map(|(topic_id, _)| *topic_id).collect();
        topic_names.retain(|topic_id, _| live.contains(topic_id));
    }

    pub fn bump_epoch(&mut self) -> bool {
        let Some(group_epoch) = crate::metadata_epoch::next_i32(self.group_epoch) else {
            return false;
        };
        self.group_epoch = group_epoch;
        true
    }

    pub fn add_or_update_member(&mut self, m: ShareMemberState) {
        self.members.insert(m.member_id.clone(), m);
        self.dirty = true;
    }

    pub fn remove_member(&mut self, member_id: &str) -> Option<ShareMemberState> {
        let r = self.members.remove(member_id);
        if r.is_some() {
            self.dirty = true;
        }
        r
    }

    /// Remove members whose `last_seen` is older than `session_timeout`, and
    /// return the evicted member ids.
    pub fn evict_expired(&mut self, now: Instant, session_timeout: Duration) -> Vec<String> {
        let expired = expired_member_ids(
            self.members
                .iter()
                .map(|(id, member)| (id.as_str(), member.last_seen)),
            now,
            session_timeout,
        );
        for id in &expired {
            self.members.remove(id);
        }
        if !expired.is_empty() {
            self.dirty = true;
        }
        expired
    }

    /// Install a freshly computed target assignment stamped with the current
    /// group epoch.
    pub fn install_target(&mut self, per_member: HashMap<String, HashMap<Uuid, Vec<i32>>>) {
        self.target = ShareTargetAssignment {
            epoch: self.group_epoch,
            per_member,
        };
    }

    /// Advance a member to the current group epoch and hand it the partitions
    /// that the latest target assignment gave it.
    pub fn advance_member_epoch(&mut self, member_id: &str) {
        if let Some(m) = self.members.get_mut(member_id) {
            m.member_epoch = self.group_epoch;
            if let Some(a) = self.target.per_member.get(member_id) {
                m.assigned_partitions.clone_from(a);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use assert2::assert;

    use super::*;

    #[test]
    fn add_member_bumps_nothing_until_reconcile() {
        let mut g = ShareGroupState::new("g1");
        assert!(g.group_epoch == 0);
        g.add_or_update_member(ShareMemberState::joining(
            "m1",
            "c1",
            "h1",
            ["t1".to_string()].into_iter().collect(),
        ));
        assert!(g.members.len() == 1);
        assert!(g.dirty);
    }

    #[test]
    fn bump_epoch_rejects_exhaustion() {
        let mut group = ShareGroupState::new("g");
        group.group_epoch = i32::MAX;

        assert!(!group.bump_epoch());
        assert!(group.group_epoch == i32::MAX);
    }

    #[test]
    fn evict_expired_removes_silent_members() {
        let mut g = ShareGroupState::new("g1");
        let mut m = ShareMemberState::joining("m1", "c1", "h1", HashSet::default());
        // `last_seen` is "now"; evaluating eviction at a point slightly in the
        // future with a tiny session timeout makes the member overdue without
        // ever subtracting from an `Instant` (which underflows on low-uptime CI).
        m.last_seen = Instant::now();
        g.add_or_update_member(m);

        // A member seen within the session timeout is retained.
        let recent = Instant::now() + Duration::from_millis(50);
        let kept = g.evict_expired(recent, Duration::from_secs(45));
        assert!(kept.is_empty());
        assert!(g.members.len() == 1);

        // The same member is overdue once the timeout shrinks below its silence.
        let later = Instant::now() + Duration::from_millis(50);
        let evicted = g.evict_expired(later, Duration::from_millis(1));
        assert!(evicted == vec!["m1".to_string()]);
        assert!(g.members.is_empty());
    }

    #[test]
    fn forgetting_names_keeps_exactly_the_initialized_topics() {
        let kept = Uuid([1; 16]);
        let dropped = Uuid([2; 16]);
        let mut g = ShareGroupState::new("g1");
        g.initialized.insert((kept, 0));
        g.topic_names.insert(kept, "orders".to_owned());
        g.topic_names.insert(dropped, "carts".to_owned());

        g.forget_unused_topic_names();

        assert!(g.topic_names == HashMap::from([(kept, "orders".to_owned())]));
    }
}
