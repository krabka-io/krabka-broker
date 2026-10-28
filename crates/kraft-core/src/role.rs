//! The replica's volatile role within the current epoch and its per-role state.

use {
    crate::types::{NodeId, SimInstant},
    std::collections::{BTreeMap, BTreeSet},
};

/// How recently a replica must have fetched for `AddRaftVoter` to call it
/// caught up: Kafka's `LeaderState.isReplicaCaughtUp` window of one hour.
const CAUGHT_UP_FETCH_WINDOW_MS: u64 = 3_600_000;

/// Per-replica replication progress that a leader tracks: a voter's for the HWM
/// and any replica's for `DescribeQuorum` and the `AddRaftVoter` catch-up check.
///
/// A zero instant means the leader has not recorded that event, as Kafka's -1
/// timestamps do.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ReplicaProgress {
    /// Highest offset the follower has acknowledged, that is, its fetch offset.
    pub fetch_offset: i64,
    /// Logical instant the leader received the most recent Fetch from this follower.
    pub last_fetch: SimInstant,
    /// Logical instant this follower was known to be caught up with the leader's log end.
    pub last_caught_up: SimInstant,
    /// The leader's log end offset when this follower last fetched: the offset
    /// the next fetch has to reach for the follower to count as having kept
    /// pace with the appends in between.
    pub last_fetch_leader_log_end: i64,
}

impl ReplicaProgress {
    /// Record a valid fetch at `fetch_offset` while the leader's log ended at
    /// `leader_log_end`, as Kafka's `ReplicaState.updateFollowerState` does.
    ///
    /// A fetch at or past the leader's log end is caught up now. A fetch short
    /// of it still counts as caught up as of the previous fetch when it reaches
    /// the log end that fetch saw, so a follower that keeps pace under
    /// continuous appends does not look stale. The caught-up time is settled
    /// before `last_fetch` moves, because the second case reads the old one.
    pub fn record_fetch(&mut self, now: SimInstant, fetch_offset: i64, leader_log_end: i64) {
        if fetch_offset >= leader_log_end {
            self.last_caught_up = self.last_caught_up.max(now);
        } else if self.last_fetch_leader_log_end > 0
            && fetch_offset >= self.last_fetch_leader_log_end
        {
            self.last_caught_up = self.last_caught_up.max(self.last_fetch);
        }
        self.last_fetch_leader_log_end = leader_log_end;
        self.last_fetch = self.last_fetch.max(now);
        self.fetch_offset = fetch_offset;
    }

    /// Kafka's `LeaderState.isReplicaCaughtUp`: the replica has been caught up
    /// at some point and fetched within the last hour.
    #[must_use]
    pub fn is_caught_up(&self, now: SimInstant) -> bool {
        self.last_caught_up > SimInstant(0) && self.fetched_within_caught_up_window(now)
    }

    /// Whether the replica fetched within the hour that
    /// [`Self::is_caught_up`] allows. A replica that has been quiet for longer
    /// can never count as caught up until it fetches again, so a leader may
    /// forget its progress.
    #[must_use]
    pub fn fetched_within_caught_up_window(&self, now: SimInstant) -> bool {
        self.last_fetch > SimInstant(0)
            && self.last_fetch.0 > now.0.saturating_sub(CAUGHT_UP_FETCH_WINDOW_MS)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, krabka_macros::EnumStr)]
#[enum_str(as_str = name)]
pub enum Role {
    /// Knows the epoch, no leader yet. May hold a non-binding pre-vote grant.
    Unattached { election_deadline: SimInstant },
    /// Cast a binding vote this epoch; now waits for a leader.
    Voted { election_deadline: SimInstant },
    /// Has a leader for the epoch and fetches from it.
    Follower {
        leader_id: NodeId,
        fetch_deadline: SimInstant,
        /// Whether a Fetch to `leader_id` has been answered successfully since
        /// this replica began following it: Kafka's
        /// `FollowerState.hasFetchedFromLeader`. Until then the leader may be
        /// unreachable from here, so the follower grants a pre-vote.
        has_fetched_from_leader: bool,
    },
    /// KIP-996 pre-vote candidate that collects non-binding grants.
    Prospective {
        granted: BTreeSet<NodeId>,
        /// The voters that refused the pre-vote. Kafka's `EpochElection`
        /// records them so that the round ends as soon as it can no longer
        /// win.
        rejected: BTreeSet<NodeId>,
        election_deadline: SimInstant,
        /// The leader of this epoch that the replica gave up on when it began
        /// the round, if it had one. Kafka's `ProspectiveState` keeps the
        /// `leaderId` of the `FollowerState` it came from, and a replica that
        /// knows a leader for its epoch grants no binding vote in it.
        /// `QuorumState::leader_id` is `None` meanwhile, so that the replica
        /// still attaches to a leader announced at this epoch.
        abandoned_leader: Option<NodeId>,
    },
    /// Real candidacy: the epoch is bumped and the replica voted for itself.
    Candidate {
        granted: BTreeSet<NodeId>,
        election_deadline: SimInstant,
    },
    /// Won the election; tracks follower progress for HWM.
    Leader {
        replicas: BTreeMap<NodeId, ReplicaProgress>,
        /// Voters that have fetched inside the current check-quorum window.
        ///
        /// This is Kafka's `LeaderState.fetchedVoters`: it never contains the
        /// leader itself, and it is emptied — and the check-quorum timer
        /// re-armed — the moment it reaches the majority the leader needs
        /// besides its own vote. A leader that reaches the deadline with the
        /// set still short of that count has lost the quorum and resigns.
        fetched_voters: BTreeSet<NodeId>,
        high_watermark: i64,
        /// Log end offset at the moment of promotion, that is, where this
        /// leader's `LeaderChange` record sits. That record is the first
        /// current-epoch record.
        ///
        /// The HWM may only advance past this offset. This enforces Raft Fig.8
        /// leader completeness: a current-epoch entry must be
        /// majority-replicated before commit.
        epoch_start_offset: i64,
    },
    /// Stepped down: told the voters to elect, and waits for its own election
    /// timer to start a pre-vote round.
    ///
    /// A leader reaches this variant when its check-quorum window expires. It
    /// no longer serves Fetch, no longer advances the high watermark, and no
    /// longer claims the leadership in `QuorumState`, so an isolated old leader
    /// stops answering as leader for its epoch.
    Resigned,
    /// Not in the voter set; only ever fetches.
    Observer {
        leader_id: Option<NodeId>,
        fetch_deadline: SimInstant,
    },
}

impl Default for Role {
    fn default() -> Self {
        Self::Unattached {
            election_deadline: SimInstant(0),
        }
    }
}

impl Role {
    #[must_use]
    pub fn is_leader(&self) -> bool { matches!(self, Self::Leader { .. }) }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};
    /// Every role reports its own name. These strings reach operators through
    /// metrics and logs, so one standing in for another is a real confusion.
    #[test]
    fn each_role_reports_its_own_name() {
        let names = [Role::default().name(), Role::Resigned.name()];
        assert2::assert!(names == ["Unattached", "Resigned"]);
        // Distinct, not merely non-empty: a single constant would satisfy the
        // latter for every variant.
        assert2::assert!(names[0] != names[1]);
    }

    use {
        rstest::fixture,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    };

    #[derive(DataTableRow)]
    struct FollowerFetch {
        at_ms: u64,
        fetch_offset: i64,
        leader_end: i64,
    }

    struct FollowerProgress {
        progress: Option<ReplicaProgress>,
    }

    #[fixture]
    fn follower() -> FollowerProgress {
        let progress = None;
        FollowerProgress { progress }
    }

    #[given("a follower that has never fetched")]
    fn fresh_follower(follower: &mut FollowerProgress) {
        follower.progress = Some(ReplicaProgress::default());
    }

    #[when("the follower fetches in this order:")]
    fn follower_fetches(follower: &mut FollowerProgress, #[datatable] rows: Rows<FollowerFetch>) {
        let progress = follower.progress.as_mut().expect("a follower was given");
        for row in rows {
            progress.record_fetch(SimInstant(row.at_ms), row.fetch_offset, row.leader_end);
        }
    }

    #[then(
        "its fetch offset is {fetch_offset}, last fetch is {last_fetch} ms, caught-up time is {last_caught_up} ms, and last observed leader end is {last_end}"
    )]
    fn recorded_progress(
        follower: &FollowerProgress,
        fetch_offset: i64,
        last_fetch: u64,
        last_caught_up: u64,
        last_end: i64,
    ) {
        let expected = ReplicaProgress {
            fetch_offset,
            last_fetch: SimInstant(last_fetch),
            last_caught_up: SimInstant(last_caught_up),
            last_fetch_leader_log_end: last_end,
        };
        assert!(follower.progress.as_ref() == Some(&expected));
    }

    scenarios!("tests/features/migrated/src/role.feature", fixtures = [follower: FollowerProgress]);

    /// `LeaderState.isReplicaCaughtUp`: caught up at some point, and fetched
    /// within the last hour.
    #[yare::parameterized(
        never_fetched = { 0, 0, false },
        fetched_not_caught_up = { 10 * 3_600_000 - 1, 0, false },
        currently_caught_up = { 10 * 3_600_000 - 1, 10 * 3_600_000 - 1, true },
        caught_up_long_ago = { 10 * 3_600_000 - 1, 5 * 3_600_000, true },
        just_inside_window = {
            10 * 3_600_000 - 3_600_000 + 1,
            10 * 3_600_000 - 3_600_000 + 1,
            true,
        },
        exact_window_edge = { 10 * 3_600_000 - 3_600_000, 10 * 3_600_000 - 3_600_000, false },
        silent_two_hours = { 8 * 3_600_000, 8 * 3_600_000, false },
    )]
    fn caught_up_needs_a_caught_up_time_and_a_fetch_within_the_hour(
        last_fetch: u64,
        last_caught_up: u64,
        expected: bool,
    ) {
        const HOUR: u64 = 3_600_000;
        let now = SimInstant(10 * HOUR);
        let progress = ReplicaProgress {
            last_fetch: SimInstant(last_fetch),
            last_caught_up: SimInstant(last_caught_up),
            ..Default::default()
        };
        assert2::assert!(progress.is_caught_up(now) == expected);
    }

    /// A leader forgets an observer that has been silent for the hour, and no
    /// sooner: until then `is_caught_up` may still read it.
    #[yare::parameterized(
        never_fetched = { 0, false },
        fetched_moment_ago = { 10 * 3_600_000 - 1, true },
        silent_six_minutes = { 10 * 3_600_000 - 360_000, true },
        just_inside_hour = { 10 * 3_600_000 - 3_600_000 + 1, true },
        exactly_one_hour = { 10 * 3_600_000 - 3_600_000, false },
    )]
    fn a_replica_is_forgotten_only_after_an_hour_of_silence(last_fetch: u64, expected: bool) {
        const HOUR: u64 = 3_600_000;
        let now = SimInstant(10 * HOUR);
        let progress = ReplicaProgress {
            last_fetch: SimInstant(last_fetch),
            ..Default::default()
        };
        assert2::assert!(progress.fetched_within_caught_up_window(now) == expected);
    }

    #[test]
    fn role_defaults_to_unattached() {
        let r = Role::default();
        assert2::assert!((matches!(r, Role::Unattached { .. }), r.is_leader()) == (true, false));
    }
}
