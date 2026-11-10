//! KIP-392 replica selection.
//!
//! The partition leader runs `select` on every consumer Fetch that carries a
//! `client.rack` (`rack_id`) and reports the chosen node id in
//! `FetchResponse.preferred_read_replica`. Returning `-1` means "no preference,
//! read from the leader".

use std::{cmp::Reverse, time::Duration};

/// One replica's view as the leader sees it, for selection purposes.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaView {
    /// Wire replica id (broker node id as `i32`).
    pub node_id: i32,
    /// The broker's configured rack, if any.
    pub rack: Option<String>,
    /// Whether this replica is currently in the ISR.
    pub in_isr: bool,
    /// Whether this replica is a data-bearing witness. A witness replicates
    /// the partition and stays in the ISR, but it serves no client traffic.
    pub is_witness: bool,
    /// Kafka's `ReplicaView.logEndOffset`: the leader's own log end for the
    /// leader, and the last fetch offset the leader recorded for a follower.
    pub log_end_offset: i64,
    /// Kafka's `ReplicaView.timeSinceLastCaughtUpMs`: zero for the leader, and
    /// for a follower the time since its fetch offset last reached the
    /// leader's log end, [`Duration::MAX`] when it never has.
    pub time_since_caught_up: Duration,
}

impl ReplicaView {
    /// Kafka's `ReplicaView.comparator()`, the order of "most caught up":
    /// the higher log end offset, then the shorter time since last caught up,
    /// then the higher node id. The greatest key wins.
    fn caught_up_rank(&self) -> CaughtUpRank {
        CaughtUpRank {
            log_end_offset: self.log_end_offset,
            time_since_caught_up: Reverse(self.time_since_caught_up),
            node_id: self.node_id,
        }
    }
}

/// The sort key of [`ReplicaView::caught_up_rank`]. Its fields compare in
/// declaration order, which is the order of Kafka's comparator.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CaughtUpRank {
    log_end_offset: i64,
    time_since_caught_up: Reverse<Duration>,
    node_id: i32,
}

/// Which built-in selector the broker uses. Maps to Kafka's
/// `replica.selector.class`, but as a native enum. Krabka does not load
/// JVM classes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReplicaSelectorKind {
    /// Always read from the leader. Default.
    #[default]
    Leader,
    /// Prefer a same-rack in-sync replica when the client advertises a rack.
    RackAware,
}

impl ReplicaSelectorKind {
    /// Parse the `replica.selector` config value. Accepts `"leader"` and
    /// `"rack-aware"`. Returns `Err(value)` on anything else.
    /// # Errors
    /// Returns an error if the trimmed string is neither `leader` nor `rack-aware`.
    pub fn from_config_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "leader" => Ok(Self::Leader),
            "rack-aware" => Ok(Self::RackAware),
            other => Err(other.to_owned()),
        }
    }

    /// Choose the preferred read replica. Returns a node id, or `-1` for
    /// "no preference, use the leader".
    ///
    /// The rack-aware rule is Kafka's `RackAwareReplicaSelector.select`,
    /// followed by `ReplicaManager.findPreferredReadReplica` dropping a leader
    /// it selected from the response: a leader in the client's rack is the
    /// answer, and no follower outranks it. Otherwise the most caught-up
    /// same-rack in-sync follower wins, by log end offset, then by the shorter
    /// time since it last caught up, then by the higher node id. No same-rack
    /// candidate is the leader as well.
    ///
    /// A witness is never a candidate. It replicates the partition and counts
    /// toward the ISR, but it serves no client traffic. The rule matters most
    /// where the witness looks most attractive: a consumer whose `client.rack`
    /// names the witness site sees an in-ISR same-rack replica there, and a
    /// redirect would send every read to a broker that answers none.
    pub(crate) fn select(
        self,
        client_rack: Option<&str>,
        leader_id: i32,
        replicas: &[ReplicaView],
    ) -> i32 {
        match self {
            Self::Leader => -1,
            Self::RackAware => {
                let Some(rack) = client_rack.filter(|r| !r.is_empty()) else {
                    return -1;
                };
                let same_rack = |replica: &&ReplicaView| replica.rack.as_deref() == Some(rack);
                if replicas
                    .iter()
                    .filter(same_rack)
                    .any(|replica| replica.node_id == leader_id)
                {
                    return -1;
                }
                replicas
                    .iter()
                    .filter(same_rack)
                    .filter(|replica| replica.in_isr && !replica.is_witness)
                    .max_by_key(|replica| replica.caught_up_rank())
                    .map_or(-1, |replica| replica.node_id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_bdd::{Given, StepParamError, table},
        std::{fmt, str::FromStr},
    };

    #[yare::parameterized(
        leader = { "leader", Some(ReplicaSelectorKind::Leader) },
        rack_aware = { "rack-aware", Some(ReplicaSelectorKind::RackAware) },
        unknown = { "bogus", None },
    )]
    fn parse_known_values(input: &str, want: Option<ReplicaSelectorKind>) {
        assert2::assert!(
            ReplicaSelectorKind::from_config_str(input).ok() == want,
            "{input}"
        );
    }

    /// The selector a scenario's leader runs.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
    enum Selector {
        Leader,
        RackAware,
    }

    impl From<Selector> for ReplicaSelectorKind {
        fn from(selector: Selector) -> Self {
            match selector {
                Selector::Leader => Self::Leader,
                Selector::RackAware => Self::RackAware,
            }
        }
    }

    /// Kafka's `timeSinceLastCaughtUpMs`, in milliseconds, or `never` for a
    /// follower whose fetch offset never reached the leader's log end.
    #[derive(Debug, Clone, Copy)]
    struct SinceCaughtUp(Duration);

    impl FromStr for SinceCaughtUp {
        type Err = StepParamError;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            if text == "never" {
                return Ok(Self(Duration::MAX));
            }
            text.parse()
                .map(|millis| Self(Duration::from_millis(millis)))
                .map_err(|_| StepParamError::new("SinceCaughtUp", text, &["never", "<millis>"]))
        }
    }

    impl fmt::Display for SinceCaughtUp {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            if self.0 == Duration::MAX {
                f.write_str("never")
            } else {
                write!(f, "{}", self.0.as_millis())
            }
        }
    }

    /// A replica's configured rack: a name, `empty` for the empty string,
    /// or `none` for a broker with no rack.
    #[derive(Debug, Clone)]
    struct Rack(Option<String>);

    impl FromStr for Rack {
        type Err = StepParamError;

        fn from_str(text: &str) -> Result<Self, Self::Err> {
            Ok(Self(match text {
                "none" => None,
                "empty" => Some(String::new()),
                name => Some(name.to_owned()),
            }))
        }
    }

    impl fmt::Display for Rack {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self.0.as_deref() {
                None => f.write_str("none"),
                Some("") => f.write_str("empty"),
                Some(name) => f.write_str(name),
            }
        }
    }

    /// One replica as the leader sees it. A missing column is an in-sync,
    /// client-serving replica at log end 100 that caught up a moment ago.
    #[derive(Debug, krabka_bdd::TableRow)]
    struct Replica {
        node: i32,
        rack: Rack,
        in_sync: Option<bool>,
        witness: Option<bool>,
        log_end: Option<i64>,
        caught_up_ms_ago: Option<SinceCaughtUp>,
    }

    impl From<Replica> for ReplicaView {
        fn from(replica: Replica) -> Self {
            Self {
                node_id: replica.node,
                rack: replica.rack.0,
                in_isr: replica.in_sync.unwrap_or(true),
                is_witness: replica.witness.unwrap_or(false),
                log_end_offset: replica.log_end.unwrap_or(100),
                time_since_caught_up: replica
                    .caught_up_ms_ago
                    .map_or(Duration::ZERO, |since| since.0),
            }
        }
    }

    /// The partition's leader and replicas, and the replica the leader named.
    struct ReadReplicaSelection {
        leader: i32,
        replicas: Vec<ReplicaView>,
        preferred: Given<i32>,
    }

    #[rstest::fixture]
    fn read_replica() -> ReadReplicaSelection {
        ReadReplicaSelection {
            leader: -1,
            replicas: Vec::new(),
            preferred: Given::new(
                "preferred",
                "When a consumer in rack <rack> fetches through the <selector> selector",
            ),
        }
    }

    impl ReadReplicaSelection {
        fn select(&mut self, selector: ReplicaSelectorKind, client_rack: Option<&str>) {
            let preferred = selector.select(client_rack, self.leader, &self.replicas);
            self.preferred.set(preferred);
        }
    }

    #[krabka_bdd::given("the partition led by node {leader} has these replicas")]
    fn partition_replicas(
        read_replica: &mut ReadReplicaSelection,
        leader: i32,
        datatable: Vec<Vec<String>>,
    ) {
        let rows: Vec<Replica> = table::rows(&datatable).expect("replica table");
        read_replica.leader = leader;
        read_replica.replicas = rows.into_iter().map(ReplicaView::from).collect();
    }

    #[krabka_bdd::when("a consumer in rack {rack} fetches through the {selector} selector")]
    fn fetch_from_rack(read_replica: &mut ReadReplicaSelection, rack: String, selector: Selector) {
        read_replica.select(selector.into(), Some(&rack));
    }

    #[krabka_bdd::when("a consumer that sends no rack fetches through the rack-aware selector")]
    fn fetch_without_rack(read_replica: &mut ReadReplicaSelection) {
        read_replica.select(ReplicaSelectorKind::RackAware, None);
    }

    #[krabka_bdd::when(
        "a consumer that sends an empty rack fetches through the rack-aware selector"
    )]
    fn fetch_with_empty_rack(read_replica: &mut ReadReplicaSelection) {
        read_replica.select(ReplicaSelectorKind::RackAware, Some(""));
    }

    #[krabka_bdd::then("the fetch prefers node {node}")]
    fn prefers_node(read_replica: &ReadReplicaSelection, node: i32) {
        assert!(*read_replica.preferred.get() == node);
    }

    #[krabka_bdd::then("the fetch names no preferred read replica")]
    fn no_preferred_replica(read_replica: &ReadReplicaSelection) {
        assert!(*read_replica.preferred.get() == -1);
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/replica_selector",
        fixtures = [read_replica: ReadReplicaSelection],
    );
}
