//! KIP-392 replica selection. The partition leader runs `select` on every
//! consumer Fetch that carries a `client.rack` (`rack_id`) and reports the
//! chosen node id in `FetchResponse.preferred_read_replica`. Returning `-1`
//! means "no preference, read from the leader".

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
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn from_config_str(s: &str) -> Result<Self, String> {
        match s.trim() {
            "leader" => Ok(Self::Leader),
            "rack-aware" => Ok(Self::RackAware),
            other => Err(other.to_string()),
        }
    }

    /// Choose the preferred read replica. Returns a node id, or `-1` for
    /// "no preference, use the leader".
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
                let winner = replicas
                    .iter()
                    .filter(|r| r.in_isr && !r.is_witness && r.rack.as_deref() == Some(rack))
                    .min_by_key(|r| r.node_id);
                match winner {
                    Some(r) if r.node_id != leader_id => r.node_id,
                    _ => -1,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn view(node_id: i32, rack: &str, in_isr: bool) -> ReplicaView {
        ReplicaView {
            node_id,
            rack: Some(rack.to_string()),
            in_isr,
            is_witness: false,
        }
    }

    fn witness(node_id: i32, rack: &str, in_isr: bool) -> ReplicaView {
        ReplicaView {
            is_witness: true,
            ..view(node_id, rack, in_isr)
        }
    }

    #[test]
    fn parse_known_values() {
        for (input, want) in [
            ("leader", ReplicaSelectorKind::Leader),
            ("rack-aware", ReplicaSelectorKind::RackAware),
        ] {
            assert!(
                ReplicaSelectorKind::from_config_str(input) == Ok(want),
                "{input}"
            );
        }
        assert!(ReplicaSelectorKind::from_config_str("bogus").is_err());
    }

    #[test]
    fn leader_kind_always_returns_minus_one() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert!(ReplicaSelectorKind::Leader.select(Some("b"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_picks_same_rack_isr_member() {
        let replicas = [view(1, "a", true), view(2, "b", true), view(3, "b", true)];
        // leader is node 1 (rack a); client in rack b -> lowest-id same-rack
        // ISR member is node 2.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == 2);
    }

    #[test]
    fn rack_aware_none_when_client_rack_missing() {
        let replicas = [view(1, "a", true), view(2, "b", true)];
        assert!(ReplicaSelectorKind::RackAware.select(None, 1, &replicas) == -1);
        assert!(ReplicaSelectorKind::RackAware.select(Some(""), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_none_when_no_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "a", true)];
        assert!(ReplicaSelectorKind::RackAware.select(Some("z"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_ignores_non_isr_same_rack_replica() {
        let replicas = [view(1, "a", true), view(2, "b", false)];
        // Node 2 is same-rack but out of ISR -> no redirect.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == -1);
    }

    #[test]
    fn rack_aware_never_redirects_a_consumer_to_a_witness() {
        // Node 1 leads in rack "a". The client rack is "b", the witness site,
        // so the witness there is an in-ISR same-rack replica and looks like
        // the best pick.
        for (name, replicas, want) in [
            (
                "witness is the only same-rack ISR member",
                vec![view(1, "a", true), witness(2, "b", true)],
                -1,
            ),
            (
                "a same-rack non-witness wins over a lower-id witness",
                vec![
                    view(1, "a", true),
                    witness(2, "b", true),
                    view(3, "b", true),
                ],
                3,
            ),
            (
                "no witness anywhere keeps the lowest-id same-rack ISR member",
                vec![view(1, "a", true), view(2, "b", true), view(3, "b", true)],
                2,
            ),
        ] {
            let got = ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas);
            assert!(got == want, "{name}: got {got}, want {want}");
        }
    }

    #[test]
    fn rack_aware_none_when_only_same_rack_replica_is_leader() {
        let replicas = [view(1, "b", true), view(2, "a", true)];
        // Client rack b matches only the leader (node 1) -> stay on leader.
        assert!(ReplicaSelectorKind::RackAware.select(Some("b"), 1, &replicas) == -1);
    }
}
