//! Tests for the stretch-cluster replica placement: the fallback to Kafka's
//! round-robin placement, the site spread and its rotation, the preferred-site
//! and witness rules for `replicas[0]`, and the requests that must return no
//! assignment at all.
//!
//! They sit in their own file because checking the rules takes about as many
//! lines as stating them.

use assert2::assert;

use super::*;

fn broker(node_id: u64, site: Option<&str>, is_witness: bool) -> SiteBrokerView {
    SiteBrokerView {
        node_id: NodeId(node_id),
        site: site.map(str::to_string),
        is_witness,
    }
}

fn replica(node_id: u64, site: &str) -> SiteBrokerView {
    broker(node_id, Some(site), false)
}

fn witness(node_id: u64, site: &str) -> SiteBrokerView {
    broker(node_id, Some(site), true)
}

// Two brokers in each of the sites "a", "b", and "c", out of node-id order
// so every test also covers the sort.
fn six_brokers() -> Vec<SiteBrokerView> {
    vec![
        replica(5, "c"),
        replica(2, "a"),
        replica(6, "c"),
        replica(3, "b"),
        replica(1, "a"),
        replica(4, "b"),
    ]
}

fn site_of(brokers: &[SiteBrokerView], node_id: NodeId) -> &str {
    brokers
        .iter()
        .find(|broker| broker.node_id == node_id)
        .and_then(|broker| broker.site.as_deref())
        .expect("the placement returns a known broker with a site")
}

fn sites_of(brokers: &[SiteBrokerView], replicas: &[NodeId]) -> Vec<String> {
    let mut sites = replicas
        .iter()
        .map(|node_id| site_of(brokers, *node_id).to_string())
        .collect::<Vec<_>>();
    sites.sort();
    sites
}

fn distinct(replicas: &[NodeId]) -> Vec<NodeId> {
    let mut ids = replicas.to_vec();
    ids.sort_unstable();
    ids.dedup();
    ids
}

#[test]
fn no_site_anywhere_falls_back_to_kafka_round_robin() {
    let brokers = vec![
        broker(3, None, false),
        broker(1, None, false),
        broker(2, None, false),
    ];
    let sorted = vec![NodeId(1), NodeId(2), NodeId(3)];

    for (partitions, replication_factor) in [(1, 1), (3, 1), (3, 2), (4, 3), (5, 2)] {
        let expected = round_robin_replicas(&sorted, partitions, replication_factor);

        assert!(stretch_replicas(&brokers, partitions, replication_factor, None) == expected);
        // No site means no site to prefer, so the hint changes nothing.
        assert!(stretch_replicas(&brokers, partitions, replication_factor, Some("a")) == expected);
    }
}

#[test]
fn one_broker_per_site_rotates_the_replica_list() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), replica(3, "c")];

    let placement = stretch_replicas(&brokers, 4, 3, None);

    assert!(
        placement
            == vec![
                vec![NodeId(1), NodeId(2), NodeId(3)],
                vec![NodeId(2), NodeId(3), NodeId(1)],
                vec![NodeId(3), NodeId(1), NodeId(2)],
                vec![NodeId(1), NodeId(2), NodeId(3)],
            ]
    );
}

#[test]
fn three_sites_hold_one_replica_each() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), replica(3, "c")];

    let placement = stretch_replicas(&brokers, 7, 3, None);

    assert!(placement.len() == 7);
    for replicas in &placement {
        assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
    }
}

#[test]
fn the_preferred_site_leads_every_partition() {
    let brokers = six_brokers();

    let placement = stretch_replicas(&brokers, 9, 3, Some("b"));

    assert!(placement.len() == 9);
    for replicas in &placement {
        assert!(site_of(&brokers, replicas[0]) == "b");
        assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
    }
}

#[test]
fn the_witness_replicates_but_never_leads() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), witness(3, "w")];

    let placement = stretch_replicas(&brokers, 6, 3, None);

    assert!(placement.len() == 6);
    for replicas in &placement {
        assert!(replicas.contains(&NodeId(3)));
        assert!(replicas[0] != NodeId(3));
    }
}

#[test]
fn a_preferred_witness_site_still_leads_on_a_non_witness() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), witness(3, "w")];

    let placement = stretch_replicas(&brokers, 6, 3, Some("w"));

    assert!(placement.len() == 6);
    for replicas in &placement {
        assert!(site_of(&brokers, replicas[0]) != "w");
        assert!(sites_of(&brokers, replicas) == vec!["a", "b", "w"]);
    }
}

#[test]
fn the_partitions_spread_over_the_brokers_of_a_site() {
    let brokers = six_brokers();

    let placement = stretch_replicas(&brokers, 12, 3, None);

    assert!(placement.len() == 12);
    for replicas in &placement {
        assert!(sites_of(&brokers, replicas) == vec!["a", "b", "c"]);
    }
    // Both brokers of every site take a share of the partitions.
    let used = distinct(&placement.concat());
    assert!(
        used == vec![
            NodeId(1),
            NodeId(2),
            NodeId(3),
            NodeId(4),
            NodeId(5),
            NodeId(6),
        ]
    );
}

#[test]
fn a_replication_factor_above_the_site_count_balances_the_sites() {
    let brokers = six_brokers();

    let placement = stretch_replicas(&brokers, 6, 5, None);

    assert!(placement.len() == 6);
    for replicas in &placement {
        assert!(distinct(replicas).len() == 5);
        // Five replicas over three sites: no site holds a third one.
        let mut per_site = ["a", "b", "c"]
            .iter()
            .map(|site| {
                replicas
                    .iter()
                    .filter(|node_id| site_of(&brokers, **node_id) == *site)
                    .count()
            })
            .collect::<Vec<_>>();
        per_site.sort_unstable();
        assert!(per_site == vec![1, 2, 2]);
    }
}

#[test]
fn an_impossible_replication_factor_returns_no_assignment() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), replica(3, "c")];

    for replication_factor in [-1_i16, 0, 4, 100] {
        assert!(stretch_replicas(&brokers, 3, replication_factor, None).is_empty());
    }
}

#[test]
fn a_cluster_of_witnesses_cannot_lead_a_partition() {
    let brokers = vec![witness(1, "a"), witness(2, "b"), witness(3, "c")];

    assert!(stretch_replicas(&brokers, 3, 3, None).is_empty());
}

#[test]
fn a_broker_without_a_site_does_not_weaken_the_site_spread() {
    let brokers = vec![replica(1, "a"), replica(2, "b"), broker(3, None, false)];

    // Two sites can hold two replicas, but not three: the third broker
    // could be in either site, so the code does not place it.
    assert!(
        stretch_replicas(&brokers, 3, 2, None)
            == vec![
                vec![NodeId(1), NodeId(2)],
                vec![NodeId(2), NodeId(1)],
                vec![NodeId(1), NodeId(2)],
            ]
    );
    assert!(stretch_replicas(&brokers, 3, 3, None).is_empty());
}

#[test]
fn a_larger_partition_count_keeps_the_placement_of_the_first_partitions() {
    // `CreatePartitions` grows a topic: it places the whole topic again
    // and keeps the tail, so the placement of a partition must depend on
    // the partition index alone.
    let brokers = six_brokers();

    let three = stretch_replicas(&brokers, 3, 3, Some("a"));
    let seven = stretch_replicas(&brokers, 7, 3, Some("a"));

    assert!(seven[..3] == three[..]);
}

#[test]
fn the_same_input_always_gives_the_same_placement() {
    let brokers = six_brokers();
    let reversed = brokers.iter().rev().cloned().collect::<Vec<_>>();

    let placement = stretch_replicas(&brokers, 7, 3, Some("c"));

    assert!(stretch_replicas(&brokers, 7, 3, Some("c")) == placement);
    // The input order is not part of the result: the code sorts by node id.
    assert!(stretch_replicas(&reversed, 7, 3, Some("c")) == placement);
}
