//! Unit tests of the `CreatePartitions` replica placement: the automatic
//! site-aware placement of the new partitions, and the validation of an
//! explicit `assignments` list.

use assert2::assert;

use super::*;
use crate::handlers::create_partitions::test_support::assn;

/// Brokers that declare no site. The placement of such a cluster is the
/// plain Kafka round-robin.
fn plain_brokers(node_ids: &[u64]) -> Vec<SiteBrokerView> {
    node_ids
        .iter()
        .map(|node_id| SiteBrokerView {
            node_id: NodeId(*node_id),
            site: None,
            is_witness: false,
        })
        .collect()
}

/// Brokers with a site each. A broker whose id is in `witnesses` also
/// carries the witness role.
fn site_brokers(brokers: &[(u64, &str)], witnesses: &[u64]) -> Vec<SiteBrokerView> {
    brokers
        .iter()
        .map(|(node_id, site)| SiteBrokerView {
            node_id: NodeId(*node_id),
            site: Some((*site).to_string()),
            is_witness: witnesses.contains(node_id),
        })
        .collect()
}

fn node_ids(brokers: &[SiteBrokerView]) -> Vec<NodeId> {
    brokers.iter().map(|broker| broker.node_id).collect()
}

fn site_of(brokers: &[(u64, &str)], node_id: NodeId) -> String {
    brokers
        .iter()
        .find(|(id, _)| NodeId(*id) == node_id)
        .map(|(_, site)| (*site).to_string())
        .expect("the placement returns a broker that declared a site")
}

/// The sites of one replica list, sorted, so the caller can compare the
/// spread without depending on the replica order.
fn sites_of(brokers: &[(u64, &str)], replicas: &[NodeId]) -> Vec<String> {
    let mut sites = replicas
        .iter()
        .map(|node_id| site_of(brokers, *node_id))
        .collect::<Vec<_>>();
    sites.sort();
    sites
}

#[test]
fn a_cluster_without_sites_places_like_round_robin() {
    let brokers = plain_brokers(&[0, 1, 2]);
    let out = resolve_new_partition_assignments(None, &brokers, 0, 3, 2, None)
        .expect("round-robin should succeed");
    assert!(out.len() == 3);
    for r in &out {
        assert!(r.len() == 2, "each replica list must be rf=2");
        for b in r {
            assert!(brokers.iter().any(|known| known.node_id == *b));
        }
    }
}

#[test]
fn a_mixed_rack_cluster_keeps_the_unracked_broker_placeable() {
    let brokers = vec![
        SiteBrokerView {
            node_id: NodeId(1),
            site: Some("a".into()),
            is_witness: false,
        },
        SiteBrokerView {
            node_id: NodeId(2),
            site: Some("b".into()),
            is_witness: false,
        },
        SiteBrokerView {
            node_id: NodeId(3),
            site: None,
            is_witness: false,
        },
    ];

    let assignments = resolve_new_partition_assignments(None, &brokers, 0, 3, 3, None)
        .expect("mixed-rack automatic placement");

    assert!(
        assignments
            .iter()
            .all(|replicas| replicas.contains(&NodeId(3)))
    );
}

#[test]
fn placement_continues_rotation_from_existing() {
    let brokers = plain_brokers(&[0, 1, 2]);
    // Topic already has 2 partitions; adding 2 more (so partitions 2..4).
    // Helper must return the *tail* of `round_robin_replicas(...,4,2)`,
    // i.e. the assignments for indices 2 and 3 — not start from rotation 0.
    let new_tail = resolve_new_partition_assignments(None, &brokers, 2, 2, 2, None)
        .expect("round-robin tail should succeed");
    let full = crate::handlers::create_topics::round_robin_replicas(&node_ids(&brokers), 4, 2);
    assert!(new_tail == full[2..]);
}

#[test]
fn three_sites_hold_one_replica_of_every_new_partition() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[]);

    let new_tail = resolve_new_partition_assignments(None, &brokers, 0, 4, 3, None)
        .expect("site placement should succeed");

    // Every list holds one broker of each site, and the leader rotates
    // over the sites.
    assert!(
        new_tail
            == vec![
                vec![NodeId(1), NodeId(2), NodeId(3)],
                vec![NodeId(2), NodeId(3), NodeId(1)],
                vec![NodeId(3), NodeId(1), NodeId(2)],
                vec![NodeId(1), NodeId(2), NodeId(3)],
            ]
    );
}

#[test]
fn the_preferred_site_leads_every_new_partition() {
    const SITES: [(u64, &str); 6] = [(1, "a"), (2, "b"), (3, "c"), (4, "a"), (5, "b"), (6, "c")];
    let brokers = site_brokers(&SITES, &[]);

    // The topic already has two partitions and grows to six.
    let new_tail = resolve_new_partition_assignments(None, &brokers, 2, 4, 3, Some("b"))
        .expect("site placement should succeed");

    let leader_sites = new_tail
        .iter()
        .map(|replicas| site_of(&SITES, replicas[0]))
        .collect::<Vec<_>>();
    assert!(leader_sites == vec!["b"; 4]);
    let spread = new_tail
        .iter()
        .map(|replicas| sites_of(&SITES, replicas))
        .collect::<Vec<_>>();
    assert!(spread == vec![vec!["a", "b", "c"]; 4]);
}

#[test]
fn a_witness_replicates_new_partitions_but_leads_none() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "w")];
    let brokers = site_brokers(&SITES, &[3]);

    let new_tail = resolve_new_partition_assignments(None, &brokers, 0, 6, 3, None)
        .expect("site placement should succeed");

    let holds_witness = new_tail
        .iter()
        .map(|replicas| replicas.contains(&NodeId(3)))
        .collect::<Vec<_>>();
    assert!(holds_witness == vec![true; 6]);
    let leaders = new_tail
        .iter()
        .map(|replicas| replicas[0])
        .collect::<Vec<_>>();
    assert!(
        leaders
            == vec![
                NodeId(1),
                NodeId(2),
                NodeId(1),
                NodeId(2),
                NodeId(1),
                NodeId(2),
            ]
    );
}

#[test]
fn site_placement_continues_rotation_from_existing() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[]);

    // The topic already has 2 partitions and grows to 5. The helper must
    // return the tail of the full five-partition placement, so the new
    // partitions do not restart the rotation.
    let new_tail = resolve_new_partition_assignments(None, &brokers, 2, 3, 3, None)
        .expect("site placement should succeed");

    let full = stretch_replicas(&brokers, 5, 3, None);
    assert!(new_tail == full[2..]);
}

#[test]
fn a_manual_assignment_overrides_the_site_placement() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[]);
    let provided = vec![assn(&[2, 3])];

    let manual = resolve_new_partition_assignments(Some(&provided), &brokers, 1, 1, 2, Some("a"))
        .expect("explicit assignments should pass validation");

    assert!(manual == vec![vec![NodeId(2), NodeId(3)]]);
    // The automatic placement of the same cluster leads in site `a`, so
    // the manual list really did override it.
    let automatic = resolve_new_partition_assignments(None, &brokers, 1, 1, 2, Some("a"))
        .expect("site placement should succeed");
    assert!(automatic == vec![vec![NodeId(1), NodeId(2)]]);
}

#[test]
fn a_cluster_of_witnesses_returns_invalid_rf() {
    const SITES: [(u64, &str); 3] = [(1, "a"), (2, "b"), (3, "c")];
    let brokers = site_brokers(&SITES, &[1, 2, 3]);

    let err = resolve_new_partition_assignments(None, &brokers, 0, 1, 3, None)
        .expect_err("a cluster that can lead no partition must fail");

    assert!(err.0 == codes::INVALID_REPLICATION_FACTOR);
}

#[test]
fn rf_exceeds_broker_count_returns_invalid_rf() {
    let brokers = plain_brokers(&[0, 1]);
    let err = resolve_new_partition_assignments(None, &brokers, 0, 1, 3, None)
        .expect_err("rf=3 against 2 brokers must fail");
    assert!(err.0 == codes::INVALID_REPLICATION_FACTOR);
}

#[test]
fn honored_assignments_pass_through_verbatim() {
    let brokers = plain_brokers(&[0, 1, 2, 3]);
    let provided = vec![assn(&[3, 1]), assn(&[2, 0]), assn(&[1, 3])];
    let out = resolve_new_partition_assignments(Some(&provided), &brokers, 0, 3, 2, None)
        .expect("explicit assignments should pass validation");
    assert!(
        out == vec![
            vec![NodeId(3), NodeId(1)],
            vec![NodeId(2), NodeId(0)],
            vec![NodeId(1), NodeId(3)],
        ]
    );
}

/// Kafka's `validateManualPartitionAssignment` refuses an explicit list with
/// these messages, on brokers 0 to 2 and a topic of replication factor 2.
#[test]
fn invalid_explicit_assignments_answer_kafkas_messages() {
    let brokers = plain_brokers(&[0, 1, 2]);
    let cases: [(&[&[i32]], &str); 5] = [
        (
            &[&[0, 1, 2]],
            "The manual partition assignment includes a partition with 3 replica(s), but this \
             is not consistent with previous partitions, which have 2 replica(s).",
        ),
        (
            &[&[1, 1]],
            "The manual partition assignment includes the broker 1 more than once.",
        ),
        (
            &[&[0, 9]],
            "The manual partition assignment includes broker 9, but no such broker is \
             registered.",
        ),
        (
            &[&[0, -1]],
            "The manual partition assignment includes broker -1, but no such broker is \
             registered.",
        ),
        (
            &[&[0, 1], &[]],
            "The manual partition assignment includes an empty replica list.",
        ),
    ];

    let (actual, expected): (Vec<_>, Vec<_>) = cases
        .into_iter()
        .map(|(lists, message)| {
            let provided: Vec<CreatePartitionsAssignment> =
                lists.iter().map(|list| assn(list)).collect();
            (
                resolve_new_partition_assignments(
                    Some(&provided),
                    &brokers,
                    0,
                    provided.len(),
                    2,
                    None,
                ),
                Err((codes::INVALID_REPLICA_ASSIGNMENT, message.to_owned())),
            )
        })
        .unzip();
    assert!(actual == expected);
}

/// An automatic placement that cannot put `rf` replicas on the brokers
/// answers Kafka's `INVALID_REPLICATION_FACTOR` message.
#[test]
fn an_unplaceable_rf_answers_kafkas_message() {
    let brokers = plain_brokers(&[0, 1]);

    let err = resolve_new_partition_assignments(None, &brokers, 0, 1, 3, None);

    assert!(
        err == Err((
            codes::INVALID_REPLICATION_FACTOR,
            "Unable to replicate the partition 3 time(s): The target replication factor of 3 \
             cannot be reached because only 2 broker(s) are registered or some brokers have \
             all their log directories cordoned."
                .to_owned(),
        ))
    );
}
