//! Unit tests of the replica placement: the round-robin baseline, the
//! site-aware spread, and the validation of an explicit assignment list.

use assert2::assert;
use krabka_metadata::MetadataRecord;
use krabka_protocol::owned::create_topics_request::{CreatableReplicaAssignment, CreatableTopic};
use krabka_raft::NodeId;

use super::{codes, manual_replicas, resolve_assignments, round_robin_replicas, site_broker_views};
use crate::config_keys::resolve_preferred_leader_site;

/// One broker in each of the sites `a`, `b`, and `c`.
const THREE_SITES: [(u64, Option<&str>); 3] = [(1, Some("a")), (2, Some("b")), (3, Some("c"))];

/// Two brokers in each of the sites `a`, `b`, and `c`.
const SIX_BROKERS: [(u64, Option<&str>); 6] = [
    (1, Some("a")),
    (2, Some("b")),
    (3, Some("c")),
    (4, Some("a")),
    (5, Some("b")),
    (6, Some("c")),
];

/// A metadata image that registers `brokers` with their racks, marks
/// `witnesses` with the witness role, and pins `preferred_site` as the
/// cluster-wide default.
fn stretch_image(
    brokers: &[(u64, Option<&str>)],
    witnesses: &[u64],
    preferred_site: Option<&str>,
) -> krabka_metadata::MetadataImage {
    let mut image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
    for (node_id, rack) in brokers {
        image.apply(&MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                node_id: NodeId(*node_id),
                broker_epoch: 0,
                incarnation_id: uuid::Uuid::from_u128(u128::from(*node_id)),
                host: "127.0.0.1".into(),
                port: 9_092,
                rack: rack.map(str::to_string),
                endpoints: vec![],
                log_dirs: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));
    }
    for node_id in witnesses {
        image.apply(&MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: NodeId(*node_id),
                config_name: crate::config_keys::BROKER_WITNESS.into(),
                config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
            },
        ));
    }
    if let Some(site) = preferred_site {
        image.apply(&MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: crate::config_keys::STRETCH_PREFERRED_LEADER_SITE.into(),
                config_value: Some(site.into()),
            },
        ));
    }
    image
}

/// A topic request that asks for automatic placement.
fn auto_topic(partitions: i32, rf: i16) -> CreatableTopic {
    CreatableTopic {
        name: "orders".into(),
        num_partitions: partitions,
        replication_factor: rf,
        ..Default::default()
    }
}

/// The `(node id, site, witness)` triple of each view, in list order.
fn view_rows(views: &[super::SiteBrokerView]) -> Vec<(NodeId, Option<String>, bool)> {
    views
        .iter()
        .map(|view| (view.node_id, view.site.clone(), view.is_witness))
        .collect()
}

fn site_of(brokers: &[(u64, Option<&str>)], node_id: NodeId) -> String {
    brokers
        .iter()
        .find(|(id, _)| NodeId(*id) == node_id)
        .and_then(|(_, rack)| *rack)
        .expect("the placement returns a broker that declared a site")
        .to_string()
}

/// The sites of one replica list, sorted, so the caller can compare the
/// spread without depending on the replica order.
fn sites_of(brokers: &[(u64, Option<&str>)], replicas: &[NodeId]) -> Vec<String> {
    let mut sites = replicas
        .iter()
        .map(|node_id| site_of(brokers, *node_id))
        .collect::<Vec<_>>();
    sites.sort();
    sites
}

#[test]
fn manual_assignments_preserve_partition_order_and_validate_brokers() {
    let topic = CreatableTopic {
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![
            CreatableReplicaAssignment {
                partition_index: 1,
                broker_ids: vec![2, 1],
                ..Default::default()
            },
            CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![1, 2],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let assignments =
        manual_replicas(&topic, &[NodeId(1), NodeId(2)]).expect("valid manual assignments");
    assert!(assignments == vec![vec![NodeId(1), NodeId(2)], vec![NodeId(2), NodeId(1)]]);

    let mut unknown_broker = topic;
    unknown_broker.assignments[0].broker_ids[0] = 3;
    assert!(
        manual_replicas(&unknown_broker, &[NodeId(1), NodeId(2)])
            == Err(codes::INVALID_REPLICA_ASSIGNMENT)
    );
}

#[test]
fn three_brokers_three_partitions_rf_three() {
    let bs = vec![NodeId(1), NodeId(2), NodeId(3)];
    let out = round_robin_replicas(&bs, 3, 3);
    // Every broker should lead exactly one partition.
    let leaders: Vec<_> = out.iter().map(|r| r[0]).collect();
    let mut sorted = leaders.clone();
    sorted.sort_unstable();
    assert!(sorted == vec![NodeId(1), NodeId(2), NodeId(3)]);
    // Each partition has all three brokers as replicas.
    for replicas in &out {
        let mut s = replicas.clone();
        s.sort_unstable();
        assert!(s == vec![NodeId(1), NodeId(2), NodeId(3)]);
    }
}

#[test]
fn offset_per_partition_means_distinct_leaders() {
    let bs = vec![NodeId(1), NodeId(2), NodeId(3)];
    let out = round_robin_replicas(&bs, 3, 1);
    assert!(out == vec![vec![NodeId(1)], vec![NodeId(2)], vec![NodeId(3)]]);
}

#[test]
fn rf_too_high_returns_empty() {
    let bs = vec![NodeId(1), NodeId(2), NodeId(3)];
    let out = round_robin_replicas(&bs, 1, 5);
    assert!(out.is_empty());
}

#[test]
fn rf_one_single_broker_preserves_replica_shape() {
    let bs = vec![NodeId(1)];
    let out = round_robin_replicas(&bs, 2, 1);
    assert!(out == vec![vec![NodeId(1)], vec![NodeId(1)]]);
}

#[test]
fn site_broker_views_read_the_rack_and_the_witness_role() {
    let image = stretch_image(&[(3, Some("c")), (1, Some("a")), (2, None)], &[3], None);

    let views = site_broker_views(&image, NodeId(9));

    // The views come back in node-id order, whatever order the image
    // holds them in.
    let expected = vec![
        (NodeId(1), Some("a".to_string()), false),
        (NodeId(2), None, false),
        (NodeId(3), Some("c".to_string()), true),
    ];
    assert!(view_rows(&views) == expected);
}

#[test]
fn an_image_without_a_registration_places_on_this_broker_alone() {
    let image = stretch_image(&[], &[], None);

    let views = site_broker_views(&image, NodeId(7));

    assert!(view_rows(&views) == vec![(NodeId(7), None, false)]);
}

#[test]
fn three_sites_hold_one_replica_of_every_partition() {
    let image = stretch_image(&THREE_SITES, &[], None);
    let views = site_broker_views(&image, NodeId(1));

    let assignments =
        resolve_assignments(&auto_topic(4, 3), &views, None).expect("automatic placement");

    // Every list holds all three brokers, one for each site, and the
    // leader rotates over the sites.
    assert!(
        assignments
            == vec![
                vec![NodeId(1), NodeId(2), NodeId(3)],
                vec![NodeId(2), NodeId(3), NodeId(1)],
                vec![NodeId(3), NodeId(1), NodeId(2)],
                vec![NodeId(1), NodeId(2), NodeId(3)],
            ]
    );
}

#[test]
fn the_preferred_site_leads_every_partition() {
    let image = stretch_image(&SIX_BROKERS, &[], Some("b"));
    let views = site_broker_views(&image, NodeId(1));

    let assignments = resolve_assignments(
        &auto_topic(6, 3),
        &views,
        resolve_preferred_leader_site(&image),
    )
    .expect("automatic placement");

    let leader_sites = assignments
        .iter()
        .map(|replicas| site_of(&SIX_BROKERS, replicas[0]))
        .collect::<Vec<_>>();
    assert!(leader_sites == vec!["b"; 6]);
    let spread = assignments
        .iter()
        .map(|replicas| sites_of(&SIX_BROKERS, replicas))
        .collect::<Vec<_>>();
    assert!(spread == vec![vec!["a", "b", "c"]; 6]);
}

#[test]
fn a_witness_replicates_but_leads_no_partition() {
    let brokers = [(1, Some("a")), (2, Some("b")), (3, Some("w"))];
    let image = stretch_image(&brokers, &[3], None);
    let views = site_broker_views(&image, NodeId(1));

    let assignments =
        resolve_assignments(&auto_topic(6, 3), &views, None).expect("automatic placement");

    // The witness takes a replica of every partition, and leadership
    // rotates over the two brokers that serve clients.
    let holds_witness = assignments
        .iter()
        .map(|replicas| replicas.contains(&NodeId(3)))
        .collect::<Vec<_>>();
    assert!(holds_witness == vec![true; 6]);
    let leaders = assignments
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
fn a_cluster_without_racks_places_like_round_robin() {
    let image = stretch_image(&[(1, None), (2, None), (3, None)], &[], None);
    let views = site_broker_views(&image, NodeId(1));
    let node_ids = vec![NodeId(1), NodeId(2), NodeId(3)];

    for (partitions, rf) in [(1, 1), (3, 1), (3, 2), (4, 3), (5, 2)] {
        let assignments = resolve_assignments(&auto_topic(partitions, rf), &views, None)
            .expect("automatic placement");

        assert!(
            assignments == round_robin_replicas(&node_ids, partitions, rf),
            "partitions {partitions}, rf {rf}"
        );
    }
}

#[test]
fn a_manual_assignment_overrides_the_site_placement() {
    let image = stretch_image(&THREE_SITES, &[], Some("c"));
    let views = site_broker_views(&image, NodeId(1));
    let preferred_site = resolve_preferred_leader_site(&image);
    let manual = CreatableTopic {
        name: "orders".into(),
        num_partitions: -1,
        replication_factor: -1,
        assignments: vec![
            CreatableReplicaAssignment {
                partition_index: 0,
                broker_ids: vec![2, 1],
                ..Default::default()
            },
            CreatableReplicaAssignment {
                partition_index: 1,
                broker_ids: vec![1, 3],
                ..Default::default()
            },
        ],
        ..Default::default()
    };

    let assignments =
        resolve_assignments(&manual, &views, preferred_site).expect("manual assignments");

    assert!(assignments == vec![vec![NodeId(2), NodeId(1)], vec![NodeId(1), NodeId(3)]]);
    // The automatic placement of the same cluster leads in site `c`, so
    // the manual lists really did override it.
    let automatic = resolve_assignments(&auto_topic(2, 2), &views, preferred_site)
        .expect("automatic placement");
    assert!(automatic == vec![vec![NodeId(3), NodeId(1)], vec![NodeId(3), NodeId(2)]]);
}

#[test]
fn an_impossible_request_gives_no_assignment() {
    // The empty outer vec is what makes the handler report
    // INVALID_REPLICATION_FACTOR.
    let image = stretch_image(&THREE_SITES, &[], None);
    let views = site_broker_views(&image, NodeId(1));

    let too_many = resolve_assignments(&auto_topic(1, 4), &views, None).expect("no error code");

    assert!(too_many.is_empty());

    // A cluster of witnesses can lead no partition at all.
    let witnesses_only = stretch_image(&THREE_SITES, &[1, 2, 3], None);
    let views = site_broker_views(&witnesses_only, NodeId(1));

    let unleadable = resolve_assignments(&auto_topic(1, 3), &views, None).expect("no error code");

    assert!(unleadable.is_empty());
}
