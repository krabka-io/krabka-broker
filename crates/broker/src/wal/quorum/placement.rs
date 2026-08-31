//! Voter placement helpers for diskless WAL quorums.

use krabka_metadata::{BrokerRegistrationRecord, NodeId};

/// Selects the WAL voters from distinct configured racks. An incomplete
/// result makes the caller fail closed instead of weakening the AZ-loss
/// durability guarantee.
pub(crate) fn select_voters(
    brokers: impl IntoIterator<Item = BrokerRegistrationRecord>,
    local_node: NodeId,
    voters: usize,
) -> Vec<NodeId> {
    let mut brokers = brokers
        .into_iter()
        .map(|broker| (broker.node_id, broker.rack))
        .collect::<Vec<_>>();
    brokers.sort_by_key(|(node_id, _)| node_id.0);
    select_voters_from_sorted_racks(&brokers, local_node, voters)
}

/// Select voters from broker/rack pairs already sorted by node id.
pub(crate) fn select_voters_from_sorted_racks(
    brokers: &[(NodeId, Option<String>)],
    local_node: NodeId,
    voters: usize,
) -> Vec<NodeId> {
    if voters == 0 {
        return Vec::new();
    }

    let mut selected = Vec::with_capacity(voters);
    let Some(local) = brokers
        .iter()
        .find(|(node_id, rack)| *node_id == local_node && rack.is_some())
    else {
        return selected;
    };
    selected.push(local.0);

    let rack_distinct = rack_distinct_candidates(brokers, &selected)
        .map(|(node_id, _)| *node_id)
        .collect::<Vec<_>>();
    for node_id in rack_distinct {
        if selected.len() == voters {
            return selected;
        }
        selected.push(node_id);
    }

    selected
}

fn rack_distinct_candidates<'a>(
    brokers: &'a [(NodeId, Option<String>)],
    selected: &'a [NodeId],
) -> impl Iterator<Item = &'a (NodeId, Option<String>)> {
    let mut used_racks = selected
        .iter()
        .filter_map(|node_id| {
            brokers
                .iter()
                .find(|(broker_id, _)| broker_id == node_id)
                .and_then(|(_, rack)| rack.as_deref())
        })
        .collect::<Vec<_>>();
    brokers.iter().filter(move |(node_id, rack)| {
        if selected.contains(node_id) {
            return false;
        }
        let Some(rack) = rack.as_deref() else {
            return false;
        };
        if used_racks.contains(&rack) {
            return false;
        }
        used_racks.push(rack);
        true
    })
}

#[cfg(test)]
mod tests {
    use krabka_metadata::BrokerEndpoint;
    use krabka_security::ListenerProtocol;

    use super::*;

    #[test]
    fn placement_prefers_rack_distinct_voters_with_local_first() {
        let selected = select_voters(
            [
                broker(3, Some("c")),
                broker(1, Some("a")),
                broker(2, Some("b")),
                broker(4, Some("a")),
            ],
            NodeId(1),
            3,
        );

        assert2::assert!((selected) == (vec![NodeId(1), NodeId(2), NodeId(3)]));
    }

    #[test]
    fn placement_refuses_to_weaken_the_rack_failure_budget() {
        let selected = select_voters(
            [broker(1, Some("a")), broker(2, Some("a")), broker(3, None)],
            NodeId(1),
            3,
        );

        assert2::assert!((selected) == (vec![NodeId(1)]));
    }

    #[test]
    fn placement_does_not_invent_an_unregistered_local_voter() {
        let selected = select_voters([broker(2, Some("a")), broker(3, Some("b"))], NodeId(1), 2);

        assert2::assert!(selected.is_empty());
    }

    #[test]
    fn placement_requires_a_rack_for_the_leader() {
        let selected = select_voters(
            [broker(1, None), broker(2, Some("a")), broker(3, Some("b"))],
            NodeId(1),
            3,
        );

        assert2::assert!(selected.is_empty());
    }

    fn broker(id: u64, rack: Option<&str>) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            node_id: NodeId(id),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: format!("broker-{id}"),
            port: 9092,
            rack: rack.map(str::to_string),
            log_dirs: vec![],
            endpoints: vec![BrokerEndpoint {
                name: "INTERNAL".into(),
                host: format!("broker-{id}"),
                port: 19092,
                protocol: ListenerProtocol::Plaintext,
            }],
            features: std::collections::BTreeMap::new(),
        }
    }
}
