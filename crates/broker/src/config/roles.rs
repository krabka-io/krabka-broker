//! `KRaft` process roles: the [`NodeRole`] set a node declares, the
//! predicates the rest of the broker gates on, and the check that a witness
//! carries the roles it depends on.

use crate::{BrokerError, config::BrokerConfig};

/// `KRaft` `process.roles`. A node is a metadata-quorum `Controller`, a data
/// `Broker`, or both. Default is the combined set `[Controller, Broker]`.
///
/// [`Witness`][NodeRole::Witness] is a modifier on that set, not a
/// replacement for it. A witness node lists all three roles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodeRole {
    /// The node votes in the `__cluster_metadata` quorum.
    Controller,
    /// The node hosts partition replicas and registers as a broker.
    Broker,
    /// The node is a data-bearing witness.
    ///
    /// A witness keeps a full copy of every partition it replicates, so it
    /// counts toward `min.insync.replicas` and an `acks=all` write commits
    /// through it. It serves no client traffic and never leads a partition.
    /// The role exists for a stretch cluster with two full sites plus a
    /// cheap third site: the third site holds the data that makes any
    /// single-site loss survivable, without the hardware that serving
    /// clients would need.
    ///
    /// The role is a modifier. A witness must also carry
    /// [`Broker`][NodeRole::Broker], because it holds replicas, and
    /// [`Controller`][NodeRole::Controller], because it votes in the
    /// metadata quorum. [`BrokerConfig::validate`] rejects the other
    /// combinations.
    Witness,
}

impl BrokerConfig {
    /// True when this node hosts data partitions and registers as a broker.
    #[must_use]
    pub fn is_broker(&self) -> bool {
        self.roles.contains(&NodeRole::Broker)
    }

    /// True when this node participates in the `__cluster_metadata` quorum.
    #[must_use]
    pub fn is_controller(&self) -> bool {
        self.roles.contains(&NodeRole::Controller)
    }

    /// True when this node is a data-bearing witness.
    ///
    /// A witness replicates partition data and votes, but serves no client
    /// traffic and never leads a partition. See [`NodeRole::Witness`].
    #[must_use]
    pub fn is_witness(&self) -> bool {
        self.roles.contains(&NodeRole::Witness)
    }

    /// Checks that [`NodeRole::Witness`] comes with the roles it depends on.
    pub(super) fn validate_witness_roles(&self) -> Result<(), BrokerError> {
        if self.is_witness() {
            if !self.is_broker() {
                return Err(BrokerError::WitnessRequiresBrokerRole);
            }
            if !self.is_controller() {
                return Err(BrokerError::WitnessRequiresControllerRole);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_raft::NodeId;

    use super::*;
    use crate::config::test_support::witness_roles;

    #[test]
    fn defaults_to_combined_roles() {
        let d = BrokerConfig::default();
        assert!(
            (d.is_controller(), d.is_broker(), d.roles)
                == (true, true, vec![NodeRole::Controller, NodeRole::Broker]),
            "default node is a combined controller+broker with the combined role set"
        );

        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(t.is_controller() && t.is_broker());
    }

    #[test]
    fn controller_only_is_not_a_broker() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        assert!(c.is_controller());
        assert!(!c.is_broker());
    }

    #[test]
    fn broker_only_is_not_a_controller() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker],
            ..BrokerConfig::default()
        };
        assert!(c.is_broker());
        assert!(!c.is_controller());
    }

    #[test]
    fn rejects_empty_roles() {
        let c = BrokerConfig {
            roles: vec![],
            ..BrokerConfig::default()
        };
        assert!(matches!(c.validate(), Err(BrokerError::EmptyRoles)));
    }

    #[test]
    fn rejects_broker_only_node_listed_as_its_own_voter() {
        // node_id 1 is in the default single-voter quorum; a broker-only
        // node must not be a voter of itself.
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker],
            node_id: NodeId(1),
            controller_quorum_voters: vec![(NodeId(1), "127.0.0.1:9093".to_string())],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::NonControllerIsVoter {
                node_id: krabka_raft::NodeId(1)
            })
        ));
    }

    #[test]
    fn combined_default_passes_role_validation() {
        BrokerConfig::default()
            .validate()
            .expect("combined default validates");
    }

    #[test]
    fn controller_only_does_not_register() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        // Registration is gated on is_broker(); a controller-only node skips it.
        assert!(!c.is_broker());
    }

    #[test]
    fn controller_only_hosts_no_partitions() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller],
            ..BrokerConfig::default()
        };
        // Partition scan/recovery is gated on is_broker().
        assert!(!c.is_broker());
    }

    #[test]
    fn witness_node_is_also_a_broker_and_a_controller() {
        let c = BrokerConfig {
            roles: witness_roles(),
            ..BrokerConfig::default()
        };
        check!(c.is_witness());
        check!(c.is_broker());
        check!(c.is_controller());
    }

    #[test]
    fn witness_role_requires_the_broker_role() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Controller, NodeRole::Witness],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::WitnessRequiresBrokerRole)
        ));
    }

    #[test]
    fn witness_role_requires_the_controller_role() {
        let c = BrokerConfig {
            roles: vec![NodeRole::Broker, NodeRole::Witness],
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::WitnessRequiresControllerRole)
        ));
    }
}
