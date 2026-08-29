//! The three-site stretch deployment profile and the check that a node's
//! rack, roles and durability settings agree with it.

use crate::{BrokerError, config::BrokerConfig};

/// Number of sites a [`StretchProfile`] names: two data sites and one
/// witness site.
const STRETCH_SITE_COUNT: i64 = 3;

/// A three-site stretch deployment: two sites that serve clients and one
/// witness site that only replicates data and votes.
///
/// The profile is the same on every node of the cluster. Each node finds its
/// own place in it through [`BrokerConfig::rack`], which must name one of
/// [`sites`][StretchProfile::sites].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StretchProfile {
    /// The three site names, in no particular order. Each name is a
    /// [`BrokerConfig::rack`] value that some node of the cluster reports.
    pub sites: Vec<String>,
    /// The site that holds the witness nodes. Nodes there carry
    /// [`NodeRole::Witness`] and lead no partition.
    pub witness_site: String,
    /// The site that partition leadership prefers while both data sites are
    /// up. It must not be [`witness_site`][StretchProfile::witness_site],
    /// because a witness never leads.
    pub preferred_leader_site: String,
}

impl BrokerConfig {
    /// Checks the three-site stretch profile against the rest of the config.
    ///
    /// The profile describes the whole cluster, so every node validates the
    /// same site list. On top of that, each node checks its own place in the
    /// profile: its [`rack`][Self::rack] must name one of the sites, and its
    /// roles must agree with the site it names.
    ///
    /// The durability check reads the replication factor from
    /// [`offsets_topic_replication_factor`][Self::offsets_topic_replication_factor].
    /// The broker has no `default.replication.factor` knob, and that field is
    /// the broker-wide replication factor it applies to the topics it creates
    /// itself.
    pub(super) fn validate_stretch(&self) -> Result<(), BrokerError> {
        let Some(profile) = self.stretch.as_ref() else {
            return Ok(());
        };

        let site_count = i64::try_from(profile.sites.len()).unwrap_or(i64::MAX);
        if site_count != STRETCH_SITE_COUNT {
            return Err(BrokerError::StretchProfileNeedsThreeSites {
                count: profile.sites.len(),
            });
        }
        for (i, site) in profile.sites.iter().enumerate() {
            if profile.sites[..i].contains(site) {
                return Err(BrokerError::StretchProfileDuplicateSite { site: site.clone() });
            }
        }
        if !profile.sites.contains(&profile.witness_site) {
            return Err(BrokerError::StretchWitnessSiteUnknown {
                site: profile.witness_site.clone(),
            });
        }
        if !profile.sites.contains(&profile.preferred_leader_site) {
            return Err(BrokerError::StretchPreferredSiteUnknown {
                site: profile.preferred_leader_site.clone(),
            });
        }
        if profile.preferred_leader_site == profile.witness_site {
            return Err(BrokerError::StretchPreferredSiteIsWitness {
                site: profile.witness_site.clone(),
            });
        }

        let Some(rack) = self.rack.as_ref() else {
            return Err(BrokerError::StretchRequiresRack);
        };
        if !profile.sites.contains(rack) {
            return Err(BrokerError::StretchRackNotInProfile { rack: rack.clone() });
        }
        if *rack == profile.witness_site && !self.is_witness() {
            return Err(BrokerError::StretchWitnessSiteNeedsWitnessRole);
        }
        if self.is_witness() && *rack != profile.witness_site {
            return Err(BrokerError::StretchWitnessRoleOutsideWitnessSite { rack: rack.clone() });
        }

        if !krabka_verified::stretch::min_insync_is_site_loss_safe(
            i64::from(self.offsets_topic_replication_factor),
            site_count,
            i64::from(self.default_min_insync_replicas),
        ) {
            return Err(BrokerError::StretchMinInsyncUnsafe {
                min_insync: self.default_min_insync_replicas,
                replication_factor: self.offsets_topic_replication_factor,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::config::{NodeRole, test_support::witness_roles};

    #[test]
    fn rack_and_selector_default_off() {
        let c = BrokerConfig::default();
        assert!(c.rack == None);
        assert!(c.replica_selector == crate::replica_selector::ReplicaSelectorKind::Leader);
        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        assert!(t.rack == None);
        assert!(t.replica_selector == crate::replica_selector::ReplicaSelectorKind::Leader);
    }

    /// Two data sites plus a witness site, with leadership on `dc-a`.
    fn three_site_profile() -> StretchProfile {
        StretchProfile {
            sites: vec!["dc-a".to_string(), "dc-b".to_string(), "dc-w".to_string()],
            witness_site: "dc-w".to_string(),
            preferred_leader_site: "dc-a".to_string(),
        }
    }

    /// A node of [`three_site_profile`], in `rack` and with `roles`.
    ///
    /// `min.insync.replicas` is 2, the only value that is safe at the default
    /// replication factor of 3 spread over three sites.
    fn stretch_node(rack: &str, roles: Vec<NodeRole>) -> BrokerConfig {
        BrokerConfig {
            roles,
            rack: Some(rack.to_string()),
            stretch: Some(three_site_profile()),
            default_min_insync_replicas: 2,
            ..BrokerConfig::default()
        }
    }

    #[test]
    fn defaults_carry_no_stretch_profile_and_no_witness_role() {
        let d = BrokerConfig::default();
        check!(d.stretch == None);
        check!(!d.is_witness());
        check!(d.roles == vec![NodeRole::Controller, NodeRole::Broker]);

        let t = BrokerConfig::for_tests(std::path::PathBuf::from("/tmp"));
        check!(t.stretch == None);
        check!(!t.is_witness());
        check!(t.roles == vec![NodeRole::Controller, NodeRole::Broker]);
    }

    #[test]
    fn a_three_site_stretch_profile_validates() {
        let data = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
        data.validate()
            .expect("a data-site node of a valid profile");

        let witness = stretch_node("dc-w", witness_roles());
        witness
            .validate()
            .expect("a witness-site node of a valid profile");
    }

    #[test]
    fn stretch_profile_needs_exactly_three_sites() {
        let mut c = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
        let profile = c.stretch.as_mut().expect("profile");
        profile.sites = vec!["dc-a".to_string(), "dc-w".to_string()];
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchProfileNeedsThreeSites { count: 2 })
        ));
    }

    #[test]
    fn stretch_profile_rejects_a_repeated_site() {
        let mut c = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
        let profile = c.stretch.as_mut().expect("profile");
        profile.sites = vec!["dc-a".to_string(), "dc-a".to_string(), "dc-w".to_string()];
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchProfileDuplicateSite { site }) if site == "dc-a"
        ));
    }

    #[test]
    fn stretch_witness_site_must_name_one_of_the_sites() {
        let mut c = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
        c.stretch.as_mut().expect("profile").witness_site = "dc-elsewhere".to_string();
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchWitnessSiteUnknown { site }) if site == "dc-elsewhere"
        ));
    }

    #[test]
    fn stretch_preferred_leader_site_must_name_one_of_the_sites() {
        let mut c = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
        c.stretch.as_mut().expect("profile").preferred_leader_site = "dc-elsewhere".to_string();
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchPreferredSiteUnknown { site }) if site == "dc-elsewhere"
        ));
    }

    #[test]
    fn stretch_preferred_leader_site_must_not_be_the_witness_site() {
        let mut c = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
        c.stretch.as_mut().expect("profile").preferred_leader_site = "dc-w".to_string();
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchPreferredSiteIsWitness { site }) if site == "dc-w"
        ));
    }

    #[test]
    fn stretch_profile_requires_a_rack() {
        let c = BrokerConfig {
            stretch: Some(three_site_profile()),
            default_min_insync_replicas: 2,
            ..BrokerConfig::default()
        };
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchRequiresRack)
        ));
    }

    #[test]
    fn stretch_rack_must_name_one_of_the_sites() {
        let c = stretch_node("dc-elsewhere", vec![NodeRole::Controller, NodeRole::Broker]);
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchRackNotInProfile { rack }) if rack == "dc-elsewhere"
        ));
    }

    #[test]
    fn a_node_in_the_witness_site_needs_the_witness_role() {
        let c = stretch_node("dc-w", vec![NodeRole::Controller, NodeRole::Broker]);
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchWitnessSiteNeedsWitnessRole)
        ));
    }

    #[test]
    fn the_witness_role_is_rejected_outside_the_witness_site() {
        let c = stretch_node("dc-a", witness_roles());
        assert!(matches!(
            c.validate(),
            Err(BrokerError::StretchWitnessRoleOutsideWitnessSite { rack }) if rack == "dc-a"
        ));
    }

    #[test]
    fn stretch_rejects_a_min_insync_that_a_site_loss_would_break() {
        // Three replicas over three sites leave two after a site loss, so 2
        // is the only safe value. 1 loses an acknowledged write with one
        // broker, and 3 stalls every acks=all write while a site is down.
        for min_insync in [1, 3] {
            let mut c = stretch_node("dc-a", vec![NodeRole::Controller, NodeRole::Broker]);
            c.offsets_topic_replication_factor = 3;
            c.default_min_insync_replicas = min_insync;
            check!(matches!(
                c.validate(),
                Err(BrokerError::StretchMinInsyncUnsafe {
                    min_insync: got,
                    replication_factor: 3
                }) if got == min_insync
            ));
        }
    }
}
