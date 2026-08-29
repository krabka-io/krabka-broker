//! The `[process]` and `[stretch]` TOML shapes.
//!
//! [`FileProcessConfig`] carries the `KRaft` `process.roles` list, and
//! [`FileStretchConfig`] carries the three-site stretch layout. The stretch
//! table is all-or-nothing: a half-written profile would let the broker start
//! with a site layout no other node agrees on.

use schemars::JsonSchema;
use serde::Deserialize;

use super::FileConfigError;

/// `[process]` TOML section — `KRaft` `process.roles`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileProcessConfig {
    /// Role strings: `"controller"`, `"broker"`, `"witness"`
    /// (case-insensitive). Empty or absent leaves the `BrokerConfig` default
    /// `[Controller, Broker]`. A `"witness"` entry is a modifier: it must
    /// come with both of the other two roles.
    #[serde(default)]
    pub roles: Vec<String>,
}

/// `[stretch]` TOML section — the three-site stretch deployment.
///
/// The table is all-or-nothing. When it is present, all three fields must
/// be, because a half-built profile would let the broker start with a site
/// layout that no node agrees on.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileStretchConfig {
    /// The three site names. Each one is a `rack` value that some node of
    /// the cluster reports.
    pub sites: Option<Vec<String>>,
    /// The site that holds the witness nodes.
    pub witness_site: Option<String>,
    /// The site that partition leadership prefers while both data sites are
    /// up. It must not be the witness site.
    pub preferred_leader_site: Option<String>,
}

impl FileStretchConfig {
    /// Converts the TOML table into a [`crate::config::StretchProfile`].
    ///
    /// Site names are not checked against each other here. That is
    /// [`crate::config::BrokerConfig::validate`]'s work, which also sees the
    /// node's rack and roles.
    pub(super) fn into_profile(self) -> Result<crate::config::StretchProfile, FileConfigError> {
        Ok(crate::config::StretchProfile {
            sites: self.sites.ok_or_else(|| missing_stretch_field("sites"))?,
            witness_site: self
                .witness_site
                .ok_or_else(|| missing_stretch_field("witness_site"))?,
            preferred_leader_site: self
                .preferred_leader_site
                .ok_or_else(|| missing_stretch_field("preferred_leader_site"))?,
        })
    }
}

fn missing_stretch_field(name: &str) -> FileConfigError {
    FileConfigError::InvalidConfig(format!(
        "[stretch] is present but stretch.{name} is missing: a stretch profile needs sites, \
         witness_site, and preferred_leader_site together"
    ))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::file_config::{FileConfig, FileConfigError};

    #[test]
    fn process_roles_controller_only_from_toml() {
        let toml = r#"
            [process]
            roles = ["controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(cfg.roles == vec![crate::config::NodeRole::Controller]);
    }
    #[test]
    fn process_roles_both_from_toml() {
        let toml = r#"
            [process]
            roles = ["broker", "controller"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.roles
                == vec![
                    crate::config::NodeRole::Broker,
                    crate::config::NodeRole::Controller
                ]
        );
    }
    #[test]
    fn process_roles_witness_from_toml() {
        let toml = r#"
            [process]
            roles = ["broker", "controller", "witness"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.roles
                == vec![
                    crate::config::NodeRole::Broker,
                    crate::config::NodeRole::Controller,
                    crate::config::NodeRole::Witness
                ]
        );
        assert!(cfg.is_witness());
    }
    #[test]
    fn process_roles_are_case_insensitive() {
        let toml = r#"
            [process]
            roles = ["BROKER", "Controller", "WiTnEsS"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.roles
                == vec![
                    crate::config::NodeRole::Broker,
                    crate::config::NodeRole::Controller,
                    crate::config::NodeRole::Witness
                ]
        );
    }
    #[test]
    fn stretch_table_becomes_a_stretch_profile() {
        let toml = r#"
            rack = "dc-a"

            [stretch]
            sites = ["dc-a", "dc-b", "dc-w"]
            witness_site = "dc-w"
            preferred_leader_site = "dc-a"
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.stretch
                == Some(crate::config::StretchProfile {
                    sites: vec!["dc-a".to_string(), "dc-b".to_string(), "dc-w".to_string()],
                    witness_site: "dc-w".to_string(),
                    preferred_leader_site: "dc-a".to_string(),
                })
        );
    }
    #[test]
    fn absent_stretch_table_leaves_no_profile() {
        let fc: FileConfig = toml::from_str("").expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(cfg.stretch == None);
    }
    #[test]
    fn partial_stretch_table_names_the_missing_field() {
        for (toml, missing) in [
            (
                r#"
                [stretch]
                witness_site = "dc-w"
                preferred_leader_site = "dc-a"
                "#,
                "stretch.sites",
            ),
            (
                r#"
                [stretch]
                sites = ["dc-a", "dc-b", "dc-w"]
                preferred_leader_site = "dc-a"
                "#,
                "stretch.witness_site",
            ),
            (
                r#"
                [stretch]
                sites = ["dc-a", "dc-b", "dc-w"]
                witness_site = "dc-w"
                "#,
                "stretch.preferred_leader_site",
            ),
        ] {
            let fc: FileConfig = toml::from_str(toml).expect("parse");
            let mut cfg = crate::config::BrokerConfig::default();
            let error = fc
                .apply_to(&mut cfg)
                .expect_err("a half-built stretch profile is rejected");
            assert!(matches!(error, FileConfigError::InvalidConfig(_)));
            assert!(
                error.to_string().contains(missing),
                "error names the missing field {missing}: {error}"
            );
        }
    }
    #[test]
    fn process_roles_rejects_unknown_role() {
        let toml = r#"
            [process]
            roles = ["wizard"]
        "#;
        let fc: FileConfig = toml::from_str(toml).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        let err = fc.apply_to(&mut cfg).expect_err("unknown role rejected");
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }
    #[test]
    fn process_section_absent_leaves_default_roles() {
        let fc: FileConfig = toml::from_str("").expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        fc.apply_to(&mut cfg).expect("apply");
        assert!(
            cfg.roles
                == vec![
                    crate::config::NodeRole::Controller,
                    crate::config::NodeRole::Broker
                ]
        );
    }
}
