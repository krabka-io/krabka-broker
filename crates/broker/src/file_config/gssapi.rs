//! The Kerberos TOML shapes: `[gssapi]` and `[inter_broker_credentials]`.
//!
//! [`FileGssapiConfig`] is the broker-global accept path for SASL/GSSAPI, and
//! [`FileInterBrokerCredentials`] is the credential this broker presents when
//! it dials a peer. Both name a keytab and a Kerberos service name, so they
//! share the protocol default for `sasl.kerberos.service.name`.

use krabka_units::Time;
use schemars::JsonSchema;
use serde::Deserialize;

/// Kafka protocol default for `sasl.kerberos.service.name`.
pub(super) const DEFAULT_KERBEROS_SERVICE_NAME: &str = "kafka";

/// TOML shape of `[gssapi]`. Maps to
/// [`krabka_security::gssapi::GssapiConfig`]. `principal_to_local_rules`
/// are parsed into `name::Rule` at `apply_to` time.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct FileGssapiConfig {
    pub keytab_path: std::path::PathBuf,
    /// `sasl.kerberos.service.name`. Defaults to `"kafka"` when omitted.
    pub service_name: Option<String>,
    /// `auth_to_local` rule specs, applied in order (first match wins).
    #[serde(default)]
    pub principal_to_local_rules: Vec<String>,
    /// Default Kerberos realm, used for principals that omit their realm.
    pub realm: Option<String>,
    /// KDC endpoint (e.g. `tcp://kdc:88`) that bypasses krb5.conf discovery;
    /// falls back to krb5.conf when omitted.
    pub kdc: Option<String>,
    /// Maximum tolerated difference between client and broker clocks.
    #[serde(default, with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<crate::file_config::schema_units::Duration>")]
    pub max_time_skew: Option<Time>,
}

/// TOML shape of `[inter_broker_credentials]`. A `type` discriminator
/// selects the variant. PLAIN/SCRAM inter-broker over TOML remain
/// intentionally unexposed.
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
pub enum FileInterBrokerCredentials {
    Gssapi {
        keytab_path: std::path::PathBuf,
        client_principal: String,
        service_name: Option<String>,
        kdc_url: String,
    },
    #[serde(rename = "oauth-bearer")]
    OAuthBearer {
        /// File containing the bearer token. A trailing newline is ignored.
        /// The token itself never appears in the parsed config's `Debug` form.
        token_path: std::path::PathBuf,
    },
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::secs;

    use crate::file_config::{FileConfig, FileConfigError};

    #[test]
    fn apply_to_gssapi_maps_all_fields() {
        let src = r#"
broker_id = 1
[gssapi]
keytab_path = "/etc/krabka/gssapi-keytab/keytab"
service_name = "kafka"
principal_to_local_rules = ["RULE:[1:$1@$0](.*@EXAMPLE.COM)s/@.*//", "DEFAULT"]
realm = "EXAMPLE.COM"
kdc = "tcp://kdc:88"
max_time_skew = "17s"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse [gssapi]");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).expect("apply [gssapi]");
        let g = cfg.gssapi.expect("gssapi config present");
        check!(g.keytab_path == std::path::PathBuf::from("/etc/krabka/gssapi-keytab/keytab"));
        check!(g.service_name.as_str() == "kafka");
        check!(g.principal_to_local_rules.len() == 2);
        // Second rule in the fixture is the bare DEFAULT rule.
        check!(matches!(
            g.principal_to_local_rules[1],
            krabka_security::gssapi::name::Rule::Default
        ));
        check!(g.realm.as_deref() == Some("EXAMPLE.COM"));
        check!(g.kdc.as_deref() == Some("tcp://kdc:88"));
        check!(g.max_time_skew == secs(17));
    }

    #[test]
    fn apply_to_gssapi_defaults_service_name_to_kafka() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["DEFAULT"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let gssapi = cfg.gssapi.unwrap();
        assert!(gssapi.service_name == "kafka");
        assert!(gssapi.max_time_skew == krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW);
    }

    #[test]
    fn apply_to_gssapi_accepts_zero_clock_skew() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
max_time_skew = "0s"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.gssapi.unwrap().max_time_skew == secs(0));
    }

    #[test]
    fn apply_to_gssapi_rejects_negative_clock_skew() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
max_time_skew = "-1s"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        assert!(file.apply_to(&mut cfg).is_err());
    }

    #[test]
    fn apply_to_gssapi_rejects_malformed_rule() {
        let src = r#"
[gssapi]
keytab_path = "/k/keytab"
principal_to_local_rules = ["NOT_A_RULE:::"]
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let err = file.apply_to(&mut cfg).unwrap_err();
        assert!(matches!(err, FileConfigError::InvalidConfig(_)));
    }
    #[test]
    fn apply_to_inter_broker_credentials_gssapi() {
        let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/etc/krabka/gssapi-keytab/keytab"
client_principal = "kafka@EXAMPLE.COM"
service_name = "kafka"
kdc_url = "tcp://kdc:88"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        let expected = crate::config::InterBrokerCredentials::Gssapi {
            keytab_path: std::path::PathBuf::from("/etc/krabka/gssapi-keytab/keytab"),
            client_principal: "kafka@EXAMPLE.COM".to_string(),
            service_name: "kafka".to_string(),
            kdc_url: "tcp://kdc:88".to_string(),
        };
        assert!(cfg.inter_broker_credentials == Some(expected));
    }

    #[test]
    fn apply_to_inter_broker_credentials_rejects_unknown_type() {
        // Unknown `type` variants are rejected at TOML parse time because
        // `FileInterBrokerCredentials` is a tagged enum with `deny_unknown_fields`.
        let src = r#"
[inter_broker_credentials]
type = "carrier-pigeon"
"#;
        assert!(toml::from_str::<FileConfig>(src).is_err());
    }

    #[test]
    fn apply_to_inter_broker_credentials_defaults_service_name_to_kafka() {
        let src = r#"
[inter_broker_credentials]
type = "gssapi"
keytab_path = "/k/keytab"
client_principal = "kafka@EXAMPLE.COM"
kdc_url = "tcp://kdc:88"
"#;
        let file: FileConfig = toml::from_str(src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.inter_broker_credentials.unwrap() {
            crate::config::InterBrokerCredentials::Gssapi { service_name, .. } => {
                assert!(service_name == "kafka");
            }
            other => panic!("expected Gssapi, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_inter_broker_credentials_oauthbearer_reads_redacted_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "header.payload.\n").unwrap();
        let src = format!(
            r#"
[inter_broker_credentials]
type = "oauth-bearer"
token_path = {}
"#,
            toml::Value::String(token_path.to_string_lossy().into_owned())
        );
        let file: FileConfig = toml::from_str(&src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        let Some(crate::config::InterBrokerCredentials::OAuthBearer {
            token_path: actual_path,
        }) = cfg.inter_broker_credentials
        else {
            panic!("expected OAuthBearer credentials");
        };
        assert!(actual_path == token_path);
        assert!(!format!("{actual_path:?}").contains("header.payload"));
    }

    #[test]
    fn apply_to_inter_broker_credentials_oauthbearer_rejects_empty_token_file() {
        let dir = tempfile::tempdir().unwrap();
        let token_path = dir.path().join("token");
        std::fs::write(&token_path, "\n").unwrap();
        let src = format!(
            r#"
[inter_broker_credentials]
type = "oauth-bearer"
token_path = {}
"#,
            toml::Value::String(token_path.to_string_lossy().into_owned())
        );
        let file: FileConfig = toml::from_str(&src).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        let error = file
            .apply_to(&mut cfg)
            .expect_err("empty bearer token is rejected");
        assert!(error.to_string().contains("token must be non-empty"));
    }
}
