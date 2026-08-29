//! The `[authorization]` TOML shape and the OPA subtable.
//!
//! [`FileAuthorizationConfig`] selects which [`crate::authorizer::Authorizer`]
//! implementation the broker builds and carries the super-user bypass list
//! every implementation consults. [`FileOpaConfig`] holds the decision-endpoint
//! knobs the `opa` variant needs, with Strimzi's defaults.

use schemars::JsonSchema;
use serde::Deserialize;

/// TOML shape of `[authorization]`. `type` (renamed to `authz_type` on
/// the Rust side to avoid shadowing the keyword) defaults to
/// `AllowAll`; `super_users` is the principal bypass list consulted by
/// every concrete authorizer impl.
///
/// `deny_unknown_fields` so a misspelled `super_user` typo at the top
/// of the `[authorization]` block is rejected at parse time rather
/// than silently producing the wrong authorizer.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuthorizationConfig {
    #[serde(rename = "type", default)]
    pub authz_type: AuthzType,
    #[serde(default)]
    pub super_users: Vec<String>,
    /// `Some` iff `authz_type == Opa`. Required in that case;
    /// `apply_to` returns [`FileConfigError::MissingSection`] when
    /// omitted.
    pub opa: Option<FileOpaConfig>,
}

/// Which [`crate::authorizer::Authorizer`] impl to instantiate.
/// `snake_case` to match the spec's `type = "allow_all" | "simple" |
/// "opa"` wire shape.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AuthzType {
    #[default]
    AllowAll,
    Simple,
    Opa,
}

/// TOML shape of `[authorization.opa]`. Mirrors the constructor
/// arguments of [`crate::authorizer::opa::OpaAuthorizer::new`]. Defaults
/// are picked to match Strimzi's `KafkaAuthorizationOpa` (`50_000` LRU
/// entries, 1 h TTL, fail-closed on OPA error).
#[derive(Debug, Clone, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileOpaConfig {
    /// OPA decision endpoint URL — must include the data-API path,
    /// e.g. `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// **Security-sensitive.** Permit the operation when the OPA call
    /// fails (timeout, 5xx, parse error). When `true`, an OPA outage
    /// authorizes *every* request (fail-open). Default `false`
    /// (fail-closed) — omitting this field denies on error, matching the
    /// upstream Open Policy Agent Kafka plugin's `allow.on.error = false`.
    #[serde(default)]
    pub allow_on_error: bool,
    /// LRU cache capacity, in entries. Default `50_000`.
    #[serde(default = "default_opa_maximum_cache_size")]
    pub maximum_cache_size: usize,
    /// Decision TTL, in milliseconds. Default `3_600_000` (1 h).
    #[serde(default = "default_opa_expire_after_ms")]
    pub expire_after_ms: i64,
}

/// Default OPA decision-cache capacity, in entries. Matches Strimzi's
/// `KafkaAuthorizationOpa` default.
const DEFAULT_OPA_MAXIMUM_CACHE_SIZE: usize = 50_000;

/// Default OPA decision TTL: 1 hour, in milliseconds. Matches Strimzi's
/// `KafkaAuthorizationOpa` default.
const DEFAULT_OPA_EXPIRE_AFTER_MS: i64 = 60 * 60 * 1_000;

fn default_opa_maximum_cache_size() -> usize {
    DEFAULT_OPA_MAXIMUM_CACHE_SIZE
}

fn default_opa_expire_after_ms() -> i64 {
    DEFAULT_OPA_EXPIRE_AFTER_MS
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt as _};

    use super::*;
    use crate::file_config::FileConfig;

    #[test]
    fn super_users_toml_populates_broker_config_set() {
        let toml = r#"
super_users = ["ANONYMOUS", "admin"]
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        let expected: std::collections::HashSet<String> =
            ["ANONYMOUS".to_string(), "admin".to_string()].into();
        assert!(cfg.super_users == expected);
    }
    // `[authorization]` TOML section → `Arc<dyn Authorizer>`.

    fn test_principal(name: &str) -> krabka_security::Principal {
        krabka_security::Principal {
            name: name.into(),
            auth_method: krabka_security::AuthMethod::SaslPlain,
            groups: vec![],
        }
    }
    #[test]
    fn authorization_section_simple_builds_simple_acl_authorizer() {
        use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        let toml = r#"
[authorization]
type = "simple"
super_users = ["admin"]
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        assert!(
            cfg.super_users.contains("admin"),
            "[authorization].super_users must populate BrokerConfig.super_users for act-as parity"
        );
        // `admin` is a super-user → bypass returns Allow even with an
        // empty MetadataImage (no ACLs). This is the SimpleAclAuthorizer
        // contract; AllowAllAuthorizer would also Allow, but the
        // default-deny SimpleAcl behavior is exercised by the
        // explicit `type = "simple"` branch's own unit tests.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let admin = test_principal("admin");
        let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &admin,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert!(cfg.authorizer.authorize(&img, &req) == AuthorizationResult::Allow);

        // Non-super-user with no matching ACL → Deny (proves we got
        // SimpleAcl, not AllowAll).
        let alice = test_principal("alice");
        let req_alice = AuthorizationRequest {
            principal: &alice,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert!(
            cfg.authorizer.authorize(&img, &req_alice) == AuthorizationResult::Deny,
            "type=simple must default-deny non-super-users with no matching ACL"
        );
    }
    #[test]
    fn authorization_section_opa_builds_opa_authorizer() {
        use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        // `OpaAuthorizer::new` captures `Handle::try_current()` — needs
        // an active tokio runtime. `Runtime::new()` defaults to
        // multi-thread, which the OPA `block_in_place` bridge requires
        // for any actual HTTP call (super-user bypass below sidesteps
        // that).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let toml = r#"
[authorization]
type = "opa"
super_users = ["ANONYMOUS"]

[authorization.opa]
url = "http://opa.invalid:8181/v1/data/k/a"
allow_on_error = false
maximum_cache_size = 100
expire_after_ms = 60000
"#;
            let file: FileConfig = toml::from_str(toml).unwrap();
            let mut cfg = crate::config::BrokerConfig::default();
            file.apply_to(&mut cfg).unwrap();

            assert!(cfg.super_users.contains("ANONYMOUS"));

            // Smoke-check via the super-user bypass — no HTTP call is
            // made (and `opa.invalid` deliberately doesn't resolve).
            let img = MetadataImage::new(uuid::Uuid::nil());
            let anon = test_principal("ANONYMOUS");
            let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let req = AuthorizationRequest {
                principal: &anon,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "t",
                operation: AclOperation::Read,
            };
            assert!(
                cfg.authorizer.authorize(&img, &req) == AuthorizationResult::Allow,
                "OPA super-user bypass must short-circuit before any HTTP call"
            );
        });
    }
    #[test]
    fn opa_allow_on_error_defaults_to_fail_closed_when_omitted() {
        // L-6: omitting `allow_on_error` must default to fail-closed
        // (false), matching the upstream OPA Kafka plugin.
        let toml = r#"
url = "http://opa.invalid:8181/v1/data/k/a"
maximum_cache_size = 100
expire_after_ms = 60000
"#;
        let opa: FileOpaConfig = toml::from_str(toml).unwrap();
        assert!(!opa.allow_on_error, "allow_on_error must default to false");

        // And the built authorizer must Deny on OPA error (fail-closed).
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

            use crate::authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer};

            let auth = crate::authorizer::opa::OpaAuthorizer::new(
                std::collections::HashSet::new(),
                // Unresolvable host → every call errors.
                "http://opa.invalid:8181/v1/data/k/a".to_string(),
                opa.allow_on_error,
                opa.maximum_cache_size,
                Time::from_millis(opa.expire_after_ms),
                crate::config::BrokerConfig::default().opa_http_timeout,
            )
            .unwrap();
            let img = MetadataImage::new(uuid::Uuid::nil());
            let p = test_principal("alice");
            let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
            let req = AuthorizationRequest {
                principal: &p,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "t",
                operation: AclOperation::Read,
            };
            assert!(
                auth.authorize(&img, &req) == AuthorizationResult::Deny,
                "OPA outage with default allow_on_error must fail closed (Deny)"
            );
        });
    }
    #[test]
    fn opa_cache_defaults_match_documented_capacity_and_ttl() {
        let toml = r#"
url = "http://opa.invalid:8181/v1/data/k/a"
"#;
        let opa: FileOpaConfig = toml::from_str(toml).unwrap();

        assert!(opa.maximum_cache_size == 50_000);
        assert!(opa.expire_after_ms == 3_600_000);
    }
    #[test]
    fn authorization_section_absent_defaults_to_allow_all() {
        use krabka_metadata::{AclOperation, MetadataImage, ResourceType};

        use crate::authorizer::{AuthorizationRequest, AuthorizationResult};

        let file: FileConfig = toml::from_str("").unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();

        // Default authorizer is AllowAll — anyone gets Allow, including
        // a principal who isn't in any super-user set.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let anyone = test_principal("anyone");
        let host: std::net::SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let req = AuthorizationRequest {
            principal: &anyone,
            host: &host,
            resource_type: ResourceType::Topic,
            resource_name: "t",
            operation: AclOperation::Read,
        };
        assert!(cfg.authorizer.authorize(&img, &req) == AuthorizationResult::Allow);
    }
}
