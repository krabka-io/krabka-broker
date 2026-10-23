//! The `[authorization]` TOML shape and the OPA subtable.
//!
//! [`FileAuthorizationConfig`] selects which [`crate::authorizer::Authorizer`]
//! implementation the broker builds and carries the super-user bypass list
//! every implementation consults. [`FileOpaConfig`] holds the decision-endpoint
//! knobs the `opa` variant needs, with Strimzi's defaults.

use {schemars::JsonSchema, serde::Deserialize};

/// TOML shape of `[authorization]`. `type` (renamed to `authz_type` on
/// the Rust side to avoid shadowing the keyword) defaults to
/// `AllowAll`; `super_users` is the principal bypass list consulted by
/// every concrete authorizer impl.
///
/// Omitting the whole block leaves the cluster with no authorizer, which is
/// what Kafka has when `authorizer.class.name` is unset: every principal is
/// allowed everything, and the ACL administration RPCs -- `DescribeAcls`,
/// `CreateAcls` and `DeleteAcls` -- answer `SECURITY_DISABLED` (54) rather
/// than an empty listing or a stored binding nothing would consult.
/// `kafka-acls` reports that refusal.
///
/// `deny_unknown_fields` so a misspelled `super_user` typo at the top
/// of the `[authorization]` block is rejected at parse time rather
/// than silently producing the wrong authorizer.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FileAuthorizationConfig {
    #[serde(rename = "type", default)]
    pub authz_type: AuthzType,
    /// Principals that bypass every ACL check, Kafka's `super.users`. The
    /// active authorizer and the delegation-token `act-as` gate both read it.
    /// An entry may use Kafka's `User:<name>` form. `User:ANONYMOUS` is
    /// accepted, as in Kafka. Empty is the default and grants no bypass.
    #[serde(default)]
    pub super_users: Vec<String>,
    /// `Some` iff `authz_type == Opa`. Required in that case;
    /// `apply_to` returns
    /// [`FileConfigError::MissingSection`][crate::file_config::FileConfigError::MissingSection]
    /// when omitted.
    pub opa: Option<FileOpaConfig>,
    /// Kafka's `allow.everyone.if.no.acl.found`. Only consulted by
    /// `type = "simple"`. When `true`, a request is allowed if NO ACL at all
    /// applies to the resource -- by type, name, and LITERAL/PREFIXED/wildcard
    /// pattern, regardless of principal, host, operation, or permission type.
    /// If at least one ACL applies to the resource and none of them matches,
    /// the request is still denied. Default `false`, matching Kafka's
    /// `StandardAuthorizer`.
    #[serde(default)]
    pub allow_everyone_if_no_acl_found: bool,
}

/// The principal name that a `super_users` entry names.
///
/// Kafka writes `super.users` entries as `User:<name>`, and an authorizer
/// matches the principal name of a connection, so the `User:` type prefix is
/// removed. An entry without the prefix is the name itself.
pub(super) fn super_user_name(entry: &str) -> String {
    entry.strip_prefix("User:").unwrap_or(entry).to_string()
}

/// Which [`crate::authorizer::Authorizer`] impl to instantiate.
/// `snake_case` to match the spec's `type = "allow_all" | "simple" |
/// "opa"` wire shape.
///
/// `allow_all` is the default and is not an authorizer: it allows every
/// principal every operation, and the ACL administration RPCs answer
/// `SECURITY_DISABLED` (54),
/// as Kafka does with no `authorizer.class.name`. `simple` and `opa` are
/// decision
/// points, and under either the ACL RPCs serve requests normally.
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

fn default_opa_maximum_cache_size() -> usize { DEFAULT_OPA_MAXIMUM_CACHE_SIZE }

fn default_opa_expire_after_ms() -> i64 { DEFAULT_OPA_EXPIRE_AFTER_MS }

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::{AuthorizationRequest, opa::OpaAuthorizer},
            config::BrokerConfig,
            file_config::{FileConfigError, test_support::configured_unwrap_parse},
            test_support::{peer, sasl_principal},
        },
        assert2::assert,
        krabka_units::{Time, convert::TimeExt as _},
    };

    /// Both `super_users` lists take Kafka's `User:<name>` form, and
    /// `User:ANONYMOUS` is a super user as in Kafka.
    #[test]
    fn super_users_take_the_kafka_principal_form() {
        let cases = [
            (
                "top level",
                r#"super_users = ["User:admin", "User:ANONYMOUS", "operator"]"#,
            ),
            (
                "[authorization]",
                r#"
[authorization]
type = "simple"
super_users = ["User:admin", "User:ANONYMOUS", "operator"]
"#,
            ),
        ];
        for (case, toml) in cases {
            let cfg = configured_unwrap_parse(toml).unwrap();

            let expected: std::collections::HashSet<String> = [
                "admin".to_string(),
                "ANONYMOUS".to_string(),
                "operator".to_string(),
            ]
            .into();
            assert!(cfg.super_users == expected, "{case}");
            assert!(cfg.validate().is_ok(), "{case}");
        }
    }

    #[test]
    fn super_users_toml_populates_broker_config_set() {
        let toml = r#"
super_users = ["operator", "admin"]
"#;
        let cfg = configured_unwrap_parse(toml).unwrap();

        let expected: std::collections::HashSet<String> =
            ["operator".to_string(), "admin".to_string()].into();
        assert!(cfg.super_users == expected);
    }
    // `[authorization]` TOML section → `Arc<dyn Authorizer>`.

    fn test_principal(name: &str) -> krabka_security::Principal { sasl_principal(name) }

    fn topic_read_request<'a>(
        principal: &'a krabka_security::Principal,
        host: &'a std::net::SocketAddr,
    ) -> AuthorizationRequest<'a> {
        AuthorizationRequest {
            principal,
            host,
            resource_type: krabka_metadata::ResourceType::Topic,
            resource_name: "t",
            operation: krabka_metadata::AclOperation::Read,
        }
    }

    #[test]
    fn authorization_section_resolves_super_users_without_building_an_authorizer() {
        let cfg = configured_unwrap_parse(
            r#"
super_users = ["top-level"]
[authorization]
type = "simple"
super_users = ["User:admin"]
"#,
        )
        .unwrap();
        assert!(cfg.super_users == ["admin".to_owned()].into());
        let authorization = cfg
            .authorization_config
            .as_ref()
            .expect("resolved authorization");
        assert!(authorization.authz_type == AuthzType::Simple);
        assert!(!authorization.allow_everyone_if_no_acl_found);
        assert!(!cfg.authorizer.is_configured());
    }

    #[yare::parameterized(
        omitted = { "", false },
        deny = { "allow_everyone_if_no_acl_found = false", false },
        allow = { "allow_everyone_if_no_acl_found = true", true },
    )]
    fn allow_everyone_if_no_acl_found_is_resolved(setting: &str, expected: bool) {
        let cfg =
            configured_unwrap_parse(&format!("[authorization]\ntype = \"simple\"\n{setting}\n"))
                .unwrap();
        assert!(
            cfg.authorization_config
                .expect("resolved authorization")
                .allow_everyone_if_no_acl_found
                == expected
        );
    }

    #[test]
    fn authorization_section_opa_resolves_without_a_tokio_runtime() {
        let cfg = configured_unwrap_parse(
            r#"
[authorization]
type = "opa"
super_users = ["User:operator"]
[authorization.opa]
url = "http://opa.invalid:8181/v1/data/k/a"
allow_on_error = false
maximum_cache_size = 100
expire_after_ms = 60000
"#,
        )
        .unwrap();
        assert!(cfg.super_users == ["operator".to_owned()].into());
        let authorization = cfg
            .authorization_config
            .as_ref()
            .expect("resolved authorization");
        assert!(authorization.authz_type == AuthzType::Opa);
        assert!(
            authorization.opa
                == Some(FileOpaConfig {
                    url: "http://opa.invalid:8181/v1/data/k/a".to_owned(),
                    allow_on_error: false,
                    maximum_cache_size: 100,
                    expire_after_ms: 60_000,
                })
        );
        assert!(!cfg.authorizer.is_configured());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn missing_opa_section_keeps_the_diagnostic() {
        let error = configured_unwrap_parse("[authorization]\ntype = \"opa\"\n")
            .expect_err("missing OPA section");
        assert!(matches!(
            error,
            FileConfigError::MissingSection(section)
                if section == "[authorization.opa]"
        ));
    }

    #[yare::parameterized(
        cache = { "maximum_cache_size = 0", "ZeroCache" },
        zero_ttl = { "expire_after_ms = 0", "InvalidCacheTtl" },
        negative_ttl = { "expire_after_ms = -1", "InvalidCacheTtl" },
    )]
    fn invalid_opa_scalars_fail_without_a_runtime(setting: &str, expected: &str) {
        let error = configured_unwrap_parse(&format!(
            "[authorization]\ntype = \"opa\"\n[authorization.opa]\nurl = \"http://opa.invalid\"\n{setting}"
        ))
        .expect_err("invalid OPA scalar");
        assert!(matches!(
            error,
            FileConfigError::OpaConfig(message) if message == expected
        ));
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
            use {
                crate::authorizer::{AuthorizationResult, Authorizer},
                krabka_metadata::MetadataImage,
            };

            let auth = OpaAuthorizer::new(
                std::collections::HashSet::new(),
                // Unresolvable host → every call errors.
                "http://opa.invalid:8181/v1/data/k/a".to_string(),
                opa.allow_on_error,
                opa.maximum_cache_size,
                Time::from_millis(opa.expire_after_ms),
                BrokerConfig::default().opa_http_timeout,
            )
            .unwrap();
            let img = MetadataImage::new(uuid::Uuid::nil());
            let p = test_principal("alice");
            let host = peer();
            let req = topic_read_request(&p, &host);
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
        use {crate::authorizer::AuthorizationResult, krabka_metadata::MetadataImage};

        let cfg = configured_unwrap_parse("").unwrap();

        // Default authorizer is AllowAll — anyone gets Allow, including
        // a principal who isn't in any super-user set.
        let img = MetadataImage::new(uuid::Uuid::nil());
        let peer_host = peer();
        let anyone = test_principal("anyone");
        let check_req = topic_read_request(&anyone, &peer_host);
        let auth_res = cfg.authorizer.authorize(&img, &check_req);
        assert!(auth_res == AuthorizationResult::Allow);
    }
}
