//! Building the SASL/OAUTHBEARER validator from `[oauthbearer]`.
//!
//! `apply_oauthbearer` picks one of the three validators — signed JWS against
//! a JWKS endpoint, RFC 7662 introspection, or the unsecured development
//! validator — from which endpoint URI the table sets, compiles the `JsonPath`
//! expressions once, and parks the JWKS refresher's shared state on the broker
//! configuration.

use krabka_units::{Time, convert::TimeExt as _};

use super::oauthbearer::{
    DEFAULT_ALLOWABLE_CLOCK_SKEW, DEFAULT_INTROSPECTION_HTTP_TIMEOUT, FileOAuthBearerConfig,
};

fn configure_introspection_validator(
    oauth: &FileOAuthBearerConfig,
    endpoint: &str,
    custom_claim_check: Option<jsonpath_rust::parser::model::JpQuery>,
    groups_claim: Option<jsonpath_rust::parser::model::JpQuery>,
    cfg: &mut crate::config::BrokerConfig,
) {
    let client_id = oauth.introspection_client_id.clone().unwrap_or_else(|| {
        panic!("[oauthbearer]: introspection endpoint requires introspection_client_id")
    });
    let secret_path = oauth
        .introspection_client_secret_path
        .clone()
        .unwrap_or_else(|| {
            panic!(
                "[oauthbearer]: introspection endpoint requires introspection_client_secret_path"
            )
        });
    let client_secret = std::fs::read_to_string(&secret_path)
        .unwrap_or_else(|error| {
            panic!(
                "[oauthbearer]: failed to read client secret {}: {error}",
                secret_path.display()
            )
        })
        .trim_end_matches(['\n', '\r'])
        .to_owned();
    let timeout = oauth
        .introspection_http_timeout_ms
        .map_or(DEFAULT_INTROSPECTION_HTTP_TIMEOUT, |ms| {
            Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX))
        });
    let client = crate::oauth_introspection::ReqwestIntrospectionClient::build(
        endpoint.to_owned(),
        oauth.userinfo_endpoint_uri.clone(),
        client_id,
        client_secret,
        oauth.idp_tls_trust.as_deref(),
        timeout,
    )
    .unwrap_or_else(|error| panic!("failed to build OAuth introspection client: {error}"));
    cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Introspection(
        krabka_security::IntrospectionValidator {
            client,
            principal_claim_name: oauth
                .principal_claim_name
                .clone()
                .unwrap_or_else(|| "sub".into()),
            custom_claim_check,
            call_userinfo: oauth.userinfo_endpoint_uri.is_some(),
            allowable_clock_skew: oauth
                .allowable_clock_skew_ms
                .map_or(DEFAULT_ALLOWABLE_CLOCK_SKEW, Time::from_millis),
            expected_audience: oauth.expected_audience.clone(),
            fallback_user_name_claim: oauth.fallback_user_name_claim.clone(),
            fallback_user_name_prefix: oauth.fallback_user_name_prefix.clone(),
            groups_claim,
            groups_claim_delimiter: oauth.groups_claim_delimiter.clone(),
        },
    );
}

pub(super) fn apply_oauthbearer(
    oauth: Option<FileOAuthBearerConfig>,
    cfg: &mut crate::config::BrokerConfig,
) {
    let Some(oauth) = oauth else { return };
    // Thread the IdP trust-store path
    // unconditionally. Inert when no HTTPS-bound endpoint is set,
    // and harmlessly carried for the unsecured validator.
    cfg.oauthbearer_idp_tls_trust
        .clone_from(&oauth.idp_tls_trust);
    // Optional session-lifetime cap. Carried unconditionally;
    // the auth handler interprets None as "no cap".
    cfg.oauthbearer_max_session_lifetime = oauth
        .max_session_lifetime_seconds
        .map(|seconds| Time::from_secs(i64::from(seconds)));

    // Compile the JsonPath expression once at load time;
    // a malformed expression panics with a descriptive error.
    let custom_claim_check_compiled = oauth.custom_claim_check.as_deref().map(|expr| {
        jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid custom_claim_check JsonPath expression {expr:?}: {e}")
        })
    });

    // Compile groups_claim JsonPath at load time.
    let groups_claim_compiled = oauth.groups_claim.as_deref().map(|expr| {
        jsonpath_rust::parser::parse_json_path(expr).unwrap_or_else(|e| {
            panic!("[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {e}")
        })
    });

    match (
        oauth.jwks_endpoint_uri.as_ref(),
        oauth.introspection_endpoint_uri.as_ref(),
    ) {
        (Some(_), Some(_)) => {
            panic!(
                "[oauthbearer]: jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive; configure exactly one"
            );
        }
        (Some(_), None) => {
            // Signed-JWT validation. The empty key handle is
            // populated by the refresher `Broker::start` spawns.
            let jwks_uri = oauth.jwks_endpoint_uri.clone().unwrap();

            // Create the signal channel + the shared
            // timestamps here so the validator's `JwksHandle` and
            // the refresher (constructed in `Broker::start`) point at
            // the same Arc-shared state. Channel capacity 1 +
            // `try_send` on the producer ⇒ signals coalesce.
            let (signal_tx, signal_rx) = tokio::sync::mpsc::channel::<()>(1);
            let last_successful = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));
            let last_on_demand = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

            let handle = krabka_security::JwksHandle::new_with_refresher_handles(
                krabka_security::Jwks::empty(),
                last_successful.clone(),
                signal_tx,
            );

            let mut v = krabka_security::SignedJwsValidator::new(handle);
            if let Some(name) = oauth.principal_claim_name {
                v.principal_claim_name = name;
            }
            if let Some(skew) = oauth.allowable_clock_skew_ms {
                v.allowable_clock_skew = Time::from_millis(skew);
            }
            v.valid_issuer = oauth.valid_issuer_uri;
            v.expected_audience = oauth.expected_audience;
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check
                .clone_from(&custom_claim_check_compiled);
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim
                .clone_from(&oauth.fallback_user_name_claim);
            v.fallback_user_name_prefix
                .clone_from(&oauth.fallback_user_name_prefix);
            v.groups_claim.clone_from(&groups_claim_compiled);
            v.groups_claim_delimiter
                .clone_from(&oauth.groups_claim_delimiter);
            // Hard cache-expiry threshold.
            v.cache_expiry = oauth
                .jwks_expiry_seconds
                .map(|s| Time::from_secs(i64::from(s)));
            cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Signed(v);
            cfg.oauthbearer_jwks_endpoint = Some(jwks_uri);
            if let Some(ms) = oauth.jwks_refresh_interval_ms {
                cfg.oauthbearer_jwks_refresh_interval =
                    Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX));
            }

            // Park signal_rx + shared state for Broker::start.
            *cfg.oauthbearer_jwks_signal_rx.lock().unwrap() = Some(signal_rx);
            cfg.oauthbearer_jwks_last_successful_fetch_ms = last_successful;
            cfg.oauthbearer_jwks_last_on_demand_refresh_ms = last_on_demand;
            cfg.oauthbearer_jwks_min_on_demand_pause = oauth
                .jwks_min_refresh_pause_seconds
                .map_or(crate::config::DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE, |s| {
                    Time::from_secs(i64::from(s))
                });
            cfg.features.oauthbearer_jwks_ignore_key_use =
                oauth.jwks_ignore_key_use.unwrap_or(false);
        }
        (None, Some(introspect_uri)) => {
            configure_introspection_validator(
                &oauth,
                introspect_uri,
                custom_claim_check_compiled.clone(),
                groups_claim_compiled.clone(),
                cfg,
            );
        }
        (None, None) => {
            // Unsecured-JWS validation (development only).
            let mut v = krabka_security::UnsecuredJwsValidator::default();
            if let Some(name) = oauth.principal_claim_name {
                v.principal_claim_name = name;
            }
            if let Some(skew) = oauth.allowable_clock_skew_ms {
                v.allowable_clock_skew = Time::from_millis(skew);
            }
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check = custom_claim_check_compiled;
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim = oauth.fallback_user_name_claim;
            v.fallback_user_name_prefix = oauth.fallback_user_name_prefix;
            v.groups_claim = groups_claim_compiled;
            v.groups_claim_delimiter = oauth.groups_claim_delimiter;
            cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Unsecured(v);
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::{minutes, secs};

    use crate::file_config::FileConfig;

    #[test]
    fn apply_to_oauthbearer_jwks_selects_signed_validator() {
        let src = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
valid_issuer_uri = "https://idp.example"
expected_audience = "kafka"
principal_claim_name = "client_id"
jwks_refresh_interval_ms = 60000
jwks_expiry_seconds = 360
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.as_deref() == Some("https://idp.example/jwks"));
        assert!(cfg.oauthbearer_jwks_refresh_interval == minutes(1));
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Signed(v) => {
                check!(v.valid_issuer.as_deref() == Some("https://idp.example"));
                check!(v.expected_audience.as_deref() == Some("kafka"));
                check!(v.principal_claim_name.as_str() == "client_id");
                check!(v.cache_expiry == Some(secs(360)));
            }
            other => panic!("jwks_endpoint_uri must select the Signed validator; got {other:?}"),
        }
    }

    #[test]
    fn apply_to_oauthbearer_without_jwks_stays_unsecured() {
        let src = r#"
[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Unsecured(v) => {
                assert!(v.allowable_clock_skew == secs(5));
            }
            other => {
                panic!("no jwks_endpoint_uri must keep the unsecured validator; got {other:?}")
            }
        }
    }

    #[test]
    fn apply_to_oauthbearer_threads_idp_tls_trust_to_broker_config() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
idp_tls_trust = "/etc/krabka/oauth/idp-ca.pem"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(
            cfg.oauthbearer_idp_tls_trust.as_deref()
                == Some(std::path::Path::new("/etc/krabka/oauth/idp-ca.pem"))
        );
    }

    #[test]
    fn apply_to_oauthbearer_without_idp_tls_trust_leaves_field_none() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.oauthbearer_idp_tls_trust.is_none());
    }

    #[test]
    fn apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "the-secret").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(matches!(
            cfg.oauthbearer_validator,
            krabka_security::OAuthBearerValidator::Introspection(_)
        ));
    }

    #[test]
    #[should_panic(expected = "mutually exclusive")]
    fn apply_to_oauthbearer_rejects_both_jwks_and_introspection_set() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/jwks"
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_id")]
    fn apply_to_oauthbearer_introspection_requires_client_id() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    #[should_panic(expected = "introspection_client_secret_path")]
    fn apply_to_oauthbearer_introspection_requires_client_secret_path() {
        let toml = r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
"#;
        let file: FileConfig = toml::from_str(toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
    }

    #[test]
    fn apply_to_oauthbearer_introspection_with_userinfo_sets_call_userinfo_true() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
userinfo_endpoint_uri = "https://idp.example/userinfo"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Introspection(v) => assert!(v.call_userinfo),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }

    #[test]
    fn apply_to_oauthbearer_introspection_without_userinfo_sets_call_userinfo_false() {
        let dir = tempfile::tempdir().unwrap();
        let secret_path = dir.path().join("client-secret");
        std::fs::write(&secret_path, "x").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let file: FileConfig = toml::from_str(&toml).unwrap();
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Introspection(v) => assert!(!v.call_userinfo),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }
}
