//! Building the SASL/OAUTHBEARER validator from `[oauthbearer]`.
//!
//! `apply_oauthbearer` picks one of the three validators — signed JWS against
//! a JWKS endpoint, RFC 7662 introspection, or the unsecured development
//! validator — from which endpoint URI the table sets, compiles the `JsonPath`
//! expressions once, and parks the JWKS refresher's shared state on the broker
//! configuration.

#[cfg(not(target_family = "wasm"))]
use super::oauthbearer::DEFAULT_INTROSPECTION_HTTP_TIMEOUT;
#[cfg(not(target_family = "wasm"))]
use crate::oauth_introspection::ReqwestIntrospectionClient;
use {
    super::{
        FileConfigError,
        oauthbearer::{
            DEFAULT_ALLOWABLE_CLOCK_SKEW, DEFAULT_UNSECURED_ALLOWABLE_CLOCK_SKEW,
            FileOAuthBearerConfig,
        },
    },
    crate::config::{BrokerConfig, DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE},
    krabka_units::{Time, convert::TimeExt as _},
};

/// Set the principal claim and the clock skew of a JWS validator from
/// `[oauthbearer]`. An unset `allowable_clock_skew_ms` takes `default_skew`,
/// which differs between the unsecured and the signed validator in Kafka.
fn apply_jws_claim_options(
    principal_claim_name: &mut String,
    allowable_clock_skew: &mut Time,
    default_skew: Time,
    oauth: &FileOAuthBearerConfig,
) {
    if let Some(name) = oauth.principal_claim_name.clone() {
        *principal_claim_name = name;
    }
    *allowable_clock_skew = oauth
        .allowable_clock_skew_ms
        .map_or(default_skew, Time::from_millis);
}

#[cfg(not(target_family = "wasm"))]
fn configure_introspection_validator(
    oauth: &FileOAuthBearerConfig,
    endpoint: &str,
    custom_claim_check: Option<jsonpath_rust::parser::model::JpQuery>,
    groups_claim: Option<jsonpath_rust::parser::model::JpQuery>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    let client_id = oauth.introspection_client_id.clone().ok_or_else(|| {
        FileConfigError::InvalidConfig(
            "[oauthbearer]: introspection endpoint requires introspection_client_id".into(),
        )
    })?;
    let secret_path = oauth
        .introspection_client_secret_path
        .clone()
        .ok_or_else(|| {
            FileConfigError::InvalidConfig(
                "[oauthbearer]: introspection endpoint requires introspection_client_secret_path"
                    .into(),
            )
        })?;
    let client_secret = std::fs::read_to_string(&secret_path)
        .map_err(|error| {
            FileConfigError::InvalidConfig(format!(
                "[oauthbearer]: failed to read client secret {}: {error}",
                secret_path.display()
            ))
        })?
        .trim_end_matches(['\n', '\r'])
        .to_owned();
    let timeout = oauth
        .introspection_http_timeout_ms
        .map_or(DEFAULT_INTROSPECTION_HTTP_TIMEOUT, |ms| {
            Time::from_millis(i64::try_from(ms).unwrap_or(i64::MAX))
        });
    let client = ReqwestIntrospectionClient::build(
        endpoint.to_owned(),
        oauth.userinfo_endpoint_uri.clone(),
        client_id,
        client_secret,
        oauth.idp_tls_trust.as_deref(),
        timeout,
    )
    .map_err(|error| {
        FileConfigError::InvalidConfig(format!(
            "[oauthbearer]: failed to build OAuth introspection client: {error}"
        ))
    })?;
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
    Ok(())
}

/// RFC 7662 introspection needs an HTTP client stack, which wasm32-wasip1 does
/// not have.
#[cfg(target_family = "wasm")]
fn configure_introspection_validator(
    _oauth: &FileOAuthBearerConfig,
    endpoint: &str,
    _custom_claim_check: Option<jsonpath_rust::parser::model::JpQuery>,
    _groups_claim: Option<jsonpath_rust::parser::model::JpQuery>,
    _cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    Err(FileConfigError::InvalidConfig(format!(
        "[oauthbearer]: introspection_endpoint_uri {endpoint:?} is set, but token introspection is unavailable on this platform"
    )))
}

/// Whether any resolved listener offers SASL/OAUTHBEARER. A listener's own
/// `sasl_mechanisms` wins over the broker-wide list, matching
/// [`crate::config::BrokerConfig::validate`].
fn oauthbearer_listener_enabled(cfg: &BrokerConfig) -> bool {
    cfg.listeners.iter().any(|listener| {
        listener
            .sasl_mechanisms
            .as_ref()
            .unwrap_or(&cfg.enabled_sasl_mechanisms)
            .contains(&krabka_security::SaslMechanism::OAuthBearer)
    })
}

/// Build the SASL/OAUTHBEARER validator named by `[oauthbearer]`.
///
/// # Errors
///
/// Returns [`FileConfigError::InvalidConfig`] when the table selects no
/// endpoint, or is absent entirely — either of which would silently fall back
/// to the unsecured-JWS (`alg:none`) validator — while a listener enables
/// OAUTHBEARER and `allow_unsecured` is not `true`.
pub(super) fn apply_oauthbearer(
    oauth: Option<FileOAuthBearerConfig>,
    cfg: &mut BrokerConfig,
) -> Result<(), FileConfigError> {
    // An absent `[oauthbearer]` table is an endpoint-less one: it must still
    // reach the `(None, None)` arm below, or a listener enabling OAUTHBEARER
    // would keep the unsecured (`alg:none`) validator without the opt-in.
    let oauth = oauth.unwrap_or_default();
    // Thread the IdP trust-store path
    // unconditionally. Inert when no HTTPS-bound endpoint is set,
    // and harmlessly carried for the unsecured validator.
    cfg.oauthbearer_idp_tls_trust
        .clone_from(&oauth.idp_tls_trust);
    // Compile the JsonPath expressions once at load time.
    let custom_claim_check_compiled = oauth
        .custom_claim_check
        .as_deref()
        .map(|expr| {
            jsonpath_rust::parser::parse_json_path(expr).map_err(|error| {
                FileConfigError::InvalidConfig(format!(
                    "[oauthbearer]: invalid custom_claim_check JsonPath expression {expr:?}: {error}"
                ))
            })
        })
        .transpose()?;

    let groups_claim_compiled = oauth
        .groups_claim
        .as_deref()
        .map(|expr| {
            jsonpath_rust::parser::parse_json_path(expr).map_err(|error| {
                FileConfigError::InvalidConfig(format!(
                    "[oauthbearer]: invalid groups_claim JsonPath expression {expr:?}: {error}"
                ))
            })
        })
        .transpose()?;

    let unsecured_required_scope = oauth
        .unsecured_required_scope
        .as_deref()
        .map(|option| {
            krabka_security::parse_required_scope(option).map_err(|error| {
                FileConfigError::InvalidConfig(format!(
                    "[oauthbearer]: unsecured_required_scope {option:?} is invalid: {error}"
                ))
            })
        })
        .transpose()?
        .unwrap_or_default();

    match (
        oauth.jwks_endpoint_uri.as_ref(),
        oauth.introspection_endpoint_uri.as_ref(),
    ) {
        (Some(_), Some(_)) => {
            return Err(FileConfigError::InvalidConfig(
                "[oauthbearer]: jwks_endpoint_uri and introspection_endpoint_uri are mutually exclusive; configure exactly one"
                    .into(),
            ));
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
            let cache_generation = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
            let last_on_demand = std::sync::Arc::new(std::sync::atomic::AtomicI64::new(0));

            let handle = krabka_security::JwksHandle::new_with_refresher_handles(
                krabka_security::Jwks::empty(),
                last_successful.clone(),
                signal_tx,
            );

            let mut v = krabka_security::SignedJwsValidator::new(handle);
            apply_jws_claim_options(
                &mut v.principal_claim_name,
                &mut v.allowable_clock_skew,
                DEFAULT_ALLOWABLE_CLOCK_SKEW,
                &oauth,
            );
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
            cfg.oauthbearer_jwks_cache_generation = cache_generation;
            cfg.oauthbearer_jwks_last_on_demand_refresh_ms = last_on_demand;
            cfg.oauthbearer_jwks_min_on_demand_pause = oauth
                .jwks_min_refresh_pause_seconds
                .map_or(DEFAULT_JWKS_MIN_ON_DEMAND_PAUSE, |s| {
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
            )?;
        }
        (None, None) => {
            // Unsecured-JWS validation (development only), behind an
            // explicit opt-in so a misspelled endpoint key cannot quietly
            // downgrade a listener to trust-the-client.
            if oauth.allow_unsecured.unwrap_or(false) {
                tracing::warn!(concat!(
                    "[oauthbearer]: allow_unsecured = true - SASL/OAUTHBEARER tokens ",
                    "are accepted without signature verification (alg:none). ",
                    "Development only."
                ));
            } else if oauthbearer_listener_enabled(cfg) {
                return Err(FileConfigError::InvalidConfig(
                    concat!(
                        "[oauthbearer]: a listener enables OAUTHBEARER but neither ",
                        "jwks_endpoint_uri nor introspection_endpoint_uri is set; that ",
                        "falls back to the unsecured-JWS (alg:none) validator. Configure ",
                        "an endpoint, or set allow_unsecured = true for development."
                    )
                    .to_owned(),
                ));
            }
            let mut v = krabka_security::UnsecuredJwsValidator::default();
            apply_jws_claim_options(
                &mut v.principal_claim_name,
                &mut v.allowable_clock_skew,
                DEFAULT_UNSECURED_ALLOWABLE_CLOCK_SKEW,
                &oauth,
            );
            // JsonPath custom_claim_check + JWT typ check.
            v.custom_claim_check = custom_claim_check_compiled;
            v.valid_token_type.clone_from(&oauth.valid_token_type);
            // Claims mapping.
            v.fallback_user_name_claim = oauth.fallback_user_name_claim;
            v.fallback_user_name_prefix = oauth.fallback_user_name_prefix;
            v.groups_claim = groups_claim_compiled;
            v.groups_claim_delimiter = oauth.groups_claim_delimiter;
            // Kafka's scope options.
            if let Some(name) = oauth.unsecured_scope_claim_name {
                v.scope_claim_name = name;
            }
            v.required_scope = unsecured_required_scope;
            cfg.oauthbearer_validator = krabka_security::OAuthBearerValidator::Unsecured(v);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            config::BrokerConfig,
            file_config::{
                FileConfig, FileConfigError,
                test_support::{configured, configured_unwrap_parse},
            },
            test_support::Es256SigningKey,
        },
        assert2::{assert, check},
        base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD as B64},
        krabka_security::OAuthBearerValidator,
        krabka_units::{minutes, secs},
    };

    /// The instant every clock-skew case validates at, in epoch milliseconds.
    const NOW_MS: i64 = 2_000_000_000_000;

    /// An unsecured (`alg:none`) token for `sub = "alice"` with the given
    /// `exp`, in epoch seconds.
    fn unsecured_token(exp_s: i64) -> String {
        let header = B64.encode(br#"{"alg":"none"}"#);
        let payload = B64.encode(serde_json::json!({"sub": "alice", "exp": exp_s}).to_string());
        format!("{header}.{payload}.")
    }

    /// Install a fresh ES256 key in `validator`'s key set and return a token
    /// it signs for `sub = "alice"` with the given `exp`, in epoch seconds.
    fn signed_token(validator: &krabka_security::SignedJwsValidator, exp_s: i64) -> String {
        let key = Es256SigningKey::generate("skew");
        validator.key_handle().store(key.jwks());
        key.sign(&serde_json::json!({"sub": "alice", "exp": exp_s}))
    }

    /// Kafka's `OAuthBearerUnsecuredValidatorCallbackHandler` defaults
    /// `unsecuredValidatorAllowableClockSkewMs` to 0, while the signed path's
    /// `sasl.oauthbearer.clock.skew.seconds` defaults to 30. Each case builds
    /// the validator from `[oauthbearer]` and offers it a token whose `exp`
    /// lies `expired_by_s` seconds before now.
    #[yare::parameterized(
        unsecured_default_refuses_one_second_past_exp = {
            "allow_unsecured = true", 1, false
        },
        unsecured_explicit_skew_admits_one_second_past_exp = {
            "allow_unsecured = true\nallowable_clock_skew_ms = 5000", 1, true
        },
        unsecured_explicit_skew_refuses_past_the_skew = {
            "allow_unsecured = true\nallowable_clock_skew_ms = 5000", 6, false
        },
        signed_default_admits_one_second_past_exp = {
            "jwks_endpoint_uri = \"https://idp.example/jwks\"", 1, true
        },
        signed_default_admits_just_inside_thirty_seconds = {
            "jwks_endpoint_uri = \"https://idp.example/jwks\"", 29, true
        },
        signed_default_refuses_past_thirty_seconds = {
            "jwks_endpoint_uri = \"https://idp.example/jwks\"", 31, false
        },
        signed_explicit_zero_skew_refuses_one_second_past_exp = {
            "jwks_endpoint_uri = \"https://idp.example/jwks\"\nallowable_clock_skew_ms = 0",
            1,
            false
        },
    )]
    fn file_config_validator_applies_the_kafka_clock_skew_default(
        table: &str,
        expired_by_s: i64,
        admitted: bool,
    ) {
        let cfg = configured_unwrap_parse(&format!("[oauthbearer]\n{table}\n")).unwrap();
        let exp_s = NOW_MS / 1000 - expired_by_s;
        let outcome = match &cfg.oauthbearer_validator {
            OAuthBearerValidator::Unsecured(v) => v.validate(&unsecured_token(exp_s), NOW_MS),
            OAuthBearerValidator::Signed(v) => v.validate(&signed_token(v, exp_s), NOW_MS),
            OAuthBearerValidator::Introspection(_) => {
                panic!("expected an unsecured or signed validator; got introspection")
            }
        };
        assert2::assert!(outcome.is_ok() == admitted, "{table:?}: {outcome:?}");
    }

    /// `unsecured_required_scope` and `unsecured_scope_claim_name` are Kafka's
    /// `unsecuredValidatorRequiredScope` and `unsecuredValidatorScopeClaimName`.
    /// Each case builds the unsecured validator from `[oauthbearer]`, offers it
    /// a token for `alice` with the extra `claims`, and renders a refusal as
    /// the RFC 7628 challenge the broker sends.
    #[yare::parameterized(
        no_required_scope_admits_a_token_without_scope = { "", "", Ok("alice") },
        every_required_item_admits = {
            "unsecured_required_scope = \"a b\"", r#", "scope": ["b", "a", "c"]"#, Ok("alice")
        },
        a_missing_item_is_insufficient_scope = {
            "unsecured_required_scope = \"a b\"",
            r#", "scope": ["a"]"#,
            Err(r#"{"status":"insufficient_scope", "scope":"[a, b]"}"#)
        },
        a_blank_required_scope_requires_none = {
            "unsecured_required_scope = \"   \"", "", Ok("alice")
        },
        the_custom_claim_name_is_read = {
            "unsecured_required_scope = \"a\"\nunsecured_scope_claim_name = \"scp\"",
            r#", "scp": "a""#,
            Ok("alice")
        },
        the_default_claim_is_ignored_under_a_custom_name = {
            "unsecured_required_scope = \"a\"\nunsecured_scope_claim_name = \"scp\"",
            r#", "scope": "a""#,
            Err(r#"{"status":"insufficient_scope", "scope":"[a]"}"#)
        },
        a_numeric_custom_claim_is_an_invalid_token = {
            "unsecured_scope_claim_name = \"scp\"",
            r#", "scp": 7"#,
            Err(r#"{"status":"invalid_token"}"#)
        },
    )]
    fn file_config_unsecured_validator_applies_the_kafka_scope_options(
        table: &str,
        claims: &str,
        expected: Result<&str, &str>,
    ) {
        let cfg =
            configured_unwrap_parse(&format!("[oauthbearer]\nallow_unsecured = true\n{table}\n"))
                .unwrap();
        let OAuthBearerValidator::Unsecured(validator) = &cfg.oauthbearer_validator else {
            panic!(
                "expected the unsecured validator; got {:?}",
                cfg.oauthbearer_validator
            )
        };
        let header = B64.encode(br#"{"alg":"none"}"#);
        let exp_s = NOW_MS / 1000 + 60;
        let payload = B64.encode(format!(r#"{{"sub": "alice", "exp": {exp_s}{claims}}}"#));
        let outcome = validator
            .validate(&format!("{header}.{payload}."), NOW_MS)
            .map(|outcome| outcome.principal.name)
            .map_err(|error| krabka_security::oauth_error_json(&error));
        assert2::assert!(
            outcome == expected.map(str::to_owned).map_err(str::to_owned),
            "{table:?}"
        );
    }

    /// Kafka fails the unsecured validator's configuration on a required-scope
    /// item outside the RFC 6749 scope-token grammar.
    #[test]
    fn an_invalid_required_scope_item_fails_the_config() {
        let error = configured_unwrap_parse(
            "[oauthbearer]\nallow_unsecured = true\nunsecured_required_scope = 'a b\"c'\n",
        )
        .expect_err("an invalid scope item must fail the config");
        let FileConfigError::InvalidConfig(message) = &error else {
            panic!("expected InvalidConfig; got {error:?}")
        };
        assert!(
            message
                == "[oauthbearer]: unsecured_required_scope \"a b\\\"c\" is invalid: invalid scope value: b\"c"
        );
    }

    fn check_unsecured(cfg: &BrokerConfig) {
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        assert!(matches!(
            cfg.oauthbearer_validator,
            krabka_security::OAuthBearerValidator::Unsecured(_)
        ));
    }

    /// A client secret file, and the directory that holds it.
    struct ClientSecret {
        dir: tempfile::TempDir,
        path: std::path::PathBuf,
    }

    fn client_secret(value: &str) -> ClientSecret {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("client-secret");
        std::fs::write(&path, value).unwrap();
        ClientSecret { dir, path }
    }

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
        let cfg = configured(src, "parse").unwrap();
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

    /// A listener offering OAUTHBEARER plus an endpoint-less `[oauthbearer]`
    /// table — the shape a misspelled `jwks_endpoint_uri` key produces.
    fn oauthbearer_listener_toml(extra: &str) -> String {
        format!(
            r#"
[[listeners]]
name = "SASL"
bind_addr = "0.0.0.0:9092"
advertised = "host:9092"
protocol = "SaslPlaintext"
sasl_config = {{ enabled_mechanisms = ["OAUTHBEARER"] }}

[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
{extra}
"#
        )
    }

    #[test]
    fn apply_to_oauthbearer_without_endpoint_rejects_unsecured_fallback() {
        let file: FileConfig = toml::from_str(&oauthbearer_listener_toml("")).expect("parse");
        let mut cfg = BrokerConfig::default();
        let error = file
            .apply_to(&mut cfg)
            .expect_err("unsecured fallback must be rejected without allow_unsecured");
        let FileConfigError::InvalidConfig(message) = &error else {
            panic!("expected InvalidConfig; got {error:?}")
        };
        assert!(message.contains("allow_unsecured"));
    }

    #[test]
    fn apply_to_oauthbearer_allow_unsecured_opts_into_unsecured_validator() {
        let cfg = configured(
            &oauthbearer_listener_toml("allow_unsecured = true"),
            "parse",
        )
        .unwrap();
        assert!(cfg.oauthbearer_jwks_endpoint.is_none());
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Unsecured(v) => {
                assert!(v.allowable_clock_skew == secs(5));
            }
            other => {
                panic!("allow_unsecured must keep the unsecured validator; got {other:?}")
            }
        }
    }

    #[test]
    fn apply_to_oauthbearer_without_endpoint_stays_unsecured_when_no_listener_offers_it() {
        let src = r#"
[oauthbearer]
principal_claim_name = "sub"
allowable_clock_skew_ms = 5000
"#;
        let cfg = configured(src, "parse").unwrap();
        check_unsecured(&cfg);
    }

    #[test]
    fn apply_to_missing_oauthbearer_table_rejects_unsecured_fallback() {
        let src = r#"
[[listeners]]
name = "SASL"
bind_addr = "0.0.0.0:9092"
advertised = "host:9092"
protocol = "SaslPlaintext"
sasl_config = { enabled_mechanisms = ["OAUTHBEARER"] }
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = BrokerConfig::default();
        let error = file
            .apply_to(&mut cfg)
            .expect_err("a missing [oauthbearer] table must not silently stay unsecured");
        let FileConfigError::InvalidConfig(message) = &error else {
            panic!("expected InvalidConfig; got {error:?}")
        };
        assert!(message.contains("allow_unsecured"));
    }

    #[test]
    fn apply_to_missing_oauthbearer_table_applies_when_no_listener_offers_it() {
        let src = r#"
[[listeners]]
name = "PLAIN"
bind_addr = "0.0.0.0:9092"
advertised = "host:9092"
protocol = "Plaintext"
"#;
        let cfg = configured(src, "parse").unwrap();
        check_unsecured(&cfg);
    }

    #[test]
    fn apply_to_oauthbearer_threads_idp_tls_trust_to_broker_config() {
        let toml = r#"
[oauthbearer]
jwks_endpoint_uri = "https://idp.example/certs"
idp_tls_trust = "/etc/krabka/oauth/idp-ca.pem"
"#;
        let cfg = configured_unwrap_parse(toml).unwrap();
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
        let cfg = configured_unwrap_parse(toml).unwrap();
        assert!(cfg.oauthbearer_idp_tls_trust.is_none());
    }

    #[test]
    fn apply_to_oauthbearer_selects_introspection_validator_when_endpoint_set() {
        let ClientSecret {
            dir: _dir,
            path: secret_path,
        } = client_secret("the-secret");
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let cfg = configured_unwrap_parse(&toml).unwrap();
        assert!(matches!(
            cfg.oauthbearer_validator,
            krabka_security::OAuthBearerValidator::Introspection(_)
        ));
    }

    #[test]
    fn apply_to_oauthbearer_rejects_both_jwks_and_introspection_set() {
        let ClientSecret {
            dir: _dir,
            path: secret_path,
        } = client_secret("x");
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
        let error = configured_unwrap_parse(&toml).expect_err("endpoints are exclusive");
        assert!(error.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn apply_to_oauthbearer_introspection_requires_client_id() {
        let ClientSecret {
            dir: _dir,
            path: secret_path,
        } = client_secret("x");
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let error = configured_unwrap_parse(&toml).expect_err("client id is required");
        assert!(error.to_string().contains("introspection_client_id"));
    }

    #[test]
    fn apply_to_oauthbearer_introspection_requires_client_secret_path() {
        let toml = r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "kafka-broker"
"#;
        let error = configured_unwrap_parse(toml).expect_err("secret path is required");
        assert!(
            error
                .to_string()
                .contains("introspection_client_secret_path")
        );
    }

    #[yare::parameterized(
        custom_claim = { "custom_claim_check = '$['", "custom_claim_check" },
        groups_claim = { "groups_claim = '$['", "groups_claim" },
    )]
    fn invalid_jsonpath_claims_return_file_config_errors(option: &str, field: &str) {
        let error = configured_unwrap_parse(&format!("[oauthbearer]\n{option}\n"))
            .expect_err("invalid JsonPath must be a configuration error");
        assert2::assert!(error.to_string().contains(field));
        assert2::assert!(error.to_string().contains("JsonPath expression"));
    }

    #[test]
    fn unreadable_introspection_secret_returns_error_without_secret_contents() {
        let dir = tempfile::tempdir().unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "broker"
introspection_client_secret_path = '{}'
"#,
            dir.path().display()
        );

        let error =
            configured_unwrap_parse(&toml).expect_err("a directory is not a readable secret file");
        assert!(error.to_string().contains("failed to read client secret"));
    }

    #[test]
    fn introspection_client_build_error_does_not_disclose_secret() {
        let dir = tempfile::tempdir().unwrap();
        let secret_contents = "never-print-this-secret";
        let secret_path = dir.path().join("client-secret");
        let ca_path = dir.path().join("invalid-ca.pem");
        std::fs::write(&secret_path, secret_contents).unwrap();
        std::fs::write(&ca_path, "not a PEM certificate").unwrap();
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
introspection_client_id = "broker"
introspection_client_secret_path = '{}'
idp_tls_trust = '{}'
"#,
            secret_path.display(),
            ca_path.display()
        );

        let error = configured_unwrap_parse(&toml).expect_err("invalid TLS trust must be returned");
        assert!(
            error
                .to_string()
                .contains("failed to build OAuth introspection client")
        );
        assert!(!error.to_string().contains(secret_contents));
    }

    #[test]
    fn apply_before_runtime_overlay_returns_oauth_configuration_errors() {
        let file: FileConfig = toml::from_str(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
"#,
        )
        .expect("valid TOML");
        let mut cfg = BrokerConfig::default();

        let error = file
            .apply_before_runtime_overlay(&mut cfg)
            .expect_err("missing introspection credentials must be returned");
        assert!(error.to_string().contains("introspection_client_id"));
    }

    #[yare::parameterized(
        configured = { "userinfo_endpoint_uri = \"https://idp.example/userinfo\"\n", true },
        absent = { "", false },
    )]
    fn introspection_calls_userinfo_only_when_an_endpoint_is_configured(
        userinfo: &str,
        expected: bool,
    ) {
        let ClientSecret {
            dir: _dir,
            path: secret_path,
        } = client_secret("x");
        let toml = format!(
            r#"
[oauthbearer]
introspection_endpoint_uri = "https://idp.example/introspect"
{userinfo}introspection_client_id = "id"
introspection_client_secret_path = '{}'
"#,
            secret_path.display()
        );
        let cfg = configured_unwrap_parse(&toml).unwrap();
        match cfg.oauthbearer_validator {
            krabka_security::OAuthBearerValidator::Introspection(v) => assert2::assert!(
                v.call_userinfo == expected,
                "userinfo endpoint: {userinfo:?}"
            ),
            other => panic!("expected Introspection, got {other:?}"),
        }
    }
}
