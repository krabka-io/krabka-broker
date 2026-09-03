//! The `[oauthbearer]` TOML shape and its defaults.
//!
//! [`FileOAuthBearerConfig`] carries every knob the three SASL/OAUTHBEARER
//! validators share, and the endpoint fields in it decide which validator the
//! broker builds. The sibling `oauthbearer_apply` module reads this table and
//! constructs the validator.

use krabka_units::{Time, secs};
use schemars::JsonSchema;
use serde::Deserialize;

/// TOML shape of `[oauthbearer]`. Maps to
/// [`krabka_security::OAuthBearerValidator`]. Setting `jwks_endpoint_uri`
/// selects the signed-JWT validator; setting
/// `introspection_endpoint_uri` selects the RFC 7662 introspection
/// validator; the two endpoint URIs are mutually
/// exclusive. With neither set, the unsecured-JWS validator
/// (development only) is used, and that fallback is rejected at
/// config-load unless `allow_unsecured = true`.
#[derive(Debug, Clone, Default, Deserialize, JsonSchema, PartialEq)]
pub struct FileOAuthBearerConfig {
    /// Claim whose value becomes the principal name. Default `sub`.
    pub principal_claim_name: Option<String>,
    /// Optional `JsonPath` expression (RFC 9535, via
    /// jsonpath-rust) evaluated against the token claim set. Token is
    /// rejected when the expression yields empty/null/false. Compiled
    /// once at broker startup; malformed expressions panic with a
    /// descriptive error.
    pub custom_claim_check: Option<String>,
    /// Optional JWT `typ` header check. When set, JWT-mode
    /// validators (unsecured + signed JWS) require the JWT header's
    /// `typ` field to equal this string. Introspection-mode skips
    /// (no JWT header). Ignored when unset.
    pub valid_token_type: Option<String>,
    /// Clock-skew tolerance, in milliseconds, for `exp` / `iat` / `nbf`.
    /// Default 30000.
    pub allowable_clock_skew_ms: Option<i64>,

    /// JWKS endpoint URL. When set, tokens are validated as signed
    /// JWTs (RS256 / ES256) against the keys fetched from this URL, and the
    /// broker spawns a background refresher. When unset, the unsecured-JWS
    /// (`alg:none`) development validator is used.
    pub jwks_endpoint_uri: Option<String>,
    /// When set, the token `iss` claim must equal this. Signed
    /// validator only.
    pub valid_issuer_uri: Option<String>,
    /// When set, the token `aud` claim must contain this. Signed
    /// validator only.
    pub expected_audience: Option<String>,
    /// JWKS re-fetch interval, in milliseconds. Default 300000
    /// (5 minutes). Signed validator only.
    pub jwks_refresh_interval_ms: Option<u64>,

    /// PEM file containing the CA
    /// certificate(s) used to verify the `IdP`'s TLS certificate on ALL
    /// outbound HTTPS to the `IdP` — JWKS endpoint, introspection
    /// endpoint, and userinfo endpoint. When set, these are
    /// the *only* trust roots used for the outbound HTTPS (replaces the
    /// default webpki-roots — Strimzi-shaped). When unset, the broker
    /// uses reqwest's default rustls webpki-roots.
    pub idp_tls_trust: Option<std::path::PathBuf>,

    /// RFC 7662 introspection endpoint URL. When set,
    /// selects the introspection validator (mutually exclusive with
    /// `jwks_endpoint_uri`).
    pub introspection_endpoint_uri: Option<String>,

    /// Optional OIDC userinfo endpoint URL. When set, the
    /// introspection validator calls `GET userinfo` after a successful
    /// introspection and merges the profile claims over the
    /// introspection claims (introspection wins for `active`, `exp`,
    /// `iat`, `nbf`, `scope`, `client_id`, `sub`).
    pub userinfo_endpoint_uri: Option<String>,

    /// `client_id` the broker uses to authenticate (HTTP Basic
    /// Auth) against the introspection endpoint. Required when
    /// `introspection_endpoint_uri` is set.
    pub introspection_client_id: Option<String>,

    /// Filesystem path to a file containing the client
    /// secret the broker uses to authenticate against the introspection
    /// endpoint. Required when `introspection_endpoint_uri` is set.
    /// File-based (not literal) so secret material doesn't sit in the
    /// TOML; operator mounts a `Secret` and writes the mount path here.
    /// The file's trailing newline (if any) is stripped at config-load.
    pub introspection_client_secret_path: Option<std::path::PathBuf>,

    /// Timeout for the introspection (and userinfo) HTTP
    /// requests, in milliseconds. Default 10 000 (10 s).
    pub introspection_http_timeout_ms: Option<u64>,

    /// Alternate claim name for principal-name fallback.
    pub fallback_user_name_claim: Option<String>,
    /// Prepended on fallback only.
    pub fallback_user_name_prefix: Option<String>,
    /// `JsonPath` expression (RFC 9535) extracting groups.
    /// Compiled once at broker startup; malformed expression panics
    /// with descriptive error.
    pub groups_claim: Option<String>,
    /// When `groups_claim` resolves to a string, split on
    /// this delimiter.
    pub groups_claim_delimiter: Option<String>,

    /// Minimum pause (seconds) between on-demand JWKS refreshes
    /// triggered by validator signals (unknown-kid / bad-signature tokens).
    /// Defaults to 1 (Strimzi parity). Signed validator only.
    pub jwks_min_refresh_pause_seconds: Option<u32>,

    /// Maximum age (seconds) of the cached JWKS before validators
    /// reject tokens until the next successful refresh. Strimzi default 360
    /// (6 minutes). Unset = no expiry check. Fails
    /// closed on prolonged `IdP` outage. Signed validator only.
    pub jwks_expiry_seconds: Option<u32>,

    /// Opt-in for the unsecured-JWS (`alg:none`) development
    /// validator. Default false. With neither `jwks_endpoint_uri` nor
    /// `introspection_endpoint_uri` set and a listener enabling
    /// `OAUTHBEARER`, the broker refuses to start rather than silently
    /// trusting unsigned tokens; set this to true to accept that
    /// fallback in development. Never set it in production.
    pub allow_unsecured: Option<bool>,

    /// When true, the JWKS parser keeps keys regardless of `use`
    /// field. Default false (filter out `use=enc`). Some identity providers
    /// publish signing keys with `use="enc"` by mistake; operators set this
    /// to true to accept them. Signed validator only.
    pub jwks_ignore_key_use: Option<bool>,
}

/// Default timeout for outbound introspection / userinfo HTTP requests (10 s).
pub(super) const DEFAULT_INTROSPECTION_HTTP_TIMEOUT: Time = secs(10);

/// Default clock-skew tolerance for `exp` / `iat` / `nbf` checks. Matches the
/// `krabka_security` validators' built-in default.
pub(super) const DEFAULT_ALLOWABLE_CLOCK_SKEW: Time = secs(30);
