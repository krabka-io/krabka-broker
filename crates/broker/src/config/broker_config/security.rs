//! Who may connect, and as whom: the listener registry and the credentials
//! the broker presents to its peers, the authorizer and the super-users that
//! bypass it, the policy objects that gate writes and signatures, and the TLS
//! and SASL settings, including the OAUTHBEARER validator and its JWKS
//! refresh plumbing.

// Link 4 of the `BrokerConfig` field chain: it adds this group to the
// fields collected so far and hands them to `coordinators_fields`.
macro_rules! security_fields {
    ($($collected:tt)*) => {
        coordinators_fields! {
            $($collected)*
            // ── Auth / listener registry ─────────────────────────────────────────
            /// Named listener definitions. When this list is empty,
            /// `effective_listeners()` builds a single PLAINTEXT listener from
            /// `listen_addr` and `advertised_listener`.
            pub listeners: Vec<ListenerSpec>,

            /// Protocol terminator for the controller listener. The default
            /// `Plaintext` keeps the legacy raw-TCP raft transport. Set it to
            /// `SaslPlaintext`, `Ssl`, or `SaslSsl` to require auth on inbound raft
            /// RPCs. Outbound raft RPCs also use auth when you pair this with
            /// `inter_broker_credentials`.
            pub controller_listener_protocol: krabka_security::ListenerProtocol,

            /// Name of the listener used for inter-broker traffic (raft, replication,
            /// heartbeat). Must match a name in `listeners` when `listeners` is
            /// non-empty. Default: `"PLAINTEXT"`.
            pub inter_broker_listener_name: String,

            /// Credentials the broker uses for outbound inter-broker connections.
            /// `None` means no SASL, which gives plaintext inter-broker traffic.
            /// This is the default.
            pub inter_broker_credentials: Option<InterBrokerCredentials>,

            /// Static PLAIN credentials: username → password. Empty by default.
            /// PLAIN auth stays disabled until you explicitly enable the mechanisms.
            pub plain_credentials: HashMap<String, String>,

            /// Usernames that bypass ACL checks (super-users). The
            /// `create_delegation_token` act-as gate reads this directly; the
            /// active [`crate::authorizer::Authorizer`] impl also reads it
            /// (`SimpleAclAuthorizer` / `OpaAuthorizer`). `file_config` populates
            /// both from the same `[authorization]` TOML stanza.
            pub super_users: std::collections::HashSet<String>,

            /// Pluggable cluster authorizer. There is one boxed instance for each
            /// broker, configured through `[authorization]` in `broker.toml`. The
            /// default is [`crate::authorizer::AllowAllAuthorizer`], an explicit
            /// "allow everything" policy.
            pub authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,

            /// KFC-7 schema validator: the registry client and its cache, shared by
            /// every produce on this broker. Configured through `[schema_registry]` in
            /// `broker.toml`. `None` is the default and means no `[schema_registry]`
            /// section was configured, so a topic that turns `schema.validation.*` on
            /// has nothing to validate against.
            pub schema_validator: Option<std::sync::Arc<crate::schema_validation::SchemaValidator>>,

            /// The operator key trust set, configured through the top-level
            /// `[[operator_keys]]` array in `broker.toml`.
            ///
            /// One set serves both signature paths: a freeze record's detached
            /// signature and a break-glass approval's. Empty is the default and means
            /// no operator key is provisioned, so nothing may demand a signature.
            pub operator_keys: OperatorKeys,

            /// Topic write-freeze policy, configured through `[freeze]`.
            pub freeze: FreezeConfig,

            /// Break-glass two-person rule policy, configured through `[break_glass]`.
            pub break_glass: BreakGlassConfig,

            /// TLS configuration. `None` means no TLS, and is the default.
            pub tls_config: Option<TlsConfig>,

            /// Which SASL mechanisms are enabled. An empty set means no SASL.
            pub enabled_sasl_mechanisms: Vec<SaslMechanism>,

            /// Validator for SASL/OAUTHBEARER bearer tokens. The broker reads it only
            /// when `OAuthBearer` is in `enabled_sasl_mechanisms`; the handshake does
            /// not advertise the mechanism otherwise. It defaults to the
            /// unsecured-JWS validator with principal claim `sub`. Set a JWKS
            /// endpoint in `[oauthbearer].jwks_endpoint_uri` to select the signed-JWT
            /// validator.
            pub oauthbearer_validator: krabka_security::OAuthBearerValidator,

            /// SASL/GSSAPI (Kerberos) configuration. `Some` only when `Gssapi` is in
            /// `enabled_sasl_mechanisms`; carries the service keytab path,
            /// `auth_to_local` rules, and KDC/realm settings for the initiate path.
            pub gssapi: Option<krabka_security::gssapi::GssapiConfig>,

            /// JWKS endpoint to fetch OAUTHBEARER signing keys from. `Some`
            /// only when `oauthbearer_validator` is the signed variant. When set,
            /// `Broker::start` spawns a background refresher that fetches this URL and
            /// rotates the validator's key set on `oauthbearer_jwks_refresh_interval`.
            pub oauthbearer_jwks_endpoint: Option<String>,

            /// How often to re-fetch the JWKS. Default 5 minutes.
            pub oauthbearer_jwks_refresh_interval: Time,

            /// Optional PEM path for outbound HTTPS to the `IdP`. JWKS,
            /// introspection, and userinfo all share it. `None` selects reqwest's
            /// default webpki-roots.
            pub oauthbearer_idp_tls_trust: Option<std::path::PathBuf>,

            /// Optional ceiling on OAUTHBEARER session lifetime. When set, the
            /// broker reports `session_lifetime_ms = min(token_exp_ms - now_ms,
            /// cap)` and the dispatch-loop re-auth timer fires at the clamped
            /// time. When unset, sessions last until the token's natural `exp`
            /// (the default).
            pub oauthbearer_max_session_lifetime: Option<Time>,

            /// Receiver half of the JWKS refresher signal channel.
            ///
            /// `apply_to` creates the channel pair. It connects the sender to the
            /// signed validator's `JwksHandle` and stores the receiver here.
            /// `Broker::start` calls `take()` on the receiver and passes it to
            /// `JwksRefresher`. This field is `None` when JWKS validation is not
            /// configured.
            ///
            /// The field is an `Arc<Mutex<…>>` so that the containing `BrokerConfig`
            /// stays `Clone`. Only `Broker::start` locks and takes the receiver, and
            /// there is only ever one `Broker::start` for each validator
            /// construction.
            pub oauthbearer_jwks_signal_rx:
                std::sync::Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<()>>>>,

            /// Shared timestamp of the last successful JWKS fetch.
            ///
            /// `apply_to` creates it as `AtomicI64::new(0)`. The validator's
            /// `JwksHandle` and the refresher both clone this `Arc`, so the
            /// validator's expiry check sees the refresher's writes.
            pub oauthbearer_jwks_last_successful_fetch_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

            /// Shared on-demand-refresh timestamp for rate-limiting.
            ///
            /// `apply_to` creates it, and `Broker::start` gives a clone to the
            /// refresher. The validator never reads it. It is refresher-only
            /// bookkeeping that `BrokerConfig` carries for symmetry.
            pub oauthbearer_jwks_last_on_demand_refresh_ms: std::sync::Arc<std::sync::atomic::AtomicI64>,

            /// Minimum pause between on-demand JWKS refreshes that validator signals
            /// trigger. `apply_to` sets it from
            /// `FileOAuthBearerConfig::jwks_min_refresh_pause_seconds`, and
            /// `Broker::start` passes it into `JwksRefresher`. Strimzi's default is
            /// 1 second, and this default is 1 second too.
            pub oauthbearer_jwks_min_on_demand_pause: Time,
        }
    };
}
