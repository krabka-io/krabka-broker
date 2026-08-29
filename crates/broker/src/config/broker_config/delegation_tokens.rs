//! KIP-48 delegation tokens: the HMAC master key that mints and verifies
//! them, and the lifetime, renew period, and expiry sweep that bound how long
//! an issued token stays usable.

// Link 7 of the `BrokerConfig` field chain: it appends this group to
// the fields collected so far and forwards them to `remote_storage_fields`.
macro_rules! delegation_tokens_fields {
    ($($collected:tt)*) => {
        $crate::config::broker_config::remote_storage::remote_storage_fields! {
            $($collected)*
            /// KIP-48: HMAC-SHA-256 master key that mints and verifies delegation
            /// tokens. When `None`, the broker rejects all four delegation-token RPCs
            /// with `DELEGATION_TOKEN_AUTH_DISABLED`, and SCRAM cannot fall back to
            /// token lookup. The broker reads the key from
            /// `KRABKA_DELEGATION_TOKEN_SECRET_KEY` or from `[delegation_token]
            /// secret_key` in `broker.toml`; the environment variable wins. The key
            /// is wrapped in `SecretBytes`, so `Debug` redacts the bytes.
            pub delegation_token_secret_key: Option<krabka_security::SecretBytes>,

            /// KIP-48: hard upper bound on delegation-token lifetime.
            /// A token's `max_timestamp_ms` is set to
            /// `issue_timestamp_ms + delegation_token_max_lifetime` and the
            /// renew handler clamps any caller-requested expiry to this. Default
            /// 7 days (`delegation.token.max.lifetime.ms` in Kafka).
            pub delegation_token_max_lifetime: Time,

            /// KIP-48: cadence of the background sweep task that
            /// proposes `V1DeleteDelegationToken` tombstones for tokens whose
            /// `expiry_timestamp_ms` or `max_timestamp_ms` is in the past. Default
            /// 1 hour (`delegation.token.expiry.check.interval.ms` in Kafka).
            pub delegation_token_expiry_check_interval: Time,

            /// KIP-48: default renew period. The broker uses it as the *initial*
            /// `expiry_timestamp_ms` offset at create time, and as the implicit renew
            /// period when `RenewDelegationToken.renew_period_ms == -1`. It differs
            /// from `delegation_token_max_lifetime`, the absolute ceiling that
            /// `Renew` can never push `expiry_timestamp_ms` past. A fresh token gets
            /// `expiry_timestamp_ms = now + min(default_renew_period,
            /// chosen_max_lifetime)` and `max_timestamp_ms = now +
            /// chosen_max_lifetime`. Default 24 hours
            /// (`delegation.token.expiry.time.ms` in Kafka).
            pub delegation_token_default_renew_period: Time,
        }
    };
}

pub(crate) use delegation_tokens_fields;
