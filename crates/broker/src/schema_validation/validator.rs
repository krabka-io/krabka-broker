//! The registry-backed record checker and its cache.
//!
//! The shape is the OPA authorizer's, for the same reasons: an LRU bounds what
//! a careless or hostile producer can make the broker hold, a TTL on each
//! entry is what makes a registry change observable, and failures are cached
//! as well as successes so that a produce storm against one bad id costs one
//! registry call rather than one per record.
//!
//! Not every failure is cached for the same length of time. "This id is not
//! registered" is an answer and keeps the full TTL; "the registry did not
//! answer" keeps [`UNAVAILABLE_TTL_MS`], because holding that one for minutes
//! would keep rejecting valid records after the registry recovered.
//!
//! It differs from the authorizer in one way that matters. `Authorizer` is a
//! synchronous trait, so `OpaAuthorizer` bridges to async with
//! `block_in_place`. This runs from `process_partition`, which is already
//! async, so a cache miss is a plain `.await` on one HTTP round trip and no
//! runtime worker is parked.

use std::{
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use krabka_schema_serde::registry::RegistryClient;
use krabka_units::{Time, convert::TimeExt as _};
use lru::LruCache;

mod cache;
mod check;
mod reject;
#[cfg(test)]
mod test_support;

use self::cache::Cached;
pub use self::reject::RejectReason;

/// How long a "the registry could not answer" result stays cached.
///
/// Much shorter than `expire_after`, and deliberately so. A 404 is the
/// registry *answering*, so it earns the full TTL: it turns a produce storm
/// against one bad id into a single registry call. A timeout or a 5xx is the
/// registry *failing* to answer, and remembering that for minutes would keep
/// rejecting valid records long after the registry came back — the opposite of
/// what `fail_open = false` is for, which is to reject only while no answer is
/// available. Caching it briefly rather than not at all is what keeps an
/// outage from becoming one registry call per record.
const UNAVAILABLE_TTL_MS: i64 = 2_000;

/// A [`SchemaValidator`] that could not be built from its configuration.
#[derive(Debug, thiserror::Error)]
pub enum SchemaValidatorError {
    /// `maximum_cache_size` was zero, which would make every record a miss.
    #[error("schema_registry.maximum_cache_size must be greater than zero")]
    ZeroCache,
    /// The HTTP client could not be built from the configured timeout.
    #[error("schema registry HTTP client: {0}")]
    Http(String),
}

/// Registry-backed record validation with a bounded, expiring cache.
///
/// One instance per broker, held on [`crate::Broker`] as an `Option`. `None`
/// is "no `[schema_registry]` section", and then no topic can turn validation
/// on.
pub struct SchemaValidator {
    client: RegistryClient,
    cache: Mutex<LruCache<u32, Cached>>,
    expire_after: Time,
    /// **Security-sensitive.** `true` admits a record the broker could not
    /// validate because the registry was unreachable, which is fail-open: for
    /// the length of a registry outage, a validated topic accepts whatever it
    /// is sent. The default is `false`, which fails the produce instead. This
    /// is the same knob, with the same default and the same argument, as
    /// `allow_on_error` on [`crate::authorizer::opa::OpaAuthorizer`].
    ///
    /// An unknown id or a body that does not match its schema is a rejection
    /// under either setting. This governs only the case where the broker could
    /// not get an answer at all.
    fail_open: bool,
    /// Clock backing the cache TTL. Production uses
    /// [`qubit_clock::SystemClock`]; tests inject a `MockClock` so an expiry
    /// is an assertion rather than a sleep.
    clock: Arc<dyn qubit_clock::Clock>,
}

impl std::fmt::Debug for SchemaValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaValidator")
            .field("expire_after", &self.expire_after)
            .field("fail_open", &self.fail_open)
            .finish_non_exhaustive()
    }
}

impl SchemaValidator {
    /// Build a validator against the registry at `url`.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaValidatorError::ZeroCache`] when `maximum_cache_size`
    /// is zero, and [`SchemaValidatorError::Http`] when the HTTP client cannot
    /// be built.
    pub fn new(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
    ) -> Result<Self, SchemaValidatorError> {
        Self::with_clock(
            url,
            fail_open,
            maximum_cache_size,
            expire_after,
            http_timeout,
            Arc::new(qubit_clock::SystemClock::new()),
        )
    }

    /// [`SchemaValidator::new`] with the clock injected, for tests that drive
    /// the cache TTL on a controlled timeline.
    ///
    /// # Errors
    ///
    /// As [`SchemaValidator::new`].
    pub fn with_clock(
        url: String,
        fail_open: bool,
        maximum_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
        clock: Arc<dyn qubit_clock::Clock>,
    ) -> Result<Self, SchemaValidatorError> {
        let capacity =
            NonZeroUsize::new(maximum_cache_size).ok_or(SchemaValidatorError::ZeroCache)?;
        let http = reqwest::Client::builder()
            .timeout(http_timeout.to_std())
            .build()
            .map_err(|e| SchemaValidatorError::Http(e.to_string()))?;
        Ok(Self {
            client: RegistryClient::with_http_client(url, http),
            cache: Mutex::new(LruCache::new(capacity)),
            expire_after,
            fail_open,
            clock,
        })
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{minutes, secs};

    use super::*;

    #[test]
    fn a_zero_sized_cache_is_a_configuration_error() {
        let got = SchemaValidator::new(
            "http://localhost:8081".into(),
            false,
            0,
            minutes(1),
            secs(5),
        );
        assert!(let Err(SchemaValidatorError::ZeroCache) = got);
    }
}
