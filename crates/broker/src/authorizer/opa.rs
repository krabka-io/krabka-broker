//! OPA authorizer. POSTs Strimzi-compatible JSON to a
//! configurable OPA decision endpoint. It adds a super-user bypass, an
//! LRU+TTL decision cache, and a fail-open-or-closed policy.
//!
//! The trait method [`Authorizer::authorize`] is synchronous, because sync
//! handler hot paths call it, but `reqwest` is async. This module bridges the
//! two with [`tokio::task::block_in_place`] and a captured runtime
//! [`tokio::runtime::Handle`]. That is acceptable for a tail authorization
//! check, which takes under a millisecond on a cache hit and low double-digit
//! milliseconds on a miss. A cache miss on a single-threaded
//! runtime would deadlock, but the broker is multi-thread.
//!
//! Cache semantics: the authorizer caches decisions on BOTH success and error.
//! Negative caching is deliberate. Under `allow_on_error = false` an
//! error becomes `Deny`, which is the safe behavior for a brief OPA
//! outage. Entries expire on TTL, so an OPA recovery is observable.
//!
//! The JSON envelope this module sends, and the mapping from Kafka's ACL
//! vocabulary onto the OPA wire strings, live in the private `wire` module. The
//! decision cache's key and entry types live in the private `cache` module.

use std::{
    collections::HashSet,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use krabka_authz::{AclSource, AuthorizationRequest, AuthorizationResult, Authorizer};
use krabka_units::{Time, convert::TimeExt as _, fmt::Human as _};
use lru::LruCache;

mod cache;
#[cfg(test)]
mod tests;
mod wire;

use self::cache::{CacheKey, CachedDecision};

/// HTTP-backed pluggable authorizer. Owns its `super_users` bypass set,
/// HTTP client, decision cache, and a captured `tokio::runtime::Handle`
/// so the synchronous [`Authorizer::authorize`] entry point can call
/// `reqwest`'s async API through `block_in_place`.
///
/// # Security
///
/// The `allow_on_error` knob is
/// **security-sensitive**. When it is `true`, any OPA outage (timeout,
/// 5xx, unparseable response) causes `error_decision`
/// to return `Allow`. An unreachable policy server then authorizes
/// *every* request, which is fail-open. The default is `false`, which is
/// fail-closed and matches the upstream Open Policy Agent Kafka plugin's
/// `allow.on.error = false`. Only enable fail-open in environments where
/// brief over-permission is strictly preferable to a block during an OPA
/// outage.
pub struct OpaAuthorizer {
    super_users: HashSet<String>,
    http_client: reqwest::Client,
    url: String,
    /// **Security-sensitive.** `true` ⇒ OPA errors authorize the request,
    /// which is fail-open. An OPA outage then authorizes every request. The
    /// secure default, which is also the upstream OPA Kafka plugin default,
    /// is `false` (fail-closed).
    allow_on_error: bool,
    cache: Mutex<LruCache<CacheKey, CachedDecision>>,
    expire_after: Time,
    runtime: tokio::runtime::Handle,
    /// Clock backing the decision-cache TTL (the `expires_at_ms` stamp and its
    /// expiry comparison). Production uses [`qubit_clock::SystemClock`], which
    /// is wall time. Tests inject a [`qubit_clock::MockClock`] so cache entries
    /// expire on a controlled timeline instead of a real `sleep`. The clock
    /// governs *only* cache freshness, never the authorization decision.
    clock: Arc<dyn qubit_clock::Clock>,
}

impl std::fmt::Debug for OpaAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Skip `http_client`, `cache`, and `runtime` — they're not
        // `Debug`-friendly (Mutex would lock, Handle prints nothing
        // useful, Client prints the whole TLS config). Field-list is
        // operator-relevant config.
        f.debug_struct("OpaAuthorizer")
            .field("super_users", &self.super_users)
            .field("url", &self.url)
            .field("allow_on_error", &self.allow_on_error)
            .field(
                "expire_after",
                &format_args!("{}", self.expire_after.human()),
            )
            .finish_non_exhaustive()
    }
}

impl OpaAuthorizer {
    /// Build a new OPA authorizer. The caller MUST call this from inside a
    /// tokio runtime, because the constructor captures the current `Handle`
    /// to drive async HTTP from the synchronous [`Authorizer::authorize`]
    /// entry point.
    ///
    /// # Errors
    ///
    /// * [`OpaConfigError::Http`] if the constructor cannot build the
    ///   `reqwest::Client`. A TLS misconfig is the realistic failure.
    /// * [`OpaConfigError::ZeroCache`] if `max_cache_size == 0`.
    /// * [`OpaConfigError::NoTokioRuntime`] if no tokio runtime is
    ///   active on the current thread.
    pub fn new(
        super_users: HashSet<String>,
        url: String,
        allow_on_error: bool,
        max_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
    ) -> Result<Self, OpaConfigError> {
        Self::with_clock(
            super_users,
            url,
            allow_on_error,
            max_cache_size,
            expire_after,
            http_timeout,
            Arc::new(qubit_clock::SystemClock::new()),
        )
    }

    /// Same as [`OpaAuthorizer::new`] but with a caller-supplied
    /// [`qubit_clock::Clock`] backing the decision-cache TTL. Production uses
    /// [`OpaAuthorizer::new`] with a [`qubit_clock::SystemClock`]. Tests pass a
    /// [`qubit_clock::MockClock`] so cached decisions expire on a controlled
    /// timeline without a real `sleep`. The clock affects *only* cache
    /// freshness, never the authorization decision.
    ///
    /// # Errors
    ///
    /// Same as [`OpaAuthorizer::new`].
    pub fn with_clock(
        super_users: HashSet<String>,
        url: String,
        allow_on_error: bool,
        max_cache_size: usize,
        expire_after: Time,
        http_timeout: Time,
        clock: Arc<dyn qubit_clock::Clock>,
    ) -> Result<Self, OpaConfigError> {
        let http_client = reqwest::Client::builder()
            .timeout(http_timeout.to_std())
            .build()
            .map_err(|e| OpaConfigError::Http(e.to_string()))?;
        let capacity = NonZeroUsize::new(max_cache_size).ok_or(OpaConfigError::ZeroCache)?;
        let cache = Mutex::new(LruCache::new(capacity));
        let runtime =
            tokio::runtime::Handle::try_current().map_err(|_| OpaConfigError::NoTokioRuntime)?;
        Ok(Self {
            super_users,
            http_client,
            url,
            allow_on_error,
            cache,
            expire_after,
            runtime,
            clock,
        })
    }

    /// What to return when OPA is unreachable or returned garbage.
    /// Fail-closed (`allow_on_error = false`, the default) denies, which is
    /// the secure behavior. Fail-open (`allow_on_error = true`) is
    /// **security-sensitive**. It authorizes every request for the
    /// duration of an OPA outage, and it is only for environments where
    /// a block during that outage is strictly worse than over-permission.
    fn error_decision(&self) -> AuthorizationResult {
        if self.allow_on_error {
            AuthorizationResult::Allow
        } else {
            AuthorizationResult::Deny
        }
    }
}

impl Authorizer for OpaAuthorizer {
    fn authorize(
        &self,
        _source: &dyn AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        // 1. Super-user bypass — no HTTP, no cache touch.
        if self.super_users.contains(&req.principal.name) {
            return AuthorizationResult::Allow;
        }
        // 2. Cache lookup. We do NOT eagerly evict expired entries; the
        //    lookup just rejects them. Lazy eviction is good enough at
        //    LRU capacities measured in the tens of thousands.
        let key = CacheKey {
            principal: format!("User:{}", req.principal.name),
            operation: req.operation,
            resource_type: req.resource_type,
            resource_name: req.resource_name.to_string(),
            host: req.host.ip(),
        };
        // Cache-freshness timestamp only — read from the injected clock so tests
        // can expire entries on a mock timeline. Not part of the decision.
        let now = self.clock.millis();
        {
            let mut cache = self.cache.lock().expect("OPA cache mutex poisoned");
            if let Some(cached) = cache.get(&key)
                && cached.expires_at_ms > now
            {
                return cached.decision;
            }
        }
        // 3. Sync→async bridge. `block_in_place` releases the current
        //    worker for other tasks; the captured runtime drives the
        //    HTTP call on its own threads.
        let decision = tokio::task::block_in_place(|| self.runtime.block_on(self.call_opa(req)));
        // 4. Cache the decision — both successes AND errors. Negative
        //    caching keeps OPA outages from amplifying broker load;
        //    TTL expiry lets recovery propagate naturally.
        let mut cache = self.cache.lock().expect("OPA cache mutex poisoned");
        cache.put(
            key,
            CachedDecision {
                decision,
                expires_at_ms: now + self.expire_after.millis_i64(),
            },
        );
        decision
    }
}

/// Constructor-time failures for [`OpaAuthorizer::new`]. They travel
/// up through `file_config::FileConfigError` at broker startup, so a
/// misconfigured deployment fails at startup and not at the first request.
#[derive(Debug, thiserror::Error)]
pub enum OpaConfigError {
    /// `reqwest::Client::build` failed, from a TLS, DNS, or proxy misconfig.
    #[error("OPA HTTP client build failed: {0}")]
    Http(String),
    /// `max_cache_size = 0` would mean the LRU rejects every entry. That is
    /// an invariant violation, not a useful "disable cache" knob.
    #[error("OPA cache size must be > 0")]
    ZeroCache,
    /// `OpaAuthorizer::new` MUST run inside a tokio runtime, because it
    /// captures the current `Handle` for the sync→async bridge in
    /// `authorize`.
    #[error("OPA authorizer requires an active tokio runtime")]
    NoTokioRuntime,
}
