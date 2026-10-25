//! The dispatch table itself: the request-quota policy, the handler-kind enum,
//! the per-`api_key` entry, and the map that [`build_registry`] fills.
//!
//! These types are the vocabulary the network layer reads back out of the
//! registry, so they sit apart from the tables that populate it.
//!
//! [`build_registry`]: super::build_registry

#[cfg(test)]
use std::ops::RangeInclusive;
use {
    super::{AuthHandler, ContextHandler, ProduceHandler, TelemetryHandler},
    crate::{
        api_catalog::dispatched_apis,
        handlers::{ApiKeyCode, ApiVersion},
    },
    krabka_protocol::api_key::ApiKey,
    std::collections::HashMap,
};

/// How the KIP-124 request quota reaches an api.
///
/// Kafka charges the request quota for every request that `KafkaApis`
/// answers, through `RequestHandlerHelper.sendResponseMaybeThrottle` and its
/// siblings. It exempts only a follower `Fetch`, `WriteTxnMarkers`, and a
/// `Produce` with `acks = 0`, and it answers `SaslHandshake` and
/// `SaslAuthenticate` in the authenticator, outside `KafkaApis`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RequestQuotaPolicy {
    /// The dispatch loop charges the quota once the handler returns, and
    /// reports the delay in the response's `ThrottleTimeMs`.
    ApplyFallbackAccounting,
    /// The quota is not charged: the apis Kafka exempts.
    InlineExempt,
    /// The handler charges the quota and sets `ThrottleTimeMs` itself.
    SelfAccounted,
}

#[derive(Clone, Copy)]
pub enum DispatchKind {
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
    Auth(AuthHandler),
    Fetch,
    SaslMetadata,
}

#[derive(Clone, Copy)]
pub struct DispatchEntry {
    api_key: ApiKeyCode,
    min_version: ApiVersion,
    max_version: ApiVersion,
    flexible_min: ApiVersion,
    quota_policy: RequestQuotaPolicy,
    kind: DispatchKind,
}

#[derive(Default)]
pub struct DispatchRegistry {
    table: HashMap<ApiKeyCode, DispatchEntry>,
}

impl DispatchEntry {
    pub fn context(api_key: ApiKeyCode, flexible_min: ApiVersion, handler: ContextHandler) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Context(handler),
        }
    }

    /// A context dispatch whose handler charges the KIP-124 request quota
    /// itself and reports the KIP-219 window on its own typed response.
    ///
    /// `ApiVersions` needs this because its `ThrottleTimeMs` sits behind the
    /// `ApiKeys` array, where the dispatch loop's leading-int32 patch cannot
    /// reach it -- see
    /// [`crate::network::dispatch::throttle_audit`]. Charging in the handler,
    /// as `Produce` and `Fetch` do, is what lets the field carry the delay.
    pub fn self_accounted_context(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            ..Self::context(api_key, flexible_min, handler)
        }
    }

    pub fn produce(flexible_min: ApiVersion, handler: ProduceHandler) -> Self {
        Self {
            api_key: ApiKey::Produce as i16,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Produce(handler),
        }
    }

    pub fn telemetry(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: TelemetryHandler,
    ) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Telemetry(handler),
        }
    }

    pub fn auth(api_key: ApiKeyCode, flexible_min: ApiVersion, handler: AuthHandler) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Auth(handler),
        }
    }

    pub fn fetch(flexible_min: ApiVersion) -> Self {
        Self {
            api_key: ApiKey::Fetch as i16,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Fetch,
        }
    }

    pub fn sasl_metadata(api_key: ApiKeyCode, flexible_min: ApiVersion) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::SaslMetadata,
        }
    }

    pub fn kind(self) -> DispatchKind { self.kind }

    pub fn quota_policy(self) -> RequestQuotaPolicy { self.quota_policy }

    pub fn body_flexible(self, version: ApiVersion) -> bool {
        self.flexible_min != i16::MAX && version >= self.flexible_min
    }

    pub fn supports_version(self, version: ApiVersion) -> bool {
        (self.min_version..=self.max_version).contains(&version)
    }

    #[cfg(test)]
    pub fn version_range(self) -> RangeInclusive<ApiVersion> { self.min_version..=self.max_version }

    #[cfg(test)]
    pub fn is_context(self) -> bool { matches!(self.kind, DispatchKind::Context(_)) }
}

impl DispatchRegistry {
    pub fn new() -> Self { Self::default() }

    pub fn register(&mut self, entry: DispatchEntry) -> bool {
        self.table.insert(entry.api_key, entry).is_none()
    }

    pub fn apply_api_catalog(&mut self) {
        for api in dispatched_apis() {
            let entry = self
                .table
                .get_mut(&api.api_key)
                .unwrap_or_else(|| panic!("advertised api_key {} is not registered", api.api_key));
            entry.min_version = api.min_version;
            entry.max_version = api.max_version;
        }
    }

    /// Exempt `api_key` from the KIP-124 request quota, as Kafka answers it
    /// through `sendResponseExemptThrottle`.
    pub fn exempt_from_request_quota(&mut self, api_key: ApiKeyCode) {
        let entry = self
            .table
            .get_mut(&api_key)
            .unwrap_or_else(|| panic!("exempted api_key {api_key} is not registered"));
        entry.quota_policy = RequestQuotaPolicy::InlineExempt;
    }

    /// Marks a registered api as one whose handler charges the request quota
    /// and sets `ThrottleTimeMs` itself.
    pub fn self_account_request_quota(&mut self, api_key: ApiKeyCode) {
        let entry = self
            .table
            .get_mut(&api_key)
            .unwrap_or_else(|| panic!("self-accounted api_key {api_key} is not registered"));
        entry.quota_policy = RequestQuotaPolicy::SelfAccounted;
    }

    pub fn get(&self, api_key: ApiKeyCode) -> Option<DispatchEntry> {
        self.table.get(&api_key).copied()
    }

    #[cfg(test)]
    pub fn get_context(&self, api_key: ApiKeyCode) -> Option<ContextHandler> {
        match self.get(api_key)?.kind {
            DispatchKind::Context(handler) => Some(handler),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn registered_api_keys(&self) -> impl Iterator<Item = ApiKeyCode> + '_ {
        self.table.keys().copied()
    }

    pub fn body_flexible(&self, api_key: ApiKeyCode, version: ApiVersion) -> bool {
        self.get(api_key)
            .is_some_and(|entry| entry.body_flexible(version))
    }
}
