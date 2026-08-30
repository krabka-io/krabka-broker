//! The dispatch table itself: the request-quota policy, the handler-kind enum,
//! the per-`api_key` entry, and the map that [`build_registry`] fills.
//!
//! These types are the vocabulary the network layer reads back out of the
//! registry, so they sit apart from the tables that populate it.
//!
//! [`build_registry`]: super::build_registry

use krabka_protocol::api_key::ApiKey;

use super::{AuthHandler, ContextHandler, PlainHandler, ProduceHandler, TelemetryHandler};
use crate::handlers::{ApiKeyCode, ApiVersion};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestQuotaPolicy {
    ApplyFallbackAccounting,
    InlineExempt,
    SelfAccounted,
}

#[derive(Clone, Copy)]
pub(crate) enum DispatchKind {
    Plain(PlainHandler),
    Context(ContextHandler),
    Produce(ProduceHandler),
    Telemetry(TelemetryHandler),
    Auth(AuthHandler),
    Fetch,
    SaslMetadata,
}

#[derive(Clone, Copy)]
pub(crate) struct DispatchEntry {
    api_key: ApiKeyCode,
    min_version: ApiVersion,
    max_version: ApiVersion,
    flexible_min: ApiVersion,
    quota_policy: RequestQuotaPolicy,
    kind: DispatchKind,
}

#[derive(Default)]
pub(crate) struct DispatchRegistry {
    table: std::collections::HashMap<ApiKeyCode, DispatchEntry>,
}

impl DispatchEntry {
    pub(crate) fn plain(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: PlainHandler,
    ) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::ApplyFallbackAccounting,
            kind: DispatchKind::Plain(handler),
        }
    }

    pub(crate) fn context(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: ContextHandler,
    ) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Context(handler),
        }
    }

    pub(crate) fn produce(flexible_min: ApiVersion, handler: ProduceHandler) -> Self {
        Self {
            api_key: ApiKey::Produce as i16,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Produce(handler),
        }
    }

    pub(crate) fn telemetry(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: TelemetryHandler,
    ) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Telemetry(handler),
        }
    }

    pub(crate) fn auth(
        api_key: ApiKeyCode,
        flexible_min: ApiVersion,
        handler: AuthHandler,
    ) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::Auth(handler),
        }
    }

    pub(crate) fn fetch(flexible_min: ApiVersion) -> Self {
        Self {
            api_key: ApiKey::Fetch as i16,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::SelfAccounted,
            kind: DispatchKind::Fetch,
        }
    }

    pub(crate) fn sasl_metadata(api_key: ApiKeyCode, flexible_min: ApiVersion) -> Self {
        Self {
            api_key,
            min_version: 0,
            max_version: 0,
            flexible_min,
            quota_policy: RequestQuotaPolicy::InlineExempt,
            kind: DispatchKind::SaslMetadata,
        }
    }

    pub(crate) fn kind(self) -> DispatchKind {
        self.kind
    }

    pub(crate) fn quota_policy(self) -> RequestQuotaPolicy {
        self.quota_policy
    }

    pub(crate) fn body_flexible(self, version: ApiVersion) -> bool {
        self.flexible_min != i16::MAX && version >= self.flexible_min
    }

    pub(crate) fn supports_version(self, version: ApiVersion) -> bool {
        (self.min_version..=self.max_version).contains(&version)
    }

    #[cfg(test)]
    pub(crate) fn version_range(self) -> std::ops::RangeInclusive<ApiVersion> {
        self.min_version..=self.max_version
    }

    #[cfg(test)]
    pub(crate) fn is_plain(self) -> bool {
        matches!(self.kind, DispatchKind::Plain(_))
    }
}

impl DispatchRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(&mut self, entry: DispatchEntry) -> bool {
        self.table.insert(entry.api_key, entry).is_none()
    }

    pub(crate) fn apply_api_catalog(&mut self) {
        for api in crate::api_catalog::supported_apis() {
            let entry = self
                .table
                .get_mut(&api.api_key)
                .unwrap_or_else(|| panic!("advertised api_key {} is not registered", api.api_key));
            entry.min_version = api.min_version;
            entry.max_version = api.max_version;
        }
    }

    pub(crate) fn get(&self, api_key: ApiKeyCode) -> Option<DispatchEntry> {
        self.table.get(&api_key).copied()
    }

    #[cfg(test)]
    pub(crate) fn get_plain(&self, api_key: ApiKeyCode) -> Option<PlainHandler> {
        match self.get(api_key)?.kind {
            DispatchKind::Plain(handler) => Some(handler),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn registered_api_keys(&self) -> impl Iterator<Item = ApiKeyCode> + '_ {
        self.table.keys().copied()
    }

    pub(crate) fn body_flexible(&self, api_key: ApiKeyCode, version: ApiVersion) -> bool {
        self.get(api_key)
            .is_some_and(|entry| entry.body_flexible(version))
    }
}
