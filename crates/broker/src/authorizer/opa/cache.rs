//! Key and entry types for the OPA decision cache. They live apart from the
//! authorizer itself because they are pure data: the cache key is the tuple
//! that identifies one authorization question, and the entry is the answer
//! plus the wall-clock stamp at which it goes stale.

use std::net::IpAddr;

use krabka_authz::AuthorizationResult;
use krabka_metadata::{AclOperation, ResourceType};

#[derive(Debug, Clone, PartialEq, Eq, std::hash::Hash)]
pub(super) struct CacheKey {
    pub(super) principal: String,
    pub(super) operation: AclOperation,
    pub(super) resource_type: ResourceType,
    pub(super) resource_name: String,
    pub(super) host: IpAddr,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct CachedDecision {
    pub(super) decision: AuthorizationResult,
    pub(super) expires_at_ms: i64,
}
