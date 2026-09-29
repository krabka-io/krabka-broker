//! Cluster authorizer.
//!
//! The trait and the ACL evaluator live in `krabka-authz`, which the gateway
//! shares. This module re-exports them, and it keeps the broker-only OPA
//! plugin.

// The OPA authorizer asks an OPA server over HTTP, and wasm32-wasip1 has no
// HTTP client stack.
#[cfg(not(target_family = "wasm"))]
pub mod opa;

pub use krabka_authz::{
    AclSource, AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer,
    SimpleAclAuthorizer, authorize_topics, authorize_topics_logged,
};
