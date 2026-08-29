//! Cluster authorizer.
//!
//! The trait and the ACL evaluator live in `krabka-authz`, which the gateway
//! shares. This module re-exports them, and it keeps the broker-only OPA
//! plugin.
pub mod opa;

pub use krabka_authz::{
    AclSource, AllowAllAuthorizer, AuthorizationRequest, AuthorizationResult, Authorizer,
    SimpleAclAuthorizer, authorize_topics,
};
