//! The operator key trust set, loaded from `[[operator_keys]]`.
//!
//! Two subsystems verify detached Ed25519 signatures against one shared set of
//! operator keys: the topic write-freeze registry and the break-glass approval
//! workflow. Both reach the keys through [`OperatorKeys`], so an operator is
//! provisioned once and a signature is checked one way.
//!
//! [`OperatorKeys::load`] reads every configured public key file, so an
//! unreadable path or a malformed key stops the broker at boot instead of in
//! the middle of an incident. Verification calls
//! [`krabka_audit::signing::verify_signature`], the same code path that checks
//! an audit checkpoint, so operator key material has the shape the audit
//! checkpoint keys already have.
//!
//! This module holds no canonical-bytes builder. The freeze subsystem and the
//! break-glass subsystem each define their own signed payload, under their own
//! domain separator, and pass the finished bytes to [`OperatorKeys::verify`].

pub use self::{
    error::OperatorKeyError,
    fingerprint::approver_set_fingerprint,
    key_file::OperatorKeyEntry,
    trust_set::{OperatorKey, OperatorKeys},
};

mod error;
mod fingerprint;
mod key_file;
mod trust_set;

#[cfg(test)]
mod tests;
