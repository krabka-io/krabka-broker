//! The startup failures that a bad `[[operator_keys]]` entry raises.
//!
//! Every variant here stops the broker at boot. A trust set is all or nothing,
//! so one unusable entry fails the load. It never shrinks the set that the
//! later signature checks run against.

use std::path::PathBuf;

/// Failures that stop [`OperatorKeys::load`](crate::operator_keys::OperatorKeys::load),
/// and with it the broker.
///
/// Every variant is a startup error. A key set that a broker cannot load is
/// never downgraded to a smaller one: a signature checked against a partial
/// trust set is a signature check that silently does nothing.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum OperatorKeyError {
    /// A `key_id` or a `principal` is blank. Neither can be matched against a
    /// signed record, so the entry could never authorize anything.
    #[error("[[operator_keys]] entry {index} has a blank {field}")]
    BlankField {
        /// Zero-based position of the entry in the configured array.
        index: usize,
        /// Name of the blank field, `key_id` or `principal`.
        field: &'static str,
    },
    /// The `public_key_path` could not be read.
    #[error("operator key {key_id:?}: cannot read {}: {source}", path.display())]
    Unreadable {
        /// The entry's `key_id`.
        key_id: String,
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying I/O failure.
        #[source]
        source: std::io::Error,
    },
    /// The file does not hold a raw Ed25519 public key.
    #[error(
        "operator key {key_id:?}: {} holds {found} bytes; a raw Ed25519 public key is 32 bytes",
        path.display()
    )]
    Malformed {
        /// The entry's `key_id`.
        key_id: String,
        /// The path that was read.
        path: PathBuf,
        /// How many bytes the file holds.
        found: usize,
    },
    /// Two entries share a `key_id`. A signed record names one key, so a
    /// repeated id makes the key it selects depend on the file order.
    #[error("duplicate operator key_id {key_id:?}")]
    DuplicateKeyId {
        /// The repeated `key_id`.
        key_id: String,
    },
    /// Two entries bind the same principal. The broker checks that a record's
    /// claimed author is the principal bound to the signing key, and a
    /// principal with two keys makes that check ambiguous.
    #[error("operator keys {first:?} and {second:?} are both bound to principal {principal:?}")]
    DuplicatePrincipal {
        /// The repeated principal.
        principal: String,
        /// The `key_id` that claimed the principal first.
        first: String,
        /// The `key_id` that claimed it again.
        second: String,
    },
}
