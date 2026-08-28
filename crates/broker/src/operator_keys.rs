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
//! [`crabka_audit::signing::verify_signature`], the same code path that checks
//! an audit checkpoint, so operator key material has the shape the audit
//! checkpoint keys already have.
//!
//! This module holds no canonical-bytes builder. The freeze subsystem and the
//! break-glass subsystem each define their own signed payload, under their own
//! domain separator, and pass the finished bytes to [`OperatorKeys::verify`].

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use sha2::{Digest as _, Sha256};

/// Byte length of a raw Ed25519 public key.
const ED25519_PUBLIC_KEY_LEN: usize = 32;

/// Failures that stop [`OperatorKeys::load`], and with it the broker.
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

/// One `[[operator_keys]]` entry, before its public key file is read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorKeyEntry {
    /// Stable identifier that a signed record names.
    pub key_id: String,
    /// The principal this key speaks for.
    pub principal: String,
    /// Path to the raw 32-byte Ed25519 public key.
    pub public_key_path: PathBuf,
}

/// One loaded operator key and the principal it is bound to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorKey {
    key_id: String,
    principal: String,
    public_key: Vec<u8>,
}

impl OperatorKey {
    /// Stable identifier that a signed record names.
    #[must_use]
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The principal this key speaks for.
    ///
    /// A record that claims another author is refused even when its signature
    /// verifies, so one operator's key cannot sign in another's name.
    #[must_use]
    pub fn principal(&self) -> &str {
        &self.principal
    }

    /// The raw 32-byte Ed25519 public key.
    #[must_use]
    pub fn public_key(&self) -> &[u8] {
        &self.public_key
    }
}

/// The loaded, validated operator key trust set.
///
/// An empty set is the default and means no operator key is provisioned. The
/// file-config layer refuses a configuration that demands a signature against
/// an empty set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OperatorKeys {
    by_id: BTreeMap<String, OperatorKey>,
}

impl OperatorKeys {
    /// Read every configured public key file and build the trust set.
    ///
    /// # Errors
    ///
    /// Returns [`OperatorKeyError`] when an entry has a blank `key_id` or
    /// `principal`, when its `public_key_path` cannot be read, when the file
    /// does not hold a 32-byte Ed25519 public key, or when two entries share a
    /// `key_id` or a `principal`.
    pub fn load(entries: &[OperatorKeyEntry]) -> Result<Self, OperatorKeyError> {
        let mut by_id: BTreeMap<String, OperatorKey> = BTreeMap::new();
        let mut by_principal: BTreeMap<String, String> = BTreeMap::new();
        for (index, entry) in entries.iter().enumerate() {
            if entry.key_id.trim().is_empty() {
                return Err(OperatorKeyError::BlankField {
                    index,
                    field: "key_id",
                });
            }
            if entry.principal.trim().is_empty() {
                return Err(OperatorKeyError::BlankField {
                    index,
                    field: "principal",
                });
            }
            if by_id.contains_key(&entry.key_id) {
                return Err(OperatorKeyError::DuplicateKeyId {
                    key_id: entry.key_id.clone(),
                });
            }
            if let Some(first) = by_principal.get(&entry.principal) {
                return Err(OperatorKeyError::DuplicatePrincipal {
                    principal: entry.principal.clone(),
                    first: first.clone(),
                    second: entry.key_id.clone(),
                });
            }
            let public_key = read_public_key(&entry.key_id, &entry.public_key_path)?;
            by_principal.insert(entry.principal.clone(), entry.key_id.clone());
            by_id.insert(
                entry.key_id.clone(),
                OperatorKey {
                    key_id: entry.key_id.clone(),
                    principal: entry.principal.clone(),
                    public_key,
                },
            );
        }
        Ok(Self { by_id })
    }

    /// The key registered under `key_id`, with the principal it is bound to.
    #[must_use]
    pub fn get(&self, key_id: &str) -> Option<&OperatorKey> {
        self.by_id.get(key_id)
    }

    /// How many keys the trust set holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether no operator key is provisioned.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Verify `signature` over `message` as made by `key_id` on behalf of
    /// `principal`.
    ///
    /// The principal binding is part of the check: a signature made by Alice's
    /// key over a record that claims Bob wrote it does not verify. The caller
    /// supplies the canonical bytes of its own signed payload, under its own
    /// domain separator.
    #[must_use]
    pub fn verify(&self, key_id: &str, principal: &str, message: &[u8], signature: &[u8]) -> bool {
        self.get(key_id).is_some_and(|key| {
            key.principal == principal
                && crabka_audit::signing::verify_signature(&key.public_key, message, signature)
        })
    }
}

/// SHA-256 hex fingerprint of a configured approver set.
///
/// The approver set comes from each broker's own `broker.toml`, so two brokers
/// can legitimately disagree during a rolling config change. Every break-glass
/// audit event records this fingerprint, which makes the disagreement visible
/// after the fact.
///
/// The input is sorted and de-duplicated first, and each name is
/// length-prefixed, so the fingerprint depends on the members alone: not on the
/// order an operator wrote them in, and not on where one name ends and the next
/// begins.
#[must_use]
pub fn approver_set_fingerprint(approvers: &[String]) -> String {
    let unique: BTreeSet<&str> = approvers.iter().map(String::as_str).collect();
    let mut hasher = Sha256::new();
    for approver in unique {
        let len = u32::try_from(approver.len()).unwrap_or(u32::MAX);
        hasher.update(len.to_be_bytes());
        hasher.update(approver.as_bytes());
    }
    hex::encode(hasher.finalize())
}

fn read_public_key(key_id: &str, path: &Path) -> Result<Vec<u8>, OperatorKeyError> {
    let bytes = std::fs::read(path).map_err(|source| OperatorKeyError::Unreadable {
        key_id: key_id.to_owned(),
        path: path.to_path_buf(),
        source,
    })?;
    if bytes.len() != ED25519_PUBLIC_KEY_LEN {
        return Err(OperatorKeyError::Malformed {
            key_id: key_id.to_owned(),
            path: path.to_path_buf(),
            found: bytes.len(),
        });
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use tempfile::TempDir;

    use super::*;

    // A fresh Ed25519 key pair plus its raw public key bytes.
    fn fresh_key() -> (Ed25519KeyPair, Vec<u8>) {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse pkcs8");
        let public = pair.public_key().as_ref().to_vec();
        (pair, public)
    }

    // Write `bytes` to `<dir>/<name>` and return the path.
    fn write_key_file(dir: &TempDir, name: &str, bytes: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, bytes).expect("write key file");
        path
    }

    fn entry(key_id: &str, principal: &str, path: PathBuf) -> OperatorKeyEntry {
        OperatorKeyEntry {
            key_id: key_id.to_owned(),
            principal: principal.to_owned(),
            public_key_path: path,
        }
    }

    #[test]
    fn load_binds_every_key_to_its_principal() {
        let dir = TempDir::new().expect("tempdir");
        let (_, alice_public) = fresh_key();
        let (_, bob_public) = fresh_key();
        let entries = vec![
            entry(
                "alice-yubi",
                "User:alice",
                write_key_file(&dir, "alice.pub", &alice_public),
            ),
            entry(
                "bob-yubi",
                "User:bob",
                write_key_file(&dir, "bob.pub", &bob_public),
            ),
        ];

        let keys = OperatorKeys::load(&entries).expect("load trust set");

        check!(keys.len() == 2);
        check!(!keys.is_empty());
        for (name, key_id, expected) in [
            (
                "alice by id",
                "alice-yubi",
                Some(("User:alice", alice_public.clone())),
            ),
            ("bob by id", "bob-yubi", Some(("User:bob", bob_public))),
            ("an unconfigured id", "carol-yubi", None),
        ] {
            let found = keys
                .get(key_id)
                .map(|key| (key.principal().to_owned(), key.public_key().to_vec()));
            let expected =
                expected.map(|(principal, public)| (principal.to_owned(), public.clone()));
            check!(found == expected, "case {name}");
        }
        check!(keys.get("alice-yubi").map(OperatorKey::key_id) == Some("alice-yubi"));
    }

    #[test]
    fn an_empty_configuration_loads_an_empty_trust_set() {
        let keys = OperatorKeys::load(&[]).expect("load empty trust set");

        check!(keys.is_empty());
        check!(keys.len() == 0);
        check!(OperatorKeys::default() == keys);
    }

    #[test]
    fn load_rejects_an_unusable_entry() {
        let dir = TempDir::new().expect("tempdir");
        let (_, public) = fresh_key();
        let good = write_key_file(&dir, "good.pub", &public);
        let short = write_key_file(&dir, "short.pub", &public[..31]);
        let long = write_key_file(&dir, "long.pub", &[public.as_slice(), b"\n"].concat());
        let missing = dir.path().join("absent.pub");

        for (name, entries, expect) in [
            (
                "a blank key_id",
                vec![entry("", "User:alice", good.clone())],
                "BlankField",
            ),
            (
                "a blank principal",
                vec![entry("alice-yubi", "  ", good.clone())],
                "BlankField",
            ),
            (
                "an unreadable public_key_path",
                vec![entry("alice-yubi", "User:alice", missing)],
                "Unreadable",
            ),
            (
                "a public key one byte short",
                vec![entry("alice-yubi", "User:alice", short)],
                "Malformed",
            ),
            (
                "a public key with a trailing newline",
                vec![entry("alice-yubi", "User:alice", long)],
                "Malformed",
            ),
            (
                "a duplicate key_id",
                vec![
                    entry("alice-yubi", "User:alice", good.clone()),
                    entry("alice-yubi", "User:bob", good.clone()),
                ],
                "DuplicateKeyId",
            ),
            (
                "a duplicate principal",
                vec![
                    entry("alice-yubi", "User:alice", good.clone()),
                    entry("alice-backup", "User:alice", good.clone()),
                ],
                "DuplicatePrincipal",
            ),
        ] {
            assert!(let Err(error) = OperatorKeys::load(&entries), "case {name}");
            let variant = match error {
                OperatorKeyError::BlankField { .. } => "BlankField",
                OperatorKeyError::Unreadable { .. } => "Unreadable",
                OperatorKeyError::Malformed { .. } => "Malformed",
                OperatorKeyError::DuplicateKeyId { .. } => "DuplicateKeyId",
                OperatorKeyError::DuplicatePrincipal { .. } => "DuplicatePrincipal",
            };
            check!(variant == expect, "case {name}");
        }
    }

    #[test]
    fn verify_accepts_only_the_bound_principal_and_the_signing_key() {
        let dir = TempDir::new().expect("tempdir");
        let (alice, alice_public) = fresh_key();
        let (_, bob_public) = fresh_key();
        let keys = OperatorKeys::load(&[
            entry(
                "alice-yubi",
                "User:alice",
                write_key_file(&dir, "alice.pub", &alice_public),
            ),
            entry(
                "bob-yubi",
                "User:bob",
                write_key_file(&dir, "bob.pub", &bob_public),
            ),
        ])
        .expect("load trust set");
        let message = b"crabka-topic-freeze-v1\0orders";
        let signature = alice.sign(message).as_ref().to_vec();

        for (name, key_id, principal, msg, expected) in [
            (
                "alice's signature under alice's key and principal",
                "alice-yubi",
                "User:alice",
                message.as_slice(),
                true,
            ),
            (
                "alice's key claiming bob",
                "alice-yubi",
                "User:bob",
                message.as_slice(),
                false,
            ),
            (
                "bob's key over alice's signature",
                "bob-yubi",
                "User:bob",
                message.as_slice(),
                false,
            ),
            (
                "an unconfigured key_id",
                "carol-yubi",
                "User:carol",
                message.as_slice(),
                false,
            ),
            (
                "a tampered message",
                "alice-yubi",
                "User:alice",
                b"crabka-topic-freeze-v1\0ordering".as_slice(),
                false,
            ),
        ] {
            check!(
                keys.verify(key_id, principal, msg, &signature) == expected,
                "case {name}"
            );
        }
    }

    #[test]
    fn approver_set_fingerprint_ignores_order_and_tracks_membership() {
        let base = ["User:alice", "User:bob", "User:carol"].map(str::to_owned);
        let baseline = approver_set_fingerprint(&base);

        for (name, approvers, expected_equal) in [
            (
                "the same set",
                vec!["User:alice", "User:bob", "User:carol"],
                true,
            ),
            (
                "reversed",
                vec!["User:carol", "User:bob", "User:alice"],
                true,
            ),
            (
                "shuffled with a repeat",
                vec!["User:bob", "User:carol", "User:alice", "User:bob"],
                true,
            ),
            (
                "one member added",
                vec!["User:alice", "User:bob", "User:carol", "User:dave"],
                false,
            ),
            ("one member removed", vec!["User:alice", "User:bob"], false),
            (
                "one member renamed",
                vec!["User:alice", "User:bob", "User:carla"],
                false,
            ),
            (
                "the same characters split differently",
                vec!["User:ali", "ceUser:bob", "User:carol"],
                false,
            ),
            ("empty", vec![], false),
        ] {
            let candidate: Vec<String> = approvers.into_iter().map(str::to_owned).collect();
            check!(
                (approver_set_fingerprint(&candidate) == baseline) == expected_equal,
                "case {name}"
            );
        }
        check!(baseline.len() == 64);
        check!(baseline.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
