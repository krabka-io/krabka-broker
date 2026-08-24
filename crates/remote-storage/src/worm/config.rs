//! Configuration for the write-once (WORM) archive backend.

use std::path::PathBuf;

/// Settings that make an object-store backend a write-once archive.
///
/// # Debug output is deliberately plain
///
/// This struct derives the ordinary [`Debug`]. It holds a key *path* and a
/// public key id, and it never holds secret material: the private key stays in
/// the file that [`Self::signing_key_path`] names, and only
/// [`FileEd25519Signer`](crabka_audit::signing::FileEd25519Signer) reads it.
/// Do not redact these fields. An operator who reads a log line must be able to
/// tell which key signed a chain. A `***` in place of the key id removes the
/// one field that answers that question.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WormConfig {
    /// Path to the `PKCS#8` Ed25519 key that signs each manifest.
    ///
    /// Default: `None`. Manifests then carry no signature, and the archive
    /// keeps only the per-object digests and the hash chain.
    pub signing_key_path: Option<PathBuf>,
    /// Stable identifier recorded in every manifest signature, so a chain
    /// stays verifiable across a key rotation.
    ///
    /// Default: `None`.
    pub signing_key_id: Option<String>,
    /// Refuse every remote fetch from this archive.
    ///
    /// Default: `false`. When `true`, remote fetch is unavailable: a consumer
    /// that asks for an offset whose local segment was evicted gets
    /// [`WormError::ReadRefused`](crate::WormError::ReadRefused), not a slow
    /// read. The archive is then a compliance sink, not a storage tier.
    pub write_only: bool,
}
