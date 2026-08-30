//! Errors raised by the write-once (WORM) archive layer.

/// A failure in the WORM archive layer.
///
/// The variants split into four groups: a broken or absent chain stamp, a
/// manifest that does not encode or decode, a refusal that the archive policy
/// demands, and an inability to reach the archive at all.
///
/// That last group is deliberately its own variant rather than a codec error.
/// A verifier must be able to tell "I looked and the archive is broken" from
/// "I could not look", because only the first is evidence of tampering. The
/// grading in `krabka-worm-verify` rests on that distinction.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WormError {
    /// The archive could not be listed or read. Says nothing about the
    /// archive's integrity — only that the store did not answer.
    #[error("WORM archive access: {0}")]
    Archive(String),
    /// The broker did not stamp a chain position on the segment metadata
    /// before the copy. A WORM backend refuses to write an unchained manifest.
    #[error("segment metadata carries no WORM chain stamp")]
    MissingChainStamp,
    /// The chain record on the segment metadata is not decodable.
    #[error("malformed WORM chain record: {0}")]
    MalformedChainRecord(String),
    /// The manifest does not encode to, or decode from, its JSON form.
    #[error("WORM manifest codec error: {0}")]
    Codec(String),
    /// An object reached the manifest with no `SHA-256` digest recorded for it.
    #[error("no digest recorded for object `{key}`")]
    MissingDigest {
        /// Object-store key of the object with no digest.
        key: String,
    },
    /// A multipart WORM write completed without a version id to pin.
    #[error("no version id recorded for multipart object `{key}`")]
    MissingVersionId {
        /// Object-store key whose completed version could not be identified.
        key: String,
    },
    /// A delete was attempted against a write-once archive.
    #[error("delete refused: `{key}` is in a write-once archive")]
    DeleteRefused {
        /// Object-store key the caller asked to delete.
        key: String,
    },
    /// A remote fetch was attempted against a write-only archive.
    #[error("read refused: this archive is configured write-only")]
    ReadRefused,
    /// The configured signing key is missing, unreadable, or not a valid
    /// `PKCS#8` Ed25519 key.
    #[error("WORM signing key: {0}")]
    SigningKey(String),
}
