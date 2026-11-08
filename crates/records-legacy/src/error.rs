//! Errors that the v0/v1 `MessageSet` codec returns.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum LegacyRecordsError {
    #[error("buffer truncated: need {needed} more byte(s)")]
    Truncated { needed: usize },

    #[error("unsupported magic byte {found} (legacy codec accepts 0 or 1)")]
    UnsupportedMagic { found: i8 },

    #[error("message CRC mismatch (expected={expected:#010x} computed={computed:#010x})")]
    CrcMismatch { expected: u32, computed: u32 },

    #[error("negative length {len} for {label}")]
    NegativeLength { label: &'static str, len: i32 },

    #[error("inconsistent magic: outer={outer} inner={inner}")]
    InconsistentMagic { outer: i8, inner: i8 },

    #[error("recursive compressed wrapper not allowed")]
    NestedCompression,

    /// A v2 batch whose codec v0/v1 cannot carry, which Kafka refuses to
    /// down-convert with `UNSUPPORTED_COMPRESSION_TYPE`.
    #[error("{codec:?} batches cannot be down-converted to v0/v1")]
    UnsupportedCompression {
        codec: krabka_compression::CompressionType,
    },

    #[error("malformed legacy record: {0}")]
    Malformed(String),

    #[error(transparent)]
    Compression(#[from] krabka_compression::CompressionError),
}
