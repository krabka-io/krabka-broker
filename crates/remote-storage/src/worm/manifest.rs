//! The segment manifest: the signed, chained record of one archived segment.
//!
//! A manifest names every object that one segment copy wrote, records a
//! `SHA-256` digest for each, and binds the whole set to the partition's hash
//! chain. [`canonical_manifest_bytes`] defines the byte encoding that the chain
//! hashes. The writer and the verifier both call it. Never reimplement the
//! layout, and never change it without changing
//! [`MANIFEST_FORMAT_VERSION`] with it.
//!
//! The parts sit in submodules: the document types, the chain linkage, the
//! canonical byte layout, and the signature.

mod body;
mod encoding;
mod hex_bytes;
mod linkage;
mod signing;

#[cfg(test)]
mod test_support;

pub use self::{
    body::{ManifestBody, ObjectEntry, SegmentIdentity, SegmentManifest},
    encoding::{MANIFEST_BODY_DOMAIN, canonical_manifest_bytes},
    hex_bytes::{HexBytes, Sha256Digest},
    linkage::{ChainHead, ChainStamp, EpochId, ManifestSeq, manifest_head},
    signing::{
        MANIFEST_DOMAIN, ManifestSignature, manifest_signing_bytes, verify_manifest_signature,
    },
};

/// Version of the manifest encoding, both the JSON shape and the canonical
/// byte layout. A change to either is a change to this number.
pub const MANIFEST_FORMAT_VERSION: u32 = 1;

/// Object-store key suffix of a segment manifest.
pub const MANIFEST_SUFFIX: &str = ".manifest";
