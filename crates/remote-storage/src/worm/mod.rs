//! Write-once (WORM) archive support for the tiered-storage backends.
//!
//! A WORM archive keeps segment data that an auditor must be able to trust
//! years after the broker that wrote it stopped running. The archive proves
//! its own integrity with three layers, each of which closes a gap the layer
//! below leaves open.
//!
//! 1. **A `SHA-256` digest per object.** Every object a segment copy writes —
//!    the log, the indexes, the producer snapshot — gets a digest in the
//!    segment's manifest. This detects a change to the bytes of any one
//!    object. It does not detect a change to the manifest itself, because the
//!    same writer produced both.
//! 2. **A hash chain per partition.** Each manifest records the chain head
//!    that preceded it, and hashes its own canonical bytes onto that head. See
//!    [`canonical_manifest_bytes`] and [`manifest_head`]. This binds every
//!    manifest to all the manifests before it, so a manifest cannot be
//!    rewritten, reordered, or removed from the middle of the chain without
//!    breaking every head after it. It does not prove who wrote the chain.
//! 3. **An Ed25519 signature per manifest.** The manifest's chain head is
//!    signed with the broker's key. See [`manifest_signing_bytes`] and
//!    [`verify_manifest_signature`]. This binds the chain to a named key, so
//!    an attacker who can write to the bucket still cannot forge a manifest.
//!
//! # Tail truncation is not detectable from the archive alone
//!
//! An attacker who deletes the last *n* manifests of a partition, and every
//! object they name, leaves a shorter chain that is internally perfect: every
//! remaining head chains correctly, and every remaining signature verifies. No
//! amount of reading the archive reveals what is gone, because nothing inside
//! the archive says how long the chain should be.
//!
//! That gap is closed **outside** the archive. The broker publishes its latest
//! head per partition, and a verifier that holds an independently obtained
//! expected head can tell that the archive stops short of it. Verification
//! without such a head proves internal consistency and nothing about
//! completeness.
//!
//! # Reused primitives
//!
//! The chain hash and the Ed25519 signing come from
//! [`krabka_audit`](krabka_audit): [`krabka_audit::chain::chain_hash`] defines
//! the chain formula, and [`krabka_audit::signing`] supplies the key provider
//! and the verifier. This module adds only the domain separation and the
//! canonical encoding that a segment manifest needs.

mod archiver;
mod chain;
mod config;
mod error;
mod manifest;
mod verify;

pub use self::{
    archiver::{SealedManifest, WormArchiver},
    chain::{WormChainRecord, next_chain_stamp},
    config::WormConfig,
    error::WormError,
    manifest::{
        ChainHead, ChainStamp, EpochId, HexBytes, MANIFEST_BODY_DOMAIN, MANIFEST_DOMAIN,
        MANIFEST_FORMAT_VERSION, MANIFEST_SUFFIX, ManifestBody, ManifestSeq, ManifestSignature,
        ObjectEntry, SegmentIdentity, SegmentManifest, Sha256Digest, canonical_manifest_bytes,
        manifest_head, manifest_signing_bytes, verify_manifest_signature,
    },
    verify::{
        ArchiveVerifyReport, EpochSpan, MAX_MANIFEST_BYTES, ObjectProtectionReport, OffsetGap,
        PartitionVerifyReport, TrustedManifestKeys, VerifyBreak, VerifyDepth, VerifyRequest,
        verify_archive,
    },
};
