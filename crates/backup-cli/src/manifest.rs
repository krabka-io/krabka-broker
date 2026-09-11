//! What a capture writes beside its artifacts, and the check that reads it
//! back.
//!
//! A capture is a directory of opaque bytes: an operator cannot look at a
//! controller checkpoint and tell whether it arrived whole. The manifest is
//! what makes the copy checkable — it names every artifact with the size and
//! the SHA-256 of the bytes that were uploaded, so `krabka-backup verify` can
//! answer the only question that matters before a disaster, which is whether
//! this copy would restore.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

/// Directory the captures live under, inside the archive.
pub const CAPTURE_ROOT: &str = "restore-inputs";

/// Object name of the manifest inside one capture.
pub const MANIFEST: &str = "manifest.json";

/// Object name of the captured RLMM snapshot.
pub const RLMM_SNAPSHOT: &str = "rlmm-snapshot";

/// Object name of the captured controller metadata checkpoint.
pub const METADATA_CHECKPOINT: &str = "cluster-metadata.checkpoint";

/// Object name of the captured committed group offsets.
pub const GROUP_OFFSETS: &str = "group-offsets.json";

/// Object name of the committed diskless-WAL projection.
pub const DISKLESS_WAL_INDEX: &str = "diskless-wal-index.json";

/// One captured artifact, as it was uploaded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    /// Object name inside the capture, one of the four constants above.
    pub name: String,
    /// Where the bytes came from: a path on the node, or the cluster address
    /// they were read from.
    pub source: String,
    /// Bytes uploaded.
    pub size_bytes: u64,
    /// Lowercase hex SHA-256 of the uploaded bytes.
    pub sha256: String,
}

/// The record of one capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The capture's own id, which is also its directory name.
    pub capture_id: String,
    /// When the capture ran, in milliseconds since the Unix epoch.
    pub captured_at_ms: u64,
    /// The log directory the file artifacts were read from, when a capture
    /// took any.
    pub log_dir: Option<String>,
    /// The cluster the group offsets were read from, when a capture took them.
    pub bootstrap_server: Option<String>,
    /// Every artifact this capture wrote, in the order it wrote them.
    pub artifacts: Vec<Artifact>,
}

impl Manifest {
    /// The artifact of this name, if the capture took one.
    #[must_use]
    pub fn artifact(&self, name: &str) -> Option<&Artifact> {
        self.artifacts.iter().find(|entry| entry.name == name)
    }
}

/// Lowercase hex SHA-256 of `bytes`.
#[must_use]
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// What is wrong with one artifact's bytes, or `None` when they are the bytes
/// the manifest recorded.
///
/// The size is checked as well as the digest, because a size mismatch names
/// the failure an operator can act on — a truncated upload — while a digest
/// mismatch alone only says the bytes differ.
#[must_use]
pub fn artifact_problem(expected: &Artifact, actual: &[u8]) -> Option<String> {
    let actual_size = actual.len() as u64;
    if actual_size != expected.size_bytes {
        return Some(format!(
            "{}: archive holds {actual_size} bytes, manifest recorded {}",
            expected.name, expected.size_bytes
        ));
    }
    let actual_sha = sha256_hex(actual);
    (actual_sha != expected.sha256).then(|| {
        format!(
            "{}: archive holds sha256 {actual_sha}, manifest recorded {}",
            expected.name, expected.sha256
        )
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{Artifact, Manifest, artifact_problem, sha256_hex};

    fn artifact(bytes: &[u8]) -> Artifact {
        Artifact {
            name: "rlmm-snapshot".to_owned(),
            source: "/var/lib/krabka/remote-log-metadata/snapshot".to_owned(),
            size_bytes: bytes.len() as u64,
            sha256: sha256_hex(bytes),
        }
    }

    #[test]
    fn the_empty_input_hashes_to_the_published_sha256_of_the_empty_string() {
        check!(
            sha256_hex(b"") == "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn bytes_that_match_the_manifest_are_no_problem() {
        check!(artifact_problem(&artifact(b"snapshot bytes"), b"snapshot bytes") == None);
    }

    #[test]
    fn a_short_read_is_reported_as_a_size_mismatch() {
        let problem = artifact_problem(&artifact(b"snapshot bytes"), b"snapshot")
            .expect("a short read is a problem");
        check!(problem.contains("8 bytes"), "got: {problem}");
        check!(problem.contains("14"), "got: {problem}");
    }

    #[test]
    fn same_length_different_bytes_are_reported_as_a_digest_mismatch() {
        let problem = artifact_problem(&artifact(b"snapshot bytes"), b"snapshot bytez")
            .expect("different bytes are a problem");
        check!(problem.contains("sha256"), "got: {problem}");
    }

    #[test]
    fn a_manifest_round_trips_through_json() {
        let manifest = Manifest {
            capture_id: "0001700000000000".to_owned(),
            captured_at_ms: 1_700_000_000_000,
            log_dir: Some("/var/lib/krabka".to_owned()),
            bootstrap_server: Some("broker-1:9092".to_owned()),
            artifacts: vec![artifact(b"snapshot bytes")],
        };
        let encoded = serde_json::to_vec(&manifest).expect("encode the manifest");
        let decoded: Manifest = serde_json::from_slice(&encoded).expect("decode the manifest");
        check!(decoded == manifest);
        check!(decoded.artifact("rlmm-snapshot").is_some());
        check!(decoded.artifact("group-offsets.json").is_none());
    }
}
