//! Reads `bootstrap.records.bin` on the broker's first start.
//! `krabka format --add-scram` produces that file.
//!
//! The file framing matches `crates/cli/src/format.rs`:
//!   [`u32_le` length][serde_wincode-encoded MetadataRecord]
//! The pair repeats until EOF.

use std::path::Path;

use krabka_metadata::MetadataRecord;
use serde_wincode::SerdeCompat;
use wincode::Deserialize;

use crate::error::BrokerError;

pub const META_PROPERTIES_VERSION: u64 = 2;

#[derive(Debug, serde::Deserialize)]
pub struct MetaProperties {
    pub cluster_id: uuid::Uuid,
    pub directory_id: uuid::Uuid,
    pub version: u64,
}

/// Selects a configured internal-topic replication factor, bounded by the
/// number of registered brokers.
pub(crate) fn internal_topic_replication_factor(desired: i16, broker_count: usize) -> usize {
    broker_count.min(usize::try_from(desired).expect("replication factor is positive"))
}

/// Reads this replica's stable directory id from `meta.properties.json`,
/// which `krabka format` writes.
///
/// KIP-853 identifies each voter by `(node_id, directory_id)`, so the broker
/// must recover its id across restarts instead of minting a fresh one.
///
/// # Errors
/// Returns an error when log I/O fails, when a record or index is corrupt, or
/// when the requested offset violates the segment state.
pub fn read_directory_id(log_dir: &Path) -> Result<uuid::Uuid, BrokerError> {
    read_meta_properties(log_dir).map(|meta| meta.directory_id)
}

/// Read and validate the identity and format stamp written by `krabka format`.
pub fn read_meta_properties(log_dir: &Path) -> Result<MetaProperties, BrokerError> {
    let path = log_dir.join("meta.properties.json");
    let bytes = std::fs::read(&path).map_err(|e| BrokerError::BootstrapFile {
        path: path.clone(),
        source: Box::new(e),
    })?;
    let meta: MetaProperties =
        serde_json::from_slice(&bytes).map_err(|e| BrokerError::BootstrapFile {
            path: path.clone(),
            source: Box::new(e),
        })?;
    if meta.version != META_PROPERTIES_VERSION {
        return Err(BrokerError::BootstrapFile {
            path,
            source: format!(
                "unsupported meta.properties version {}; this build requires version {}; run krabka-format on a fresh directory and restore the topic data",
                meta.version, META_PROPERTIES_VERSION
            )
            .into(),
        });
    }
    Ok(meta)
}

/// Read the format stamp and reject a configured identity for another cluster.
pub fn read_and_validate_meta_properties(
    log_dir: &Path,
    configured_cluster_id: Option<uuid::Uuid>,
) -> Result<MetaProperties, BrokerError> {
    let meta = read_meta_properties(log_dir)?;
    if configured_cluster_id.is_some_and(|configured| configured != meta.cluster_id) {
        return Err(BrokerError::BootstrapFile {
            path: log_dir.join("meta.properties.json"),
            source: format!(
                "INCONSISTENT_CLUSTER_ID: configured cluster id {} does not match {}",
                configured_cluster_id.expect("checked as some"),
                meta.cluster_id
            )
            .into(),
        });
    }
    Ok(meta)
}

/// Extracts the initial voter set from the bootstrap records.
///
/// The last `V1Voters` record wins. This mirrors how the controller applies a
/// stream of `VotersRecord` values, where the most recent one is
/// authoritative. The function returns an empty set when the records hold no
/// `V1Voters` record, which is the joiner path.
#[must_use]
pub fn initial_voters(records: &[MetadataRecord]) -> krabka_metadata::VoterSet {
    records
        .iter()
        .rev()
        .find_map(|r| match r {
            MetadataRecord::V1Voters(v) => Some(v.voters.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

/// # Errors
/// Returns an error when log I/O fails, when a record or index is corrupt, or
/// when the requested offset violates the segment state.
pub fn load_bootstrap_records(log_dir: &Path) -> Result<Vec<MetadataRecord>, BrokerError> {
    let path = log_dir.join("bootstrap.records.bin");
    if !path.exists() {
        return Ok(vec![]);
    }
    let bytes = std::fs::read(&path).map_err(|e| BrokerError::BootstrapFile {
        path: path.clone(),
        source: Box::new(e),
    })?;
    let mut out = Vec::new();
    let mut cur = &bytes[..];
    while !cur.is_empty() {
        if cur.len() < 4 {
            return Err(BrokerError::BootstrapFile {
                path,
                source: "truncated length prefix".into(),
            });
        }
        let len = u32::from_le_bytes([cur[0], cur[1], cur[2], cur[3]]) as usize;
        cur = &cur[4..];
        if cur.len() < len {
            return Err(BrokerError::BootstrapFile {
                path,
                source: "truncated record body".into(),
            });
        }
        let rec = <SerdeCompat<MetadataRecord>>::deserialize(&cur[..len]).map_err(|e| {
            BrokerError::BootstrapFile {
                path: path.clone(),
                source: Box::new(std::io::Error::other(format!("decode: {e}"))),
            }
        })?;
        out.push(rec);
        cur = &cur[len..];
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::ScramCredentialRecord;
    use krabka_security::SaslMechanism;
    use serde_wincode::SerdeCompat;
    use wincode::Serialize;

    use super::*;

    #[test]
    fn internal_topic_replication_factor_uses_configured_value_and_broker_cap() {
        assert!(internal_topic_replication_factor(2, 3) == 2);
        assert!(internal_topic_replication_factor(4, 3) == 3);
    }

    fn write_frame(out: &mut Vec<u8>, rec: &MetadataRecord) {
        let bytes = <SerdeCompat<MetadataRecord>>::serialize(rec).unwrap();
        out.extend_from_slice(
            &u32::try_from(bytes.len())
                .expect("record too large for u32")
                .to_le_bytes(),
        );
        out.extend_from_slice(&bytes);
    }

    #[test]
    fn returns_empty_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert!(got.is_empty());
    }

    #[test]
    fn decodes_v1_scram_credential() {
        let dir = tempfile::tempdir().unwrap();
        let rec = MetadataRecord::V1ScramCredential(ScramCredentialRecord {
            user: "alice".into(),
            mechanism: SaslMechanism::ScramSha512,
            salt: vec![1; 16],
            stored_key: vec![2; 64],
            server_key: vec![3; 64],
            iterations: 4096,
        });
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &rec);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        let got = load_bootstrap_records(dir.path()).unwrap();
        assert!(got.len() == 1);
        match &got[0] {
            MetadataRecord::V1ScramCredential(r) => assert!(r.user == "alice"),
            _ => panic!("wrong variant"),
        }
    }

    #[test]
    fn initial_voters_roundtrips_seeded_set() {
        use krabka_metadata::{Voter, VoterEndpoint, VoterSet, VotersRecord};
        let dir = tempfile::tempdir().unwrap();
        let seeded = VoterSet::from_voters([Voter {
            id: krabka_audit::NodeId(7),
            directory_id: uuid::Uuid::from_u128(7),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: "h7".into(),
                port: 9093,
            }],
            kraft_version: krabka_metadata::KRaftVersionRange::default(),
        }]);
        // Frame the records exactly like `krabka format` does.
        let mut bytes = Vec::new();
        write_frame(
            &mut bytes,
            &MetadataRecord::V1KRaftVersion(krabka_metadata::KRaftVersionRecord {
                kraft_version: 1,
            }),
        );
        write_frame(
            &mut bytes,
            &MetadataRecord::V1Voters(VotersRecord {
                voters: seeded.clone(),
            }),
        );
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();

        let records = load_bootstrap_records(dir.path()).unwrap();
        assert!(records.len() == 2);
        assert!(initial_voters(&records) == seeded);
    }

    #[test]
    fn initial_voters_empty_when_no_voters_record() {
        let recs = vec![MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord { kraft_version: 1 },
        )];
        assert!(initial_voters(&recs).is_empty());
    }

    #[test]
    fn read_directory_id_roundtrips() {
        let dir = tempfile::tempdir().unwrap();
        let id = uuid::Uuid::new_v4();
        let meta = serde_json::json!({
            "cluster_id": uuid::Uuid::new_v4().to_string(),
            "directory_id": id.to_string(),
            "version": META_PROPERTIES_VERSION,
        });
        std::fs::write(
            dir.path().join("meta.properties.json"),
            serde_json::to_vec_pretty(&meta).unwrap(),
        )
        .unwrap();
        assert!(read_directory_id(dir.path()).unwrap() == id);
    }

    #[test]
    fn read_directory_id_errors_when_absent() {
        let dir = tempfile::tempdir().unwrap();
        assert!(matches!(
            read_directory_id(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn rejects_unknown_meta_properties_version() {
        let dir = tempfile::tempdir().unwrap();
        let meta = serde_json::json!({
            "cluster_id": uuid::Uuid::new_v4(),
            "directory_id": uuid::Uuid::new_v4(),
            "version": META_PROPERTIES_VERSION - 1,
        });
        std::fs::write(
            dir.path().join("meta.properties.json"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();
        let error = read_meta_properties(dir.path()).unwrap_err().to_string();
        assert!(error.contains("unsupported meta.properties version"));
        assert!(error.contains("krabka-format"));
    }

    #[test]
    fn rejects_configured_cluster_id_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let on_disk = uuid::Uuid::new_v4();
        let configured = uuid::Uuid::new_v4();
        let meta = serde_json::json!({
            "cluster_id": on_disk,
            "directory_id": uuid::Uuid::new_v4(),
            "version": META_PROPERTIES_VERSION,
        });
        std::fs::write(
            dir.path().join("meta.properties.json"),
            serde_json::to_vec(&meta).unwrap(),
        )
        .unwrap();

        let error = read_and_validate_meta_properties(dir.path(), Some(configured))
            .unwrap_err()
            .to_string();
        assert!(error.contains("INCONSISTENT_CLUSTER_ID"));
        assert!(error.contains(&configured.to_string()));
        assert!(error.contains(&on_disk.to_string()));
    }

    #[test]
    fn refuses_truncated_length_prefix() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bootstrap.records.bin"), [0u8, 0u8, 0u8]).unwrap();
        let err = load_bootstrap_records(dir.path()).unwrap_err();
        assert!(matches!(err, BrokerError::BootstrapFile { .. }));
    }

    #[test]
    fn refuses_truncated_record_body() {
        let dir = tempfile::tempdir().unwrap();
        // Length prefix says 100 bytes follow; only write 4.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn refuses_undecodable_record() {
        let dir = tempfile::tempdir().unwrap();
        let mut bytes = Vec::new();
        // Length prefix=8, body=random bytes that aren't valid bincode for MetadataRecord.
        bytes.extend_from_slice(&8u32.to_le_bytes());
        bytes.extend_from_slice(&[0xFFu8; 8]);
        std::fs::write(dir.path().join("bootstrap.records.bin"), &bytes).unwrap();
        assert!(matches!(
            load_bootstrap_records(dir.path()),
            Err(BrokerError::BootstrapFile { .. })
        ));
    }

    #[test]
    fn zero_length_record_has_body_decode_error_not_prefix_truncation() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("bootstrap.records.bin"), 0u32.to_le_bytes()).unwrap();
        let err = load_bootstrap_records(dir.path()).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("decode:"), "unexpected error: {msg}");
        assert!(!msg.contains("truncated length prefix"));
    }
}
