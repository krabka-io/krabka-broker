//! On-disk record of the WAL quorum voter set for one shard.
//!
//! `QuorumWalStore` writes this descriptor when it first creates a shard, and
//! reads it back on every reopen, so a voter set that changed under the broker
//! is refused instead of silently re-bootstrapped. The replace is crash-safe:
//! the new bytes go to a temporary file, the previous descriptor moves aside as
//! a backup, and a failed rename puts that backup back.

use std::{fs, io::Write as _};

use krabka_kraft_core::NodeId;

use crate::error::BrokerError;

pub(super) const QUORUM_STATE_FILE: &str = "quorum-state.json";
pub(super) const QUORUM_STATE_BACKUP_FILE: &str = "quorum-state.json.bak";

#[derive(Debug, serde::Deserialize, serde::Serialize)]
struct PersistedQuorumMembership {
    voters: Vec<u64>,
}

pub(super) fn load_or_prepare_quorum_membership(
    root: &std::path::Path,
    voter_ids: &[NodeId],
) -> Result<bool, BrokerError> {
    fs::create_dir_all(root)?;
    let path = root.join(QUORUM_STATE_FILE);
    let backup = root.join(QUORUM_STATE_BACKUP_FILE);
    let existing = if path.exists() {
        Some(&path)
    } else if backup.exists() {
        Some(&backup)
    } else {
        None
    };
    if let Some(existing) = existing {
        let bytes = fs::read(existing)?;
        let persisted: PersistedQuorumMembership =
            serde_json::from_slice(&bytes).map_err(|err| {
                BrokerError::Replication(format!(
                    "decode WAL quorum membership {}: {err}",
                    existing.display()
                ))
            })?;
        let persisted_ids = persisted.voters.into_iter().map(NodeId).collect::<Vec<_>>();
        if persisted_ids != voter_ids {
            return Err(BrokerError::Replication(format!(
                "WAL quorum voter set changed for {}: persisted {:?}, configured {:?}",
                existing.display(),
                persisted_ids,
                voter_ids
            )));
        }
        return Ok(false);
    }

    Ok(true)
}

pub(super) fn persist_quorum_membership(
    root: &std::path::Path,
    voter_ids: &[NodeId],
) -> Result<(), BrokerError> {
    let path = root.join(QUORUM_STATE_FILE);
    let persisted = PersistedQuorumMembership {
        voters: voter_ids.iter().map(|id| id.0).collect(),
    };
    let bytes = serde_json::to_vec_pretty(&persisted).map_err(|err| {
        BrokerError::Replication(format!(
            "encode WAL quorum membership {}: {err}",
            path.display()
        ))
    })?;
    let temporary = path.with_extension("json.tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);

    let backup = root.join(QUORUM_STATE_BACKUP_FILE);
    if backup.exists() {
        if path.exists() {
            fs::remove_file(&backup)?;
        } else {
            fs::rename(&backup, &path)?;
        }
    }
    if path.exists() {
        fs::rename(&path, &backup)?;
    }
    if let Err(error) = fs::rename(&temporary, &path) {
        restore_membership_backup(&backup, &path);
        return Err(error.into());
    }
    if backup.exists() {
        fs::remove_file(&backup)?;
    }

    // A durable file is not enough on filesystems where the rename itself is
    // only stable after the parent directory is synced. Rust does not expose
    // directory handles that can be flushed on Windows; the file sync above is
    // the strongest portable guarantee there, matching `krabka-log`.
    #[cfg(unix)]
    fs::File::open(root)?.sync_all()?;
    Ok(())
}

fn restore_membership_backup(backup: &std::path::Path, path: &std::path::Path) {
    if let (Ok(true), Ok(false)) = (backup.try_exists(), path.try_exists()) {
        let _ = fs::rename(backup, path);
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn quorum_membership_descriptor_survives_reopen() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        assert!(load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let is_new = load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap();
        let persisted: PersistedQuorumMembership =
            serde_json::from_slice(&fs::read(root.path().join(QUORUM_STATE_FILE)).unwrap())
                .unwrap();

        assert!(!is_new);
        assert!(persisted.voters == vec![0, 1, 2]);
        assert!(root.path().join(QUORUM_STATE_FILE).exists());
        assert!(
            !root
                .path()
                .join(QUORUM_STATE_FILE)
                .with_extension("json.tmp")
                .exists()
        );
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_membership_persist_replaces_a_stale_temporary_file() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        assert!(load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
        let temporary = root
            .path()
            .join(QUORUM_STATE_FILE)
            .with_extension("json.tmp");
        fs::write(&temporary, b"incomplete").unwrap();

        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        assert!(!temporary.exists());
        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_persist_replaces_an_existing_descriptor() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        fs::create_dir_all(root.path()).unwrap();
        fs::write(
            root.path().join(QUORUM_STATE_FILE),
            serde_json::to_vec(&serde_json::json!({
                "voters": [0, 1, 2],
                "leader_epoch": 4,
                "leader_id": 1,
            }))
            .unwrap(),
        )
        .unwrap();

        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let persisted: serde_json::Value =
            serde_json::from_slice(&fs::read(root.path().join(QUORUM_STATE_FILE)).unwrap())
                .unwrap();
        assert!(persisted == serde_json::json!({"voters": [0, 1, 2]}));
        assert!(!root.path().join(QUORUM_STATE_BACKUP_FILE).exists());
    }

    #[test]
    fn quorum_membership_loads_backup_left_between_replace_renames() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        persist_quorum_membership(root.path(), &voter_ids).unwrap();
        fs::rename(
            root.path().join(QUORUM_STATE_FILE),
            root.path().join(QUORUM_STATE_BACKUP_FILE),
        )
        .unwrap();

        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_restore_only_uses_a_backup_when_the_primary_is_missing() {
        let root = tempfile::tempdir().unwrap();
        let primary = root.path().join(QUORUM_STATE_FILE);
        let backup = root.path().join(QUORUM_STATE_BACKUP_FILE);
        fs::write(&backup, b"backup-only").unwrap();

        restore_membership_backup(&backup, &primary);

        assert!(fs::read(&primary).unwrap() == b"backup-only");
        assert!(!backup.exists());

        fs::write(&primary, b"current").unwrap();
        fs::write(&backup, b"stale-backup").unwrap();
        restore_membership_backup(&backup, &primary);
        assert!(fs::read(&primary).unwrap() == b"current");
        assert!(fs::read(&backup).unwrap() == b"stale-backup");
    }

    #[test]
    fn legacy_quorum_state_descriptor_ignores_unused_election_fields() {
        let root = tempfile::tempdir().unwrap();
        let cluster_id = Uuid::from_u128(17);
        fs::write(
            root.path().join(QUORUM_STATE_FILE),
            serde_json::json!({
                "cluster_id": cluster_id,
                "voters": [0, 1, 2],
                "kraft_version": 1,
                "leader_epoch": 7,
                "leader_id": 9,
                "voted_key": {"id": 9, "directory_id": Uuid::from_u128(99)},
            })
            .to_string(),
        )
        .unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];

        assert!(!load_or_prepare_quorum_membership(root.path(), &voter_ids).unwrap());
    }

    #[test]
    fn quorum_membership_rejects_changed_voter_set() {
        let root = tempfile::tempdir().unwrap();
        let voter_ids = vec![NodeId(0), NodeId(1), NodeId(2)];
        persist_quorum_membership(root.path(), &voter_ids).unwrap();

        let changed = vec![NodeId(0), NodeId(1), NodeId(3)];
        assert!(load_or_prepare_quorum_membership(root.path(), &changed).is_err());
    }
}
