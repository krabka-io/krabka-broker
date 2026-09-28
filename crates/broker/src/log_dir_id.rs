//! Per-`log.dir` stable UUIDs (KIP-858 directory ids).
//!
//! Each configured `log.dir` carries a `directory_id` in its
//! `meta.properties.json`, in Kafka's 22-character base64 form. `krabka
//! format` writes it for every directory it was given. This module creates and
//! persists the id for an extra JBOD dir that was added after the format, on
//! its first boot.
//!
//! The resulting map from path to uuid lets the broker stamp
//! `AssignReplicasToDirs` and `offline_log_dirs` with stable ids that the
//! controller can map back to partitions.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use uuid::Uuid;

/// Immutable per-dir UUID table built once at startup.
#[derive(Clone, Debug, Default)]
pub struct LogDirIds {
    by_path: HashMap<PathBuf, Uuid>,
}

impl LogDirIds {
    /// Resolves a stable UUID for every dir in `log_dirs`, by a read or by a
    /// new id. A dir whose `meta.properties.json` already carries a
    /// `directory_id` keeps it. A dir without one, such as a fresh JBOD disk,
    /// gets a new id, as Kafka's `DirectoryId.random` makes one, which this
    /// method persists into a `meta.properties.json` in that dir.
    #[must_use]
    pub fn resolve(log_dirs: &[PathBuf]) -> Self {
        let mut by_path = HashMap::new();
        for dir in log_dirs {
            let id = read_or_mint(dir);
            by_path.insert(dir.clone(), id);
        }
        Self { by_path }
    }

    #[must_use]
    pub fn id_for(&self, dir: &Path) -> Option<Uuid> {
        self.by_path.get(dir).copied()
    }

    /// All `(path, uuid)` pairs, sorted by path for deterministic output.
    #[must_use]
    pub fn entries(&self) -> Vec<(PathBuf, Uuid)> {
        let mut v: Vec<_> = self.by_path.iter().map(|(p, u)| (p.clone(), *u)).collect();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    /// The UUIDs of the supplied dirs. It skips a dir that the table does not
    /// hold.
    #[must_use]
    pub fn ids_for(&self, dirs: &[PathBuf]) -> Vec<Uuid> {
        dirs.iter().filter_map(|d| self.id_for(d)).collect()
    }
}

/// Reads the `directory_id` from `<dir>/meta.properties.json`, or creates and
/// persists a fresh one.
///
/// On any I/O or parse failure, this function still returns a stable in-memory
/// id. The partition therefore stays usable, and only the faithful-wire
/// reporting degrades.
fn read_or_mint(dir: &Path) -> Uuid {
    let path = dir.join("meta.properties.json");
    if let Ok(bytes) = std::fs::read(&path)
        && let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes)
        && let Some(id) = v["directory_id"]
            .as_str()
            .and_then(|s| s.parse::<krabka_format::DirectoryId>().ok())
    {
        return id.into();
    }
    let id = krabka_format::DirectoryId::random();
    // Persist, merging into any existing object so we don't clobber a
    // cluster_id/version written by `krabka format`.
    let mut obj = std::fs::read(&path)
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    obj.insert("directory_id".into(), serde_json::json!(id.to_string()));
    obj.entry("version")
        .or_insert(serde_json::json!(crate::bootstrap::META_PROPERTIES_VERSION));
    if let Ok(serialized) = serde_json::to_vec_pretty(&serde_json::Value::Object(obj)) {
        let _ = std::fs::create_dir_all(dir);
        let _ = std::fs::write(&path, serialized);
    }
    id.into()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    /// A minted id is persisted in Kafka's form and read back unchanged.
    #[test]
    fn mints_and_persists_for_dir_without_meta() {
        let tmp = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        let first = ids.id_for(tmp.path()).expect("minted");
        let v: serde_json::Value = serde_json::from_slice(
            &std::fs::read(tmp.path().join("meta.properties.json")).unwrap(),
        )
        .unwrap();
        assert!(v["directory_id"] == krabka_format::DirectoryId(first).to_string());
        let ids2 = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        assert!(ids2.id_for(tmp.path()) == Some(first));
    }

    #[test]
    fn reads_existing_directory_id_without_clobbering_siblings() {
        let tmp = tempdir().unwrap();
        let id = Uuid::from_u128(0xABCD);
        std::fs::write(
            tmp.path().join("meta.properties.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "cluster_id": "c-1",
                "directory_id": krabka_format::DirectoryId(id).to_string(),
                "version": 1,
            }))
            .unwrap(),
        )
        .unwrap();
        let ids = LogDirIds::resolve(&[tmp.path().to_path_buf()]);
        assert!(ids.id_for(tmp.path()) == Some(id));
        let v: serde_json::Value = serde_json::from_slice(
            &std::fs::read(tmp.path().join("meta.properties.json")).unwrap(),
        )
        .unwrap();
        assert!(v["cluster_id"] == "c-1");
    }

    #[test]
    fn distinct_dirs_get_distinct_ids() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[a.path().to_path_buf(), b.path().to_path_buf()]);
        assert!(ids.id_for(a.path()) != ids.id_for(b.path()));
        assert!(ids.entries().len() == 2);
    }

    #[test]
    fn ids_for_returns_requested_known_dirs_in_order() {
        let a = tempdir().unwrap();
        let b = tempdir().unwrap();
        let unknown = tempdir().unwrap();
        let ids = LogDirIds::resolve(&[a.path().to_path_buf(), b.path().to_path_buf()]);
        let a_id = ids.id_for(a.path()).expect("a id");
        let b_id = ids.id_for(b.path()).expect("b id");

        let selected = ids.ids_for(&[
            b.path().to_path_buf(),
            unknown.path().to_path_buf(),
            a.path().to_path_buf(),
        ]);

        assert!(selected == vec![b_id, a_id]);
    }
}
