//! A native filesystem backend for embedding applications. Use one directory
//! per cluster, shared by every potential coordinator. Only the current group
//! owner may mutate a group's entry. A local directory survives a process
//! restart, but does not by itself provide cross-node availability.
use {
    super::{
        BackendFuture, StorageFailure, StoredDescription, TopologyDescription,
        TopologyDescriptionBackend,
    },
    std::{
        fmt::Write as _,
        io::{Error, ErrorKind},
        path::{Path, PathBuf},
    },
    tokio::{fs, io::AsyncWriteExt as _},
};

/// Versioned plugin-private format. Adding a writer version must retain the
/// reader for version 1; this is not the broker's internal-topic serialization.
#[derive(serde::Serialize, serde::Deserialize)]
struct FileDescription {
    format_version: u8,
    stored: StoredDescription,
}

#[derive(Debug)]
pub struct FileTopologyDescriptionBackend {
    root: PathBuf,
}

impl FileTopologyDescriptionBackend {
    /// Create the private storage directory. The operator is responsible for
    /// directory permissions, capacity, backups, and sharing it across nodes.
    ///
    /// # Errors
    /// Returns an IO error if the directory cannot be created.
    pub async fn new(root: impl AsRef<Path>) -> Result<Self, Error> {
        fs::create_dir_all(root.as_ref()).await?;
        Ok(Self {
            root: root.as_ref().to_path_buf(),
        })
    }

    fn path(&self, group_id: &str) -> PathBuf {
        // Hex is injective and prevents group IDs from escaping the root.
        let mut name = String::from("group-");
        for byte in group_id.bytes() {
            let _ = write!(name, "{byte:02x}");
        }
        self.root.join(name)
    }
}

impl TopologyDescriptionBackend for FileTopologyDescriptionBackend {
    fn set<'a>(
        &'a self,
        group_id: &'a str,
        epoch: i32,
        description: &'a TopologyDescription,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            let value = FileDescription {
                format_version: 1,
                stored: StoredDescription {
                    topology_epoch: epoch,
                    description: description.clone(),
                },
            };
            let bytes = serde_json::to_vec(&value).map_err(|_| StorageFailure::Permanent)?;
            let path = self.path(group_id);
            // Each attempt owns its inode. Reusing a fixed staging name lets
            // a delayed writer mutate another attempt's published envelope.
            // This prevents byte aliasing, not stale-owner writes: the host
            // must still fence coordinators before admitting backend calls.
            let staging = self.root.join(format!(".pending-{}", uuid::Uuid::new_v4()));
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)
                .await
                .map_err(|_| StorageFailure::Transient)?;
            let result = async {
                file.write_all(&bytes).await?;
                file.sync_all().await?;
                drop(file);
                fs::rename(&staging, path).await
            }
            .await;
            if result.is_err() {
                let _ = fs::remove_file(staging).await;
            }
            result.map_err(|_| StorageFailure::Transient)
        })
    }

    fn get<'a>(
        &'a self,
        group_id: &'a str,
        epoch: i32,
    ) -> BackendFuture<'a, Option<TopologyDescription>> {
        Box::pin(async move {
            let bytes = match fs::read(self.path(group_id)).await {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
                Err(_) => return Err(StorageFailure::Transient),
            };
            let value: FileDescription =
                serde_json::from_slice(&bytes).map_err(|_| StorageFailure::Permanent)?;
            if value.format_version != 1 {
                return Err(StorageFailure::Permanent);
            }
            Ok((value.stored.topology_epoch == epoch).then_some(value.stored.description))
        })
    }

    fn delete<'a>(&'a self, group_id: &'a str) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            for path in [
                self.path(group_id),
                self.path(group_id).with_extension("pending"),
            ] {
                match fs::remove_file(path).await {
                    Ok(()) => {}
                    Err(e) if e.kind() == ErrorKind::NotFound => {}
                    Err(_) => return Err(StorageFailure::Transient),
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[tokio::test]
    async fn filesystem_store_survives_reopen_and_fences_epochs() {
        let directory = tempfile::tempdir().expect("directory");
        let description = TopologyDescription {
            subtopologies: vec![],
            global_stores: vec![],
        };
        let backend = FileTopologyDescriptionBackend::new(directory.path())
            .await
            .expect("backend");
        backend
            .set("../../app", 7, &description)
            .await
            .expect("set");
        drop(backend);
        let reopened = FileTopologyDescriptionBackend::new(directory.path())
            .await
            .expect("reopen");
        assert!(reopened.get("../../app", 7).await == Ok(Some(description)));
        assert!(reopened.get("../../app", 8).await == Ok(None));
        assert!(reopened.get("other", 7).await == Ok(None));
        reopened.delete("../../app").await.expect("delete");
        reopened
            .delete("../../app")
            .await
            .expect("idempotent delete");
        assert!(reopened.get("../../app", 7).await == Ok(None));
    }

    /// A delayed writer may retain an open inode across handoff. A later
    /// successful set must never publish that inode under the group's name.
    #[tokio::test]
    async fn delayed_staging_writer_cannot_corrupt_a_published_description() {
        let directory = tempfile::tempdir().expect("directory");
        let backend = FileTopologyDescriptionBackend::new(directory.path())
            .await
            .expect("backend");
        let mut old_writer = fs::File::create(backend.path("app").with_extension("pending"))
            .await
            .expect("old staging inode");
        let description = TopologyDescription {
            subtopologies: vec![],
            global_stores: vec![],
        };
        backend
            .set("app", 8, &description)
            .await
            .expect("new owner set");
        // The old descriptor is still at offset zero. With a reused staging
        // path, this overwrites the newly renamed final envelope.
        old_writer
            .write_all(b"corrupt")
            .await
            .expect("delayed write");
        old_writer.sync_all().await.expect("delayed sync");
        assert!(backend.get("app", 8).await == Ok(Some(description)));
    }

    /// Independent backend handles exercise real filesystem IO. Every set
    /// succeeds and the final file is exactly one complete submitted envelope;
    /// this intentionally makes no winner-order or ownership-fencing claim.
    #[tokio::test]
    async fn concurrent_backend_instances_publish_complete_envelopes() {
        let directory = tempfile::tempdir().expect("directory");
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(16));
        let mut writers = tokio::task::JoinSet::new();
        for _ in 0..16 {
            let backend = FileTopologyDescriptionBackend::new(directory.path())
                .await
                .expect("backend");
            let start = std::sync::Arc::clone(&barrier);
            writers.spawn(async move {
                start.wait().await;
                backend
                    .set("app", 7, &TopologyDescription {
                        subtopologies: vec![],
                        global_stores: vec![],
                    })
                    .await
            });
        }
        while let Some(result) = writers.join_next().await {
            assert!(result.expect("writer task") == Ok(()));
        }
        let backend = FileTopologyDescriptionBackend::new(directory.path())
            .await
            .expect("reopen");
        let value: FileDescription = serde_json::from_slice(
            &fs::read(backend.path("app"))
                .await
                .expect("published envelope"),
        )
        .expect("complete versioned envelope");
        assert!(value.format_version == 1);
        assert!(value.stored.topology_epoch == 7);
        assert!(
            backend.get("app", value.stored.topology_epoch).await
                == Ok(Some(value.stored.description))
        );
        let mut entries = fs::read_dir(directory.path()).await.expect("directory");
        let mut count = 0;
        while entries.next_entry().await.expect("entry").is_some() {
            count += 1;
        }
        assert!(count == 1, "completed attempts remove their staging names");
    }

    #[tokio::test]
    async fn version_one_golden_bytes_remain_readable_and_writers_are_versioned() {
        let directory = tempfile::tempdir().expect("directory");
        let backend = FileTopologyDescriptionBackend::new(directory.path())
            .await
            .expect("backend");
        let golden = br#"{"format_version":1,"stored":{"topology_epoch":7,"description":{"subtopologies":[],"global_stores":[]}}}"#;
        fs::write(backend.path("app"), golden)
            .await
            .expect("fixture");
        let expected = TopologyDescription {
            subtopologies: vec![],
            global_stores: vec![],
        };
        assert!(backend.get("app", 7).await == Ok(Some(expected.clone())));
        backend
            .set("app", 7, &expected)
            .await
            .expect("write version one");
        assert!(fs::read(backend.path("app")).await.expect("bytes") == golden);
        let unknown = br#"{"format_version":2,"stored":{"topology_epoch":7,"description":{"subtopologies":[],"global_stores":[]}}}"#;
        fs::write(backend.path("app"), unknown)
            .await
            .expect("future fixture");
        assert!(backend.get("app", 7).await == Err(StorageFailure::Permanent));
    }
}
