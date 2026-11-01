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
            let value = StoredDescription {
                topology_epoch: epoch,
                description: description.clone(),
            };
            let bytes = serde_json::to_vec(&value).map_err(|_| StorageFailure::Permanent)?;
            let path = self.path(group_id);
            let staging = path.with_extension("pending");
            let mut file = fs::File::create(&staging)
                .await
                .map_err(|_| StorageFailure::Transient)?;
            file.write_all(&bytes)
                .await
                .map_err(|_| StorageFailure::Transient)?;
            file.sync_all()
                .await
                .map_err(|_| StorageFailure::Transient)?;
            drop(file);
            fs::rename(staging, path)
                .await
                .map_err(|_| StorageFailure::Transient)
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
            let stored: StoredDescription =
                serde_json::from_slice(&bytes).map_err(|_| StorageFailure::Permanent)?;
            Ok((stored.topology_epoch == epoch).then_some(stored.description))
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
}
