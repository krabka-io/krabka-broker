//! The concrete [`ObjectOps`] implementation over an `object_store` handle.
//!
//! [`ObjectStoreClient`] is the crate's only implementation of the trait, so
//! this file is where the operation logic actually lives: the
//! multipart-threshold branch in `put_from_path`, the optional whole-object
//! `SHA-256` digest, and the translation of every backend call into an
//! [`ObjectStoreError`].

use std::sync::Arc;

use bytes::Bytes;
use object_store::{
    GetOptions, GetRange, MultipartUpload, ObjectMeta, ObjectStoreExt as _, PutOptions, PutPayload,
    PutResult, UploadPart, WriteMultipart, path::Path,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

#[cfg(test)]
mod tests;

use crate::{
    error::ObjectStoreError,
    ops::{ObjectOps, PutOutcome, PutRequest},
};

/// The single concrete [`ObjectOps`] implementation.
///
/// It wraps any `object_store::ObjectStore` handle, for example a handle from
/// [`build_object_store`](crate::build_object_store), or an
/// `object_store::memory::InMemory` handle in tests.
#[derive(Clone)]
pub struct ObjectStoreClient {
    inner: Arc<dyn object_store::ObjectStore>,
}

#[derive(Debug)]
struct AbortOnDrop {
    upload: Option<Box<dyn MultipartUpload>>,
    key: Path,
}

impl AbortOnDrop {
    fn new(upload: Box<dyn MultipartUpload>, key: &Path) -> Self {
        Self {
            upload: Some(upload),
            key: key.clone(),
        }
    }

    fn upload(&mut self) -> &mut Box<dyn MultipartUpload> {
        self.upload.as_mut().expect("upload is still active")
    }
}

#[async_trait::async_trait]
impl MultipartUpload for AbortOnDrop {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.upload().put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let result = self.upload().complete().await;
        if result.is_ok() {
            self.upload.take();
        }
        result
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let Some(mut upload) = self.upload.take() else {
            return Ok(());
        };
        let key = self.key.clone();
        tokio::spawn(async move {
            let result = upload.abort().await;
            if let Err(error) = &result {
                tracing::warn!(%key, %error, "failed to abort multipart upload");
            }
            result
        })
        .await
        .map_err(|error| object_store::Error::Generic {
            store: "multipart upload",
            source: Box::new(error),
        })?
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        let Some(mut upload) = self.upload.take() else {
            return;
        };
        let key = self.key.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(%key, "cannot abort multipart upload outside a Tokio runtime");
            return;
        };
        runtime.spawn(async move {
            if let Err(error) = upload.abort().await {
                tracing::warn!(%key, %error, "failed to abort multipart upload");
            }
        });
    }
}

impl ObjectStoreClient {
    /// Wrap an existing object-store handle.
    #[must_use]
    pub fn new(inner: Arc<dyn object_store::ObjectStore>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl ObjectOps for ObjectStoreClient {
    async fn put(
        &self,
        key: &Path,
        bytes: Bytes,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError> {
        let size_bytes = bytes.len() as u64;
        let sha256 = req.digest.then(|| {
            let mut hasher = Sha256::new();
            hasher.update(&bytes);
            hasher.finalize().into()
        });
        let result = self
            .inner
            .put_opts(
                key,
                PutPayload::from_bytes(bytes),
                PutOptions {
                    mode: req.mode,
                    ..Default::default()
                },
            )
            .await?;
        Ok(PutOutcome {
            size_bytes,
            sha256,
            e_tag: result.e_tag,
            version_id: result.version,
        })
    }

    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError> {
        if chunk_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "multipart chunk size must be greater than zero",
            )
            .into());
        }

        let len = tokio::fs::metadata(src).await?.len();
        if len < threshold {
            let bytes = tokio::fs::read(src).await?;
            let size_bytes = bytes.len() as u64;
            let sha256 = req.digest.then(|| {
                let mut hasher = Sha256::new();
                hasher.update(&bytes);
                hasher.finalize().into()
            });
            let result = self
                .inner
                .put_opts(
                    key,
                    PutPayload::from(bytes),
                    PutOptions {
                        mode: req.mode,
                        ..Default::default()
                    },
                )
                .await?;
            return Ok(PutOutcome {
                size_bytes,
                sha256,
                e_tag: result.e_tag,
                version_id: result.version,
            });
        }
        // `req.mode` cannot reach the multipart path: object_store 0.13's
        // `PutMultipartOptions` carries only tags, attributes, and extensions
        // — it has no `mode` field, and `MultipartUpload::complete` takes no
        // precondition. A non-`Overwrite` mode on a file at or above
        // `threshold` therefore degrades to a plain multipart put.
        let upload = Box::new(AbortOnDrop::new(self.inner.put_multipart(key).await?, key));
        let mut writer = WriteMultipart::new_with_chunk_size(upload, chunk_size);
        let mut file = tokio::fs::File::open(src).await?;
        let mut buf = vec![0u8; chunk_size];
        let mut hasher = req.digest.then(Sha256::new);
        let mut size_bytes = 0u64;
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            if let Some(hasher) = hasher.as_mut() {
                hasher.update(&buf[..n]);
            }
            size_bytes += n as u64;
            writer.write(&buf[..n]);
        }
        let result = writer.finish().await?;
        Ok(PutOutcome {
            size_bytes,
            sha256: hasher.map(|hasher| hasher.finalize().into()),
            e_tag: result.e_tag,
            version_id: result.version,
        })
    }

    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError> {
        Ok(self.inner.get(key).await?.bytes().await?)
    }

    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError> {
        let opts = GetOptions {
            range: Some(range),
            ..Default::default()
        };
        Ok(self.inner.get_opts(key, opts).await?.bytes().await?)
    }

    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError> {
        Ok(self.inner.head(key).await?)
    }

    async fn list(&self, prefix: Option<Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError> {
        use futures_util::stream::TryStreamExt as _;
        Ok(self
            .inner
            .list(prefix.as_ref())
            .try_collect::<Vec<_>>()
            .await?)
    }

    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError> {
        self.inner.delete(key).await?;
        Ok(())
    }
}
