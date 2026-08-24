//! The shared object-store operation surface.
//!
//! This module holds an async, mockable [`ObjectOps`] trait and its single
//! concrete implementation [`ObjectStoreClient`] over `object_store`. Consumers
//! route their put, get, delete, and multipart calls through it, so the
//! operation logic lives in one place. That logic includes the
//! multipart-threshold branch and the `object_store::Error` to
//! [`ObjectStoreError`] mapping.

use std::sync::Arc;

use bytes::Bytes;
/// Precondition for a write, re-exported so callers need not depend on
/// `object_store` directly.
pub use object_store::PutMode;
use object_store::{
    GetOptions, GetRange, ObjectMeta, ObjectStoreExt as _, PutOptions, PutPayload, WriteMultipart,
    path::Path,
};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncReadExt as _;

use crate::error::ObjectStoreError;

/// How one put should behave.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PutRequest {
    /// Precondition for the write. Defaults to `PutMode::Overwrite`.
    pub mode: PutMode,
    /// Whether to compute a SHA-256 digest of the payload during upload.
    /// Defaults to `false`; only the WORM archive path needs it, and hashing
    /// every tiered byte is not free.
    pub digest: bool,
}

/// What a put produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOutcome {
    /// Bytes written.
    pub size_bytes: u64,
    /// Whole-object SHA-256, present only when `PutRequest::digest` was set.
    pub sha256: Option<[u8; 32]>,
    /// Backend entity tag, when the backend returned one.
    pub e_tag: Option<String>,
    /// Version id on a versioned bucket, when the backend returned one.
    pub version_id: Option<String>,
}

/// Async object-store operations. The trait is `Send + Sync`, so tasks can
/// share it.
///
/// The trait stays dyn-safe and `#[automock]`-able. It expresses multipart
/// upload as [`ObjectOps::put_from_path`] over a filesystem path, not over a
/// generic reader. The trait thus mocks cleanly for mutation-testable IO
/// decision logic.
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait ObjectOps: Send + Sync {
    /// Single-PUT an in-memory payload under the preconditions in `req`.
    async fn put(
        &self,
        key: &Path,
        bytes: Bytes,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError>;

    /// Upload a local file. The method uses single-PUT below `threshold`
    /// bytes, and streaming multipart in `chunk_size` parts at or above
    /// `threshold`.
    async fn put_from_path(
        &self,
        key: &Path,
        src: &std::path::Path,
        threshold: u64,
        chunk_size: usize,
        req: PutRequest,
    ) -> Result<PutOutcome, ObjectStoreError>;

    /// Fetch a whole object.
    async fn get(&self, key: &Path) -> Result<Bytes, ObjectStoreError>;

    /// Fetch a byte range of an object.
    async fn get_range(&self, key: &Path, range: GetRange) -> Result<Bytes, ObjectStoreError>;

    /// Fetch object metadata, such as the size and the etag.
    async fn head(&self, key: &Path) -> Result<ObjectMeta, ObjectStoreError>;

    /// List objects under an optional prefix.
    async fn list(&self, prefix: Option<Path>) -> Result<Vec<ObjectMeta>, ObjectStoreError>;

    /// Delete an object.
    async fn delete(&self, key: &Path) -> Result<(), ObjectStoreError>;
}

/// The single concrete [`ObjectOps`] implementation.
///
/// It wraps any `object_store::ObjectStore` handle, for example a handle from
/// [`build_object_store`](crate::build_object_store), or an
/// `object_store::memory::InMemory` handle in tests.
#[derive(Clone)]
pub struct ObjectStoreClient {
    inner: Arc<dyn object_store::ObjectStore>,
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
        let upload = self.inner.put_multipart(key).await?;
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

#[cfg(test)]
mod tests {
    use std::{io::Write, sync::Arc};

    use assert2::{assert, check};
    use object_store::{GetRange, path::Path};
    use sha2::{Digest as _, Sha256};

    use super::*;

    fn client() -> ObjectStoreClient {
        ObjectStoreClient::new(Arc::new(object_store::memory::InMemory::new()))
    }

    /// SHA-256 of `bytes`, computed independently of the upload path under
    /// test.
    fn sha256_of(bytes: &[u8]) -> [u8; 32] {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        hasher.finalize().into()
    }

    #[tokio::test]
    async fn put_get_round_trips() {
        let c = client();
        let key = Path::from("a/b");
        c.put(
            &key,
            bytes::Bytes::from_static(b"hello"),
            PutRequest::default(),
        )
        .await
        .unwrap();
        let got = c.get(&key).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn get_range_returns_slice() {
        let c = client();
        let key = Path::from("a/b");
        c.put(
            &key,
            bytes::Bytes::from_static(b"hello world"),
            PutRequest::default(),
        )
        .await
        .unwrap();
        let got = c.get_range(&key, GetRange::Bounded(0..5)).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn get_missing_maps_to_not_found() {
        let c = client();
        let err = c.get(&Path::from("nope")).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }

    #[tokio::test]
    async fn head_and_list_and_delete() {
        let c = client();
        let key = Path::from("p/x");
        c.put(
            &key,
            bytes::Bytes::from_static(b"1234"),
            PutRequest::default(),
        )
        .await
        .unwrap();
        assert!(c.head(&key).await.unwrap().size == 4);
        let listed = c.list(Some(Path::from("p"))).await.unwrap();
        assert!(listed.iter().any(|m| m.location == key));
        c.delete(&key).await.unwrap();
        assert!(matches!(
            c.get(&key).await.unwrap_err(),
            ObjectStoreError::NotFound(_)
        ));
    }

    /// The default request asks for no digest, and the outcome reports the
    /// payload size plus whatever identifiers the backend returned. `InMemory`
    /// hands out sequential etags from `0`, so the whole outcome is
    /// predictable.
    #[tokio::test]
    async fn put_outcome_reports_size_and_backend_identifiers() {
        let c = client();

        let got = c
            .put(
                &Path::from("a/b"),
                bytes::Bytes::from_static(b"hello"),
                PutRequest::default(),
            )
            .await
            .unwrap();

        assert!(
            got == PutOutcome {
                size_bytes: 5,
                sha256: None,
                e_tag: Some("0".to_owned()),
                version_id: None,
            }
        );
    }

    /// `PutRequest::digest` turns on whole-object hashing, and the digest must
    /// be of the bytes actually stored.
    #[tokio::test]
    async fn put_with_digest_returns_payload_sha256() {
        let c = client();
        let payload = bytes::Bytes::from_static(b"hello");

        let got = c
            .put(
                &Path::from("a/b"),
                payload.clone(),
                PutRequest {
                    digest: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        assert!(
            got == PutOutcome {
                size_bytes: 5,
                sha256: Some(sha256_of(&payload)),
                e_tag: Some("0".to_owned()),
                version_id: None,
            }
        );
    }

    /// `PutMode::Create` is a precondition, not a hint: the second write to a
    /// key must fail rather than clobber the first.
    #[tokio::test]
    async fn put_create_mode_rejects_an_existing_key() {
        let c = client();
        let key = Path::from("worm/manifest");
        let req = PutRequest {
            mode: PutMode::Create,
            digest: false,
        };
        c.put(&key, bytes::Bytes::from_static(b"first"), req.clone())
            .await
            .unwrap();

        let err = c
            .put(&key, bytes::Bytes::from_static(b"second"), req)
            .await
            .unwrap_err();

        assert!(matches!(err, ObjectStoreError::AlreadyExists(p) if p == key));
        assert!(&c.get(&key).await.unwrap()[..] == b"first");
    }

    #[tokio::test]
    async fn put_from_path_single_put_below_threshold() {
        let c = client();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"tiny").unwrap();
        let key = Path::from("seg/small");
        c.put_from_path(&key, f.path(), 8, 4, PutRequest::default())
            .await
            .unwrap();
        assert!(&c.get(&key).await.unwrap()[..] == b"tiny");
    }

    #[tokio::test]
    async fn put_from_path_multipart_above_threshold() {
        let c = client();
        let payload = vec![7u8; 20]; // 20 bytes, threshold 8, chunk 4 -> multipart
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&payload).unwrap();
        let key = Path::from("seg/big");
        c.put_from_path(&key, f.path(), 8, 4, PutRequest::default())
            .await
            .unwrap();
        assert!(c.get(&key).await.unwrap()[..] == payload[..]);
    }

    /// Both upload paths must produce the same digest over the same file, and
    /// the multipart path must fold every chunk in — a partial fold would give
    /// a different hash. The cases differ only in file length, which selects
    /// the path: below `threshold` is single-PUT, at or above it is multipart
    /// across several `chunk_size` reads.
    #[tokio::test]
    async fn put_from_path_digests_the_whole_file_on_both_paths() {
        for (case, len) in [("single put", 7usize), ("multipart", 21usize)] {
            let c = client();
            let payload: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all(&payload).unwrap();

            let got = c
                .put_from_path(
                    &Path::from("seg/x"),
                    f.path(),
                    8,
                    4,
                    PutRequest {
                        digest: true,
                        ..Default::default()
                    },
                )
                .await
                .unwrap();

            check!(
                got == PutOutcome {
                    size_bytes: len as u64,
                    sha256: Some(sha256_of(&payload)),
                    e_tag: Some("0".to_owned()),
                    version_id: None,
                },
                "{case}"
            );
        }
    }

    /// Without `digest`, neither path pays for hashing, so the outcome carries
    /// `None` while still reporting the byte count.
    #[tokio::test]
    async fn put_from_path_omits_digest_when_not_requested() {
        for (case, len) in [("single put", 7usize), ("multipart", 21usize)] {
            let c = client();
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all(&vec![3u8; len]).unwrap();

            let got = c
                .put_from_path(&Path::from("seg/x"), f.path(), 8, 4, PutRequest::default())
                .await
                .unwrap();

            check!(
                got == PutOutcome {
                    size_bytes: len as u64,
                    sha256: None,
                    e_tag: Some("0".to_owned()),
                    version_id: None,
                },
                "{case}"
            );
        }
    }

    /// A delegating spy that counts which upload path `put_from_path` takes.
    /// Both paths produce identical object bytes. Only the call counts can thus
    /// pin the threshold comparison `len < threshold` at its boundary.
    #[derive(Debug)]
    struct CountingStore {
        inner: object_store::memory::InMemory,
        puts: std::sync::atomic::AtomicUsize,
        multiparts: std::sync::atomic::AtomicUsize,
    }

    impl CountingStore {
        fn new() -> Self {
            Self {
                inner: object_store::memory::InMemory::new(),
                puts: std::sync::atomic::AtomicUsize::new(0),
                multiparts: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl std::fmt::Display for CountingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "CountingStore({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for CountingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.puts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.multiparts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Pins the `len < threshold` boundary. One byte under the threshold must
    /// take the single-PUT path and must never take multipart.
    #[tokio::test]
    async fn put_from_path_at_threshold_minus_one_takes_single_put() {
        let store = Arc::new(CountingStore::new());
        let c = ObjectStoreClient::new(store.clone());
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&[1u8; 7]).unwrap(); // len 7, threshold 8
        c.put_from_path(
            &Path::from("b/under"),
            f.path(),
            8,
            4,
            PutRequest::default(),
        )
        .await
        .unwrap();
        assert!(store.puts.load(std::sync::atomic::Ordering::SeqCst) == 1);
        assert!(store.multiparts.load(std::sync::atomic::Ordering::SeqCst) == 0);
    }

    /// Pins the other side of the boundary. Exactly the threshold must take
    /// the multipart path, because the comparison is a strict `<`, not a
    /// `<=`.
    #[tokio::test]
    async fn put_from_path_at_exact_threshold_takes_multipart() {
        let store = Arc::new(CountingStore::new());
        let c = ObjectStoreClient::new(store.clone());
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&[2u8; 8]).unwrap(); // len 8 == threshold 8
        c.put_from_path(&Path::from("b/at"), f.path(), 8, 4, PutRequest::default())
            .await
            .unwrap();
        assert!(store.puts.load(std::sync::atomic::Ordering::SeqCst) == 0);
        assert!(store.multiparts.load(std::sync::atomic::Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn put_from_path_rejects_zero_chunk_size() {
        let c = client();
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(b"tiny").unwrap();
        let key = Path::from("seg/bad");

        let err = c
            .put_from_path(&key, f.path(), 8, 0, PutRequest::default())
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            ObjectStoreError::Io(e) if e.kind() == std::io::ErrorKind::InvalidInput
        ));
    }

    /// The single-PUT path honours `PutMode::Create`; the multipart path
    /// cannot, because `object_store` 0.13's `PutMultipartOptions` has no mode.
    /// A `Create` write of a file at or above the threshold therefore
    /// overwrites, and this test pins that documented limitation.
    #[tokio::test]
    async fn put_from_path_create_mode_only_binds_below_the_threshold() {
        for (case, len, expect_conflict) in
            [("single put", 7usize, true), ("multipart", 8usize, false)]
        {
            let c = client();
            let key = Path::from("seg/once");
            let mut f = tempfile::NamedTempFile::new().unwrap();
            f.write_all(&vec![9u8; len]).unwrap();
            let req = PutRequest {
                mode: PutMode::Create,
                digest: false,
            };
            c.put_from_path(&key, f.path(), 8, 4, req.clone())
                .await
                .unwrap();

            let second = c.put_from_path(&key, f.path(), 8, 4, req).await;

            check!(
                matches!(second, Err(ObjectStoreError::AlreadyExists(_))) == expect_conflict,
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn mock_seam_compiles_and_returns() {
        let mut mock = MockObjectOps::new();
        mock.expect_get()
            .returning(|_| Ok(bytes::Bytes::from_static(b"x")));
        mock.expect_put().returning(|_, bytes, _| {
            Ok(PutOutcome {
                size_bytes: bytes.len() as u64,
                sha256: None,
                e_tag: None,
                version_id: None,
            })
        });

        let got = mock.get(&Path::from("k")).await.unwrap();
        let outcome = mock
            .put(
                &Path::from("k"),
                bytes::Bytes::from_static(b"xy"),
                PutRequest::default(),
            )
            .await
            .unwrap();

        assert!(&got[..] == b"x");
        assert!(
            outcome
                == PutOutcome {
                    size_bytes: 2,
                    sha256: None,
                    e_tag: None,
                    version_id: None,
                }
        );
    }
}
