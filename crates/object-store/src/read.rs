//! Capped whole-object reads, buffered or hashed as they stream.
//!
//! The module does a `head()` on an object, rejects the object if it exceeds a
//! byte cap, then does a conditional, streaming GET. It centralises the OOM guard that runs before
//! the code buffers a whole object, such as an index snapshot, into memory.

use {
    super::{error::ObjectStoreError, object_store_api as object_store},
    bytes::{Bytes, BytesMut},
    futures_util::TryStreamExt as _,
    object_store::{GetOptions, ObjectStoreExt as _, path::Path},
    sha2::{Digest as _, Sha256},
    std::sync::Arc,
};

/// The largest diskless WAL object any krabka tool reads whole.
///
/// `krabka backup` hashes each WAL object a capture names under this cap
/// before it signs the capture, and `krabka restore` refuses to read one
/// above it. The two must agree: a cap that drifted on one side alone would
/// let backup sign a capture that restore cannot read.
pub const MAX_WAL_OBJECT_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// The size and `SHA-256` of one object, hashed as it streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CappedDigest {
    /// Bytes the object holds.
    pub size_bytes: u64,
    /// Whole-object `SHA-256`.
    pub sha256: [u8; 32],
}

/// Read a whole object, and reject it if its size exceeds `max_bytes`.
///
/// The function does a `head()` first, so it refuses an oversized object
/// *before* it buffers any bytes. This is the guard against OOM on a corrupt or
/// malicious object.
/// The GET uses the HEAD version and etag when available. Its response size
/// and streamed chunks are also checked, so an overwrite cannot bypass the cap.
///
/// # Errors
///
/// - [`ObjectStoreError::TooLarge`] if the object is larger than `max_bytes`.
/// - [`ObjectStoreError::NotFound`] if the object does not exist.
/// - [`ObjectStoreError::Precondition`] if its etag changed after the HEAD.
/// - [`ObjectStoreError::Backend`] for any other backend failure.
pub async fn read_capped(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &Path,
    max_bytes: u64,
) -> Result<Bytes, ObjectStoreError> {
    let mut bytes = BytesMut::new();
    stream_capped(store, key, max_bytes, |chunk| {
        bytes.extend_from_slice(chunk);
    })
    .await?;
    Ok(bytes.freeze())
}

/// Hash a whole object under the same cap and checks as [`read_capped`],
/// without buffering it.
///
/// The body is hashed chunk by chunk, so the memory cost does not follow the
/// object size.
///
/// # Errors
///
/// The same as [`read_capped`].
pub async fn digest_capped(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &Path,
    max_bytes: u64,
) -> Result<CappedDigest, ObjectStoreError> {
    let mut hasher = Sha256::new();
    let size_bytes = stream_capped(store, key, max_bytes, |chunk| {
        hasher.update(chunk);
    })
    .await?;
    Ok(CappedDigest {
        size_bytes,
        sha256: hasher.finalize().into(),
    })
}

/// Stream a whole object into `on_chunk` under the [`read_capped`] checks,
/// and return how many bytes it held.
async fn stream_capped(
    store: &Arc<dyn object_store::ObjectStore>,
    key: &Path,
    max_bytes: u64,
    mut on_chunk: impl FnMut(&[u8]),
) -> Result<u64, ObjectStoreError> {
    let too_large = |size| ObjectStoreError::TooLarge {
        key: key.clone(),
        size,
        max_bytes,
    };
    let meta = store.head(key).await?;
    if meta.size > max_bytes {
        return Err(too_large(meta.size));
    }
    let result = store
        .get_opts(key, GetOptions {
            version: meta.version,
            if_match: meta.e_tag,
            ..GetOptions::default()
        })
        .await?;
    if result.meta.size > max_bytes {
        return Err(too_large(result.meta.size));
    }
    let mut stream = result.into_stream();
    let mut size = 0_u64;
    while let Some(chunk) = stream.try_next().await? {
        size = size.saturating_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX));
        if size > max_bytes {
            return Err(too_large(size));
        }
        on_chunk(&chunk);
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use {
        super::{object_store, *},
        assert2::assert,
        futures_util::{StreamExt as _, stream},
        object_store::{PutPayload, memory::InMemory, path::Path},
        std::{
            fmt, iter,
            sync::{
                Arc,
                atomic::{AtomicUsize, Ordering::SeqCst},
            },
        },
    };

    async fn store_with(key: &str, bytes: &'static [u8]) -> Arc<dyn object_store::ObjectStore> {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        store
            .put(&Path::from(key), PutPayload::from(bytes))
            .await
            .unwrap();
        store
    }

    #[tokio::test]
    async fn under_cap_returns_bytes() {
        let store = store_with("k", b"hello").await;
        let got = read_capped(&store, &Path::from("k"), 1024).await.unwrap();
        assert!(&got[..] == b"hello");
    }

    #[tokio::test]
    async fn digest_hashes_the_whole_object_without_returning_it() {
        let store = store_with("k", b"hello").await;
        let digest = digest_capped(&store, &Path::from("k"), 5).await.unwrap();
        assert!(
            digest
                == CappedDigest {
                    size_bytes: 5,
                    sha256: Sha256::digest(b"hello").into(),
                }
        );
    }

    #[yare::parameterized(
        empty = { 0, 0 },
        one_byte = { 1, 1 },
        at_the_cap = { 4096, 4096 },
        one_over_the_cap = { 4097, 4096 },
        far_over_the_cap = { 1_000_000, 10 },
    )]
    #[test_macro(tokio::test)]
    async fn digest_agrees_with_hashing_the_buffered_read(len: u32, max_bytes: u64) {
        let body = (0..len)
            .map(|index| index.to_le_bytes()[0] ^ index.to_le_bytes()[1])
            .collect::<Vec<u8>>();
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let key = Path::from("k");
        store.put(&key, PutPayload::from(body)).await.unwrap();

        let buffered = read_capped(&store, &key, max_bytes)
            .await
            .map(|bytes| CappedDigest {
                size_bytes: u64::try_from(bytes.len()).unwrap(),
                sha256: Sha256::digest(&bytes).into(),
            });
        let streamed = digest_capped(&store, &key, max_bytes).await;

        assert2::assert!(format!("{streamed:?}") == format!("{buffered:?}"));
    }

    #[tokio::test]
    async fn over_cap_returns_too_large() {
        let store = store_with("k", b"hello world").await;
        let err = read_capped(&store, &Path::from("k"), 4).await.unwrap_err();
        assert!(matches!(err, ObjectStoreError::TooLarge {
            size: 11,
            max_bytes: 4,
            ..
        }));
    }

    #[tokio::test]
    async fn missing_object_maps_to_not_found() {
        let store: Arc<dyn object_store::ObjectStore> = Arc::new(InMemory::new());
        let err = read_capped(&store, &Path::from("absent"), 1024)
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectStoreError::NotFound(_)));
    }

    #[derive(Debug, Clone, Copy)]
    enum Race {
        Etag,
        Version,
        Metadata,
        Body,
    }

    #[derive(Debug)]
    struct ChangingStore {
        inner: InMemory,
        race: Race,
        chunks: Arc<AtomicUsize>,
    }

    impl fmt::Display for ChangingStore {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("ChangingStore") }
    }

    #[krabka_macros::object_store_delegate(
        put_multipart_opts,
        delete_stream,
        list,
        list_with_delimiter,
        copy_opts
    )]
    #[async_trait::async_trait]
    impl object_store::ObjectStore for ChangingStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            if options.head {
                let mut result = self.inner.get_opts(location, options).await?;
                if matches!(self.race, Race::Version) {
                    result.meta.version = Some("original".into());
                } else if !matches!(self.race, Race::Etag) {
                    result.meta.e_tag = None;
                }
                return Ok(result);
            }
            if matches!(self.race, Race::Version) {
                assert!(options.version.as_deref() == Some("original"));
                let original = self
                    .inner
                    .get_opts(location, GetOptions {
                        version: None,
                        ..options
                    })
                    .await?;
                self.inner
                    .put(location, PutPayload::from_static(b"123456789"))
                    .await?;
                return Ok(original);
            }
            self.inner
                .put(location, PutPayload::from_static(b"123456789"))
                .await?;
            let mut result = self.inner.get_opts(location, options).await?;
            if matches!(self.race, Race::Body) {
                result.meta.size = 3;
                let chunks = Arc::clone(&self.chunks);
                result.payload = object_store::GetResultPayload::Stream(
                    stream::iter(iter::repeat_with(|| Ok(Bytes::from_static(b"123"))).take(3))
                        .inspect(move |_| {
                            chunks.fetch_add(1, SeqCst);
                        })
                        .boxed(),
                );
            }
            Ok(result)
        }
    }

    #[yare::parameterized(
        etag = { Race::Etag, false },
        metadata = { Race::Metadata, false },
        body = { Race::Body, false },
        digest_etag = { Race::Etag, true },
        digest_metadata = { Race::Metadata, true },
        digest_body = { Race::Body, true },
    )]
    #[test_macro(tokio::test)]
    async fn overwrite_between_head_and_get_cannot_bypass_the_cap(race: Race, digest: bool) {
        let changing = Arc::new(ChangingStore {
            inner: InMemory::new(),
            race,
            chunks: Arc::default(),
        });
        let key = Path::from("changing");
        changing
            .put(&key, PutPayload::from_static(b"123"))
            .await
            .unwrap();
        let store: Arc<dyn object_store::ObjectStore> = Arc::<ChangingStore>::clone(&changing);
        let error = if digest {
            digest_capped(&store, &key, 5).await.map(drop)
        } else {
            read_capped(&store, &key, 5).await.map(drop)
        }
        .unwrap_err();
        match race {
            Race::Etag => assert2::assert!(matches!(error, ObjectStoreError::Precondition { .. })),
            Race::Version => unreachable!("the versioned case returns the original bytes"),
            Race::Metadata => {
                assert2::assert!(matches!(error, ObjectStoreError::TooLarge { size: 9, .. }));
            }
            Race::Body => {
                assert2::assert!(matches!(error, ObjectStoreError::TooLarge { size: 6, .. }));
                assert2::assert!(changing.chunks.load(SeqCst) == 2);
            }
        }
    }

    #[tokio::test]
    async fn versioned_get_reads_the_head_version_after_an_overwrite() {
        let changing = Arc::new(ChangingStore {
            inner: InMemory::new(),
            race: Race::Version,
            chunks: Arc::default(),
        });
        let key = Path::from("versioned");
        changing
            .put(&key, PutPayload::from_static(b"123"))
            .await
            .unwrap();
        let store: Arc<dyn object_store::ObjectStore> = Arc::<ChangingStore>::clone(&changing);
        assert!(read_capped(&store, &key, 5).await.unwrap().as_ref() == b"123");
        assert!(changing.inner.head(&key).await.unwrap().size == 9);
    }
}
