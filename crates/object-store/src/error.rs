//! Error taxonomy for object-store construction and access.

use {
    super::object_store_api as object_store,
    object_store::path::{Path as ObjectPath, Path},
    std::io,
};

/// Errors raised by the object-store substrate.
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreError {
    /// A local-filesystem I/O failure, for example during a read of a segment
    /// file to upload.
    #[error("object store I/O error: {0}")]
    Io(#[from] io::Error),
    /// The backend builder rejected the config: a bad bucket, region,
    /// endpoint, or credential.
    #[error("invalid object store config: {0}")]
    InvalidConfig(String),
    /// A specific object was not found. The variant is structured, so
    /// consumers can upgrade it to their own domain error without string
    /// matching.
    #[error("object not found: {0}")]
    NotFound(ObjectPath),
    /// An object exceeded a caller-supplied size cap during a buffered read.
    /// This guards against OOM on a corrupt or malicious oversized object.
    #[error("object `{key}` is {size} bytes, exceeds cap of {max_bytes} bytes")]
    TooLarge {
        /// The object that breached the cap.
        key: Path,
        /// The metadata size or the bytes observed when streaming exceeded the cap.
        size: u64,
        /// The cap in bytes.
        max_bytes: u64,
    },
    /// A conditional create (`PutMode::Create`) found an object already at
    /// the key.
    #[error("object already exists: {0}")]
    AlreadyExists(ObjectPath),
    /// A conditional read or write's precondition did not hold.
    #[error("precondition failed for `{key}`: {reason}")]
    Precondition {
        /// The object whose precondition was evaluated.
        key: ObjectPath,
        /// Why the backend rejected the operation.
        reason: String,
    },
    /// The store could not serve the request now, so the same request can
    /// succeed later and a caller may retry it.
    ///
    /// This is a throttle, a server error, a timeout or a lost connection that
    /// outlasted `object_store`'s own retries. It is also every failure that
    /// `object_store` leaves unclassified: the crate does not expose the HTTP
    /// status of such a failure, so a `400` lands here beside a `503`. A
    /// permanent failure taken for a transient one costs a wasted retry; the
    /// reverse would turn a throttled store into a hard error.
    ///
    /// The error becomes a string, so the public surface stays stable.
    #[error("object store unavailable: {0}")]
    Unavailable(String),
    /// A backend failure that the same request meets again, so a retry does
    /// not help: a refused or missing credential, an operation or a path the
    /// backend does not support, or an answer this crate cannot decode.
    ///
    /// The error becomes a string, so the public surface stays stable.
    #[error("object store backend error: {0}")]
    Backend(String),
}

impl ObjectStoreError {
    /// Convert a backend error from a write to `key`, naming `key` itself when
    /// the object already exists.
    ///
    /// A backend reports the location it refused in its own terms: the local
    /// filesystem names the absolute host path of the file, not the object key.
    /// A caller that learns a key is taken needs the key it asked for.
    pub(crate) fn from_write(key: &ObjectPath, err: object_store::Error) -> Self {
        match err {
            object_store::Error::AlreadyExists { .. } => Self::AlreadyExists(key.clone()),
            other => Self::from(other),
        }
    }
}

impl From<object_store::Error> for ObjectStoreError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::NotFound { path, .. } => Self::NotFound(ObjectPath::from(path)),
            object_store::Error::AlreadyExists { path, .. } => {
                Self::AlreadyExists(ObjectPath::from(path))
            }
            object_store::Error::Precondition { path, source } => Self::Precondition {
                key: ObjectPath::from(path),
                reason: source.to_string(),
            },
            generic @ object_store::Error::Generic { .. } => Self::Unavailable(generic.to_string()),
            other => Self::Backend(other.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{object_store, *},
        assert2::assert,
        object_store::path::Path,
        std::{io, io::ErrorKind},
    };

    #[test]
    fn io_error_converts_via_from() {
        let io = io::Error::new(ErrorKind::NotFound, "nope");
        let err: ObjectStoreError = io.into();
        assert!(matches!(err, ObjectStoreError::Io(_)));
    }

    #[test]
    fn not_found_maps_to_structured_variant() {
        let err = object_store::Error::NotFound {
            path: "tenant/block".to_owned(),
            source: "missing".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::NotFound(p) if p.to_string() == "tenant/block")
        );
    }

    #[test]
    fn already_exists_maps_to_structured_variant() {
        let err = object_store::Error::AlreadyExists {
            path: "worm/manifest.json".to_owned(),
            source: "exists".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::AlreadyExists(p) if p.to_string() == "worm/manifest.json")
        );
    }

    #[test]
    fn precondition_maps_to_structured_variant() {
        let err = object_store::Error::Precondition {
            path: "worm/manifest.json".to_owned(),
            source: "etag mismatch".into(),
        };
        let mapped = ObjectStoreError::from(err);
        assert!(
            matches!(&mapped, ObjectStoreError::Precondition { key, reason }
                if key.to_string() == "worm/manifest.json" && reason == "etag mismatch")
        );
    }

    #[test]
    fn too_large_display_includes_sizes() {
        let err = ObjectStoreError::TooLarge {
            key: Path::from("index/snapshot.json"),
            size: 1000,
            max_bytes: 256,
        };
        let msg = err.to_string();
        assert!(msg.contains("1000"));
        assert!(msg.contains("256"));
    }

    /// A failure `object_store` leaves unclassified can be retried; one it
    /// names as permanent cannot.
    #[yare::parameterized(
        generic = { object_store::Error::Generic { store: "S3", source: "503 SlowDown".into() }, true },
        permission_denied = { object_store::Error::PermissionDenied { path: "seg.log".to_owned(), source: "403".into() }, false },
        unauthenticated = { object_store::Error::Unauthenticated { path: "seg.log".to_owned(), source: "401".into() }, false },
        not_supported = { object_store::Error::NotSupported { source: "no copy_if_not_exists".into() }, false },
        not_implemented = { object_store::Error::NotImplemented { operation: "copy".to_owned(), implementer: "S3".to_owned() }, false },
        unknown_configuration_key = { object_store::Error::UnknownConfigurationKey { store: "S3", key: "region2".to_owned() }, false },
    )]
    fn backend_failures_split_by_whether_a_retry_can_help(
        err: object_store::Error,
        retryable: bool,
    ) {
        let got = match ObjectStoreError::from(err) {
            ObjectStoreError::Unavailable(_) => Some(true),
            ObjectStoreError::Backend(_) => Some(false),
            _ => None,
        };
        assert2::check!(got == Some(retryable));
    }
}
