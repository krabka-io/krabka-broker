//! Object-key derivation for the S3 backend.
//!
//! The layout mirrors [`LocalTieredStorage`](crate::LocalTieredStorage): a
//! partition directory holding one object per artifact, named by base offset
//! and segment id. A write derives one key, below the prefix in the spelling
//! the [`KeyPrefixGate`](crate::KeyPrefixGate) selects. A read or a delete
//! derives the keys under both spellings, because the bucket can hold
//! segments written on either side of the `krabka.version` finalization.

#[cfg(test)]
use crate::storage_manager::IndexType;
use {
    super::S3RemoteStorage,
    crate::{
        metadata::RemoteLogSegmentMetadata,
        storage_manager::{partition_dir_name, segment_file_name},
    },
    krabka_object_store::ObjectStoreError,
    object_store::path::Path as ObjectPath,
    std::iter,
};

/// The keys one artifact of a segment can sit under.
pub(super) struct SegmentKeys {
    /// The key a copy writes now.
    pub(super) write: ObjectPath,
    /// The key under the other spelling of the prefix, when it is a different
    /// key. A prefix with no stray characters has only one.
    pub(super) other: Option<ObjectPath>,
}

impl SegmentKeys {
    /// Reads the written key, and the other spelling only when the first is
    /// absent.
    pub(super) fn read_first<T>(
        &self,
        read: impl Fn(&ObjectPath) -> Result<T, ObjectStoreError>,
    ) -> Result<T, ObjectStoreError> {
        match (read(&self.write), &self.other) {
            (Err(ObjectStoreError::NotFound(_)), Some(other)) => read(other),
            (result, _) => result,
        }
    }

    /// Every key, the written one first.
    pub(super) fn all(&self) -> impl Iterator<Item = &ObjectPath> {
        iter::once(&self.write).chain(self.other.as_ref())
    }
}

/// The key of one artifact of a segment below `prefix`.
///
/// `ObjectPath::from` splits the joined key on `/` and drops the empty parts,
/// so separators around the prefix collapse. Whitespace does not, which is why
/// the two spellings of a padded prefix are two different keys.
fn key_under(
    prefix: Option<&str>,
    metadata: &RemoteLogSegmentMetadata,
    suffix: &str,
) -> ObjectPath {
    let mut key = String::new();
    if let Some(p) = prefix {
        key.push_str(p);
        key.push('/');
    }
    key.push_str(&partition_dir_name(metadata));
    key.push('/');
    key.push_str(&segment_file_name(metadata, suffix));
    ObjectPath::from(key)
}

impl S3RemoteStorage {
    /// The key one artifact of a segment is written under: below the prefix
    /// in the spelling the gate selects now.
    pub(super) fn segment_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        suffix: &str,
    ) -> ObjectPath {
        key_under(self.prefix.roots().write, metadata, suffix)
    }

    /// The keys one artifact of a segment can sit under, from one read of the
    /// gate.
    pub(super) fn segment_keys(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        suffix: &str,
    ) -> SegmentKeys {
        let roots = self.prefix.roots();
        let write = key_under(roots.write, metadata, suffix);
        let other = key_under(roots.other, metadata, suffix);
        SegmentKeys {
            other: (other != write).then_some(other),
            write,
        }
    }

    pub(super) fn log_key(&self, metadata: &RemoteLogSegmentMetadata) -> ObjectPath {
        self.segment_key(metadata, ".log")
    }

    #[cfg(test)]
    pub(super) fn index_key(
        &self,
        metadata: &RemoteLogSegmentMetadata,
        index_type: IndexType,
    ) -> ObjectPath {
        self.segment_key(metadata, index_type.suffix())
    }
}

#[cfg(test)]
mod scenarios;

#[cfg(test)]
mod tests {
    use {
        super::S3RemoteStorage,
        crate::{KeyPrefixForm, KeyPrefixGate, s3::test_support::sample_metadata},
        assert2::{assert, check},
        object_store::{memory::InMemory, path::Path as ObjectPath},
        std::sync::Arc,
    };

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn prefix_isolates_clusters() {
        let store_a =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("cluster-a".to_owned()));
        let _ = store_a;
        // Single cluster keys live under the prefix; we verify the key
        // construction at the unit level (no cross-cluster fixture
        // available without sharing the InMemory backend, which we don't
        // because each cluster gets its own bucket in practice).
        let md = sample_metadata(uuid::Uuid::from_u128(30));
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some("c".to_owned()));
        let key = store.log_key(&md);
        let expected = concat!(
            "c/orders-0-AAAAAAAAAAAAAAAAAAAAAQ/",
            "00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log"
        );
        assert!(
            key.as_ref() == expected,
            "unexpected Kafka-compatible object key {key:?}",
        );
    }

    /// The sample segment's directory and log file, below any prefix.
    const SAMPLE_LOG: &str =
        "orders-0-AAAAAAAAAAAAAAAAAAAAAQ/00000000000000000000-AAAAAAAAAAAAAAAAAAAAHg.log";

    /// Golden keys for the sample segment's log under each configured
    /// prefix. The as-configured column is what v1.1.5 writes, captured from
    /// that code before the trimmed spelling existed; the trimmed column is
    /// what `krabka.version` 3 writes.
    #[yare::parameterized(
        plain = { Some("tier"), "tier/", "tier/" },
        nested = { Some("a/b"), "a/b/", "a/b/" },
        padded = { Some(" tier/ "), " tier/ /", "tier/" },
        slashed = { Some("/tier/"), "tier/", "tier/" },
        padded_and_slashed = { Some(" /tier/ "), " /tier/ /", "tier/" },
        tab = { Some("\ttier"), "%09tier/", "tier/" },
        blank = { Some("   "), "   /", "" },
        slash_only = { Some("/"), "", "" },
        empty = { Some(""), "", "" },
        absent = { None, "", "" },
    )]
    fn golden_keys_in_each_prefix_form(
        prefix: Option<&str>,
        as_configured: &str,
        normalized: &str,
    ) {
        let gate = KeyPrefixGate::default();
        let store =
            S3RemoteStorage::with_store(Arc::new(InMemory::new()), prefix.map(str::to_owned))
                .with_key_prefix_gate(gate.clone());
        let md = sample_metadata(uuid::Uuid::from_u128(30));
        let as_configured_key = store.log_key(&md).to_string();
        gate.set(KeyPrefixForm::Normalized);
        let normalized_key = store.log_key(&md).to_string();
        check!(
            (as_configured_key, normalized_key)
                == (
                    format!("{as_configured}{SAMPLE_LOG}"),
                    format!("{normalized}{SAMPLE_LOG}")
                )
        );
    }

    /// The object-store path facts the two spellings rest on: separators
    /// around and inside a key collapse, whitespace and characters outside
    /// the path alphabet do not.
    #[yare::parameterized(
        leading_and_trailing_slashes = { "/tier/x/", "tier/x" },
        doubled_slashes = { "tier//x", "tier/x" },
        spaces_survive = { " tier / x ", " tier / x " },
        a_blank_part_survives = { "tier/ /x", "tier/ /x" },
        tab_is_percent_encoded = { "\ttier/x", "%09tier/x" },
    )]
    fn object_paths_collapse_separators_and_keep_whitespace(raw: &str, stored: &str) {
        check!(ObjectPath::from(raw).as_ref() == stored);
    }

    /// One read of the gate yields the key a copy writes and, only when it
    /// differs, the other spelling.
    #[yare::parameterized(
        padded_as_configured = { " tier ", KeyPrefixForm::AsConfigured, " tier ", Some("tier") },
        padded_normalized = { " tier ", KeyPrefixForm::Normalized, "tier", Some(" tier ") },
        clean_as_configured = { "tier", KeyPrefixForm::AsConfigured, "tier", None },
        slashed_normalized = { "/tier/", KeyPrefixForm::Normalized, "tier", None },
    )]
    fn segment_keys_list_the_other_spelling_only_when_it_differs(
        prefix: &str,
        form: KeyPrefixForm,
        write_root: &str,
        other_root: Option<&str>,
    ) {
        let store = S3RemoteStorage::with_store(Arc::new(InMemory::new()), Some(prefix.to_owned()))
            .with_key_prefix_gate(KeyPrefixGate::new(form));
        let keys = store.segment_keys(&sample_metadata(uuid::Uuid::from_u128(30)), ".log");
        check!(
            (
                keys.write.to_string(),
                keys.other.map(|key| key.to_string())
            ) == (
                format!("{write_root}/{SAMPLE_LOG}"),
                other_root.map(|root| format!("{root}/{SAMPLE_LOG}"))
            )
        );
    }
}
