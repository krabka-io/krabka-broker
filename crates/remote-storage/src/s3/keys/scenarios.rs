//! Step definitions for `tests/features/key_prefixing`: where a tiered segment
//! lands under a configured key prefix, on each side of the `krabka.version`
//! finalization that trims it.

use {
    crate::{
        KeyPrefixForm, KeyPrefixGate, S3RemoteStorage,
        metadata::RemoteLogSegmentMetadata,
        storage_manager::{IndexType, RemoteStorageManager, partition_dir_name, segment_file_name},
        test_support::{TransactionIndex, sample_data, sample_metadata},
    },
    assert2::check,
    futures_util::TryStreamExt as _,
    object_store::{ObjectStore, memory::InMemory},
    std::{convert::Infallible, str::FromStr, sync::Arc},
    tempfile::TempDir,
    tokio::runtime::{self, Runtime},
};

/// The first `krabka.version` level that writes the trimmed prefix.
///
/// This mirrors `krabka_metadata::krabka_version::TIERED_KEY_PREFIX_NORMALIZED_LEVEL`.
/// The remote tier has no metadata dependency, so the broker maps the level to
/// a [`KeyPrefixForm`] and these steps repeat that mapping.
const TIERED_KEY_PREFIX_NORMALIZED_LEVEL: i16 = 3;

/// The [`KeyPrefixForm`] a cluster at `level` writes.
fn form_at(level: i16) -> KeyPrefixForm {
    if level >= TIERED_KEY_PREFIX_NORMALIZED_LEVEL {
        KeyPrefixForm::Normalized
    } else {
        KeyPrefixForm::AsConfigured
    }
}

/// A prefix as a feature file spells it: Gherkin trims table cells, so `·`
/// stands for a space.
struct SpelledPrefix(String);

impl FromStr for SpelledPrefix {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> { Ok(Self(text.replace('·', " "))) }
}

/// A remote tier over an in-memory bucket the steps can list.
struct RemoteTier {
    runtime: Runtime,
    bucket: Arc<InMemory>,
    gate: KeyPrefixGate,
    storage: S3RemoteStorage,
    /// Holds the segment files the copy reads.
    source: TempDir,
}

impl RemoteTier {
    fn new(prefix: String) -> Self {
        let bucket = Arc::new(InMemory::new());
        let gate = KeyPrefixGate::default();
        let storage = S3RemoteStorage::with_store(Arc::<InMemory>::clone(&bucket), Some(prefix))
            .with_key_prefix_gate(gate.clone());
        Self {
            runtime: runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
            bucket,
            gate,
            storage,
            source: TempDir::new().unwrap(),
        }
    }

    /// Runs one synchronous storage call the way the broker does: off the
    /// runtime's workers, with its handle in reach.
    fn blocking<T>(&self, call: impl FnOnce(&S3RemoteStorage) -> T) -> T {
        let _entered = self.runtime.enter();
        call(&self.storage)
    }

    fn copy(&self, metadata: &RemoteLogSegmentMetadata) {
        let data = sample_data(self.source.path(), TransactionIndex::Omitted);
        self.blocking(|storage| storage.copy_log_segment_data(metadata, &data))
            .unwrap();
    }

    /// Every key the bucket holds, sorted.
    fn keys(&self) -> Vec<String> {
        self.runtime.block_on(async {
            let mut keys = Vec::new();
            let mut listing = self.bucket.list(None);
            while let Some(meta) = listing.try_next().await.unwrap() {
                keys.push(meta.location.to_string());
            }
            keys.sort();
            keys
        })
    }
}

/// What a consumer read back from the remote tier, or why it could not.
#[derive(Debug, PartialEq, Eq)]
struct ReadBack {
    log: Result<Vec<u8>, String>,
    offset_index: Result<Vec<u8>, String>,
}

struct KeyPrefixScenario {
    tier: Option<RemoteTier>,
    metadata: RemoteLogSegmentMetadata,
    read_back: Option<ReadBack>,
}

impl KeyPrefixScenario {
    fn tier(&self) -> &RemoteTier { self.tier.as_ref().expect("a remote tier was given") }

    /// The sample segment's log file, below any prefix.
    fn relative_log_key(&self) -> String {
        format!(
            "{}/{}",
            partition_dir_name(&self.metadata),
            segment_file_name(&self.metadata, ".log")
        )
    }
}

#[rstest::fixture]
fn key_prefixing() -> KeyPrefixScenario {
    KeyPrefixScenario {
        tier: None,
        metadata: sample_metadata(uuid::Uuid::from_u128(30)),
        read_back: None,
    }
}

#[rstest_bdd_macros::given("a remote tier with key prefix {prefix}")]
fn remote_tier(key_prefixing: &mut KeyPrefixScenario, prefix: SpelledPrefix) {
    key_prefixing.tier = Some(RemoteTier::new(prefix.0));
}

#[rstest_bdd_macros::given("the cluster has finalized krabka.version {level}")]
fn finalized(key_prefixing: &mut KeyPrefixScenario, level: i16) {
    key_prefixing.tier().gate.set(form_at(level));
}

#[rstest_bdd_macros::given(
    "a segment of orders partition 0 was copied to it at krabka.version {written}"
)]
fn copied_at(key_prefixing: &mut KeyPrefixScenario, written: i16) {
    let tier = key_prefixing.tier();
    tier.gate.set(form_at(written));
    tier.copy(&key_prefixing.metadata);
}

#[rstest_bdd_macros::when("a segment of orders partition 0 is copied to the remote tier")]
fn copy_segment(key_prefixing: &mut KeyPrefixScenario) {
    key_prefixing.tier().copy(&key_prefixing.metadata);
}

#[rstest_bdd_macros::when("a consumer reads the segment back from the remote tier")]
fn read_segment(key_prefixing: &mut KeyPrefixScenario) {
    let metadata = &key_prefixing.metadata;
    let read_back = key_prefixing.tier().blocking(|storage| ReadBack {
        log: storage
            .fetch_log_segment(metadata, 0, None)
            .map_err(|error| error.to_string()),
        offset_index: storage
            .fetch_index(metadata, IndexType::Offset)
            .map_err(|error| error.to_string()),
    });
    key_prefixing.read_back = Some(read_back);
}

#[rstest_bdd_macros::when("the remote log manager deletes the segment")]
fn delete_segment(key_prefixing: &mut KeyPrefixScenario) {
    let metadata = &key_prefixing.metadata;
    key_prefixing
        .tier()
        .blocking(|storage| storage.delete_log_segment_data(metadata))
        .unwrap();
}

#[rstest_bdd_macros::then("the segment log is stored in its partition directory under {root}")]
fn stored_under(key_prefixing: &KeyPrefixScenario, root: SpelledPrefix) {
    let expected = format!("{}/{}", root.0, key_prefixing.relative_log_key());
    let keys = key_prefixing.tier().keys();
    check!(keys.contains(&expected), "{keys:?}");
}

#[rstest_bdd_macros::then(
    "the segment log is stored in its partition directory at the bucket root"
)]
fn stored_at_root(key_prefixing: &KeyPrefixScenario) {
    let keys = key_prefixing.tier().keys();
    check!(keys.contains(&key_prefixing.relative_log_key()), "{keys:?}");
}

#[rstest_bdd_macros::then("the remote tier returns the segment as it was copied")]
fn returned_as_copied(key_prefixing: &KeyPrefixScenario) {
    check!(
        key_prefixing.read_back
            == Some(ReadBack {
                log: Ok(b"0123456789".to_vec()),
                offset_index: Ok(b"OFFSET-IDX".to_vec()),
            })
    );
}

#[rstest_bdd_macros::then("the remote tier holds no object of the segment")]
fn holds_nothing(key_prefixing: &KeyPrefixScenario) {
    check!(key_prefixing.tier().keys() == Vec::<String>::new());
}

rstest_bdd_macros::scenarios!("tests/features/key_prefixing", fixtures = [key_prefixing: KeyPrefixScenario]);
