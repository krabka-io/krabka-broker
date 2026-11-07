//! Scenarios for `tests/features/key_prefixing`: how an operator's prefix
//! names one archive root in a shared bucket, and how keys and listings stay
//! beneath it.

#![cfg(test)]

use {
    bytes::Bytes,
    krabka_bdd::Given,
    krabka_object_store::{
        ObjectOps as _, ObjectStoreClient, ObjectStoreError, PutMode, PutOutcome, PutRequest,
        normalize_prefix, prefixed_key,
    },
    object_store::{
        ObjectStore, ObjectStoreExt as _, local::LocalFileSystem, memory::InMemory, path::Path,
    },
    std::{convert::Infallible, str::FromStr, sync::Arc},
    tempfile::TempDir,
    tokio::runtime::{Builder, Runtime},
};

const ARCHIVED: &str = "Given a cluster has archived a WAL object under the prefix <first>";
const WRITE: &str = "When a second cluster attempts a create-only write of the same WAL object \
                     under the prefix <second>";
const LISTED: &str = "When a broker lists the prefix <prefix>";

/// Where every cluster keeps its WAL object, relative to its root.
const SEGMENT: &str = "orders-0/segment.log";

/// The bytes the first cluster archives.
const FIRST_BYTES: &[u8] = b"first cluster segment";

/// The bytes the second cluster archives.
const SECOND_BYTES: &[u8] = b"second cluster segment";

/// The store backend a scenario's bucket lives in.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Backend {
    InMemory,
    LocalFilesystem,
}

/// A comma-separated list of prefixes or keys, as a step writes it.
#[derive(Clone, Debug)]
struct Names(Vec<String>);

impl FromStr for Names {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Self(
            text.split(',').map(|name| name.trim().to_owned()).collect(),
        ))
    }
}

/// One bucket that clusters and backup archives share, each beneath the
/// prefix its operator configured.
struct Bucket {
    runtime: Runtime,
    store: Arc<dyn ObjectStore>,
    client: ObjectStoreClient,
    /// Held so a local-filesystem store outlives the scenario.
    _root: Option<TempDir>,
}

impl Bucket {
    fn new(backend: Backend) -> Self {
        let (store, root): (Arc<dyn ObjectStore>, _) = match backend {
            Backend::InMemory => (Arc::new(InMemory::new()), None),
            Backend::LocalFilesystem => {
                let root = tempfile::tempdir().unwrap();
                let store = LocalFileSystem::new_with_prefix(root.path()).unwrap();
                (Arc::new(store), Some(root))
            }
        };
        Self {
            runtime: Builder::new_current_thread().enable_all().build().unwrap(),
            client: ObjectStoreClient::new(Arc::clone(&store)),
            store,
            _root: root,
        }
    }

    /// The key a broker derives for `relative` from its configured prefix.
    fn key(prefix: Option<&str>, relative: &str) -> Path {
        prefixed_key(normalize_prefix(prefix).as_deref(), relative)
    }

    fn write(&self, prefix: Option<&str>, bytes: &'static [u8], mode: PutMode) -> Write {
        let request = PutRequest {
            mode,
            digest: false,
        };
        self.runtime.block_on(self.client.put(
            &Self::key(prefix, SEGMENT),
            Bytes::from_static(bytes),
            request,
        ))
    }

    fn read(&self, prefix: Option<&str>) -> Bytes {
        self.runtime.block_on(async {
            self.store
                .get(&Self::key(prefix, SEGMENT))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        })
    }

    fn list(&self, prefix: Option<&str>) -> Vec<String> {
        let root = normalize_prefix(prefix).map(Path::from);
        let listing = self.runtime.block_on(self.client.list(root)).unwrap();
        let mut keys: Vec<String> = listing
            .into_iter()
            .map(|object| object.location.to_string())
            .collect();
        keys.sort();
        keys
    }
}

type Write = Result<PutOutcome, ObjectStoreError>;

struct PrefixScenario {
    bucket: Bucket,
    first: Given<String>,
    second: Given<String>,
    write: Given<Write>,
    listing: Given<Vec<String>>,
}

#[rstest::fixture]
fn prefix_scenario() -> PrefixScenario {
    PrefixScenario {
        bucket: Bucket::new(Backend::InMemory),
        first: Given::new("first", ARCHIVED),
        second: Given::new("second", WRITE),
        write: Given::new("write", WRITE),
        listing: Given::new("listing", LISTED),
    }
}

fn fill(bucket: &Bucket, prefixes: &Names) {
    for prefix in &prefixes.0 {
        let written = bucket.write(Some(prefix), FIRST_BYTES, PutMode::Create);
        assert2::assert!(let Ok(_) = written);
    }
}

#[rstest_bdd_macros::given("a cluster has archived a WAL object under the prefix {prefix}")]
fn cluster_archived(prefix_scenario: &mut PrefixScenario, prefix: String) {
    let archived = prefix_scenario
        .bucket
        .write(Some(&prefix), FIRST_BYTES, PutMode::Create);
    assert2::assert!(let Ok(_) = archived);
    prefix_scenario.first.set(prefix);
}

#[rstest_bdd_macros::given("a bucket holds a WAL object under each of the prefixes {prefixes}")]
fn bucket_holds(prefix_scenario: &mut PrefixScenario, prefixes: Names) {
    fill(&prefix_scenario.bucket, &prefixes);
}

#[rstest_bdd_macros::given(
    "a bucket in the {store} store holds a WAL object under each of the prefixes {prefixes}"
)]
fn bucket_in_store_holds(prefix_scenario: &mut PrefixScenario, store: Backend, prefixes: Names) {
    prefix_scenario.bucket = Bucket::new(store);
    fill(&prefix_scenario.bucket, &prefixes);
}

fn write_segment(scenario: &PrefixScenario, relative: &str, prefix: Option<&str>) {
    assert2::assert!(relative == SEGMENT);
    let written = scenario.bucket.write(prefix, FIRST_BYTES, PutMode::Create);
    assert2::assert!(let Ok(_) = written);
}

#[rstest_bdd_macros::when("a broker writes the WAL object {relative} under the prefix {prefix}")]
fn write_under_prefix(prefix_scenario: &mut PrefixScenario, relative: String, prefix: String) {
    write_segment(prefix_scenario, &relative, Some(&prefix));
}

#[rstest_bdd_macros::when(
    "a broker writes the WAL object {relative} under a space-padded prefix {prefix}"
)]
fn write_under_padded_prefix(
    prefix_scenario: &mut PrefixScenario,
    relative: String,
    prefix: String,
) {
    write_segment(prefix_scenario, &relative, Some(&format!("  {prefix}  ")));
}

#[rstest_bdd_macros::when("a broker writes the WAL object {relative} under an empty prefix")]
fn write_under_empty_prefix(prefix_scenario: &mut PrefixScenario, relative: String) {
    write_segment(prefix_scenario, &relative, Some(""));
}

#[rstest_bdd_macros::when(
    "a broker writes the WAL object {relative} under a prefix of only spaces"
)]
fn write_under_blank_prefix(prefix_scenario: &mut PrefixScenario, relative: String) {
    write_segment(prefix_scenario, &relative, Some("   "));
}

#[rstest_bdd_macros::when("a broker with no prefix writes the WAL object {relative}")]
fn write_without_prefix(prefix_scenario: &mut PrefixScenario, relative: String) {
    write_segment(prefix_scenario, &relative, None);
}

#[rstest_bdd_macros::when(
    "a second cluster attempts a create-only write of the same WAL object under the prefix \
     {prefix}"
)]
fn second_cluster_writes(prefix_scenario: &mut PrefixScenario, prefix: String) {
    let write = prefix_scenario
        .bucket
        .write(Some(&prefix), SECOND_BYTES, PutMode::Create);
    prefix_scenario.second.set(prefix);
    prefix_scenario.write.set(write);
}

#[rstest_bdd_macros::when("a broker lists the prefix {prefix}")]
fn broker_lists(prefix_scenario: &mut PrefixScenario, prefix: String) {
    let listing = prefix_scenario.bucket.list(Some(&prefix));
    prefix_scenario.listing.set(listing);
}

#[rstest_bdd_macros::then("the bucket holds exactly the key {key}")]
fn holds_exactly(prefix_scenario: &PrefixScenario, key: String) {
    assert2::assert!(prefix_scenario.bucket.list(None) == vec![key]);
}

#[rstest_bdd_macros::then("the write is accepted")]
fn accepted(prefix_scenario: &PrefixScenario) {
    assert2::assert!(let Ok(_) = prefix_scenario.write.get());
}

#[rstest_bdd_macros::then("the write is refused because the object already exists")]
fn refused_already_exists(prefix_scenario: &PrefixScenario) {
    let first = prefix_scenario.first.get();
    let expected = Bucket::key(Some(first), SEGMENT).to_string();
    assert2::assert!(let Err(ObjectStoreError::AlreadyExists(refused)) = prefix_scenario.write.get());
    assert2::assert!(refused.as_ref() == expected);
}

#[rstest_bdd_macros::then("each cluster reads back its own object bytes")]
fn each_reads_own(prefix_scenario: &PrefixScenario) {
    let bucket = &prefix_scenario.bucket;
    let read = (
        bucket.read(Some(prefix_scenario.first.get())),
        bucket.read(Some(prefix_scenario.second.get())),
    );
    assert2::assert!(
        read == (
            Bytes::from_static(FIRST_BYTES),
            Bytes::from_static(SECOND_BYTES)
        )
    );
}

#[rstest_bdd_macros::then("the first cluster still reads back its own object bytes")]
fn first_reads_own(prefix_scenario: &PrefixScenario) {
    let read = prefix_scenario
        .bucket
        .read(Some(prefix_scenario.first.get()));
    assert2::assert!(read == FIRST_BYTES);
}

#[rstest_bdd_macros::then("the listing holds exactly {keys}")]
fn listing_holds(prefix_scenario: &PrefixScenario, keys: Names) {
    assert2::assert!(*prefix_scenario.listing.get() == keys.0);
}

krabka_bdd::scenarios!(
    "tests/features/key_prefixing",
    fixtures = [prefix_scenario: PrefixScenario]
);
