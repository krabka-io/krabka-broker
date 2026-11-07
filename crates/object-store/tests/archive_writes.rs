//! Scenarios for `tests/features/archive_writes`: the write-once rule of the
//! WORM archive and the receipt every write returns.

#![cfg(test)]

use {
    bytes::Bytes,
    krabka_bdd::Given,
    krabka_object_store::{
        ObjectOps as _, ObjectStoreClient, ObjectStoreError, PutMode, PutOutcome, PutRequest,
    },
    object_store::{ObjectStore, ObjectStoreExt as _, local::LocalFileSystem, memory::InMemory},
    std::{convert::Infallible, fmt::Write as _, fs, str::FromStr, sync::Arc},
    tempfile::TempDir,
    tokio::runtime::{Builder, Runtime},
};

const ARCHIVED: &str = "Given a tiered segment has been archived";
const WRITE: &str = "When a broker attempts a write";

/// Where the first archived segment lives.
const SEGMENT_KEY: &str = "cluster/topic-0/00000000000000000000.log";

/// The bytes of the first archived segment.
const FIRST_SEGMENT: &[u8] = b"first archived segment";

/// The bytes a later write tries to put at the same key.
const OTHER_BYTES: &[u8] = b"other bytes";

/// The part size of a multipart upload. The rules do not depend on it, only
/// on whether a file is uploaded in parts at all.
const PART_SIZE: usize = 64 * 1024;

/// The store backend a scenario archives to.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Backend {
    InMemory,
    LocalFilesystem,
}

/// What protects an archived object against being replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Protection {
    /// The store refused to create the object if the key already existed.
    AtomicCreate,
    /// The bucket's retention policy protects the completed version.
    BucketRetention,
}

/// A segment payload as a step writes it: literal text, or `empty`.
#[derive(Clone, Debug)]
struct Payload(Bytes);

impl FromStr for Payload {
    type Err = Infallible;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        Ok(Self(if text == "empty" {
            Bytes::new()
        } else {
            Bytes::copy_from_slice(text.as_bytes())
        }))
    }
}

/// An archive bucket a broker writes tiered segments to, and the local
/// directory its segment files are staged in.
struct Archive {
    runtime: Runtime,
    store: Arc<dyn ObjectStore>,
    client: ObjectStoreClient,
    staging: TempDir,
    multipart_threshold: u64,
    /// Held so a local-filesystem store outlives the scenario.
    _root: Option<TempDir>,
}

impl Archive {
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
            staging: tempfile::tempdir().unwrap(),
            multipart_threshold: 1024,
            _root: root,
        }
    }

    fn write(&self, key: &str, bytes: Bytes, request: PutRequest) -> Write {
        self.runtime
            .block_on(self.client.put(&key.into(), bytes, request))
    }

    fn upload(&self, key: &str, contents: &[u8], request: PutRequest) -> Write {
        let file = self.staging.path().join("segment.log");
        fs::write(&file, contents).unwrap();
        self.runtime.block_on(self.client.put_from_path(
            &key.into(),
            &file,
            self.multipart_threshold,
            PART_SIZE,
            request,
        ))
    }

    fn stored(&self, key: &str) -> Bytes {
        self.runtime.block_on(async {
            self.store
                .get(&key.into())
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
        })
    }

    fn entity_tag(&self, key: &str) -> Option<String> {
        self.runtime
            .block_on(self.store.head(&key.into()))
            .unwrap()
            .e_tag
    }
}

type Write = Result<PutOutcome, ObjectStoreError>;

fn create_only(digest: bool) -> PutRequest {
    PutRequest {
        mode: PutMode::Create,
        digest,
    }
}

fn hex(digest: &[u8; 32]) -> String {
    digest.iter().fold(String::new(), |mut out, byte| {
        write!(out, "{byte:02x}").unwrap();
        out
    })
}

struct ArchiveScenario {
    archive: Archive,
    key: Given<&'static str>,
    write: Given<Write>,
}

#[rstest::fixture]
fn archive_scenario() -> ArchiveScenario {
    ArchiveScenario {
        archive: Archive::new(Backend::InMemory),
        key: Given::new("key", ARCHIVED),
        write: Given::new("write", WRITE),
    }
}

fn archive_first_segment(scenario: &mut ArchiveScenario) {
    let archived = scenario.archive.write(
        SEGMENT_KEY,
        Bytes::from_static(FIRST_SEGMENT),
        create_only(true),
    );
    assert2::assert!(let Ok(_) = archived);
    scenario.key.set(SEGMENT_KEY);
}

#[rstest_bdd_macros::given("a tiered segment has been archived")]
fn segment_archived(archive_scenario: &mut ArchiveScenario) {
    archive_first_segment(archive_scenario);
}

#[rstest_bdd_macros::given("a tiered segment has been archived to the {store} store")]
fn segment_archived_to(archive_scenario: &mut ArchiveScenario, store: Backend) {
    archive_scenario.archive = Archive::new(store);
    archive_first_segment(archive_scenario);
}

#[rstest_bdd_macros::given("segment files of {threshold} bytes and more upload in parts")]
fn multipart_threshold(archive_scenario: &mut ArchiveScenario, threshold: u64) {
    archive_scenario.archive.multipart_threshold = threshold;
}

#[rstest_bdd_macros::when("a broker attempts a create-only write of other bytes to the same key")]
fn create_only_write_to_same_key(archive_scenario: &mut ArchiveScenario) {
    let key = *archive_scenario.key.get();
    let write =
        archive_scenario
            .archive
            .write(key, Bytes::from_static(OTHER_BYTES), create_only(true));
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when("a broker attempts an overwrite of other bytes to the same key")]
fn overwrite_same_key(archive_scenario: &mut ArchiveScenario) {
    let key = *archive_scenario.key.get();
    let write =
        archive_scenario
            .archive
            .write(key, Bytes::from_static(OTHER_BYTES), PutRequest::default());
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when(
    "a broker attempts a create-only write of a tiered segment to a fresh key"
)]
fn create_only_write_to_fresh_key(archive_scenario: &mut ArchiveScenario) {
    let write = archive_scenario.archive.write(
        SEGMENT_KEY,
        Bytes::from_static(OTHER_BYTES),
        create_only(true),
    );
    archive_scenario.key.set(SEGMENT_KEY);
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when(
    "a broker attempts a create-only upload of a segment file at the multipart threshold to the \
     same key"
)]
fn create_only_multipart_upload_to_same_key(archive_scenario: &mut ArchiveScenario) {
    let key = *archive_scenario.key.get();
    let archive = &archive_scenario.archive;
    let size = usize::try_from(archive.multipart_threshold).unwrap();
    let write = archive.upload(key, &vec![b'x'; size], create_only(true));
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when("a broker uploads a segment file of {size} bytes create-only")]
fn upload_create_only(archive_scenario: &mut ArchiveScenario, size: usize) {
    let write = archive_scenario
        .archive
        .upload(SEGMENT_KEY, &vec![b'x'; size], create_only(false));
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when(
    "a broker uploads a segment file of one million a characters with a digest"
)]
fn upload_one_million_a(archive_scenario: &mut ArchiveScenario) {
    let write =
        archive_scenario
            .archive
            .upload(SEGMENT_KEY, &vec![b'a'; 1_000_000], create_only(true));
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when("a broker writes the segment {payload} with a digest")]
fn write_digested(archive_scenario: &mut ArchiveScenario, payload: Payload) {
    let write = archive_scenario
        .archive
        .write(SEGMENT_KEY, payload.0, create_only(true));
    archive_scenario.key.set(SEGMENT_KEY);
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::when("a broker writes the segment {payload} without a digest")]
fn write_undigested(archive_scenario: &mut ArchiveScenario, payload: Payload) {
    let write = archive_scenario
        .archive
        .write(SEGMENT_KEY, payload.0, create_only(false));
    archive_scenario.write.set(write);
}

#[rstest_bdd_macros::then("the write is refused because the object already exists")]
fn refused_already_exists(archive_scenario: &ArchiveScenario) {
    let key = *archive_scenario.key.get();
    assert2::assert!(let Err(ObjectStoreError::AlreadyExists(refused)) = archive_scenario.write.get());
    assert2::assert!(refused.as_ref() == key);
}

#[rstest_bdd_macros::then("the write is accepted")]
fn accepted(archive_scenario: &ArchiveScenario) {
    assert2::assert!(let Ok(_) = archive_scenario.write.get());
}

#[rstest_bdd_macros::then("the key still holds the first archived segment")]
fn still_holds_first(archive_scenario: &ArchiveScenario) {
    let key = *archive_scenario.key.get();
    assert2::assert!(archive_scenario.archive.stored(key) == FIRST_SEGMENT);
}

#[rstest_bdd_macros::then("the key holds the new bytes")]
fn holds_new_bytes(archive_scenario: &ArchiveScenario) {
    let key = *archive_scenario.key.get();
    assert2::assert!(archive_scenario.archive.stored(key) == OTHER_BYTES);
}

#[rstest_bdd_macros::then("the receipt says the object is protected by {protection}")]
fn protected_by(archive_scenario: &ArchiveScenario, protection: Protection) {
    assert2::assert!(let Ok(receipt) = archive_scenario.write.get());
    let reported = if receipt.create_precondition {
        Protection::AtomicCreate
    } else {
        Protection::BucketRetention
    };
    assert2::assert!(reported == protection);
}

#[rstest_bdd_macros::then("the receipt reports a size of {size} bytes")]
fn reports_size(archive_scenario: &ArchiveScenario, size: u64) {
    assert2::assert!(let Ok(receipt) = archive_scenario.write.get());
    assert2::assert!(receipt.size_bytes == size);
}

#[rstest_bdd_macros::then("the receipt reports the SHA-256 {digest}")]
fn reports_digest(archive_scenario: &ArchiveScenario, digest: String) {
    assert2::assert!(let Ok(receipt) = archive_scenario.write.get());
    assert2::assert!(receipt.sha256.as_ref().map(hex) == Some(digest));
}

#[rstest_bdd_macros::then("the receipt carries no digest")]
fn no_digest(archive_scenario: &ArchiveScenario) {
    assert2::assert!(let Ok(receipt) = archive_scenario.write.get());
    assert2::assert!(receipt.sha256 == None);
}

#[rstest_bdd_macros::then("the receipt carries the entity tag the store reports for the key")]
fn carries_entity_tag(archive_scenario: &ArchiveScenario) {
    let key = *archive_scenario.key.get();
    assert2::assert!(let Ok(receipt) = archive_scenario.write.get());
    let reported = archive_scenario.archive.entity_tag(key);
    assert2::assert!(reported.is_some());
    assert2::assert!(receipt.e_tag == reported);
}

krabka_bdd::scenarios!(
    "tests/features/archive_writes",
    fixtures = [archive_scenario: ArchiveScenario]
);
