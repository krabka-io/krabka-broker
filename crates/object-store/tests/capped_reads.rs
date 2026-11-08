//! Scenarios for `tests/features/capped_reads`: the size cap on a buffered
//! read of a whole archive object, and the overwrite race it must survive.

#![cfg(test)]

use {
    async_trait::async_trait,
    bytes::Bytes,
    krabka_bdd::Given,
    krabka_object_store::{
        ObjectOps as _, ObjectStoreClient, ObjectStoreError,
        fault::{FaultInjectingStore, FaultKind, FaultPolicy, OpFault, StoreOp},
    },
    object_store::{
        GetOptions, GetResult, ObjectStore, ObjectStoreExt as _, PutOptions, PutPayload, PutResult,
        memory::InMemory, path::Path,
    },
    std::{fmt, sync::Arc},
    tokio::runtime::{Builder, Runtime},
};

const ARCHIVED: &str = "Given a remote index of 64 bytes has been archived";
const READ: &str = "When a broker attempts to read the remote index with a cap of 64 bytes";

/// Where the remote index of a tiered segment lives.
const INDEX_KEY: &str = "cluster/topic-0/00000000000000000000.index";

/// Where the segment manifest of a tiered segment lives.
const MANIFEST_KEY: &str = "cluster/topic-0/00000000000000000000.manifest.json";

/// The byte every archived object is filled with.
const ORIGINAL_BYTE: u8 = b'o';

/// The byte every replacement object is filled with.
const REPLACEMENT_BYTE: u8 = b'r';

/// The archive object a scenario reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum ArchiveObject {
    #[step_param("remote index")]
    RemoteIndex,
    #[step_param("segment manifest")]
    SegmentManifest,
}

impl ArchiveObject {
    const fn key(self) -> &'static str {
        match self {
            Self::RemoteIndex => INDEX_KEY,
            Self::SegmentManifest => MANIFEST_KEY,
        }
    }
}

/// A store that keeps no entity tags, as some S3-compatible stores do not
/// return one. Its reads cannot be pinned to the object a size check saw.
#[derive(Debug)]
struct UntaggedStore {
    inner: InMemory,
}

impl fmt::Display for UntaggedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("UntaggedStore") }
}

#[krabka_macros::object_store_delegate(
    put_multipart_opts,
    delete_stream,
    list,
    list_with_delimiter,
    copy_opts
)]
#[async_trait]
impl ObjectStore for UntaggedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        result.e_tag = None;
        Ok(result)
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        result.meta.e_tag = None;
        Ok(result)
    }
}

/// A tiered-storage archive bucket, and how it misbehaves while a broker reads
/// from it.
struct Archive {
    runtime: Runtime,
    store: Arc<dyn ObjectStore>,
    policy: FaultPolicy,
    replacement: Option<Bytes>,
    /// The store the last read went through, kept to count its requests.
    served: Option<Arc<FaultInjectingStore>>,
}

impl Archive {
    fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            runtime: Builder::new_current_thread().enable_all().build().unwrap(),
            store,
            policy: FaultPolicy::none(),
            replacement: None,
            served: None,
        }
    }

    fn seed(&self, key: &str, size: usize) {
        let payload = PutPayload::from(vec![ORIGINAL_BYTE; size]);
        self.runtime
            .block_on(self.store.put(&key.into(), payload))
            .unwrap();
    }

    fn read(&mut self, key: &str, cap: u64) -> Read {
        let mut faulty = FaultInjectingStore::new(Arc::clone(&self.store), self.policy.clone());
        if let Some(replacement) = &self.replacement {
            faulty = faulty.with_get_overwrite(replacement.clone());
        }
        let faulty = Arc::new(faulty);
        self.served = Some(Arc::clone(&faulty));
        let client = ObjectStoreClient::new(faulty);
        self.runtime.block_on(client.get_capped(&key.into(), cap))
    }

    /// How many size checks and reads the last read sent to the store.
    fn requests(&self) -> u64 {
        self.served
            .as_ref()
            .map_or(0, |store| store.attempts(StoreOp::Get))
    }
}

type Read = Result<Bytes, ObjectStoreError>;

struct CappedReadScenario {
    archive: Archive,
    object: Given<ArchiveObject>,
    replacement_size: Given<usize>,
    read: Given<Read>,
}

#[rstest::fixture]
fn capped_read_scenario() -> CappedReadScenario {
    CappedReadScenario {
        archive: Archive::new(Arc::new(InMemory::new())),
        object: Given::new("object", ARCHIVED),
        replacement_size: Given::new(
            "replacement_size",
            "Given the remote index is replaced by one of 128 bytes after its size is checked",
        ),
        read: Given::new("read", READ),
    }
}

fn archive_object(scenario: &mut CappedReadScenario, object: ArchiveObject, size: usize) {
    scenario.archive.seed(object.key(), size);
    scenario.object.set(object);
}

fn read_object(scenario: &mut CappedReadScenario, object: ArchiveObject, cap: u64) {
    let read = scenario.archive.read(object.key(), cap);
    scenario.read.set(read);
}

#[rstest_bdd_macros::given("a {object} of {size} bytes has been archived")]
fn object_archived(
    capped_read_scenario: &mut CappedReadScenario,
    object: ArchiveObject,
    size: usize,
) {
    archive_object(capped_read_scenario, object, size);
}

#[rstest_bdd_macros::given(
    "a {object} of {size} bytes has been archived to a store without entity tags"
)]
fn object_archived_untagged(
    capped_read_scenario: &mut CappedReadScenario,
    object: ArchiveObject,
    size: usize,
) {
    capped_read_scenario.archive = Archive::new(Arc::new(UntaggedStore {
        inner: InMemory::new(),
    }));
    archive_object(capped_read_scenario, object, size);
}

#[rstest_bdd_macros::given("the store fails every read with a server error")]
fn store_fails_reads(capped_read_scenario: &mut CappedReadScenario) {
    capped_read_scenario.archive.policy =
        FaultPolicy::none().with(StoreOp::Get, OpFault::always(FaultKind::ServerError));
}

#[rstest_bdd_macros::given(
    "the {object} is replaced by one of {size} bytes after its size is checked"
)]
fn replaced_after_size_check(
    capped_read_scenario: &mut CappedReadScenario,
    object: ArchiveObject,
    size: usize,
) {
    assert2::assert!(*capped_read_scenario.object.get() == object);
    capped_read_scenario.archive.replacement = Some(Bytes::from(vec![REPLACEMENT_BYTE; size]));
    capped_read_scenario.replacement_size.set(size);
}

#[rstest_bdd_macros::when("a broker reads the {object} with a cap of {cap} bytes")]
fn broker_reads(capped_read_scenario: &mut CappedReadScenario, object: ArchiveObject, cap: u64) {
    read_object(capped_read_scenario, object, cap);
}

#[rstest_bdd_macros::when("a broker attempts to read the {object} with a cap of {cap} bytes")]
fn broker_attempts_read(
    capped_read_scenario: &mut CappedReadScenario,
    object: ArchiveObject,
    cap: u64,
) {
    read_object(capped_read_scenario, object, cap);
}

#[rstest_bdd_macros::when("an auditor attempts to read the {object} with a cap of {cap} bytes")]
fn auditor_attempts_read(
    capped_read_scenario: &mut CappedReadScenario,
    object: ArchiveObject,
    cap: u64,
) {
    read_object(capped_read_scenario, object, cap);
}

#[rstest_bdd_macros::when("a broker attempts to read a {object} that was never archived")]
fn broker_reads_missing(capped_read_scenario: &mut CappedReadScenario, object: ArchiveObject) {
    // The cap plays no part: nothing exists to measure against it.
    read_object(capped_read_scenario, object, u64::MAX);
    capped_read_scenario.object.set(object);
}

#[rstest_bdd_macros::then("the broker receives the whole {object}")]
fn receives_whole(capped_read_scenario: &CappedReadScenario, object: ArchiveObject) {
    let stored = capped_read_scenario
        .archive
        .runtime
        .block_on(async {
            let store = &capped_read_scenario.archive.store;
            store.get(&object.key().into()).await?.bytes().await
        })
        .unwrap();
    assert2::assert!(let Ok(read) = capped_read_scenario.read.get());
    assert2::assert!(*read == stored);
}

#[rstest_bdd_macros::then("the broker receives the replacement {object}")]
fn receives_replacement(capped_read_scenario: &CappedReadScenario, object: ArchiveObject) {
    assert2::assert!(*capped_read_scenario.object.get() == object);
    let size = *capped_read_scenario.replacement_size.get();
    assert2::assert!(let Ok(read) = capped_read_scenario.read.get());
    assert2::assert!(*read == vec![REPLACEMENT_BYTE; size]);
}

#[rstest_bdd_macros::then(
    "the read is refused because the {object} is {size} bytes, above the cap of {cap} bytes"
)]
fn refused_too_large(
    capped_read_scenario: &CappedReadScenario,
    object: ArchiveObject,
    size: u64,
    cap: u64,
) {
    assert2::assert!(let Err(ObjectStoreError::TooLarge { key, size: reported, max_bytes }) = capped_read_scenario.read.get());
    assert2::assert!((key.as_ref(), *reported, *max_bytes) == (object.key(), size, cap));
}

#[rstest_bdd_macros::then("the store was asked only for the size of the {object}")]
fn asked_only_for_size(capped_read_scenario: &CappedReadScenario, object: ArchiveObject) {
    assert2::assert!(*capped_read_scenario.object.get() == object);
    // A size check and a read are both reads to the store; one request means
    // the size check was the only one.
    assert2::assert!(capped_read_scenario.archive.requests() == 1);
}

#[rstest_bdd_macros::then("the read reports the {object} as not found")]
fn reports_not_found(capped_read_scenario: &CappedReadScenario, object: ArchiveObject) {
    assert2::assert!(let Err(ObjectStoreError::NotFound(key)) = capped_read_scenario.read.get());
    assert2::assert!(key.as_ref() == object.key());
}

#[rstest_bdd_macros::then("the read fails with a store error")]
fn fails_with_store_error(capped_read_scenario: &CappedReadScenario) {
    assert2::assert!(let Err(ObjectStoreError::Unavailable(_)) = capped_read_scenario.read.get());
}

#[rstest_bdd_macros::then(
    "the read is refused because the {object} changed after its size was checked"
)]
fn refused_changed(capped_read_scenario: &CappedReadScenario, object: ArchiveObject) {
    assert2::assert!(let Err(ObjectStoreError::Precondition { key, .. }) = capped_read_scenario.read.get());
    assert2::assert!(key.as_ref() == object.key());
}

krabka_bdd::scenarios!(
    "tests/features/capped_reads",
    fixtures = [capped_read_scenario: CappedReadScenario]
);
