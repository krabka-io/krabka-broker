//! The native GCS backend against a Cloud Storage emulator.
//!
//! `docs/config-reference.md` calls keyless Workload Identity "the primary
//! production path", and `[remote_storage.gcs]` reaches it through
//! [`S3RemoteStorage::from_gcs_config`]. Until this suite existed the only GCS
//! coverage in the tree was builder-constructs-ok unit tests and an
//! `InMemory`-backed round trip, so no byte of the GCS wire path -- the XML
//! object API `object_store`'s GCS client actually speaks, the generation
//! preconditions a conditional create rides on, and the `Buckets.get` control
//! plane that gates archive startup -- was ever executed against a server.
//!
//! The container lifecycle follows the pattern `crates/restore/tests/
//! minio_roundtrip.rs` uses: a `docker run -d` guard that removes the
//! container on drop, and a per-process published port so two container suites
//! can run at once.
//!
//! ## What this suite cannot reach
//!
//! The multipart branch of a copy. `object_store` 0.14's GCS client implements
//! `put_multipart` exclusively over the Cloud Storage *XML multipart* API --
//! `POST ?uploads`, `PUT ?partNumber&uploadId`, `POST ?uploadId` -- and
//! neither `fsouza/fake-gcs-server` nor Google's own
//! `googleapis/storage-testbench` implements those routes. A copy whose `.log`
//! exceeds `multipart_threshold` therefore cannot be served by any GCS
//! emulator that exists. What this suite does cover is that the threshold
//! `GcsConfig` carries reaches the engine and that a copy under it round-trips
//! against a real server; the multipart lane stays covered by the S3 suites
//! and by `crates/object-store`'s `put_from_path` unit tests.

use std::{
    io::Write as _,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use krabka_remote_storage::{
    GcsConfig, IndexType, LogSegmentData, RemoteLogSegmentDetails, RemoteLogSegmentId,
    RemoteLogSegmentMetadata, RemoteLogSegmentState, RemoteStorageError, RemoteStorageManager as _,
    S3RemoteStorage, TopicIdPartition, WormConfig,
};
use uuid::Uuid;

/// The emulator image, pinned by the same digest table the broker's container
/// suites load from (`//bazel/images`).
const FAKE_GCS_IMAGE: &str = "mirror.gcr.io/fsouza/fake-gcs-server:1.56.1";

/// The port the emulator listens on inside the container.
const CONTAINER_PORT: u16 = 4443;

/// A service-account key that turns `object_store`'s OAuth exchange off, so
/// the client signs nothing and the emulator authorises everything. The GKE
/// path this backend exists for has no key at all: the metadata server hands
/// out bearer tokens, and there is no metadata server in a test.
const DISABLE_OAUTH_KEY: &str = r#"{"private_key":"unused","private_key_id":"unused","client_email":"unused","disable_oauth":true}"#;

/// The archive sits under a prefix, so the operator-visible prefix is
/// exercised against a real server rather than only against `InMemory`.
const PREFIX: &str = "tier";

const TOPIC: &str = "orders";

const LOG_BODY: &[u8] = b"0123456789abcdef";
const OFFSET_INDEX_BODY: &[u8] = b"OFFSET-IDX";
const TIME_INDEX_BODY: &[u8] = b"TIME-IDX";
const SNAPSHOT_BODY: &[u8] = b"SNAP";
const EPOCH_BODY: &[u8] = b"EPOCH-BYTES";

/// A free ephemeral port on the loopback interface, so two container suites
/// running at once do not fight over a fixed 4443.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener
        .local_addr()
        .expect("the bound listener has an address")
        .port()
}

/// A `docker run -d` emulator, removed on drop so an aborted test leaves
/// nothing behind.
struct FakeGcs {
    name: String,
    port: u16,
}

impl FakeGcs {
    async fn start() -> Self {
        let port = free_port();
        let name = format!("krabka-fake-gcs-{}", Uuid::new_v4().simple());
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                &format!("{port}:{CONTAINER_PORT}"),
                FAKE_GCS_IMAGE,
                "-scheme",
                "http",
                "-port",
                &CONTAINER_PORT.to_string(),
                "-backend",
                "memory",
                // `object_store` addresses objects as `{endpoint}/{bucket}/
                // {object}`, which the emulator only routes when the request's
                // host matches its public host. The published port is
                // per-process, so name the host alone and let the emulator
                // compare host parts.
                "-public-host",
                "127.0.0.1",
                "-external-url",
                &format!("http://127.0.0.1:{port}"),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .expect("spawn docker run fake-gcs-server");
        assert!(status.success(), "docker run fake-gcs-server failed");
        let server = Self { name, port };
        server.wait_until_ready().await;
        server
    }

    /// The base URL `GcsConfig::endpoint` points at.
    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    /// Poll the control plane until the emulator answers a real Cloud Storage
    /// request, so the first backend call does not race container startup.
    async fn wait_until_ready(&self) {
        let client = reqwest::Client::new();
        let url = self.buckets_url();
        for _ in 0..60 {
            let served = match client.get(&url).send().await {
                Ok(response) => response.status().is_success(),
                Err(_) => false,
            };
            if served {
                return;
            }
            // Intentional: a bounded readiness poll of an external process. No
            // krabka metric reflects its listener coming up.
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        panic!(
            "fake-gcs-server never served a bucket listing on 127.0.0.1:{}",
            self.port,
        );
    }

    /// The control-plane collection buckets are listed from and created in.
    fn buckets_url(&self) -> String {
        format!("{}/storage/v1/b?project=krabka", self.endpoint())
    }

    /// Create a bucket through the emulator's JSON control plane, which is the
    /// one operation no `object_store` API reaches. Returns the bucket name.
    async fn create_bucket(&self, versioning: bool) -> String {
        let bucket = format!("krabka-gcs-{}", Uuid::new_v4().simple());
        let response = reqwest::Client::new()
            .post(self.buckets_url())
            .header("content-type", "application/json")
            .body(
                serde_json::to_vec(&serde_json::json!({
                    "name": bucket,
                    "versioning": {"enabled": versioning},
                }))
                .expect("serialise the bucket request"),
            )
            .send()
            .await
            .expect("create a bucket");
        assert!(
            response.status().is_success(),
            "create bucket {bucket}: {}",
            response.status(),
        );
        bucket
    }
}

impl Drop for FakeGcs {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

/// The backend configuration every case builds on: the emulator's endpoint,
/// plaintext HTTP, and the credential that turns OAuth off.
fn gcs_config(server: &FakeGcs, bucket: String) -> GcsConfig {
    GcsConfig {
        bucket,
        prefix: Some(PREFIX.to_owned()),
        endpoint: Some(server.endpoint()),
        service_account_key: Some(DISABLE_OAUTH_KEY.to_owned()),
        allow_http: true,
        ..GcsConfig::default()
    }
}

fn sample_metadata(topic_id: Uuid, partition: i32) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(topic_id, TOPIC, partition),
            Uuid::new_v4(),
        ),
        0,
        99,
        123,
        1,
        456,
        RemoteLogSegmentDetails::new(
            i32::try_from(LOG_BODY.len()).expect("fixture segment fits i32"),
            RemoteLogSegmentState::CopySegmentFinished,
            maplit::btreemap! {LeaderEpoch(0) => 0},
        ),
    )
    .expect("valid remote metadata")
}

fn write_file(dir: &Path, name: &str, contents: &[u8]) -> PathBuf {
    let path = dir.join(name);
    std::fs::File::create(&path)
        .expect("create a fixture file")
        .write_all(contents)
        .expect("write a fixture file");
    path
}

fn sample_data(src: &Path) -> LogSegmentData {
    LogSegmentData {
        log_segment: write_file(src, "00.log", LOG_BODY),
        offset_index: write_file(src, "00.index", OFFSET_INDEX_BODY),
        time_index: write_file(src, "00.timeindex", TIME_INDEX_BODY),
        transaction_index: None,
        producer_snapshot_index: Some(write_file(src, "00.snapshot", SNAPSHOT_BODY)),
        leader_epoch_index: Bytes::from_static(EPOCH_BODY),
    }
}

/// A copy through `from_gcs_config` puts every artifact of a segment into a
/// real bucket, and each fetch reads back exactly what was written.
///
/// Multi-thread on purpose: `S3RemoteStorage`'s sync trait methods bridge to
/// the async store with `block_in_place`, which a current-thread runtime
/// cannot do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_segment_copied_into_the_emulator_fetches_back_byte_for_byte() {
    let server = FakeGcs::start().await;
    let config = gcs_config(&server, server.create_bucket(false).await);
    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    let src = tempfile::tempdir().expect("fixture tempdir");
    let metadata = sample_metadata(Uuid::new_v4(), 0);

    tokio::task::spawn_blocking(move || {
        storage
            .copy_log_segment_data(&metadata, &sample_data(src.path()))
            .expect("copy the segment into the emulator");

        check!(
            storage
                .fetch_log_segment(&metadata, 0, None)
                .expect("fetch the whole segment")
                == LOG_BODY
        );
        check!(
            storage
                .fetch_log_segment(&metadata, 4, Some(7))
                .expect("fetch a byte range of the segment")
                == &LOG_BODY[4..=7]
        );
        for (index_type, expected) in [
            (IndexType::Offset, OFFSET_INDEX_BODY),
            (IndexType::Timestamp, TIME_INDEX_BODY),
            (IndexType::ProducerSnapshot, SNAPSHOT_BODY),
            (IndexType::LeaderEpoch, EPOCH_BODY),
        ] {
            check!(
                storage
                    .fetch_index(&metadata, index_type)
                    .expect("fetch an index")
                    == expected,
                "{index_type:?}",
            );
        }
    })
    .await
    .expect("the blocking half of the test");
}

/// A delete removes every artifact the copy wrote, so a later fetch reports
/// the segment as absent rather than serving a stale body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_deleted_segment_is_gone_from_the_bucket() {
    let server = FakeGcs::start().await;
    let config = gcs_config(&server, server.create_bucket(false).await);
    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    let src = tempfile::tempdir().expect("fixture tempdir");
    let metadata = sample_metadata(Uuid::new_v4(), 3);

    tokio::task::spawn_blocking(move || {
        storage
            .copy_log_segment_data(&metadata, &sample_data(src.path()))
            .expect("copy the segment into the emulator");
        storage
            .delete_log_segment_data(&metadata)
            .expect("delete the segment from the emulator");

        assert!(let Err(error) = storage.fetch_log_segment(&metadata, 0, None));
        check!(matches!(error, RemoteStorageError::SegmentNotFound(_)));
        assert!(let Err(error) = storage.fetch_index(&metadata, IndexType::Offset));
        check!(matches!(error, RemoteStorageError::SegmentNotFound(_)));

        // Kafka's SPI requires an idempotent delete: a retried expiry of a
        // segment already gone must not fail the retention pass.
        storage
            .delete_log_segment_data(&metadata)
            .expect("a second delete is a no-op");
    })
    .await
    .expect("the blocking half of the test");
}

/// The multipart threshold `GcsConfig` carries reaches the engine, and a
/// segment under it round-trips through the real server.
///
/// The multipart side of that branch is unreachable against an emulator; the
/// module docs say why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn a_copy_under_a_tuned_multipart_threshold_stays_a_single_put() {
    let server = FakeGcs::start().await;
    let config = GcsConfig {
        // One byte above the fixture segment: the largest threshold at which
        // this copy is still a single PUT, so a regression that moved the
        // comparison to `<=` would take the multipart branch and fail here.
        multipart_threshold: u64::try_from(LOG_BODY.len()).expect("fixture body fits u64") + 1,
        multipart_chunk_size: 5 * 1024 * 1024,
        ..gcs_config(&server, server.create_bucket(false).await)
    };
    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    let src = tempfile::tempdir().expect("fixture tempdir");
    let metadata = sample_metadata(Uuid::new_v4(), 7);

    tokio::task::spawn_blocking(move || {
        storage
            .copy_log_segment_data(&metadata, &sample_data(src.path()))
            .expect("copy the segment into the emulator");
        check!(
            storage
                .fetch_log_segment(&metadata, 0, None)
                .expect("fetch the whole segment")
                == LOG_BODY
        );
    })
    .await
    .expect("the blocking half of the test");
}

/// `with_worm` gates archive startup on `Buckets.get`, and a bucket with
/// neither versioning nor a retention policy fails that gate against a real
/// server rather than only against a stubbed response body.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worm_startup_refuses_an_unversioned_emulator_bucket() {
    let server = FakeGcs::start().await;
    let config = gcs_config(&server, server.create_bucket(false).await);

    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    assert!(let Err(error) = storage.with_worm(&WormConfig::default()));
    check!(
        error
            .to_string()
            .contains("requires GCS versioning enabled")
    );
}

/// The next rung of the same gate: versioning alone is not a WORM bucket. GCS
/// protects a completed object with a *locked* retention policy, and a bucket
/// carrying none is refused with that named as the reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker"]
async fn worm_startup_refuses_a_versioned_bucket_with_no_retention_policy() {
    let server = FakeGcs::start().await;
    let config = gcs_config(&server, server.create_bucket(true).await);

    let storage = S3RemoteStorage::from_gcs_config(&config).expect("open the emulator bucket");
    assert!(let Err(error) = storage.with_worm(&WormConfig::default()));
    check!(error.to_string().contains("no GCS retention policy"));
}
