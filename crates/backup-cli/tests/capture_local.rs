//! A capture of a node's restore inputs, read back and checked.
//!
//! The unit tests cover the choices the tool makes. This suite covers the round
//! trip an operator depends on: the bytes on the node reach the archive, the
//! manifest describes them, `verify` says so, and `verify` says the opposite
//! when the archive no longer holds what was written. It runs against a local
//! archive directory, which is the same object-store layer an S3 bucket goes
//! through, so nothing here needs a network.
//!
//! The offsets half of the tool needs a live cluster and is covered by
//! `crates/restore/tests/dr_roundtrip.rs`.

use {
    assert2::{assert, check},
    krabka_backup::{
        BackupError, BackupEvent, EXIT_INTEGRITY,
        archive::{Archive, ArchiveArgs},
        capture::capture_key,
        manifest::{MANIFEST, METADATA_CHECKPOINT, Manifest, RLMM_SNAPSHOT, sha256_hex},
        run,
    },
};

/// The RLMM snapshot bytes a fixture node holds. The capture copies bytes and
/// decodes nothing, so any content proves the same thing.
const RLMM_BYTES: &[u8] = b"rlmm snapshot bytes";

/// The newest checkpoint's bytes.
const NEWEST_CHECKPOINT_BYTES: &[u8] = b"the newest controller checkpoint";

krabka_macros::snapshot_node_fixture!(node_with_rlmm);

/// One broker log directory, with the two files a restore needs on it.
struct FixtureNode {
    log_dir: tempfile::TempDir,
}

impl FixtureNode {
    fn build() -> Self {
        let log_dir = node_with_rlmm();

        let metadata = log_dir.path().join("__cluster_metadata-0");
        std::fs::create_dir_all(&metadata).expect("create the metadata dir");
        std::fs::write(
            metadata.join("00000000000000000009-0000000000.checkpoint"),
            b"an older controller checkpoint",
        )
        .expect("write the older checkpoint");
        std::fs::write(
            metadata.join("00000000000000000042-0000000001.checkpoint"),
            NEWEST_CHECKPOINT_BYTES,
        )
        .expect("write the newest checkpoint");
        Self { log_dir }
    }
}

fn archive_args(root: &std::path::Path) -> ArchiveArgs {
    ArchiveArgs {
        local: Some(root.to_path_buf()),
        ..ArchiveArgs::default()
    }
}

async fn read_manifest(archive_root: &std::path::Path, capture: &str) -> Manifest {
    let store = Archive::open(&archive_args(archive_root)).expect("open the archive");
    let bytes = store
        .get(&capture_key(capture, MANIFEST))
        .await
        .expect("read the manifest");
    serde_json::from_slice(&bytes).expect("decode the manifest")
}

/// A fixture node, the archive root a capture of it was taken into, the arguments that open that archive, and the capture's id.
struct CapturedNode {
    node: FixtureNode,
    archive_root: tempfile::TempDir,
    args: ArchiveArgs,
    capture: String,
}

async fn captured_node() -> CapturedNode {
    let node = FixtureNode::build();
    let archive_root = tempfile::tempdir().expect("archive root");
    let args = archive_args(archive_root.path());
    let capture = run::capture(Some(node.log_dir.path()), None, &args)
        .await
        .result
        .expect("capture the node");
    CapturedNode {
        node,
        archive_root,
        args,
        capture,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_capture_copies_both_snapshots_and_records_what_it_wrote() {
    let node = FixtureNode::build();
    let archive_root = tempfile::tempdir().expect("archive root");

    let report = run::capture(
        Some(node.log_dir.path()),
        None,
        &archive_args(archive_root.path()),
    )
    .await;
    let capture = report.result.expect("capture the node");
    assert!(let [BackupEvent::Capture { manifest, notes, missing }] = report.events.as_slice());
    check!(manifest.capture_id == capture);
    check!(manifest.artifacts.len() == 2);
    check!(notes.len() == 1);
    check!(notes[0].contains("the age of its newest record is unknown"));
    check!(missing.is_empty());

    let manifest = read_manifest(archive_root.path(), &capture).await;
    check!(manifest.capture_id == capture);
    check!(manifest.log_dir == Some(node.log_dir.path().display().to_string()));
    check!(manifest.bootstrap_server == None);

    let rlmm = manifest
        .artifact(RLMM_SNAPSHOT)
        .expect("the capture took the rlmm snapshot");
    check!(rlmm.size_bytes == RLMM_BYTES.len() as u64);
    check!(rlmm.sha256 == sha256_hex(RLMM_BYTES));

    let checkpoint = manifest
        .artifact(METADATA_CHECKPOINT)
        .expect("the capture took a metadata checkpoint");
    check!(checkpoint.sha256 == sha256_hex(NEWEST_CHECKPOINT_BYTES));
    check!(
        checkpoint
            .source
            .ends_with("00000000000000000042-0000000001.checkpoint"),
        "the newest checkpoint is the one that was captured, got {}",
        checkpoint.source
    );

    // The bytes in the archive are the bytes on the node, which is the whole
    // claim: a restore reads them from here.
    let store = Archive::open(&archive_args(archive_root.path())).expect("open the archive");
    check!(
        store
            .get(&capture_key(&capture, RLMM_SNAPSHOT))
            .await
            .expect("read the captured snapshot")
            == RLMM_BYTES
    );
    check!(
        store
            .get(&capture_key(&capture, METADATA_CHECKPOINT))
            .await
            .expect("read the captured checkpoint")
            == NEWEST_CHECKPOINT_BYTES
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_accepts_a_capture_it_just_wrote_and_list_names_it() {
    let CapturedNode {
        node: _node,
        archive_root: _archive_root,
        args,
        capture,
    } = captured_node().await;

    check!(run::list(&args).await.result.expect("list the captures") == vec![capture.clone()]);
    for selector in ["latest", capture.as_str()] {
        let report = run::verify(selector, &args).await;
        check!(report.result.is_ok());
        assert!(let [
            BackupEvent::ArtifactVerified { name: rlmm, size_bytes: rlmm_size },
            BackupEvent::ArtifactVerified { name: checkpoint, size_bytes: checkpoint_size },
            BackupEvent::CaptureVerified { id, artifacts },
        ] = report.events.as_slice());
        check!(
            (
                rlmm.as_str(),
                *rlmm_size,
                checkpoint.as_str(),
                *checkpoint_size,
                id.as_str(),
                *artifacts
            ) == (
                RLMM_SNAPSHOT,
                RLMM_BYTES.len() as u64,
                METADATA_CHECKPOINT,
                NEWEST_CHECKPOINT_BYTES.len() as u64,
                capture.as_str(),
                2
            )
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_reports_an_artifact_the_archive_no_longer_holds_whole() {
    let CapturedNode {
        node: _node,
        archive_root,
        args,
        capture,
    } = captured_node().await;

    // A truncated object is what a half-finished upload leaves behind, and it
    // is exactly what a restore must not be handed.
    let damaged = archive_root
        .path()
        .join(capture_key(&capture, RLMM_SNAPSHOT));
    std::fs::write(&damaged, b"rlmm snapshot byt").expect("truncate the captured snapshot");

    let error = run::verify("latest", &args)
        .await
        .result
        .expect_err("a truncated artifact fails verification");
    check!(matches!(error, BackupError::Integrity(_)), "got: {error}");
    let message = error.to_string();
    check!(message.contains(RLMM_SNAPSHOT), "got: {message}");
    check!(error.exit_code() == EXIT_INTEGRITY);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_capture_that_finds_nothing_is_a_failure_and_not_an_empty_capture() {
    let empty = tempfile::tempdir().expect("an empty log dir");
    let archive_root = tempfile::tempdir().expect("archive root");
    let args = archive_args(archive_root.path());

    let message = run::capture(Some(empty.path()), None, &args)
        .await
        .result
        .expect_err("a capture that finds nothing fails")
        .to_string();
    check!(message.contains("captured nothing"), "got: {message}");
    check!(
        run::list(&args)
            .await
            .result
            .expect("list the captures")
            .is_empty()
    );
}

#[tokio::test]
async fn capture_returns_missing_artifacts_without_another_archive_read() {
    let node = node_with_rlmm();
    let archive_root = tempfile::tempdir().expect("archive root");
    let report = run::capture(Some(node.path()), None, &archive_args(archive_root.path())).await;
    let id = report.result.expect("capture the available snapshot");
    assert!(let [BackupEvent::Capture { manifest, notes, missing }] = report.events.as_slice());
    check!(manifest.capture_id == id);
    check!(manifest.artifacts.len() == 1);
    check!(notes.is_empty());
    check!(
        missing
            == &[format!(
                "{} holds no controller or observer metadata checkpoint",
                node.path().display()
            )]
    );
}

#[tokio::test]
async fn verification_keeps_successful_checks_before_a_later_read_failure() {
    let CapturedNode {
        node: _node,
        archive_root,
        args,
        capture,
    } = captured_node().await;
    std::fs::remove_file(
        archive_root
            .path()
            .join(capture_key(&capture, METADATA_CHECKPOINT)),
    )
    .expect("remove the second artifact");
    let report = run::verify(&capture, &args).await;
    check!(report.result.is_err());
    assert!(let [BackupEvent::ArtifactVerified { name, size_bytes }] = report.events.as_slice());
    check!(name == RLMM_SNAPSHOT);
    check!(*size_bytes == RLMM_BYTES.len() as u64);
}
