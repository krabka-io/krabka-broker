//! What each subcommand does, and the one place that talks to a cluster.
//!
//! Every function here returns a [`BackupError`], and only [`crate::run()`] turns
//! one into an exit code. A runbook that wraps the tool gets the code; a test
//! that drives the library keeps the error.

use std::collections::{BTreeMap, BTreeSet};

use krabka_client_admin::AdminClient;
use krabka_client_core::{
    Client, CoordinatorKeyType, build_find_coordinator, coordinator_endpoint,
};
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::{
    archive::{Archive, ArchiveArgs},
    capture::{RLMM_SNAPSHOT_RELATIVE, capture_id, capture_key, newest_metadata_checkpoint},
    cli::LATEST,
    error::BackupError,
    manifest::{
        Artifact, CAPTURE_ROOT, GROUP_OFFSETS, MANIFEST, METADATA_CHECKPOINT, Manifest,
        RLMM_SNAPSHOT, artifact_problem, sha256_hex,
    },
    offsets::{GroupOffsetsFile, commit_refusals, commit_request, group_offsets},
};

/// Milliseconds since the Unix epoch, saturating at zero on a clock set before
/// it. The value names a capture directory and nothing branches on it, so a
/// nonsensical clock produces an odd name rather than a failure.
fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_millis()),
    )
    .unwrap_or(0)
}

/// Copy this node's restore inputs into the archive.
///
/// # Errors
///
/// Returns [`BackupError::Io`] when the capture found nothing to take, which is
/// what a wrong `--log-dir` looks like, and the archive's or the cluster's own
/// error otherwise.
pub async fn capture(
    log_dir: Option<&std::path::Path>,
    bootstrap_server: Option<&str>,
    archive: &ArchiveArgs,
) -> Result<String, BackupError> {
    let store = archive.open()?;
    let id = capture_id(now_ms());
    let mut artifacts: Vec<Artifact> = Vec::new();
    let mut absent: Vec<String> = Vec::new();

    if let Some(log_dir) = log_dir {
        let rlmm = log_dir.join(RLMM_SNAPSHOT_RELATIVE);
        match tokio::fs::read(&rlmm).await {
            Ok(bytes) => {
                artifacts.push(
                    upload(
                        &store,
                        &id,
                        RLMM_SNAPSHOT,
                        &rlmm.display().to_string(),
                        bytes,
                    )
                    .await?,
                );
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                absent.push(format!("{} is absent", rlmm.display()));
            }
            Err(error) => return Err(BackupError::Io(error)),
        }

        match newest_metadata_checkpoint(log_dir) {
            Some(path) => {
                let bytes = tokio::fs::read(&path).await?;
                artifacts.push(
                    upload(
                        &store,
                        &id,
                        METADATA_CHECKPOINT,
                        &path.display().to_string(),
                        bytes,
                    )
                    .await?,
                );
            }
            None => absent.push(format!(
                "{} holds no controller or observer metadata checkpoint",
                log_dir.display()
            )),
        }
    }

    if let Some(bootstrap) = bootstrap_server {
        let offsets = fetch_group_offsets(bootstrap).await?;
        let bytes = serde_json::to_vec_pretty(&offsets).map_err(|source| BackupError::Json {
            context: GROUP_OFFSETS.to_owned(),
            source,
        })?;
        artifacts.push(upload(&store, &id, GROUP_OFFSETS, bootstrap, bytes).await?);
    }

    if artifacts.is_empty() {
        return Err(BackupError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!(
                "captured nothing: {}",
                if absent.is_empty() {
                    "pass --log-dir, --bootstrap-server, or both".to_owned()
                } else {
                    absent.join("; ")
                }
            ),
        )));
    }

    let manifest = Manifest {
        capture_id: id.clone(),
        captured_at_ms: now_ms(),
        log_dir: log_dir.map(|dir| dir.display().to_string()),
        bootstrap_server: bootstrap_server.map(ToOwned::to_owned),
        artifacts,
    };
    let encoded = serde_json::to_vec_pretty(&manifest).map_err(|source| BackupError::Json {
        context: MANIFEST.to_owned(),
        source,
    })?;
    store.put(&capture_key(&id, MANIFEST), encoded).await?;

    println!("capture {id}");
    for artifact in &manifest.artifacts {
        println!(
            "  {} <- {} ({} bytes, sha256 {})",
            artifact.name, artifact.source, artifact.size_bytes, artifact.sha256
        );
    }
    for missing in &absent {
        println!("  WARNING: {missing}");
    }
    Ok(id)
}

/// Put one artifact into the capture and record what was written.
async fn upload(
    store: &Archive,
    id: &str,
    name: &str,
    source: &str,
    bytes: Vec<u8>,
) -> Result<Artifact, BackupError> {
    let artifact = Artifact {
        name: name.to_owned(),
        source: source.to_owned(),
        size_bytes: bytes.len() as u64,
        sha256: sha256_hex(&bytes),
    };
    store.put(&capture_key(id, name), bytes).await?;
    Ok(artifact)
}

/// Print every capture in the archive, newest last.
///
/// # Errors
///
/// Returns the archive's own error when the listing or a manifest read fails.
pub async fn list(archive: &ArchiveArgs) -> Result<Vec<String>, BackupError> {
    let store = archive.open()?;
    let ids = store.child_directories(CAPTURE_ROOT).await?;
    if ids.is_empty() {
        println!("no captures under {CAPTURE_ROOT}");
    }
    for id in &ids {
        match read_manifest(&store, id).await {
            Ok(manifest) => {
                let names: Vec<&str> = manifest
                    .artifacts
                    .iter()
                    .map(|artifact| artifact.name.as_str())
                    .collect();
                println!("{id}  {}", names.join(" "));
            }
            Err(error) => println!("{id}  UNREADABLE: {error}"),
        }
    }
    Ok(ids.into_iter().collect())
}

/// Re-read a capture and check every artifact against its recorded digest.
///
/// # Errors
///
/// Returns [`BackupError::Integrity`] when an artifact's bytes are not the
/// bytes the manifest recorded, and the archive's own error when the capture
/// cannot be read at all.
pub async fn verify(capture: &str, archive: &ArchiveArgs) -> Result<(), BackupError> {
    let store = archive.open()?;
    let id = resolve_capture(&store, capture).await?;
    let manifest = read_manifest(&store, &id).await?;

    let mut problems: Vec<String> = Vec::new();
    for artifact in &manifest.artifacts {
        let bytes = store.get(&capture_key(&id, &artifact.name)).await?;
        match artifact_problem(artifact, &bytes) {
            Some(problem) => problems.push(problem),
            None => println!("  {} ok ({} bytes)", artifact.name, artifact.size_bytes),
        }
    }
    if problems.is_empty() {
        println!(
            "capture {id} verified: {} artifacts",
            manifest.artifacts.len()
        );
        return Ok(());
    }
    Err(BackupError::Integrity(format!(
        "capture {id}: {}",
        problems.join("; ")
    )))
}

/// Commit a capture's group offsets into a restored cluster.
///
/// # Errors
///
/// Returns [`BackupError::Integrity`] when the captured offsets do not match
/// their recorded digest, [`BackupError::Cluster`] when the cluster refuses a
/// commit, and the archive's own error when the capture cannot be read.
pub async fn restore_offsets(
    capture: &str,
    bootstrap_server: &str,
    dry_run: bool,
    archive: &ArchiveArgs,
) -> Result<usize, BackupError> {
    let store = archive.open()?;
    let id = resolve_capture(&store, capture).await?;
    let manifest = read_manifest(&store, &id).await?;
    let recorded = manifest.artifact(GROUP_OFFSETS).ok_or_else(|| {
        BackupError::NoSuchCapture(format!("capture {id} holds no {GROUP_OFFSETS}"))
    })?;
    let bytes = store.get(&capture_key(&id, GROUP_OFFSETS)).await?;
    if let Some(problem) = artifact_problem(recorded, &bytes) {
        return Err(BackupError::Integrity(format!("capture {id}: {problem}")));
    }
    let offsets: GroupOffsetsFile =
        serde_json::from_slice(&bytes).map_err(|source| BackupError::Json {
            context: capture_key(&id, GROUP_OFFSETS),
            source,
        })?;

    if dry_run {
        for group in &offsets.groups {
            for offset in &group.offsets {
                println!(
                    "would commit {}: {}-{} = {}",
                    group.group, offset.topic, offset.partition, offset.offset
                );
            }
        }
        return Ok(offsets.offset_count());
    }

    let topic_ids = topic_ids(bootstrap_server, &offsets).await?;
    let client = Client::builder()
        .bootstrap(bootstrap_server)
        .client_id("krabka-backup")
        .build()
        .await
        .map_err(|error| {
            BackupError::Cluster(format!("cannot reach {bootstrap_server}: {error}"))
        })?;

    let mut committed = 0;
    for group in &offsets.groups {
        if group.offsets.is_empty() {
            continue;
        }
        let found = client
            .send(build_find_coordinator(
                &group.group,
                CoordinatorKeyType::Group,
            ))
            .await
            .map_err(|error| {
                BackupError::Cluster(format!("FindCoordinator for {}: {error}", group.group))
            })?;
        let coordinator = coordinator_endpoint(&group.group, found).map_err(|error| {
            BackupError::Cluster(format!("FindCoordinator for {}: {error}", group.group))
        })?;
        let response = client
            .broker(coordinator.node_id)
            .send(commit_request(group, &topic_ids))
            .await
            .map_err(|error| {
                BackupError::Cluster(format!("OffsetCommit for {}: {error}", group.group))
            })?;
        let refusals = commit_refusals(&response);
        if !refusals.is_empty() {
            return Err(BackupError::Cluster(format!(
                "OffsetCommit for {}: {}",
                group.group,
                refusals.join("; ")
            )));
        }
        committed += group.offsets.len();
        println!("{}: {} offsets committed", group.group, group.offsets.len());
    }
    Ok(committed)
}

/// The topic id the restored cluster gave each topic the capture names.
///
/// `OffsetCommit` v10 puts the topic id on the wire instead of the name, so a
/// commit that carried names only would silently commit nothing there.
async fn topic_ids(
    bootstrap_server: &str,
    offsets: &GroupOffsetsFile,
) -> Result<BTreeMap<String, WireUuid>, BackupError> {
    let names: BTreeSet<&str> = offsets
        .groups
        .iter()
        .flat_map(|group| group.offsets.iter().map(|offset| offset.topic.as_str()))
        .collect();
    if names.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut admin = connect_admin(bootstrap_server).await?;
    let wanted: Vec<&str> = names.into_iter().collect();
    let metadata = admin
        .metadata(&wanted)
        .await
        .map_err(|error| BackupError::Cluster(format!("Metadata: {error}")))?;
    Ok(metadata
        .topics
        .into_iter()
        .filter_map(|topic| {
            topic
                .topic_id
                .map(|id| (topic.name, WireUuid(id.into_bytes())))
        })
        .collect())
}

/// Read every group's committed offsets from a live cluster.
async fn fetch_group_offsets(bootstrap_server: &str) -> Result<GroupOffsetsFile, BackupError> {
    let mut admin = connect_admin(bootstrap_server).await?;
    let groups = admin
        .list_groups()
        .await
        .map_err(|error| BackupError::Cluster(format!("ListGroups: {error}")))?;
    let mut captured = GroupOffsetsFile::default();
    for group in groups {
        let fetched = admin
            .list_consumer_group_offsets(&group)
            .await
            .map_err(|error| BackupError::Cluster(format!("OffsetFetch for {group}: {error}")))?;
        if fetched.is_empty() {
            continue;
        }
        captured.groups.push(group_offsets(&group, &fetched));
    }
    Ok(captured)
}

async fn connect_admin(bootstrap_server: &str) -> Result<AdminClient, BackupError> {
    AdminClient::connect(&[bootstrap_server.to_owned()])
        .await
        .map_err(|error| BackupError::Cluster(format!("cannot reach {bootstrap_server}: {error}")))
}

/// Turn `latest` into the newest capture id, and check that a named one exists.
async fn resolve_capture(store: &Archive, capture: &str) -> Result<String, BackupError> {
    let ids = store.child_directories(CAPTURE_ROOT).await?;
    if capture == LATEST {
        return ids.into_iter().next_back().ok_or_else(|| {
            BackupError::NoSuchCapture(format!("the archive holds no capture under {CAPTURE_ROOT}"))
        });
    }
    if ids.contains(capture) {
        return Ok(capture.to_owned());
    }
    Err(BackupError::NoSuchCapture(format!(
        "the archive holds no capture {capture}"
    )))
}

async fn read_manifest(store: &Archive, id: &str) -> Result<Manifest, BackupError> {
    let key = capture_key(id, MANIFEST);
    let bytes = store.get(&key).await?;
    serde_json::from_slice(&bytes).map_err(|source| BackupError::Json {
        context: key,
        source,
    })
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::{
        BackupError, GROUP_OFFSETS, MANIFEST, METADATA_CHECKPOINT, RLMM_SNAPSHOT, capture, list,
        restore_offsets, verify,
    };
    use crate::{
        archive::{Archive, ArchiveArgs},
        capture::capture_key,
        manifest::{Artifact, Manifest, sha256_hex},
        offsets::{CommittedOffset, GroupOffsets, GroupOffsetsFile},
    };

    /// The RLMM snapshot bytes a fixture node holds. The capture copies bytes
    /// and decodes nothing, so any content proves the same thing.
    const RLMM_BYTES: &[u8] = b"rlmm snapshot bytes";

    /// The newest checkpoint's bytes.
    const CHECKPOINT_BYTES: &[u8] = b"the newest controller checkpoint";

    fn archive_args(root: &std::path::Path) -> ArchiveArgs {
        ArchiveArgs {
            local: Some(root.to_path_buf()),
            ..ArchiveArgs::default()
        }
    }

    /// A log directory holding both of the files a capture takes off a node.
    fn node_with_both_inputs() -> tempfile::TempDir {
        let log_dir = tempfile::tempdir().expect("log dir");
        let rlmm = log_dir.path().join("remote-log-metadata");
        std::fs::create_dir_all(&rlmm).expect("create the rlmm dir");
        std::fs::write(rlmm.join("snapshot"), RLMM_BYTES).expect("write the rlmm snapshot");
        write_checkpoint(log_dir.path());
        log_dir
    }

    /// The newest controller checkpoint, in the directory a controller writes.
    fn write_checkpoint(log_dir: &std::path::Path) {
        let metadata = log_dir.join("__cluster_metadata/@metadata-0");
        std::fs::create_dir_all(&metadata).expect("create the metadata dir");
        std::fs::write(
            metadata.join("00000000000000000042-0000000001.checkpoint"),
            CHECKPOINT_BYTES,
        )
        .expect("write the newest checkpoint");
    }

    /// An address on the loopback interface that nothing is listening on, so
    /// a connection attempt is refused rather than timing out.
    fn closed_address() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind a probe listener");
        let port = listener.local_addr().expect("the probe's address").port();
        drop(listener);
        format!("127.0.0.1:{port}")
    }

    /// Write a capture that no [`capture`] call produced: a manifest naming one
    /// group-offsets artifact, and whatever bytes the caller wants under it.
    /// It is how the read paths are driven over inputs a healthy capture cannot
    /// leave behind.
    async fn write_offsets_capture(store: &Archive, id: &str, bytes: &[u8], recorded: &[u8]) {
        let manifest = Manifest {
            capture_id: id.to_owned(),
            captured_at_ms: 1_700_000_000_000,
            log_dir: None,
            bootstrap_server: Some("broker-1:9092".to_owned()),
            artifacts: vec![Artifact {
                name: GROUP_OFFSETS.to_owned(),
                source: "broker-1:9092".to_owned(),
                size_bytes: recorded.len() as u64,
                sha256: sha256_hex(recorded),
            }],
        };
        store
            .put(
                &capture_key(id, MANIFEST),
                serde_json::to_vec(&manifest).expect("encode the manifest"),
            )
            .await
            .expect("write the manifest");
        store
            .put(&capture_key(id, GROUP_OFFSETS), bytes.to_vec())
            .await
            .expect("write the offsets");
    }

    /// One group with one committed offset, as JSON bytes.
    fn offsets_json() -> Vec<u8> {
        serde_json::to_vec(&GroupOffsetsFile {
            groups: vec![GroupOffsets {
                group: "analytics".to_owned(),
                offsets: vec![CommittedOffset {
                    topic: "orders".to_owned(),
                    partition: 0,
                    offset: 42,
                }],
            }],
        })
        .expect("encode the offsets")
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_records_both_snapshots_with_their_digests() {
        let node = node_with_both_inputs();
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let id = capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");

        let store = args.open().expect("open the archive");
        let manifest: Manifest = serde_json::from_slice(
            &store
                .get(&capture_key(&id, MANIFEST))
                .await
                .expect("read the manifest"),
        )
        .expect("decode the manifest");
        check!(manifest.capture_id == id);
        check!(manifest.bootstrap_server == None);
        check!(
            manifest.artifact(RLMM_SNAPSHOT).map(|a| a.sha256.clone())
                == Some(sha256_hex(RLMM_BYTES))
        );
        check!(
            manifest
                .artifact(METADATA_CHECKPOINT)
                .map(|a| a.sha256.clone())
                == Some(sha256_hex(CHECKPOINT_BYTES))
        );
        check!(manifest.artifact(GROUP_OFFSETS) == None);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_reports_each_input_it_did_not_find_and_takes_the_other() {
        let log_dir = tempfile::tempdir().expect("log dir");
        write_checkpoint(log_dir.path());
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let id = capture(Some(log_dir.path()), None, &args)
            .await
            .expect("a capture that found one input succeeds");

        let store = args.open().expect("open the archive");
        let manifest: Manifest = serde_json::from_slice(
            &store
                .get(&capture_key(&id, MANIFEST))
                .await
                .expect("read the manifest"),
        )
        .expect("decode the manifest");
        check!(manifest.artifact(RLMM_SNAPSHOT) == None);
        check!(manifest.artifact(METADATA_CHECKPOINT).is_some());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_finds_nothing_names_the_flags_that_would_have_helped() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let message = capture(None, None, &args)
            .await
            .expect_err("a capture with no source fails")
            .to_string();
        check!(message.contains("pass --log-dir"), "got: {message}");

        let empty = tempfile::tempdir().expect("an empty log dir");
        let message = capture(Some(empty.path()), None, &args)
            .await
            .expect_err("a capture that found nothing fails")
            .to_string();
        check!(message.contains("is absent"), "got: {message}");
        check!(message.contains("checkpoint"), "got: {message}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_unreadable_snapshot_is_reported_rather_than_treated_as_absent() {
        let log_dir = tempfile::tempdir().expect("log dir");
        // A directory where the snapshot file belongs: the read fails with
        // something other than `NotFound`, which is not "the node has none".
        std::fs::create_dir_all(log_dir.path().join("remote-log-metadata/snapshot"))
            .expect("create a directory in the snapshot's place");
        let archive_root = tempfile::tempdir().expect("archive root");

        let error = capture(
            Some(log_dir.path()),
            None,
            &archive_args(archive_root.path()),
        )
        .await
        .expect_err("an unreadable snapshot fails the capture");
        assert!(let BackupError::Io(_) = &error, "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_of_group_offsets_reports_the_cluster_it_could_not_reach() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let address = closed_address();

        let error = capture(None, Some(&address), &archive_args(archive_root.path()))
            .await
            .expect_err("a capture cannot read offsets from a cluster that is not there");
        assert!(let BackupError::Cluster(_) = &error, "got: {error}");
        check!(error.to_string().contains(&address), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_names_every_capture_and_says_so_when_there_are_none() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        check!(list(&args).await.expect("list an empty archive").is_empty());

        let node = node_with_both_inputs();
        let first = capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");
        // A second capture id that sorts after the first, written by hand so
        // the two do not depend on the clock ticking between them.
        let store = args.open().expect("open the archive");
        let second = "9999999999999999";
        write_offsets_capture(&store, second, &offsets_json(), &offsets_json()).await;

        check!(list(&args).await.expect("list the captures") == vec![first, second.to_owned()]);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn list_reports_a_capture_whose_manifest_cannot_be_read_and_keeps_going() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = args.open().expect("open the archive");
        store
            .put(&capture_key("0000000000000001", MANIFEST), b"{".to_vec())
            .await
            .expect("write a truncated manifest");

        check!(
            list(&args)
                .await
                .expect("an unreadable manifest is listed, not fatal")
                == vec!["0000000000000001".to_owned()]
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn verify_accepts_a_capture_it_just_wrote_under_either_selector() {
        let node = node_with_both_inputs();
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let id = capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");

        check!(verify("latest", &args).await.is_ok());
        check!(verify(&id, &args).await.is_ok());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn verify_reports_an_artifact_the_archive_no_longer_holds_whole() {
        let node = node_with_both_inputs();
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let id = capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");

        // What a half-finished upload leaves behind, and exactly what a
        // restore must not be handed.
        std::fs::write(
            archive_root.path().join(capture_key(&id, RLMM_SNAPSHOT)),
            b"rlmm snapshot byt",
        )
        .expect("truncate the captured snapshot");

        let error = verify("latest", &args)
            .await
            .expect_err("a truncated artifact fails verification");
        assert!(let BackupError::Integrity(_) = &error, "got: {error}");
        check!(error.to_string().contains(RLMM_SNAPSHOT), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_is_not_in_the_archive_is_named_rather_than_guessed_at() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());

        let error = verify("latest", &args)
            .await
            .expect_err("an empty archive has no newest capture");
        assert!(let BackupError::NoSuchCapture(_) = &error, "got: {error}");

        let node = node_with_both_inputs();
        capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");
        let error = verify("0000000000000007", &args)
            .await
            .expect_err("a capture the archive does not hold");
        assert!(let BackupError::NoSuchCapture(_) = &error, "got: {error}");
        check!(
            error.to_string().contains("0000000000000007"),
            "got: {error}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_dry_run_reports_every_offset_and_commits_nothing() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = args.open().expect("open the archive");
        let offsets = offsets_json();
        write_offsets_capture(&store, "0000000000000001", &offsets, &offsets).await;

        // The cluster address is one nothing is listening on: a dry run must
        // not reach for it at all.
        check!(
            restore_offsets("latest", &closed_address(), true, &args)
                .await
                .expect("a dry run reads the capture alone")
                == 1
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_without_offsets_cannot_restore_them() {
        let node = node_with_both_inputs();
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        capture(Some(node.path()), None, &args)
            .await
            .expect("capture the node");

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("a capture of the two files holds no offsets");
        assert!(let BackupError::NoSuchCapture(_) = &error, "got: {error}");
        check!(error.to_string().contains(GROUP_OFFSETS), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offsets_that_do_not_match_their_digest_are_never_committed() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = args.open().expect("open the archive");
        write_offsets_capture(&store, "0000000000000001", b"{}", &offsets_json()).await;

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("offsets that do not match the manifest are an integrity failure");
        assert!(let BackupError::Integrity(_) = &error, "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn offsets_that_are_not_the_captured_shape_name_the_object_that_holds_them() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = args.open().expect("open the archive");
        let bytes = b"[]";
        write_offsets_capture(&store, "0000000000000001", bytes, bytes).await;

        let error = restore_offsets("latest", &closed_address(), true, &args)
            .await
            .expect_err("a JSON array is not the captured shape");
        assert!(let BackupError::Json { .. } = &error, "got: {error}");
        check!(error.to_string().contains(GROUP_OFFSETS), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_commit_reports_the_cluster_it_could_not_reach() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = args.open().expect("open the archive");
        let offsets = offsets_json();
        write_offsets_capture(&store, "0000000000000001", &offsets, &offsets).await;
        let address = closed_address();

        // The topic ids come first, so this is the Metadata lookup failing.
        let error = restore_offsets("latest", &address, false, &args)
            .await
            .expect_err("a commit cannot reach a cluster that is not there");
        assert!(let BackupError::Cluster(_) = &error, "got: {error}");
        check!(error.to_string().contains(&address), "got: {error}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_capture_that_holds_no_offsets_commits_nothing_and_asks_the_cluster_nothing() {
        let archive_root = tempfile::tempdir().expect("archive root");
        let args = archive_args(archive_root.path());
        let store = args.open().expect("open the archive");
        let empty = serde_json::to_vec(&GroupOffsetsFile::default()).expect("encode the offsets");
        write_offsets_capture(&store, "0000000000000001", &empty, &empty).await;

        // A capture taken while no group had committed anything names no
        // topic, so there is no metadata to look up and no commit to send:
        // the restore succeeds having asked the cluster for nothing, which is
        // why the address below is one nothing is listening on.
        check!(
            restore_offsets("latest", &closed_address(), false, &args)
                .await
                .expect("an empty capture restores nothing")
                == 0
        );
    }
}
