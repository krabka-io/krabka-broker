//! The structured report `restore` returns, compared as a whole `RestoreReport`
//! against the counts the fixture must produce, and then rendered through the
//! `--report json` path.

use {
    crate::{
        args::{RestoreOptions, restore_args},
        fixture::{Fixture, build_fixture},
        restored::fresh_restore,
    },
    assert2::check,
    krabka_audit::FileEd25519Signer,
    krabka_ids::Offset,
    krabka_remote_storage::MANIFEST_SUFFIX,
    krabka_restore::{
        MetadataRestoreReport, PartitionReport, ReportFormat, RestoreReport, SegmentOutcome,
        restore,
    },
    ring::{rand::SystemRandom, signature::Ed25519KeyPair},
    std::{path::PathBuf, sync::Arc},
    tempfile::TempDir,
};

/// A fixture whose archive one WORM key signed, with the key and heads an
/// authenticated restore of it trusts.
struct Signed {
    fixture: Fixture,
    _keys: TempDir,
    public_key: PathBuf,
    heads: Vec<(String, String)>,
}

fn signed_fixture() -> Signed {
    let fixture = build_fixture();
    let keys = tempfile::tempdir().unwrap();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let signer =
        Arc::new(FileEd25519Signer::from_pkcs8_bytes(pkcs8.as_ref(), "key".to_owned()).unwrap());
    let public_key = keys.path().join("key.pub");
    std::fs::write(&public_key, signer.public_key()).unwrap();
    let heads = fixture.sign_worm(&[signer]);
    Signed {
        fixture,
        _keys: keys,
        public_key,
        heads,
    }
}

/// Restore `orders` from a signed fixture, authenticated against its key and
/// pinned to its heads.
async fn restore_orders(signed: &Signed) -> RestoreReport {
    let mut extra = vec![
        "--topic".to_owned(),
        "orders".to_owned(),
        "--worm-key-id".to_owned(),
        "key".to_owned(),
        "--worm-public-key".to_owned(),
        signed.public_key.display().to_string(),
    ];
    for (partition, head) in &signed.heads {
        extra.push("--worm-expect-head".to_owned());
        extra.push(format!("{partition}={head}"));
    }
    let target = tempfile::tempdir().unwrap();
    let refs = extra.iter().map(String::as_str).collect::<Vec<_>>();
    let args = restore_args(
        signed.fixture.archive_root.path(),
        target.path(),
        RestoreOptions {
            extra: &refs,
            ..Default::default()
        },
    );
    restore(&args).await.unwrap()
}

#[tokio::test]
async fn authenticated_object_count_covers_only_the_selected_topic() {
    let signed = signed_fixture();
    let fixture = &signed.fixture;
    let expected = std::fs::read_dir(fixture.archive_root.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("orders-"))
        .flat_map(|entry| {
            std::fs::read_dir(entry.path())
                .unwrap()
                .filter_map(Result::ok)
        })
        .filter(|entry| {
            !entry
                .file_name()
                .to_string_lossy()
                .ends_with(MANIFEST_SUFFIX)
        })
        .count() as u64;
    let report = restore_orders(&signed).await;
    check!(report.authentication.unwrap().objects == expected);
}

/// A source lost mid-upload leaves a segment's objects without the manifest
/// that would have named them. An authenticated restore leaves that segment
/// out and says so, instead of refusing the whole archive: no signed claim
/// covers it, and the pinned heads prove no manifest was taken away.
#[tokio::test]
async fn an_upload_without_its_manifest_is_skipped_by_an_authenticated_restore() {
    let signed = signed_fixture();
    let fixture = &signed.fixture;

    // Copy one archived segment of `orders` under a base offset past its end,
    // with no manifest: the bytes of an upload the loss interrupted.
    let partition_dir = std::fs::read_dir(fixture.archive_root.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("orders-"))
        })
        .unwrap();
    let mut copied = 0;
    for entry in std::fs::read_dir(&partition_dir)
        .unwrap()
        .filter_map(Result::ok)
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(MANIFEST_SUFFIX) || !name.starts_with(&format!("{:020}", 0)) {
            continue;
        }
        // Its own segment id too: the first character of the encoded id,
        // `-` sitting between the base offset and it, is changed.
        let first = if name.as_bytes()[21] == b'A' {
            'B'
        } else {
            'A'
        };
        let orphan = format!("{:020}-{first}{}", 99_999_999, &name[22..]);
        std::fs::copy(entry.path(), partition_dir.join(orphan)).unwrap();
        copied += 1;
    }
    check!(copied > 0);
    // And a partition whose only upload was interrupted: no manifest, no head.
    let dir_name = partition_dir
        .file_name()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let (number, topic_id) = dir_name
        .strip_prefix("orders-")
        .and_then(|rest| rest.split_once('-'))
        .unwrap();
    let interrupted: i32 = number.parse().unwrap();
    let lone = fixture
        .archive_root
        .path()
        .join(format!("orders-7-{topic_id}"));
    check!(interrupted != 7);
    std::fs::create_dir(&lone).unwrap();
    for entry in std::fs::read_dir(&partition_dir)
        .unwrap()
        .filter_map(Result::ok)
    {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&format!("{:020}", 99_999_999)) {
            std::fs::copy(entry.path(), lone.join(name)).unwrap();
        }
    }

    let report = restore_orders(&signed).await;
    let mut skipped: Vec<(String, i32, String)> = report
        .skipped
        .iter()
        .map(|segment| {
            (
                segment.topic.clone(),
                segment.partition,
                segment.reason.clone(),
            )
        })
        .collect();
    skipped.sort();
    let reason = "uploaded without a manifest: no signed claim names it".to_owned();
    check!(
        skipped
            == [
                ("orders".to_owned(), interrupted, reason.clone()),
                ("orders".to_owned(), 7, reason)
            ]
    );
    // The lone partition is not restored at all: nothing signed names it.
    check!(
        !report
            .partitions
            .iter()
            .any(|partition| partition.partition == 7)
    );
}

/// 5. `restore()`'s report -- the structured value `--report json` renders --
/// carries exactly the record and segment counts this fixture should
/// produce, compared as whole structs rather than field by field.
#[tokio::test]
async fn json_report_matches_the_fixtures_exact_record_and_segment_counts() {
    let fixture = build_fixture();
    let restored = fresh_restore(fixture.archive_root.path()).await;

    let expected = RestoreReport {
        dry_run: false,
        log_dir: restored.log_dir.clone(),
        cluster_id: restored.cluster_id,
        authentication: None,
        diskless: None,
        metadata: MetadataRestoreReport {
            snapshot: None,
            topic_configs: 0,
            access_control_entries: 0,
            client_quotas: 0,
            scram_credentials: 0,
            feature_levels: 0,
            topics_without_configuration: vec!["orders".to_owned(), "payments".to_owned()],
        },
        partitions: fixture
            .partitions()
            .iter()
            .map(|partition| PartitionReport {
                topic: partition.topic.to_owned(),
                partition: partition.partition,
                topic_id: partition.topic_id,
                segments: partition
                    .segments
                    .iter()
                    .map(|segment| {
                        let base_offset = Offset(segment.batch.base_offset);
                        let end_offset = Offset(
                            segment.batch.base_offset + i64::from(segment.batch.last_offset_delta),
                        );
                        SegmentOutcome {
                            segment_id: segment.segment_id,
                            base_offset,
                            end_offset,
                            batches_kept: 1,
                            batches_rewritten: 0,
                            batches_emptied: 0,
                            records_kept: u64::try_from(segment.batch.records.len())
                                .expect("fixture batches stay tiny"),
                            records_dropped: 0,
                            bytes_written: u64::try_from(segment.batch.encoded_len())
                                .expect("fixture batches stay tiny"),
                        }
                    })
                    .collect(),
            })
            .collect(),
        skipped: Vec::new(),
    };
    check!(restored.report == expected);

    // Also render and reparse as JSON, exercising the `--report json` path
    // itself rather than only the underlying struct it renders.
    let json = restored.report.render(ReportFormat::Json);
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    check!(value["cluster_id"] == serde_json::json!(restored.cluster_id.to_string()));
    check!(value["partitions"][0]["topic"] == serde_json::json!("orders"));
    check!(value["partitions"][0]["partition"] == serde_json::json!(0));
    check!(
        value["partitions"][0]["segments"]
            .as_array()
            .expect("segments array")
            .len()
            == 2
    );
    check!(
        value["skipped"]
            .as_array()
            .expect("skipped array")
            .is_empty()
    );
}
