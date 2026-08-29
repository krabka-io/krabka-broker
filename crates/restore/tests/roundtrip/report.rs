//! The structured report `restore` returns, compared as a whole `RestoreReport`
//! against the counts the fixture must produce, and then rendered through the
//! `--report json` path.

use assert2::check;
use krabka_ids::Offset;
use krabka_restore::{PartitionReport, ReportFormat, RestoreReport, SegmentOutcome, restore};
use uuid::Uuid;

use crate::{args::restore_args, fixture::build_fixture};

/// 5. `restore()`'s report -- the structured value `--report json` renders --
/// carries exactly the record and segment counts this fixture should
/// produce, compared as whole structs rather than field by field.
#[tokio::test]
async fn json_report_matches_the_fixtures_exact_record_and_segment_counts() {
    let fixture = build_fixture();
    let target = tempfile::tempdir().expect("target parent");
    let log_dir = target.path().join("restored");
    let cluster_id = Uuid::new_v4();
    let args = restore_args(
        fixture.archive_root.path(),
        &log_dir,
        &["--cluster-id", &cluster_id.to_string()],
    );

    let report = restore(&args).await.expect("restore");

    let expected = RestoreReport {
        dry_run: false,
        log_dir: log_dir.clone(),
        cluster_id,
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
    check!(report == expected);

    // Also render and reparse as JSON, exercising the `--report json` path
    // itself rather than only the underlying struct it renders.
    let json = report.render(ReportFormat::Json);
    let value: serde_json::Value = serde_json::from_str(&json).expect("valid JSON");
    check!(value["cluster_id"] == serde_json::json!(cluster_id.to_string()));
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
