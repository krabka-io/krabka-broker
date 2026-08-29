//! The two bounds that name where the restore stops: `--to-offset` and
//! `--to-timestamp`.
//!
//! Both truncate a tail rather than punching a hole, so what they prove is the
//! inclusivity of the boundary itself and that everything after it is absent
//! from the restored log.

use assert2::check;
use krabka_ids::Offset;
use krabka_log::LogConfig;
use krabka_protocol::records::RecordBatch;

use crate::{
    archive::build_archive,
    fixtures::{BASE_TIMESTAMP, plain_batch, timestamped_record, value_record},
    harness::{reopen, run_restore},
};

// ---------------------------------------------------------------------
// Scenario 1: --to-offset is boundary-inclusive and truncates the tail.
// ---------------------------------------------------------------------

#[tokio::test]
async fn to_offset_bound_is_inclusive_and_truncates_the_tail() {
    let mut fixture: Vec<RecordBatch> = (0..10)
        .map(|i: i32| plain_batch(vec![value_record(0, &format!("v{i}"))]))
        .collect();
    let archive = build_archive("orders", 0, &mut fixture);

    let (_target, target_dir) = run_restore(archive.path(), &["--to-offset", "orders:0=5"]).await;

    let log = reopen(&target_dir, "orders", 0);
    check!(log.log_end_offset() == Offset(6));
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    check!(read.batches == fixture[..=5].to_vec());
}

// ---------------------------------------------------------------------
// Scenario 2: --to-timestamp keeps only records strictly before the bound.
// ---------------------------------------------------------------------

#[tokio::test]
async fn to_timestamp_keeps_only_records_strictly_before_the_bound() {
    let mut fixture = vec![plain_batch(vec![
        timestamped_record(0, 0, "t0"),
        timestamped_record(1, 10, "t1"),
        timestamped_record(2, 20, "t2"),
        timestamped_record(3, 30, "t3"),
    ])];
    let archive = build_archive("orders", 0, &mut fixture);
    let bound = (BASE_TIMESTAMP + 25).to_string();

    let (_target, target_dir) = run_restore(archive.path(), &["--to-timestamp", &bound]).await;

    let log = reopen(&target_dir, "orders", 0);
    let read = log
        .read(Offset(0), LogConfig::default().segment_size)
        .expect("read back");
    let expected = vec![RecordBatch {
        records: vec![
            timestamped_record(0, 0, "t0"),
            timestamped_record(1, 10, "t1"),
            timestamped_record(2, 20, "t2"),
        ],
        ..fixture[0].clone()
    }];
    check!(read.batches == expected);
}
