use assert2::check;
use clap::Parser as _;

use super::*;

fn partition(topic: &str, index: i32) -> PartitionRef {
    PartitionRef {
        topic: topic.to_owned(),
        partition: index,
    }
}

#[test]
fn topic_names_follow_kafka_rules() {
    for good in [
        "orders",
        "a",
        "a.b_c-d",
        "0",
        &"x".repeat(MAX_TOPIC_NAME_LEN),
    ] {
        check!(parse_topic_name(good) == Ok(good.to_owned()), "{good:?}");
    }
    for bad in [
        "",
        ".",
        "..",
        "has space",
        "has/slash",
        "has:colon",
        "has=equals",
        "café",
        &"x".repeat(MAX_TOPIC_NAME_LEN + 1),
    ] {
        check!(parse_topic_name(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn partition_refs_split_on_the_colon() {
    check!(parse_partition_ref("orders:0") == Ok(partition("orders", 0)));
    check!(parse_partition_ref("a.b-c:12") == Ok(partition("a.b-c", 12)));
}

#[test]
fn partition_refs_reject_malformed_input() {
    for bad in [
        "orders",
        "orders:",
        ":0",
        "orders:-1",
        "orders:x",
        "orders:0:1",
        "or ders:0",
        "orders:99999999999",
    ] {
        check!(parse_partition_ref(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn partition_ref_displays_the_kafka_spelling() {
    check!(partition("orders", 3).to_string() == "orders-3");
}

#[test]
fn offset_bounds_parse_to_an_inclusive_last_offset() {
    check!(
        parse_offset_bound("orders:0=1000")
            == Ok(OffsetBound {
                partition: partition("orders", 0),
                last_offset: Offset(1000),
            })
    );
    check!(
        parse_offset_bound("orders:7=0")
            == Ok(OffsetBound {
                partition: partition("orders", 7),
                last_offset: Offset(0),
            })
    );
}

#[test]
fn offset_bounds_reject_malformed_input() {
    for bad in [
        "orders:0",
        "orders=1000",
        "orders:0=",
        "orders:0=-1",
        "orders:0=x",
        "=1000",
        "orders:0=1000=2000",
    ] {
        check!(parse_offset_bound(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn offset_ranges_normalize_to_a_half_open_interval() {
    check!(
        parse_offset_range("orders:0=100..200")
            == Ok(OffsetRange {
                partition: partition("orders", 0),
                start: Offset(100),
                end_exclusive: Offset(200),
            })
    );
    check!(
        parse_offset_range("orders:0=100..=200")
            == Ok(OffsetRange {
                partition: partition("orders", 0),
                start: Offset(100),
                end_exclusive: Offset(201),
            })
    );
    check!(
        parse_offset_range("orders:0=0..1")
            == Ok(OffsetRange {
                partition: partition("orders", 0),
                start: Offset(0),
                end_exclusive: Offset(1),
            })
    );
}

#[test]
fn offset_ranges_reject_empty_inverted_and_malformed_input() {
    for bad in [
        "orders:0=100..100",
        "orders:0=200..100",
        "orders:0=100..=99",
        "orders:0=100",
        "orders:0=..200",
        "orders:0=100..",
        "orders:0=-1..5",
        "orders:0=1..-5",
        "orders:0",
        "orders:0=a..b",
        &format!("orders:0=0..={}", i64::MAX),
    ] {
        check!(parse_offset_range(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn header_patterns_split_on_the_first_equals() {
    let parsed = parse_header_pattern("trace-id=^abc[0-9]+$").expect("pattern");
    check!(
        parsed
            == HeaderPattern {
                name: "trace-id".to_owned(),
                pattern: Regex::new("^abc[0-9]+$").expect("pattern"),
            }
    );
    let with_equals = parse_header_pattern("op=a=b").expect("pattern");
    check!(with_equals.name == "op");
    check!(with_equals.pattern.as_str() == "a=b");
}

#[test]
fn header_patterns_reject_malformed_input() {
    for bad in ["trace-id", "=^abc$", "trace-id=[unclosed"] {
        check!(parse_header_pattern(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn producer_ids_reject_the_no_producer_sentinel() {
    check!(parse_producer_id("42") == Ok(ProducerId(42)));
    check!(parse_producer_id(" 7 ") == Ok(ProducerId(7)));
    for bad in ["-1", "x", "", "1.5"] {
        check!(parse_producer_id(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn node_ids_take_an_integer_and_nothing_else() {
    check!(parse_node_id("3") == Ok(NodeId(3)));
    for bad in ["-1", "x", ""] {
        check!(parse_node_id(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn regexes_compile_or_report_why_not() {
    check!(parse_regex("^ok$").is_ok());
    check!(parse_regex("[unclosed").is_err());
}

#[test]
fn timestamps_accept_bare_epoch_milliseconds() {
    for (input, expected) in [
        ("0", 0),
        ("1", 1),
        ("+1", 1),
        ("-1", -1),
        ("1713000000000", 1_713_000_000_000),
        (" 1713000000000 ", 1_713_000_000_000),
    ] {
        check!(parse_timestamp(input) == Ok(expected), "{input:?}");
    }
}

#[test]
fn timestamps_accept_rfc3339_instants() {
    for (input, expected) in [
        ("1970-01-01T00:00:00Z", 0),
        ("1970-01-01T00:00:00.001Z", 1),
        ("1970-01-01T00:00:00.000999999Z", 0),
        ("1969-12-31T23:59:59Z", -1_000),
        ("2026-08-24T12:00:00Z", 1_787_572_800_000),
        ("2026-08-24t12:00:00z", 1_787_572_800_000),
        ("2026-08-24 12:00:00Z", 1_787_572_800_000),
        ("2026-08-24T12:00:00+00:00", 1_787_572_800_000),
        ("2026-08-24T14:00:00+02:00", 1_787_572_800_000),
        ("2026-08-24T07:00:00-05:00", 1_787_572_800_000),
        ("2026-08-24T12:00:00.250Z", 1_787_572_800_250),
        ("2026-08-24T12:00:00.2Z", 1_787_572_800_200),
        ("2024-02-29T00:00:00Z", 1_709_164_800_000),
        ("2000-02-29T00:00:00Z", 951_782_400_000),
    ] {
        check!(parse_timestamp(input) == Ok(expected), "{input:?}");
    }
}

#[test]
fn timestamps_reject_impossible_and_zoneless_input() {
    for bad in [
        "",
        "   ",
        "not-a-time",
        "2026-08-24",
        "2026-08-24T12:00:00",
        "2026-08-24X12:00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-00-01T00:00:00Z",
        "2026-08-32T00:00:00Z",
        "2026-08-00T00:00:00Z",
        "2023-02-29T00:00:00Z",
        "1900-02-29T00:00:00Z",
        "0000-01-01T00:00:00Z",
        "2026-08-24T24:00:00Z",
        "2026-08-24T12:60:00Z",
        "2026-08-24T12:00:60Z",
        "2026-08-24T12:00:00.Z",
        "2026-08-24T12:00:00.0000000000Z",
        "2026-08-24T12:00:00+0200",
        "2026-08-24T12:00:00+24:00",
        "2026-08-24T12:00:00+02:60",
        "2026-08-24T12:00:00*02:00",
        "2026/08/24T12:00:00Z",
        "26-08-24T12:00:00Z",
    ] {
        check!(parse_timestamp(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn civil_days_match_known_epochs() {
    for (year, month, day, expected) in [
        (1970, 1, 1, 0),
        (1970, 1, 2, 1),
        (1969, 12, 31, -1),
        (2000, 3, 1, 11_017),
        (2026, 8, 24, 20_689),
        (1600, 1, 1, -135_140),
    ] {
        check!(
            days_from_civil(year, month, day) == expected,
            "{year}-{month}-{day}"
        );
    }
}

fn args_from(extra: &[&str]) -> Result<RestoreArgs, clap::Error> {
    let mut argv = vec![
        "krabka-restore",
        "--archive-local",
        "/archive",
        "--log-dir",
        "/target",
    ];
    argv.extend_from_slice(extra);
    crate::Cli::try_parse_from(argv).map(|cli| cli.args)
}

#[test]
fn an_archive_source_is_required_and_exclusive() {
    check!(crate::Cli::try_parse_from(["krabka-restore", "--log-dir", "/target"]).is_err());
    check!(
        crate::Cli::try_parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-local",
            "/archive",
            "--archive-s3-bucket",
            "b",
        ])
        .is_err()
    );
    check!(
        crate::Cli::try_parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-s3-bucket",
            "b",
            "--archive-gcs-bucket",
            "g",
        ])
        .is_err()
    );
    check!(args_from(&[]).is_ok());
}

#[test]
fn backend_sub_flags_require_their_bucket() {
    for stray in [
        vec!["--archive-s3-region", "eu-west-1"],
        vec!["--archive-s3-endpoint", "http://minio:9000"],
        vec!["--archive-s3-access-key-id", "key"],
        vec!["--archive-s3-secret-access-key", "secret"],
        vec!["--archive-s3-allow-http"],
        vec!["--archive-gcs-service-account-path", "/etc/sa.json"],
        vec!["--archive-gcs-endpoint", "http://fake-gcs:4443"],
        vec!["--archive-gcs-allow-http"],
    ] {
        // The archive source in `args_from` is `--archive-local`, so every one
        // of these names a backend that was not selected.
        let args = args_from(&stray).expect("args");
        check!(args.validate().is_err(), "{stray:?}");
    }
}

#[test]
fn backend_sub_flags_are_accepted_with_their_bucket() {
    let args = crate::Cli::try_parse_from([
        "krabka-restore",
        "--log-dir",
        "/target",
        "--archive-s3-bucket",
        "backups",
        "--archive-s3-region",
        "eu-west-1",
        "--archive-s3-allow-http",
    ])
    .expect("args")
    .args;
    check!(args.validate().is_ok());
}

#[test]
fn the_three_controller_modes_conflict() {
    check!(args_from(&["--standalone", "--no-initial-controllers"]).is_err());
    check!(args_from(&["--standalone", "--initial-controllers", "1@h:1:u"]).is_err());
    check!(
        args_from(&[
            "--no-initial-controllers",
            "--initial-controllers",
            "1@h:1:u",
        ])
        .is_err()
    );
}

#[test]
fn repeatable_flags_accumulate() {
    let args = args_from(&[
        "--topic",
        "orders",
        "--topic",
        "payments",
        "--to-offset",
        "orders:0=10",
        "--to-offset",
        "payments:1=20",
        "--exclude-producer-id",
        "1",
        "--exclude-producer-id",
        "2",
        "--exclude-offset",
        "orders:0=5..7",
    ])
    .expect("args");
    check!(args.topic == vec!["orders".to_owned(), "payments".to_owned()]);
    check!(args.to_offset.len() == 2);
    check!(args.exclude_producer_id == vec![ProducerId(1), ProducerId(2)]);
    check!(
        args.exclude_offset
            == vec![OffsetRange {
                partition: partition("orders", 0),
                start: Offset(5),
                end_exclusive: Offset(7),
            }]
    );
}

#[test]
fn the_report_format_defaults_to_text() {
    check!(args_from(&[]).expect("args").report == ReportFormat::Text);
    check!(args_from(&["--report", "json"]).expect("args").report == ReportFormat::Json);
    check!(args_from(&["--report", "yaml"]).is_err());
}

#[test]
fn an_empty_topic_list_selects_everything() {
    let args = args_from(&[]).expect("args");
    check!(args.selects_topic("anything"));

    let args = args_from(&["--topic", "orders"]).expect("args");
    check!(args.selects_topic("orders"));
    check!(!args.selects_topic("payments"));
}

#[test]
fn validate_rejects_two_bounds_on_one_partition() {
    let args =
        args_from(&["--to-offset", "orders:0=10", "--to-offset", "orders:0=20"]).expect("args");
    check!(args.validate().is_err());
}

#[test]
fn validate_accepts_bounds_on_distinct_partitions() {
    let args =
        args_from(&["--to-offset", "orders:0=10", "--to-offset", "orders:1=20"]).expect("args");
    check!(args.validate().is_ok());
}

#[test]
fn validate_rejects_a_bound_on_an_unselected_topic() {
    let args = args_from(&["--topic", "orders", "--to-offset", "payments:0=10"]).expect("args");
    check!(args.validate().is_err());

    let args =
        args_from(&["--topic", "orders", "--exclude-offset", "payments:0=1..2"]).expect("args");
    check!(args.validate().is_err());
}

#[test]
fn validate_accepts_a_bound_on_a_selected_topic() {
    let args = args_from(&["--topic", "orders", "--to-offset", "orders:0=10"]).expect("args");
    check!(args.validate().is_ok());
}
