//! `krabka-format` against `kafka-storage format`, through the binary.
//!
//! Each case runs the binary as a subprocess and reads back what it wrote and
//! printed. The Kafka behaviour each case pins was observed with
//! `kafka-storage format` on `apache/kafka:4.3.1`; `docs/format-divergences.md`
//! lists the places krabka differs on purpose.

use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use krabka_format::FAIL_AFTER_ENV;
use krabka_metadata::MetadataRecord;

const STANDALONE: &[&str] = &[
    "--standalone",
    "--node-id",
    "1",
    "--controller-listener",
    "controller-1:9093",
];

/// A cluster id in Kafka's form, and the same id in the hyphenated form.
const CLUSTER_ID: &str = "AQIDBAUGBwgJCgsMDQ4PEA";
const CLUSTER_ID_HYPHENATED: &str = "01020304-0506-0708-090a-0b0c0d0e0f10";

fn krabka_format(args: &[&str], fault: Option<&str>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_krabka-format"));
    command.args(args).env_remove(FAIL_AFTER_ENV);
    if let Some(name) = fault {
        command.env(FAIL_AFTER_ENV, name);
    }
    command.output().expect("run krabka-format")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("utf-8 stdout")
}

fn stderr(output: &Output) -> String {
    String::from_utf8(output.stderr.clone()).expect("utf-8 stderr")
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("utf-8 path")
}

/// The ids in a directory's `meta.properties.json`, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WrittenIds {
    cluster_id: String,
    directory_id: String,
}

fn written_ids(dir: &Path) -> WrittenIds {
    let meta: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.join("meta.properties.json")).expect("meta.properties.json"),
    )
    .expect("meta.properties.json is json");
    WrittenIds {
        cluster_id: meta["cluster_id"].as_str().expect("cluster_id").to_owned(),
        directory_id: meta["directory_id"]
            .as_str()
            .expect("directory_id")
            .to_owned(),
    }
}

fn manifest_cluster_id(dir: &Path) -> String {
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("bootstrap.json")).expect("manifest"))
            .expect("manifest is json");
    manifest["cluster_id"]
        .as_str()
        .expect("cluster_id")
        .to_owned()
}

fn checkpoint(dir: &Path) -> PathBuf {
    dir.join("__cluster_metadata")
        .join("@metadata-0")
        .join("00000000000000000000-0000000000.checkpoint")
}

/// Every file under `dir` with its bytes, for an unchanged-on-rerun check.
fn snapshot(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("list") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
            } else {
                files.push((path.clone(), std::fs::read(&path).expect("read")));
            }
        }
    }
    files.sort();
    files
}

/// The id form: whatever `--cluster-id` names, the directory, the manifest,
/// and stdout carry Kafka's 22-character form. The reserved ids parse, as
/// they do in `kafka-storage format -t AAAAAAAAAAAAAAAAAAAAAA`. clap refuses a
/// bad id with its usage error, exit 2, before anything is written.
#[test]
fn the_cluster_id_is_written_and_printed_in_kafka_form() {
    // (what, --cluster-id, the id written, or None for a refused one)
    let cases: &[(&str, &str, Option<&str>)] = &[
        ("Kafka form", CLUSTER_ID, Some(CLUSTER_ID)),
        ("hyphenated form", CLUSTER_ID_HYPHENATED, Some(CLUSTER_ID)),
        (
            "upper-case hyphenated form",
            "01020304-0506-0708-090A-0B0C0D0E0F10",
            Some(CLUSTER_ID),
        ),
        (
            "Kafka form with canonical padding",
            "AQIDBAUGBwgJCgsMDQ4PEA==",
            Some(CLUSTER_ID),
        ),
        (
            "unused low bits set in the last character",
            "AQIDBAUGBwgJCgsMDQ4PEB",
            Some(CLUSTER_ID),
        ),
        (
            "the reserved zero id",
            "AAAAAAAAAAAAAAAAAAAAAA",
            Some("AAAAAAAAAAAAAAAAAAAAAA"),
        ),
        (
            "the reserved one id",
            "AAAAAAAAAAAAAAAAAAAAAQ",
            Some("AAAAAAAAAAAAAAAAAAAAAQ"),
        ),
        ("too short", "AQIDBAUGBwgJCgsMDQ4P", None),
        ("too long", "AQIDBAUGBwgJCgsMDQ4PEBES", None),
        (
            "outside the url-safe alphabet",
            "AQIDBAUGBwgJCgsMDQ4P+A",
            None,
        ),
        (
            "hyphenated but not hex",
            "01020304-0506-0708-090a-0b0c0d0e0fzz",
            None,
        ),
        ("unhyphenated hex", "0102030405060708090a0b0c0d0e0f10", None),
    ];
    for (what, input, written) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let out = krabka_format(
            &[
                "--log-dir",
                path_str(&dir),
                "--cluster-id",
                input,
                "--no-initial-controllers",
            ],
            None,
        );
        if let Some(id) = written {
            assert2::assert!(out.status.code() == Some(0), "{what}: {}", stderr(&out));
            let got = (
                written_ids(&dir).cluster_id,
                manifest_cluster_id(&dir),
                stdout(&out).contains(&format!("with cluster-id {id} ")),
            );
            assert2::assert!(got == ((*id).to_owned(), (*id).to_owned(), true), "{what}");
        } else {
            assert2::assert!(out.status.code() == Some(2), "{what}");
            assert2::assert!(!dir.exists(), "{what}: nothing is written");
        }
    }
}

/// `--directory-id` and the directory id of an `--initial-controllers` entry
/// take either form and are written in Kafka's.
#[test]
fn directory_ids_are_written_in_kafka_form() {
    const DIRECTORY_ID: &str = "AAAAAAAAAAAAAAAAAAAAZA";
    let cases: &[(&str, &[&str])] = &[
        (
            "--directory-id, Kafka form",
            &["--directory-id", DIRECTORY_ID],
        ),
        (
            "--directory-id, hyphenated form",
            &["--directory-id", "00000000-0000-0000-0000-000000000064"],
        ),
        (
            "--initial-controllers, Kafka form",
            &[
                "--node-id",
                "1",
                "--initial-controllers",
                "1@controller-1:9093:AAAAAAAAAAAAAAAAAAAAZA",
            ],
        ),
        (
            "--initial-controllers, hyphenated form",
            &[
                "--node-id",
                "1",
                "--initial-controllers",
                "1@controller-1:9093:00000000-0000-0000-0000-000000000064",
            ],
        ),
    ];
    for (what, extra) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("data");
        let mut args = vec!["--log-dir", path_str(&dir), "--cluster-id", CLUSTER_ID];
        args.extend_from_slice(extra);
        let out = krabka_format(&args, None);
        assert2::assert!(out.status.code() == Some(0), "{what}: {}", stderr(&out));
        assert2::assert!(
            written_ids(&dir)
                == WrittenIds {
                    cluster_id: CLUSTER_ID.to_owned(),
                    directory_id: DIRECTORY_ID.to_owned(),
                },
            "{what}"
        );
        // The broker reads back exactly what was written.
        let meta = krabka_broker::bootstrap::read_meta_properties(&dir).expect("broker reads it");
        assert2::assert!(meta.directory_id == uuid::Uuid::from_u128(100), "{what}");
    }
}

/// What one directory of a multi-directory format holds.
#[derive(Debug, PartialEq, Eq)]
struct Formatted {
    cluster_id: String,
    has_checkpoint: bool,
    bootstrap_records: usize,
}

fn formatted(dir: &Path) -> Formatted {
    Formatted {
        cluster_id: written_ids(dir).cluster_id,
        has_checkpoint: checkpoint(dir).is_file(),
        bootstrap_records: krabka_broker::bootstrap::load_bootstrap_records(dir)
            .expect("bootstrap records")
            .len(),
    }
}

/// Spells the `--log-dir` flags for two directories.
type Spelling = fn(&str, &str) -> Vec<String>;

/// Repeated and comma-separated `--log-dir` format every directory in one
/// run, as `kafka-storage format` formats every entry of `log.dirs`. The set
/// shares one cluster id, each directory has its own id, and only the first
/// directory -- the metadata log directory, as `metadata.log.dir` defaults to
/// the first of `log.dirs` -- gets the `__cluster_metadata` checkpoint.
#[test]
fn every_log_dir_is_formatted_in_one_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let seed_records = {
        let probe = tmp.path().join("probe");
        let mut args = vec!["--log-dir", path_str(&probe), "--cluster-id", CLUSTER_ID];
        args.extend_from_slice(STANDALONE);
        assert2::assert!(krabka_format(&args, None).status.success());
        krabka_broker::bootstrap::load_bootstrap_records(&probe)
            .expect("records")
            .len()
    };

    let spellings: [(&str, Spelling); 2] = [
        ("repeated", |a, b| {
            vec!["--log-dir".into(), a.into(), "--log-dir".into(), b.into()]
        }),
        ("comma-separated", |a, b| {
            vec!["--log-dir".into(), format!("{a},{b}")]
        }),
    ];
    for (what, spell) in spellings {
        let run = tmp.path().join(what);
        let (meta_dir, data_dir) = (run.join("meta"), run.join("data"));
        let mut args = spell(path_str(&meta_dir), path_str(&data_dir));
        args.extend(["--cluster-id", CLUSTER_ID].map(String::from));
        args.extend(STANDALONE.iter().map(|a| (*a).to_owned()));
        let args: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = krabka_format(&args, None);
        assert2::assert!(out.status.code() == Some(0), "{what}: {}", stderr(&out));

        let expected = |has_checkpoint| Formatted {
            cluster_id: CLUSTER_ID.to_owned(),
            has_checkpoint,
            bootstrap_records: seed_records,
        };
        assert2::assert!(
            [formatted(&meta_dir), formatted(&data_dir)] == [expected(true), expected(false)],
            "{what}"
        );
        let directory_ids: BTreeSet<String> = [&meta_dir, &data_dir]
            .iter()
            .map(|dir| written_ids(dir).directory_id)
            .collect();
        assert2::assert!(directory_ids.len() == 2, "{what}: {directory_ids:?}");
        assert2::assert!(
            stdout(&out).lines().take(2).collect::<Vec<_>>()
                == vec![
                    format!(
                        "Formatting dynamic metadata voter directory {} with metadata.version \
                         4.3-IV0.",
                        meta_dir.display()
                    ),
                    format!(
                        "Formatting data directory {} with metadata.version 4.3-IV0.",
                        data_dir.display()
                    ),
                ],
            "{what}"
        );
    }
}

/// Without `--ignore-formatted`, one formatted directory refuses the whole
/// run with Kafka's message, and the others are left alone. With it, the
/// formatted directories are skipped, the rest are formatted under the same
/// cluster id, and a second run changes nothing.
#[test]
fn ignore_formatted_skips_the_formatted_and_formats_the_rest() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (a, b) = (tmp.path().join("a"), tmp.path().join("b"));
    let both = format!("{},{}", a.display(), b.display());

    let first = krabka_format(
        &["--log-dir", path_str(&a), "--cluster-id", CLUSTER_ID],
        None,
    );
    assert2::assert!(first.status.success());
    let a_ids = written_ids(&a);

    let refused = krabka_format(&["--log-dir", &both, "--cluster-id", CLUSTER_ID], None);
    assert2::assert!(
        (refused.status.code(), stderr(&refused))
            == (
                Some(3),
                format!(
                    "Log directory {} is already formatted. Use --ignore-formatted to ignore \
                     this directory and format the others.\n",
                    a.display()
                )
            )
    );
    assert2::assert!(!b.exists(), "a refused run writes nothing");

    let other_cluster = krabka_format(
        &[
            "--log-dir",
            &both,
            "--cluster-id",
            "BQIDBAUGBwgJCgsMDQ4PEA",
            "--ignore-formatted",
        ],
        None,
    );
    assert2::assert!(
        (other_cluster.status.code(), stderr(&other_cluster))
            == (
                Some(3),
                format!(
                    "Invalid cluster.id in: {}. Expected BQIDBAUGBwgJCgsMDQ4PEA, but read {CLUSTER_ID}\n",
                    a.join("meta.properties.json").display()
                )
            )
    );

    // No --cluster-id: the formatted directory supplies it.
    let mixed = krabka_format(&["--log-dir", &both, "--ignore-formatted"], None);
    assert2::assert!(mixed.status.code() == Some(0), "{}", stderr(&mixed));
    assert2::assert!(written_ids(&a) == a_ids);
    let b_ids = written_ids(&b);
    assert2::assert!(b_ids.cluster_id == CLUSTER_ID);
    assert2::assert!(b_ids.directory_id != a_ids.directory_id);
    assert2::assert!(stdout(&mixed).contains(&format!(
        "Formatting data directory {} with metadata.version 4.3-IV0.",
        b.display()
    )));

    let before = (snapshot(&a), snapshot(&b));
    let again = krabka_format(
        &[
            "--log-dir",
            &both,
            "--cluster-id",
            CLUSTER_ID,
            "--ignore-formatted",
        ],
        None,
    );
    assert2::assert!(again.status.code() == Some(0), "{}", stderr(&again));
    assert2::assert!(
        stdout(&again)
            .lines()
            .last()
            .is_some_and(|line| line == "All of the log directories are already formatted.")
    );
    assert2::assert!((snapshot(&a), snapshot(&b)) == before);
}

/// A run that stops after any file it writes leaves a directory the next run
/// formats without an `rm -rf`, because `meta.properties.json` is the last
/// file and is published by a rename. Once the marker has landed, the
/// directory is formatted, and only `--ignore-formatted` passes it.
#[test]
fn an_interrupted_run_can_be_run_again() {
    // (the file the run stops after, exit of a plain rerun)
    let cases: &[(&str, i32)] = &[
        ("00000000000000000000-0000000000.checkpoint", 0),
        ("bootstrap.records.bin", 0),
        ("bootstrap.json", 0),
        ("meta.properties.json.tmp", 0),
        ("meta.properties.json", 3),
    ];
    for (fault, rerun_exit) in cases {
        let tmp = tempfile::tempdir().expect("tempdir");
        let (meta_dir, data_dir) = (tmp.path().join("meta"), tmp.path().join("data"));
        let dirs = format!("{},{}", meta_dir.display(), data_dir.display());
        let mut args = vec!["--log-dir", &dirs, "--cluster-id", CLUSTER_ID];
        args.extend_from_slice(STANDALONE);

        let interrupted = krabka_format(&args, Some(fault));
        assert2::assert!(interrupted.status.code() == Some(4), "{fault}");
        assert2::assert!(
            stderr(&interrupted).contains("injected failure after"),
            "{fault}"
        );

        let rerun = krabka_format(&args, None);
        assert2::assert!(
            rerun.status.code() == Some(*rerun_exit),
            "{fault}: {}",
            stderr(&rerun)
        );
        if *rerun_exit != 0 {
            let mut ignoring = args.clone();
            ignoring.push("--ignore-formatted");
            let finished = krabka_format(&ignoring, None);
            assert2::assert!(
                finished.status.code() == Some(0),
                "{fault}: {}",
                stderr(&finished)
            );
        }

        for dir in [&meta_dir, &data_dir] {
            let meta = krabka_broker::bootstrap::read_meta_properties(dir)
                .unwrap_or_else(|e| panic!("{fault}: {} does not read: {e}", dir.display()));
            assert2::assert!(
                meta.cluster_id == uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10)
            );
            assert2::assert!(!dir.join("meta.properties.json.tmp").exists(), "{fault}");
            assert2::assert!(
                !krabka_broker::bootstrap::load_bootstrap_records(dir)
                    .expect("records")
                    .is_empty(),
                "{fault}"
            );
        }
        assert2::assert!(checkpoint(&meta_dir).is_file(), "{fault}");
        let records = krabka_broker::bootstrap::load_bootstrap_records(&meta_dir).expect("records");
        assert2::assert!(
            records
                .iter()
                .all(|r| !matches!(r, MetadataRecord::V1Voters(_))),
            "{fault}: voters live in the checkpoint"
        );
    }
}
