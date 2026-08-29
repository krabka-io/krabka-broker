//! Reading a JVM broker's on-disk log with the bundled `kafka-dump-log`.
//!
//! Equal log end offsets cannot prove that a follower truncated a divergent
//! suffix, because both tails can hold the same number of records. The
//! truncation scenarios therefore read the JVM broker's actual bytes. This
//! module dumps a partition directory inside the running container and parses
//! the offsets back out of the dump text.

use std::process::Command;

/// Dump a partition segment that lives INSIDE the running JVM broker container
/// with `docker exec` and the container's bundled `kafka-dump-log`.
pub fn dump_log_in_container(container: &str, partition_dir: &str) -> String {
    let listed = Command::new("docker")
        .args([
            "exec",
            container,
            "find",
            partition_dir,
            "-maxdepth",
            "1",
            "-type",
            "f",
            "-name",
            "*.log",
            "-print",
        ])
        .output()
        .expect("list JVM log segments");
    if !listed.status.success() {
        return String::from_utf8_lossy(&listed.stderr).to_string();
    }
    let mut log_files = String::from_utf8_lossy(&listed.stdout)
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    log_files.sort();
    if log_files.is_empty() {
        return String::new();
    }
    let files = log_files.join(",");
    let out = Command::new("docker")
        .args([
            "exec",
            container,
            "/opt/kafka/bin/kafka-dump-log.sh",
            "--files",
            &files,
            "--print-data-log",
        ])
        .output()
        .expect("spawn dump-log exec");
    let mut dump = String::from_utf8_lossy(&out.stdout).to_string();
    if !out.status.success() {
        dump.push_str(&String::from_utf8_lossy(&out.stderr));
    }
    dump
}

/// Extract the highest record offset reported by a `kafka-dump-log`
/// `--print-data-log` dump (max of any `lastOffset:` / `offset:` field).
pub fn max_offset_in_dump(dump: &str) -> Option<i64> {
    let mut max = None;
    let mut tokens = dump.split_whitespace().peekable();
    while let Some(token) = tokens.next() {
        let raw = if matches!(token, "lastOffset:" | "offset:") {
            tokens.next()
        } else {
            ["lastOffset:", "offset:"]
                .iter()
                .find_map(|key| token.strip_prefix(key))
                .filter(|value| !value.is_empty())
        };
        if let Some(raw) = raw
            && let Ok(value) = raw
                .trim_matches(|character: char| !character.is_ascii_digit() && character != '-')
                .parse::<i64>()
        {
            max = Some(max.map_or(value, |current: i64| current.max(value)));
        }
    }
    max
}

/// Pull just the `baseOffset`/`lastOffset` summary lines for log readability.
pub fn grep_base_offsets(dump: &str) -> String {
    dump.lines()
        .filter(|l| l.contains("baseOffset"))
        .take(40)
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn kafka_dump_offset_parser_accepts_spaced_values() {
    let dump = "baseOffset: 0 lastOffset: 9 count: 10\n\
                offset: 10 position: 211 payload: value";
    assert2::assert!(max_offset_in_dump(dump) == Some(10));
}
