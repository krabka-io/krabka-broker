//! The host the run executes on: the evidence directory, the measured
//! environment, the wall clock, and Docker commands that report a failure
//! instead of panicking.

use {
    serde::Serialize,
    std::{
        collections::BTreeMap,
        fs::{File, OpenOptions},
        io::Write as _,
        path::{Path, PathBuf},
        process::Command,
        sync::Mutex,
        time::{SystemTime, UNIX_EPOCH},
    },
};

/// The wall clock in Unix milliseconds, the coordinate every timestamp of the
/// evidence is in.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_millis()).unwrap_or(i64::MAX)
        })
}

/// Run `docker` with `args` and return its trimmed stdout, or a message with
/// its stderr.
///
/// Fault injection and cleanup run through this rather than through the
/// panicking `support::docker`: a cleanup that fails is recorded in the
/// evidence, and a panic in a `Drop` would hide the real failure.
pub(crate) fn docker_try(args: &[&str]) -> Result<String, String> {
    let output = Command::new("docker")
        .args(args)
        .output()
        .map_err(|error| format!("spawn docker {args:?}: {error}"))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
    } else {
        Err(format!(
            "docker {} exited {:?}: {}",
            args.join(" "),
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

/// Where the run's evidence goes: `KRABKA_SOAK_EVIDENCE_DIR` when set, else
/// Bazel's undeclared-outputs directory, else nothing and the caller makes a
/// directory of its own.
///
/// A variable exported empty counts as unset, so it does not hide the one
/// below it.
pub(crate) fn chosen_evidence_dir(
    explicit: Option<String>,
    bazel_outputs: Option<String>,
) -> Option<PathBuf> {
    let set = |dir: Option<String>| dir.filter(|dir| !dir.is_empty());
    set(explicit)
        .or_else(|| set(bazel_outputs))
        .map(PathBuf::from)
}

/// The evidence directory for this run, created if need be.
pub(crate) fn evidence_dir() -> PathBuf {
    let dir = chosen_evidence_dir(
        std::env::var("KRABKA_SOAK_EVIDENCE_DIR").ok(),
        std::env::var("TEST_UNDECLARED_OUTPUTS_DIR").ok(),
    )
    .unwrap_or_else(|| {
        // Kept, not removed on drop: the evidence is the point of the run.
        tempfile::Builder::new()
            .prefix("soak-contract-")
            .tempdir()
            .expect("create an evidence directory")
            .keep()
    });
    std::fs::create_dir_all(&dir)
        .unwrap_or_else(|error| panic!("create {}: {error}", dir.display()));
    dir
}

/// Write `value` as pretty JSON to `dir/name`.
pub(crate) fn write_json(dir: &Path, name: &str, value: &impl Serialize) {
    let text = serde_json::to_string_pretty(value).expect("evidence serializes");
    std::fs::write(dir.join(name), text).unwrap_or_else(|error| panic!("write {name}: {error}"));
}

/// `faults.jsonl`: one line when a fault is injected and one when it is
/// cleared, appended as it happens, so a run that dies mid-way still leaves
/// its fault timeline behind.
pub(crate) struct FaultLog {
    file: Mutex<File>,
}

impl FaultLog {
    pub(crate) fn create(dir: &Path) -> Self {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(dir.join("faults.jsonl"))
            .expect("open faults.jsonl");
        Self {
            file: Mutex::new(file),
        }
    }

    pub(crate) fn append(&self, record: &impl Serialize) {
        let Ok(line) = serde_json::to_string(record) else {
            return;
        };
        if let Ok(mut file) = self.file.lock() {
            // Best effort: a full disk must not turn a fault into a panic
            // that leaves the fault in place.
            let _ = writeln!(file, "{line}");
            let _ = file.flush();
        }
    }
}

/// `MemTotal` in bytes, from a `/proc/meminfo` body.
pub(crate) fn mem_total_bytes(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let kibibytes: u64 = fields.next()?.parse().ok()?;
    (fields.next()? == "kB").then_some(kibibytes * 1024)
}

/// The available bytes `df -B1 --output=avail` printed: a header, then the
/// number.
pub(crate) fn df_available_bytes(output: &str) -> Option<u64> {
    output.lines().nth(1)?.trim().parse().ok()
}

/// Bytes free to an unprivileged writer on the file system holding `path`.
///
/// Read through `df`, which every runner has, because the broker's
/// dependencies expose no `statvfs`.
pub(crate) fn free_bytes(path: &Path) -> Option<u64> {
    let output = Command::new("df")
        .args(["-B1", "--output=avail"])
        .arg(path)
        .output()
        .ok()?;
    df_available_bytes(&String::from_utf8_lossy(&output.stdout))
}

/// The broker image and how the daemon identifies it: its id, and its
/// registry digests where it has any.
pub(crate) fn image_identity(image: &str) -> String {
    match docker_try(&[
        "image",
        "inspect",
        "--format",
        "{{.Id}} {{join .RepoDigests \",\"}}",
        image,
    ]) {
        Ok(identity) => format!("{image} ({identity})"),
        Err(error) => format!("{image} (not inspectable: {error})"),
    }
}

/// What the run measured its host as.
pub(crate) fn environment(data_dir: &Path, image: &str) -> BTreeMap<String, String> {
    let mut environment = BTreeMap::new();
    let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    environment.insert("cpus".to_owned(), cpus.to_string());
    if let Some(memory) = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .as_deref()
        .and_then(mem_total_bytes)
    {
        environment.insert("memory_bytes".to_owned(), memory.to_string());
    }
    if let Some(free) = free_bytes(data_dir) {
        environment.insert("data_dir_free_bytes".to_owned(), free.to_string());
    }
    environment.insert("data_dir".to_owned(), data_dir.display().to_string());
    environment.insert(
        "docker_version".to_owned(),
        docker_try(&["version", "--format", "{{.Server.Version}}"]).unwrap_or_else(|error| error),
    );
    environment.insert("broker_image".to_owned(), image_identity(image));
    environment
}

#[cfg(test)]
mod tests {
    use {
        super::{chosen_evidence_dir, df_available_bytes, mem_total_bytes},
        assert2::check,
        std::path::PathBuf,
    };

    #[yare::parameterized(
        explicit_wins = { Some("/e"), Some("/b"), Some("/e") },
        bazel_outputs = { None, Some("/b"), Some("/b") },
        neither = { None, None, None },
        empty_is_unset = { Some(""), None, None },
        empty_does_not_hide_bazel_outputs = { Some(""), Some("/b"), Some("/b") },
        empty_bazel_outputs_is_unset = { None, Some(""), None },
    )]
    fn the_evidence_goes_where_the_runner_asks(
        explicit: Option<&str>,
        bazel: Option<&str>,
        chosen: Option<&str>,
    ) {
        check!(
            chosen_evidence_dir(explicit.map(str::to_owned), bazel.map(str::to_owned))
                == chosen.map(PathBuf::from)
        );
    }

    #[yare::parameterized(
        kibibytes = { "MemTotal:       16384 kB\nMemFree: 1 kB\n", Some(16_384 * 1024) },
        absent = { "MemFree: 1 kB\n", None },
        other_unit = { "MemTotal: 16 MB\n", None },
    )]
    fn memory_is_read_in_kibibytes(meminfo: &str, bytes: Option<u64>) {
        check!(mem_total_bytes(meminfo) == bytes);
    }

    #[yare::parameterized(
        plain = { "   Avail\n25769803776\n", Some(25_769_803_776) },
        header_only = { "Avail\n", None },
        garbage = { "Avail\nlots\n", None },
    )]
    fn free_space_is_the_number_under_the_header(output: &str, bytes: Option<u64>) {
        check!(df_available_bytes(output) == bytes);
    }
}
