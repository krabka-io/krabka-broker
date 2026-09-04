//! One `apache/kafka:4.3.1` broker in a container, and the same admin tool run
//! against it and against krabka.
//!
//! This is the harness `tests/unavailable_partitions_jvm.rs` writes inline,
//! lifted so more than one suite can use it. The shape it establishes is the
//! whole point of the differential: the *same* release's tool, from the *same*
//! image, is aimed first at a stock broker and then at krabka, and the two
//! answers are parsed and compared. A hand-written expectation can be wrong
//! about Kafka. An oracle cannot: if the rule a case states is wrong, the
//! oracle half fails first and krabka is never blamed for it.
//!
//! # Why the two sides are addressed differently
//!
//! The oracle broker listens inside its own container, so its tools run there
//! too, through `docker exec`, and it publishes no port -- two suites running
//! at once cannot collide. krabka runs on the host, so its tools run in a
//! throwaway `docker run --rm` container that reaches back through
//! `--add-host=host.docker.internal:host-gateway`. [`crate::jvm_acceptance`]
//! documents that arrangement and why `--network host` is not used.
//!
//! [`Side`] is what makes a case blind to that difference: it takes a tool
//! name, its arguments and the files the tool needs, and answers with what the
//! tool printed. A case therefore states each invocation once.
//!
//! # Layout note
//!
//! The module physically lives beside the `jvm_acceptance_cli` parts and is
//! re-based into `jvm_acceptance_sasl` and `jvm_acceptance_reassign` with
//! `#[path]`, so the three binaries share one copy rather than three that can
//! drift.

// Three binaries link this module and each uses only the part of it its own
// cases need, the same arrangement as `tests/jvm_acceptance/mod.rs`.
#![allow(dead_code)]

use std::{
    io::Write as _,
    process::{Command, ExitStatus, Output, Stdio},
    time::{Duration, Instant},
};

use assert2::assert;

use crate::support;

/// The release both halves of every differential run: the oracle broker is
/// this image, and so is the client aimed at krabka.
pub(crate) const ORACLE_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.3.1";

/// Where the tools live inside [`ORACLE_IMAGE`]. They are not on `PATH`.
const BIN: &str = "/opt/kafka/bin";

/// The listener the oracle's own tools bootstrap against, from inside its
/// container.
pub(crate) const ORACLE_BOOTSTRAP: &str = "localhost:9092";

/// How long a container gets to boot, and a cluster to settle.
const READY_BUDGET: Duration = Duration::from_secs(120);

/// The pause between one poll and the next.
const POLL_GAP: Duration = Duration::from_secs(1);

/// What one CLI invocation did: how it exited, and both streams it wrote.
///
/// Half of the cases here are about a tool that fails -- a reset on a live
/// group, a delete of a group that is not there, an election that was not
/// needed -- so a run is captured rather than asserted on, and each case
/// decides what the exit status means.
#[derive(Debug, Clone)]
pub(crate) struct CliRun {
    /// Which side ran it, for the panic message when a case is disappointed.
    pub(crate) side: String,
    status: ExitStatus,
    /// Standard output on its own. The table and CSV parsers read this, so a
    /// stack trace on stderr cannot be mistaken for a data row.
    pub(crate) stdout: String,
    /// Standard error on its own.
    pub(crate) stderr: String,
}

impl CliRun {
    fn new(side: &str, tool: &str, args: &[&str], out: &Output) -> Self {
        let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
        let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
        eprintln!(
            "KRABKA[test] {side} {tool} {args:?} status={}\nstdout:\n{stdout}\nstderr:\n{stderr}",
            out.status,
        );
        Self {
            side: side.to_owned(),
            status: out.status,
            stdout,
            stderr,
        }
    }

    /// Whether the tool exited zero.
    pub(crate) fn succeeded(&self) -> bool {
        self.status.success()
    }

    /// Both streams as one text.
    ///
    /// The JVM tools split a failure across the two in ways that differ per
    /// tool, so a case about a refusal searches both.
    pub(crate) fn text(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }

    /// Fail the case unless the tool exited zero.
    pub(crate) fn expect_success(self) -> Self {
        assert!(
            self.succeeded(),
            "{}: the tool was expected to succeed:\n{}",
            self.side,
            self.text(),
        );
        self
    }
}

/// A file one invocation needs, named by where the tool will look for it.
///
/// The two sides put it there differently -- krabka's tool gets a bind mount,
/// the oracle's gets a write into its container -- and a case says neither.
pub(crate) struct ToolFile {
    /// The absolute path inside the container the tool runs in.
    pub(crate) container_path: String,
    pub(crate) contents: String,
}

impl ToolFile {
    pub(crate) fn new(container_path: &str, contents: &str) -> Self {
        Self {
            container_path: container_path.to_owned(),
            contents: contents.to_owned(),
        }
    }
}

/// One half of a differential: something that runs a 4.3.1 admin tool.
pub(crate) enum Side<'a> {
    /// krabka on the host, reached from a throwaway container.
    Krabka {
        /// What the tool passes to `--bootstrap-server`.
        bootstrap: &'a str,
    },
    /// The stock broker, whose tools run inside its own container.
    Oracle(&'a Oracle),
}

impl Side<'_> {
    /// A label for the panic message, so a failure says which side failed.
    pub(crate) fn label(&self) -> &'static str {
        match self {
            Self::Krabka { .. } => "krabka",
            Self::Oracle(_) => "apache/kafka:4.3.1",
        }
    }

    /// What this side's tools pass to `--bootstrap-server`.
    pub(crate) fn bootstrap(&self) -> &str {
        match self {
            Self::Krabka { bootstrap } => bootstrap,
            Self::Oracle(_) => ORACLE_BOOTSTRAP,
        }
    }

    /// Run `<tool>.sh <args>` on this side.
    pub(crate) fn run(&self, tool: &str, args: &[&str]) -> CliRun {
        self.run_with_files(tool, args, &[], None)
    }

    /// Start a tool that is meant to keep running, and hand back a handle
    /// that removes its container when it is dropped.
    ///
    /// A live consumer is the only way to put a group in a state that is not
    /// `Empty`, which is what `--reset-offsets` and `--delete` are required to
    /// refuse. Both sides start it as a detached container so that neither
    /// case depends on a process the harness would have to reap by name: the
    /// krabka side reaches the host through the bridge gateway, and the oracle
    /// side joins the oracle's own network namespace, where `localhost:9092`
    /// means what it means to the oracle's own tools.
    pub(crate) fn run_detached(&self, tool: &str, args: &[&str]) -> DetachedTool {
        let name = support::unique_container_name("krabka-oracle-detached");
        let mut command = Command::new("docker");
        command.args(["run", "-d", "--name", &name]);
        match self {
            Self::Krabka { .. } => {
                command.arg("--add-host=host.docker.internal:host-gateway");
            }
            Self::Oracle(oracle) => {
                command.arg(format!("--network=container:{}", oracle.name));
            }
        }
        command
            .arg(ORACLE_IMAGE)
            .arg(format!("{BIN}/{tool}.sh"))
            .args(args);
        let out = command.output().expect("spawn docker run -d");
        assert!(
            out.status.success(),
            "{}: starting a detached {tool} failed: {}",
            self.label(),
            String::from_utf8_lossy(&out.stderr),
        );
        DetachedTool { name }
    }

    /// [`Self::run`] with the files the invocation names, and optionally with
    /// text on the tool's stdin.
    ///
    /// `files` are placed at their own `container_path` on whichever side is
    /// running, so the argument list that names them is identical on both.
    pub(crate) fn run_with_files(
        &self,
        tool: &str,
        args: &[&str],
        files: &[ToolFile],
        stdin: Option<&str>,
    ) -> CliRun {
        match self {
            Self::Krabka { .. } => run_against_host(self.label(), tool, args, files, stdin),
            Self::Oracle(oracle) => {
                for file in files {
                    oracle.put_file(&file.container_path, &file.contents);
                }
                oracle.run(tool, args, stdin)
            }
        }
    }
}

/// Run a 4.3.1 tool from a throwaway container against a broker on the host.
///
/// Each `files` entry is written to a host tempfile and bind-mounted at the
/// path the tool was told to read, which is what lets one argument list serve
/// both sides.
fn run_against_host(
    side: &str,
    tool: &str,
    args: &[&str],
    files: &[ToolFile],
    stdin: Option<&str>,
) -> CliRun {
    // The tempfiles must outlive the container, so they are held in this
    // vector until the run has finished.
    let staged: Vec<(tempfile::NamedTempFile, &str)> = files
        .iter()
        .map(|file| (host_tempfile(&file.contents), file.container_path.as_str()))
        .collect();
    let mounts: Vec<String> = staged
        .iter()
        .map(|(tmp, at)| format!("{}:{at}:ro", tmp.path().display()))
        .collect();

    let name = support::unique_container_name("krabka-oracle-client");
    let mut command = Command::new("docker");
    command.args(["run", "--rm", "--name", &name]);
    if stdin.is_some() {
        command.arg("-i");
    }
    for mount in &mounts {
        command.arg("-v").arg(mount);
    }
    command
        .arg("--add-host=host.docker.internal:host-gateway")
        .arg(ORACLE_IMAGE)
        .arg(format!("{BIN}/{tool}.sh"))
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let out = feed(command, stdin);
    CliRun::new(side, tool, args, &out)
}

/// Write `contents` to a tempfile the non-root container user can read.
///
/// `tempfile` creates files `0600`, which the image's `appuser` reads as
/// `Permission denied` from inside the bind mount -- a silent `IOException`
/// the tool reports as a missing file.
fn host_tempfile(contents: &str) -> tempfile::NamedTempFile {
    let tmp = tempfile::NamedTempFile::new().expect("tempfile");
    std::fs::write(tmp.path(), contents).expect("write tool file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o644))
            .expect("chmod tool file");
    }
    tmp
}

/// Run a prepared command, feeding `stdin` in when there is any.
fn feed(mut command: Command, stdin: Option<&str>) -> Output {
    match stdin {
        None => {
            command.stdin(Stdio::null());
            command.output().expect("spawn docker")
        }
        Some(text) => {
            command.stdin(Stdio::piped());
            let mut child = command.spawn().expect("spawn docker");
            child
                .stdin
                .as_mut()
                .expect("the container has a piped stdin")
                .write_all(text.as_bytes())
                .expect("write to the tool's stdin");
            drop(child.stdin.take());
            child.wait_with_output().expect("wait for docker")
        }
    }
}

/// A single-node `apache/kafka:4.3.1` broker in `KRaft` combined mode.
///
/// Dropping it removes the container, so a case that panics leaves nothing
/// behind for the next run to collide with.
pub(crate) struct Oracle {
    name: String,
}

impl Oracle {
    /// Boot the stock broker and wait until its own tools can reach it.
    pub(crate) fn start(label: &str) -> Self {
        Self::start_with_env(label, &[])
    }

    /// [`Self::start`] with extra `KAFKA_*` environment, for a case that needs
    /// the stock broker configured -- an authorizer, say.
    pub(crate) fn start_with_env(label: &str, extra_env: &[&str]) -> Self {
        let name = support::unique_container_name(&format!("krabka-oracle-{label}"));
        let mut args: Vec<String> = ["run", "-d", "--name", &name]
            .iter()
            .map(|s| (*s).to_owned())
            .collect();
        // A single-partition, single-replica coordinator topic and no initial
        // rebalance delay: a group has to form inside a case's budget, and the
        // 50-partition default at replication factor 3 cannot open on one node.
        for env in [
            "KAFKA_NODE_ID=1",
            "KAFKA_PROCESS_ROLES=broker,controller",
            "KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
            "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092",
            "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
            "KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS=1",
            "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
            "CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk",
        ]
        .iter()
        .chain(extra_env.iter())
        {
            args.push("-e".to_owned());
            args.push((*env).to_owned());
        }
        args.push(ORACLE_IMAGE.to_owned());
        let borrowed: Vec<&str> = args.iter().map(String::as_str).collect();
        let out = Command::new("docker")
            .args(&borrowed)
            .output()
            .expect("spawn docker run -d");
        assert!(
            out.status.success(),
            "starting the oracle failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );

        let oracle = Self { name };
        oracle.wait_ready();
        oracle
    }

    /// Run a command inside the container and hand back what it did.
    pub(crate) fn exec(&self, args: &[&str]) -> Output {
        let mut full: Vec<&str> = vec!["exec", &self.name];
        full.extend_from_slice(args);
        Command::new("docker")
            .args(&full)
            .output()
            .expect("spawn docker exec")
    }

    /// Run `<tool>.sh <args>` inside the container.
    pub(crate) fn run(&self, tool: &str, args: &[&str], stdin: Option<&str>) -> CliRun {
        let path = format!("{BIN}/{tool}.sh");
        let mut command = Command::new("docker");
        command.arg("exec");
        if stdin.is_some() {
            command.arg("-i");
        }
        command
            .arg(&self.name)
            .arg(&path)
            .args(args)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let out = feed(command, stdin);
        CliRun::new("apache/kafka:4.3.1", tool, args, &out)
    }

    /// Write `contents` to `path` inside the container, world-readable.
    ///
    /// `docker cp` would need a host tempfile and a second process; the shell
    /// redirect needs neither, and the image has a shell.
    ///
    /// `path` has to be somewhere the image's own user can write, which means
    /// `/tmp`: `apache/kafka` does not run as root, so a path at the container
    /// root fails with `Permission denied`. A file the container should see at
    /// a fixed path outside `/tmp` is a bind mount on `docker run` instead,
    /// which the directory's permissions do not apply to.
    pub(crate) fn put_file(&self, path: &str, contents: &str) {
        let mut command = Command::new("docker");
        command
            .args(["exec", "-i", &self.name, "sh", "-c"])
            .arg(format!("cat > {path} && chmod 0644 {path}"))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let out = feed(command, Some(contents));
        assert!(
            out.status.success(),
            "writing {path} into the oracle failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );
    }

    /// Read a file back out of the container.
    pub(crate) fn read_file(&self, path: &str) -> String {
        let out = self.exec(&["cat", path]);
        assert!(
            out.status.success(),
            "reading {path} out of the oracle failed: {}",
            String::from_utf8_lossy(&out.stderr),
        );
        String::from_utf8_lossy(&out.stdout).into_owned()
    }

    /// Poll `kafka-topics --list` until the broker answers it.
    fn wait_ready(&self) {
        let deadline = Instant::now() + READY_BUDGET;
        loop {
            let out = self.exec(&[
                &format!("{BIN}/kafka-topics.sh"),
                "--bootstrap-server",
                ORACLE_BOOTSTRAP,
                "--list",
            ]);
            if out.status.success() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{ORACLE_IMAGE} did not answer within {READY_BUDGET:?}:\n{}",
                self.logs(),
            );
            std::thread::sleep(POLL_GAP);
        }
    }

    /// The container's own logs, for the panic message when it never came up.
    fn logs(&self) -> String {
        let out = Command::new("docker")
            .args(["logs", &self.name])
            .output()
            .expect("spawn docker logs");
        format!(
            "stdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        )
    }
}

impl Drop for Oracle {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// A tool left running in its own container, removed when this is dropped.
pub(crate) struct DetachedTool {
    name: String,
}

impl Drop for DetachedTool {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .output();
    }
}
