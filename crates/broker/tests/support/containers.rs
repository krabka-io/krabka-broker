//! Addressing and fixture paths for the suites that drive Kafka containers.
//!
//! A suite that runs a real Kafka container needs three things from here: ports
//! that a concurrent run has not already taken, a container name that a
//! concurrent run has not already taken, and the directory its fixtures were
//! staged in, which is not the same under Cargo and under Bazel.

/// A free TCP port on the loopback interface.
///
/// Bound and immediately dropped, so the port is free when the caller binds it.
/// That leaves a window in which something else could take it; the alternative
/// is a fixed port, which is not a window but a certainty whenever two tests run
/// at once.
#[must_use]
pub fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

/// Runs a Docker fixture command and returns its trimmed stdout.
pub fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker")
        .args(args)
        .output()
        .unwrap_or_else(|e| panic!("spawn docker {args:?}: {e}"));
    assert2::assert!(
        out.status.success(),
        "docker {args:?} exited {:?}\nstdout: {}\nstderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).trim().to_owned()
}

/// Resolve Docker's `Gateway|Subnet` pair, including daemons that omit the gateway.
/// Docker assigns the bridge the subnet's first address in that case.
pub fn bridge_gateway(rendered: &str) -> Option<String> {
    let (gateway, subnet) = rendered.trim().split_once('|')?;
    if gateway.parse::<std::net::IpAddr>().is_ok() {
        return Some(gateway.to_owned());
    }
    let (base, _prefix) = subnet.split_once('/')?;
    let base: std::net::Ipv4Addr = base.parse().ok()?;
    let first = u32::from(base).checked_add(1)?;
    Some(std::net::Ipv4Addr::from(first).to_string())
}

/// Capture a Docker tool's output after delivering its optional complete stdin.
pub fn docker_output(
    command: &mut std::process::Command,
    stdin: Option<&str>,
    write_context: &str,
) -> std::process::Output {
    use std::{io::Write as _, process::Stdio};

    match stdin {
        None => command
            .stdin(Stdio::null())
            .output()
            .expect("spawn docker run"),
        Some(text) => {
            let mut child = command
                .stdin(Stdio::piped())
                .spawn()
                .expect("spawn docker run");
            child
                .stdin
                .as_mut()
                .expect("the container has a piped stdin")
                .write_all(text.as_bytes())
                .expect(write_context);
            drop(child.stdin.take());
            child.wait_with_output().expect("wait for docker run")
        }
    }
}

/// Listen and advertised addresses for a broker the JVM containers talk to.
///
/// The suites that drive real Kafka containers used to hard-code `9092`/`9093`,
/// which is why they had to run one at a time: two of them, or two tests inside
/// one of them, would race for the same port and the loser reported `Address
/// already in use` as a test failure. Each caller gets its own pair now.
///
/// `advertised` keeps the `host.docker.internal` name. Containers resolve it
/// through `--add-host=host.docker.internal:host-gateway`; the host resolves it
/// through an `/etc/hosts` entry pointing at loopback, which CI adds before
/// running these suites.
pub struct JvmListeners {
    /// What the broker binds, e.g. `0.0.0.0:41551`.
    pub listen: String,
    /// What it advertises and what the containers bootstrap against.
    pub advertised: String,
    /// The controller listener, on its own port.
    pub controller: String,
}

impl JvmListeners {
    /// Allocate a fresh set.
    #[must_use]
    pub fn allocate() -> Self {
        let client = free_port();
        let controller = free_port();
        Self {
            listen: format!("0.0.0.0:{client}"),
            advertised: format!("host.docker.internal:{client}"),
            controller: format!("0.0.0.0:{controller}"),
        }
    }

    /// The controller as containers address it.
    #[must_use]
    pub fn controller_advertised(&self) -> String {
        let port = self
            .controller
            .rsplit(':')
            .next()
            .expect("controller addr has a port");
        format!("host.docker.internal:{port}")
    }
}

/// Listeners shared by the tests in a single JVM test binary.
pub fn jvm_listeners() -> &'static JvmListeners {
    static LISTENERS: std::sync::OnceLock<JvmListeners> = std::sync::OnceLock::new();
    LISTENERS.get_or_init(JvmListeners::allocate)
}

/// Host-loopback address for native clients in a JVM test binary.
pub fn jvm_client_addr() -> &'static str {
    static ADDRESS: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    ADDRESS.get_or_init(|| jvm_listeners().listen.replace("0.0.0.0", "127.0.0.1"))
}

/// Read the finalized level from the named feature's own CLI output line.
pub fn jvm_finalized_level(stdout: &str, feature: &str) -> Option<i64> {
    let line = stdout
        .lines()
        .find(|line| line.contains(&format!("Feature: {feature}")))?;
    let (_, level) = line.split_once("FinalizedVersionLevel:")?;
    level.split_whitespace().next()?.parse().ok()
}

/// Boot a single host broker using this test binary's listeners.
pub async fn start_jvm_single(
    default_filter: &str,
    adjust: impl FnOnce(&mut krabka_broker::BrokerConfig),
) -> (krabka_broker::BrokerHandle, tempfile::TempDir) {
    init_jvm_tracing(default_filter);
    let listeners = jvm_listeners();
    let dir = tempfile::tempdir().expect("broker directory");
    let mut config = jvm_single_broker_config(
        dir.path().to_path_buf(),
        &listeners.listen,
        &listeners.advertised,
        &listeners.controller,
    );
    adjust(&mut config);
    let broker = krabka_broker::Broker::start(config)
        .await
        .expect("start broker");
    eprintln!(
        "KRABKA[test] broker started listen={} advertised={}",
        listeners.listen, listeners.advertised
    );
    (broker, dir)
}

/// Initialize tracing with the suite's default filter.
pub fn init_jvm_tracing(default_filter: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_filter)),
        )
        .with_test_writer()
        .try_init();
}

/// Common configuration for host brokers addressed by Kafka containers.
pub fn jvm_broker_config(
    id: u64,
    listen: std::net::SocketAddr,
    controller: std::net::SocketAddr,
    advertised: &str,
    log_dir: std::path::PathBuf,
    voters: &[(u64, std::net::SocketAddr)],
) -> krabka_broker::BrokerConfig {
    krabka_broker::BrokerConfig {
        broker_id: i32::try_from(id).expect("broker id"),
        listen_addr: listen,
        advertised_listener: advertised.into(),
        log_dir,
        log_config: krabka_log::LogConfig::default(),
        node_id: krabka_broker::NodeId(id),
        controller_listen_addr: controller,
        controller_quorum_voters: voters
            .iter()
            .map(|(id, addr)| (krabka_broker::NodeId(*id), addr.to_string()))
            .collect(),
        heartbeat_interval: krabka_units::millis(3_000),
        heartbeat_timeout: krabka_units::millis(9_000),
        replica_lag_time_max: krabka_units::millis(30_000),
        controller_election_timeout: krabka_units::secs(5),
        controller_heartbeat_interval: krabka_units::millis(500),
        bootstrap_mode: krabka_broker::BootstrapMode::Bootstrap,
        ..krabka_broker::BrokerConfig::default().with_internal_topics_for(voters.len())
    }
}

/// Common single-voter configuration, with listener strings supplied by a suite.
pub fn jvm_single_broker_config(
    log_dir: std::path::PathBuf,
    listen: &str,
    advertised: &str,
    controller: &str,
) -> krabka_broker::BrokerConfig {
    let controller = controller.parse().expect("controller address");
    jvm_broker_config(
        1,
        listen.parse().expect("client address"),
        controller,
        advertised,
        log_dir,
        &[(1, controller)],
    )
}

/// Prepare a disposable tool container that can reach the host broker.
pub fn jvm_docker_command(
    image: &str,
    mounts: &[&str],
    args: &[&str],
    interactive: bool,
) -> std::process::Command {
    use std::process::{Command, Stdio};

    let mut command = Command::new("docker");
    command.args([
        "run",
        "--rm",
        "--add-host=host.docker.internal:host-gateway",
    ]);
    if interactive {
        command.arg("-i");
    }
    for mount in mounts {
        command.args(["-v", mount]);
    }
    command
        .arg(image)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Run a JVM tool while leaving success and output assertions to its caller.
pub fn jvm_docker_run(image: &str, args: &[&str]) -> std::process::Output {
    let out = jvm_docker_command(image, &[], args, false)
        .output()
        .expect("docker run");
    eprintln!(
        "KRABKA[test] docker {image} {args:?} status={} stderr={}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    out
}

/// Write a console tool's complete input and capture its output.
pub fn jvm_stdin_output(
    command: &mut std::process::Command,
    payload: &[u8],
) -> std::process::Output {
    use std::{io::Write, process::Stdio};

    let mut child = command
        .stdin(Stdio::piped())
        .spawn()
        .expect("spawn JVM tool");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(payload)
        .expect("write stdin");
    drop(child.stdin.take());
    child.wait_with_output().expect("wait JVM tool")
}

/// Start all static voters concurrently on the suite's fixed port pairs.
pub async fn start_jvm_cluster<const N: usize>(
    client_ports: [u16; N],
    controller_ports: [u16; N],
    adjust: impl Fn(&mut krabka_broker::BrokerConfig),
) -> Vec<(krabka_broker::BrokerHandle, tempfile::TempDir)> {
    let voters: Vec<_> = controller_ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            (
                u64::try_from(index + 1).expect("node id"),
                format!("127.0.0.1:{port}")
                    .parse()
                    .expect("controller address"),
            )
        })
        .collect();
    let starts: Vec<_> = client_ports
        .iter()
        .enumerate()
        .map(|(index, port)| {
            let dir = tempfile::tempdir().expect("broker directory");
            let mut config = jvm_broker_config(
                voters[index].0,
                format!("0.0.0.0:{port}").parse().expect("client address"),
                format!("0.0.0.0:{}", controller_ports[index])
                    .parse()
                    .expect("controller address"),
                &format!("host.docker.internal:{port}"),
                dir.path().to_path_buf(),
                &voters,
            );
            adjust(&mut config);
            (
                tokio::spawn(async move {
                    krabka_broker::Broker::start(config)
                        .await
                        .expect("broker start")
                }),
                dir,
            )
        })
        .collect();
    let mut brokers = Vec::with_capacity(N);
    for (start, dir) in starts {
        brokers.push((start.await.expect("broker start task"), dir));
    }
    brokers
}

/// A container name unlikely to collide with a concurrent run.
///
/// `docker run --name` fails outright when the name is taken, so a fixed name is
/// a second reason these suites could not overlap -- and a stale container from
/// a killed run blocks every later run until someone removes it by hand.
#[must_use]
pub fn unique_container_name(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    )
}

/// This crate's directory, wherever the test is running from.
///
/// Cargo exports `CARGO_MANIFEST_DIR` to a test process, so under Cargo this is
/// the path `env!` would have produced. It is read rather than expanded because
/// `env!` bakes an absolute build path into the binary, which ties the test to
/// the directory it was compiled in -- `rules_rust` rejects such a binary
/// outright, and under Cargo it only works when launched from that same path.
///
/// Bazel sets no such variable; it stages a target's `data` under
/// `$TEST_SRCDIR/$TEST_WORKSPACE/<package>`. Falling back to that is what lets
/// the TLS suites find their fixtures under both.
///
/// # Panics
///
/// Panics when neither Cargo's variable nor Bazel's pair is set, which means the
/// test was launched by something that stages fixtures differently again.
#[must_use]
pub fn manifest_dir() -> std::path::PathBuf {
    if let Ok(dir) = std::env::var("CARGO_MANIFEST_DIR") {
        return std::path::PathBuf::from(dir);
    }
    let srcdir = std::env::var("TEST_SRCDIR")
        .expect("CARGO_MANIFEST_DIR (cargo) or TEST_SRCDIR (bazel) must be set");
    let workspace =
        std::env::var("TEST_WORKSPACE").expect("TEST_WORKSPACE accompanies TEST_SRCDIR");
    std::path::PathBuf::from(srcdir)
        .join(workspace)
        .join("crates/broker")
}

/// A cache directory under the system temp dir whose name carries a digest of
/// the named `tests/fixtures/security/` files.
///
/// The JVM suites stage keystores there and reuse them across runs. Keying the
/// directory on the fixture bytes means a rotated certificate gets a fresh
/// directory instead of the stale artifact built from the old one.
///
/// # Panics
///
/// Panics when a fixture cannot be read.
#[must_use]
pub fn fixture_cache_dir(prefix: &str, fixtures: &[&str]) -> std::path::PathBuf {
    use std::hash::{Hash, Hasher};

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for name in fixtures {
        let path = manifest_dir().join("tests/fixtures/security").join(name);
        std::fs::read(&path)
            .unwrap_or_else(|error| panic!("read fixture {}: {error}", path.display()))
            .hash(&mut hasher);
    }
    std::env::temp_dir().join(format!("{prefix}-{:016x}", hasher.finish()))
}
