use {assert2::assert, std::process::Command};

fn broker_bin() -> std::path::PathBuf {
    let exe = std::env::var_os("CARGO_BIN_EXE_krabka-broker")
        .expect("cargo provides CARGO_BIN_EXE_<bin> in test env");
    std::path::PathBuf::from(exe)
}

/// Formats a fresh standalone log directory.
///
/// KIP-853 needs every node formatted before `krabka-broker` boots: the step
/// seeds `meta.properties` and the singleton `VotersRecord`, and the broker
/// treats an unformatted dir as operator error and aborts startup.
///
/// Called in process rather than spawned. The formatting is setup for the boot
/// test below, not the thing under test -- `krabka-format`'s own `format_smoke`
/// suite runs the real binary -- and a subprocess would need a Cargo working
/// tree to build from, which a Bazel test sandbox does not have. This test is
/// synchronous, so it drives the async formatter on a current-thread runtime.
fn run_krabka_format(log_dir: &std::path::Path, node_id: u32, controller_listener: &str) {
    format_log_dir(log_dir, node_id, &[
        "--standalone",
        "--controller-listener",
        controller_listener,
    ]);
}

/// Formats a fresh log directory for `node_id` with the `quorum` flags of
/// `krabka-format`, as [`run_krabka_format`] does.
fn format_log_dir(log_dir: &std::path::Path, node_id: u32, quorum: &[&str]) {
    let node_id = node_id.to_string();
    let mut argv = vec![
        "krabka-format",
        "--log-dir",
        log_dir.to_str().unwrap(),
        "--node-id",
        &node_id,
    ];
    argv.extend_from_slice(quorum);
    let code = current_thread_runtime().block_on(krabka_format::run_from_args(argv));
    assert!(code == 0, "krabka-format exited {code}");
}

fn current_thread_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime")
}

#[test]
fn help_mentions_cluster_id_and_advertised_listener() {
    let out = Command::new(broker_bin()).arg("--help").output().unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let help = String::from_utf8(out.stdout).unwrap();
    assert!(
        help.contains("--cluster-id"),
        "help missing --cluster-id:\n{help}"
    );
    assert!(
        help.contains("--advertised-listener"),
        "help missing --advertised-listener:\n{help}"
    );
}

#[test]
fn version_returns_zero() {
    let out = Command::new(broker_bin())
        .arg("--version")
        .output()
        .unwrap();
    assert!(out.status.success());
}

/// Boot `krabka-broker` with `--config-file` set to a minimal TOML, and
/// assert that the process binds the listener declared in the file. The
/// address comes from the file, not from a CLI flag. The file binds port 0, so
/// no other process can take the port first, and the startup line names the
/// port the listener got. The controller listener takes a [`free_port`].
#[test]
fn boots_with_config_file_listener() {
    use std::io::Write as _;

    let tmp = tempfile::tempdir().expect("tempdir");

    let cfg_path = tmp.path().join("broker.toml");
    let mut f = std::fs::File::create(&cfg_path).unwrap();
    writeln!(
        f,
        r#"
inter_broker_listener_name = "PLAIN"

[[listeners]]
name = "PLAIN"
bind_addr = "127.0.0.1:0"
advertised = "127.0.0.1:0"
protocol = "Plaintext"
"#
    )
    .unwrap();

    let running = start_retrying("the config-file node", |attempt| {
        // KIP-853: the broker refuses to boot an unformatted log dir, so seed
        // it first. `krabka format` creates the directory itself (it must be
        // empty or non-existent), so don't pre-create it. The controller
        // listener goes into the voter set, so it needs its port up front.
        let log_dir = tmp.path().join(format!("data-{attempt}"));
        let controller = free_port();
        run_krabka_format(&log_dir, 1, &controller.to_string());
        Process::spawn(&[
            format!("--config-file={}", cfg_path.display()),
            format!("--log-dir={}", log_dir.display()),
            "--broker-id=1".to_owned(),
            format!("--controller-listen-addr={controller}"),
        ])
        .wait_until_started()
    });
    let addr = running
        .started
        .listen_addr
        .expect("the startup line names the listener");

    assert!(addr.port() != 0);
    assert!(std::net::TcpStream::connect(addr).is_ok());
}

#[test]
fn errors_when_config_file_and_listen_addr_both_set() {
    let out = Command::new(broker_bin())
        .arg("--config-file=/tmp/nonexistent.toml")
        .arg("--listen-addr=127.0.0.1:9092")
        .output()
        .expect("spawn krabka-broker");

    assert!(!out.status.success(), "expected non-zero exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("config-file") && stderr.contains("listen-addr"),
        "expected clap mutual-exclusion error, got stderr:\n{stderr}"
    );
}

/// A log directory formatted for node 1 does not start node 2. The broker
/// exits 1 with the message of Kafka's `MetaPropertiesEnsemble.verify`, as
/// `KafkaRaftServer.initializeLogDirs` refuses the same directory.
#[test]
fn refuses_a_log_dir_formatted_for_another_node() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let log_dir = tmp.path().join("data");
    run_krabka_format(&log_dir, 1, "127.0.0.1:9093");

    let mut cmd = Command::new(broker_bin());
    cmd.args([
        format!("--log-dir={}", log_dir.display()),
        "--broker-id=2".into(),
        "--listen-addr=127.0.0.1:0".into(),
        "--controller-listen-addr=127.0.0.1:0".into(),
        "--metrics-listen-addr=none".into(),
    ]);
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::piped());
    let mut child = cmd.spawn().expect("spawn krabka-broker");
    let timeout = std::time::Duration::from_secs(30);
    let deadline = std::time::Instant::now() + timeout;
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll krabka-broker") {
            break Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            break None;
        }
        // intentional: waiting on a spawned krabka-broker subprocess to exit;
        // it has no in-process handle to await.
        std::thread::sleep(std::time::Duration::from_millis(100));
    };
    let output = child.wait_with_output().expect("collect stderr");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        status.and_then(|status| status.code()) == Some(1),
        "{stderr}"
    );
    let want = format!(
        "startup failed: Stored node id 1 doesn't match previous node id 2 in {}. If you moved \
         your data, make sure your configured node id matches. If you intend to create a new \
         node, you should remove all data in your data directories.",
        log_dir.join("meta.properties").display()
    );
    assert!(stderr.contains(&want), "{stderr}");
}

/// The cluster id of the nodes that [`each_role_opens_only_its_own_listeners`]
/// starts.
const CLUSTER_ID: &str = "I2eXt9rvSnyhct8BYmW6-w";

/// How long a node has to log its startup line. A broker-only node waits for
/// its first unfence, which a few heartbeats bring.
const STARTUP_DEADLINE: std::time::Duration = std::time::Duration::from_secs(90);

/// The line that a node logs when its start is complete.
const STARTED: &str = "krabka-broker started";

/// A loopback port that nothing listens on and nothing else can bind while
/// the reservation lives: a socket bound to it that never listens. A
/// connection to the port is refused, as to a port nobody holds, and a node
/// that tried to open it would fail to bind it.
struct Reserved {
    socket: socket2::Socket,
    addr: std::net::SocketAddr,
}

fn reserve_port() -> Reserved {
    let socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::STREAM, None)
        .expect("create a socket");
    let any: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
    socket
        .bind(&any.into())
        .expect("bind an ephemeral loopback port");
    let addr = socket
        .local_addr()
        .ok()
        .and_then(|addr| addr.as_socket())
        .expect("a bound IPv4 socket has an address");
    Reserved { socket, addr }
}

/// A free loopback port for a listener that must be known before the node
/// starts, such as a controller listener that `krabka-format` writes into the
/// voter set. The reservation is released when the node binds the port, so
/// another process can take it first; [`start_node`] retries a node that
/// fails to bind.
fn free_port() -> std::net::SocketAddr {
    let Reserved { socket, addr } = reserve_port();
    drop(socket);
    addr
}

/// How many times [`start_node`] tries a node whose listener another process
/// took between [`free_port`] and the node's bind.
const START_ATTEMPTS: usize = 5;

/// The message a node exits with when its listener's port is taken.
const ADDR_IN_USE: &str = "Address already in use";

/// Run `start`, which formats and starts a node on fresh ports in a fresh log
/// directory for each attempt it is given, until the node starts. A node that
/// exits with [`ADDR_IN_USE`] is tried again, up to [`START_ATTEMPTS`] times:
/// another process took a [`free_port`] before the node bound it. Any other
/// exit fails the test.
fn start_retrying<T>(node: &str, mut start: impl FnMut(usize) -> Result<T, Exited>) -> T {
    let mut last = None;
    for attempt in 0..START_ATTEMPTS {
        match start(attempt) {
            Ok(started) => return started,
            Err(exit) if exit.stderr.contains(ADDR_IN_USE) => last = Some(exit),
            Err(exit) => panic!("{node}: {exit}"),
        }
    }
    panic!(
        "{node} could not bind its listeners in {START_ATTEMPTS} attempts: another process took \
         a free port each time, or the node opened a port it must leave closed; last: {}",
        last.expect("at least one attempt ran")
    );
}

/// A running `krabka-broker` process. Dropping it kills the process, so a
/// failed assertion leaves none behind.
struct Process {
    child: std::process::Child,
    /// The stdout lines of the process, one JSON object each.
    lines: std::sync::mpsc::Receiver<String>,
    /// The stderr of the process, once it closes.
    stderr: Option<std::thread::JoinHandle<String>>,
}

/// A process that exited before it logged its startup line.
#[derive(Debug)]
struct Exited {
    /// Its stdout lines.
    logged: Vec<String>,
    stderr: String,
}

impl std::fmt::Display for Exited {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "the process exited before `{STARTED}`; stderr:\n{}\nstdout:\n{}",
            self.stderr,
            self.logged.join("\n")
        )
    }
}

impl Process {
    fn spawn(args: &[String]) -> Self {
        let mut child = Command::new(broker_bin())
            .args(args)
            .arg("--metrics-listen-addr=none")
            .arg("--health-listen-addr=none")
            // The startup oracle consumes an INFO event, so request that level
            // explicitly for each child, independent of the test runner's filter.
            .env("RUST_LOG", krabka_broker::config::DEFAULT_LOG_FILTER)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("spawn krabka-broker");
        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let (sender, lines) = std::sync::mpsc::channel();
        // The readers drain both pipes for the whole life of the process, so
        // the broker never blocks on a full pipe.
        std::thread::spawn(move || {
            use std::io::BufRead as _;
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if sender.send(line).is_err() {
                    return;
                }
            }
        });
        let stderr = std::thread::spawn(move || {
            use std::io::Read as _;
            let mut text = String::new();
            let _ = std::io::BufReader::new(stderr).read_to_string(&mut text);
            // Pass it on, as an inherited stderr would.
            eprint!("{text}");
            text
        });
        Self {
            child,
            lines,
            stderr: Some(stderr),
        }
    }

    /// The fields of the startup line, or what the process printed when it
    /// exited without it. Fails the test when the process neither logs the
    /// line nor exits within [`STARTUP_DEADLINE`].
    fn wait_until_started(mut self) -> Result<Running, Exited> {
        let deadline = std::time::Instant::now() + STARTUP_DEADLINE;
        let mut logged = Vec::new();
        loop {
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            let line = match self.lines.recv_timeout(left) {
                Ok(line) => line,
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
                    let _ = self.child.wait();
                    let stderr = self
                        .stderr
                        .take()
                        .and_then(|reader| reader.join().ok())
                        .unwrap_or_default();
                    return Err(Exited { logged, stderr });
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => panic!(
                    "no `{STARTED}` line within {STARTUP_DEADLINE:?}; the process logged:\n{}",
                    logged.join("\n")
                ),
            };
            if let Ok(entry) = serde_json::from_str::<serde_json::Value>(&line)
                && entry.get("message").and_then(serde_json::Value::as_str) == Some(STARTED)
            {
                let field = |name: &str| {
                    entry
                        .get(name)
                        .and_then(serde_json::Value::as_str)
                        .map(|addr| {
                            addr.parse()
                                .expect("the startup line names a socket address")
                        })
                };
                let started = Started {
                    listen_addr: field("listen_addr"),
                    controller_listen_addr: field("controller_listen_addr"),
                };
                return Ok(Running {
                    _process: self,
                    started,
                });
            }
            logged.push(line);
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A process that logged its startup line, and the fields of that line.
struct Running {
    /// Kept so the process runs until this is dropped.
    _process: Process,
    started: Started,
}

/// The listener addresses that the startup line names.
#[derive(Debug, PartialEq, Eq)]
struct Started {
    listen_addr: Option<std::net::SocketAddr>,
    controller_listen_addr: Option<std::net::SocketAddr>,
}

/// What a port answers.
#[derive(Debug, PartialEq, Eq)]
enum Listener {
    /// The connection is refused: nothing listens on the port.
    Closed,
    /// `ApiVersions` advertises `Produce`: a broker listener.
    Broker,
    /// `ApiVersions` advertises no `Produce`: a controller listener, as Kafka
    /// tags `Produce` for the broker listeners only.
    Controller,
}

fn probe(runtime: &tokio::runtime::Runtime, addr: std::net::SocketAddr) -> Listener {
    match std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(5)) {
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
            return Listener::Closed;
        }
        Err(error) => panic!("connect to {addr}: {error}"),
        Ok(stream) => drop(stream),
    }
    let connection = runtime
        .block_on(krabka_client_core::Connection::connect(
            addr,
            krabka_client_core::ConnectionOptions {
                client_id: "cli-smoke".to_owned(),
                ..krabka_client_core::ConnectionOptions::default()
            },
        ))
        .unwrap_or_else(|error| panic!("ApiVersions on {addr}: {error}"));
    let produce = connection.advertised_api_range(krabka_protocol::owned::produce_request::API_KEY);
    connection.close();
    if produce.is_some() {
        Listener::Broker
    } else {
        Listener::Controller
    }
}

/// What one node logs and what its two ports answer once it has started.
#[derive(Debug, PartialEq, Eq)]
struct Observed {
    roles: &'static str,
    /// Whether the startup line names a client listener.
    names_client: bool,
    /// The controller listener that the startup line names.
    controller_listen_addr: Option<std::net::SocketAddr>,
    client_port: Listener,
    controller_port: Listener,
}

/// One node of [`each_role_opens_only_its_own_listeners`].
struct Case {
    /// `--process-roles`.
    roles: &'static str,
    node_id: u32,
    /// Whether the node is an observer of the quorum of the node before it,
    /// rather than the standalone voter of its own quorum.
    joins_quorum: bool,
    /// Whether the startup line names the client listener and the controller
    /// listener.
    names: NamedListeners,
    /// What the client port and the controller port answer.
    ports: PortAnswers,
}

/// Which listeners a node's startup line names, which are the listeners it
/// opens.
struct NamedListeners {
    client: bool,
    controller: bool,
}

/// What a node's client port and controller port answer.
struct PortAnswers {
    client: Listener,
    controller: Listener,
}

/// A node that [`start_node`] started, and the ports it was given.
struct Node {
    running: Running,
    /// The client port to probe: the one the node bound, or the reserved
    /// port of a client listener the node must not open.
    client: std::net::SocketAddr,
    controller: std::net::SocketAddr,
    /// The ports the node must leave closed, held until the node is dropped.
    _reserved: Vec<Reserved>,
}

/// Format and start the node of `case`. `quorum` is the controller listener
/// of the quorum that a joining node observes.
///
/// A listener the node opens binds port 0 where nothing needs its address in
/// advance, which is the client listener. The controller listener of a voter
/// goes into the formatted voter set, so it takes a [`free_port`], under
/// [`start_retrying`]. A listener the node must not open gets a [`Reserved`]
/// port.
fn start_node(dir: &std::path::Path, case: &Case, quorum: Option<std::net::SocketAddr>) -> Node {
    start_retrying(&format!("{} ({})", case.roles, case.node_id), |attempt| {
        let mut reserved = Vec::new();
        let mut closed_port = || {
            let port = reserve_port();
            let addr = port.addr;
            reserved.push(port);
            addr
        };
        let client = if case.names.client {
            "127.0.0.1:0".parse().unwrap()
        } else {
            closed_port()
        };
        let controller = if case.names.controller {
            free_port()
        } else {
            closed_port()
        };
        let log_dir = dir.join(format!("node-{}-{attempt}", case.node_id));
        let controller_text = controller.to_string();
        let mut quorum_flags = vec!["--cluster-id", CLUSTER_ID];
        if !case.joins_quorum {
            quorum_flags.extend(["--standalone", "--controller-listener", &controller_text]);
        }
        format_log_dir(&log_dir, case.node_id, &quorum_flags);
        let mut args = vec![
            format!("--log-dir={}", log_dir.display()),
            format!("--broker-id={}", case.node_id),
            format!("--process-roles={}", case.roles),
            format!("--listen-addr={client}"),
            format!("--controller-listen-addr={controller}"),
        ];
        if case.joins_quorum {
            let quorum = quorum.expect("a joining node has a quorum to join");
            args.push(format!("--controller-quorum-voters=1@{quorum}"));
        }
        let running = Process::spawn(&args).wait_until_started()?;
        let client = running.started.listen_addr.unwrap_or(client);
        Ok(Node {
            running,
            client,
            controller,
            _reserved: reserved,
        })
    })
}

/// Every `process.roles` value opens the listeners of its roles and no other,
/// as Kafka does. Kafka's `ControllerServer` opens only the listeners that
/// `controller.listener.names` names, and `KafkaConfig` refuses a
/// controller-only node whose `listeners` name another one. So a client cannot
/// reach a controller-only node on the client port, and two controller-only
/// nodes on one host do not compete for that port.
///
/// Each node gets a `--listen-addr` and a `--controller-listen-addr` of its
/// own. The startup line names the listeners that the node opened. A port the
/// node must leave closed is held by the test, so nothing else answers on it.
///
/// The controller-only node is the quorum of the broker-only node, which
/// cannot start without one, so the cases run in order and every node runs
/// until the end. The combined node is a standalone quorum of its own.
#[test]
fn each_role_opens_only_its_own_listeners() {
    let runtime = current_thread_runtime();
    let tmp = tempfile::tempdir().expect("tempdir");
    let cases = [
        Case {
            roles: "controller",
            node_id: 1,
            joins_quorum: false,
            names: NamedListeners {
                client: false,
                controller: true,
            },
            ports: PortAnswers {
                client: Listener::Closed,
                controller: Listener::Controller,
            },
        },
        Case {
            roles: "broker",
            node_id: 2,
            joins_quorum: true,
            names: NamedListeners {
                client: true,
                controller: false,
            },
            ports: PortAnswers {
                client: Listener::Broker,
                controller: Listener::Closed,
            },
        },
        Case {
            roles: "broker,controller",
            node_id: 3,
            joins_quorum: false,
            names: NamedListeners {
                client: true,
                controller: true,
            },
            ports: PortAnswers {
                client: Listener::Broker,
                controller: Listener::Controller,
            },
        },
    ];

    let mut running = Vec::new();
    let mut observed = Vec::new();
    let mut expected = Vec::new();
    let mut quorum = None;
    for case in cases {
        let node = start_node(tmp.path(), &case, quorum);
        quorum = quorum.or(Some(node.controller));
        observed.push(Observed {
            roles: case.roles,
            names_client: node.running.started.listen_addr.is_some(),
            controller_listen_addr: node.running.started.controller_listen_addr,
            client_port: probe(&runtime, node.client),
            controller_port: probe(&runtime, node.controller),
        });
        expected.push(Observed {
            roles: case.roles,
            names_client: case.names.client,
            controller_listen_addr: case.names.controller.then_some(node.controller),
            client_port: case.ports.client,
            controller_port: case.ports.controller,
        });
        running.push(node);
    }
    drop(running);

    assert!(observed == expected);
}
