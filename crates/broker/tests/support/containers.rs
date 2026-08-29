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
