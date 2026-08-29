//! `MinIO` container lifecycle and bucket setup for the tiered-storage suites.
//!
//! The container is owned by a guard that removes it on drop, so an aborted
//! test does not leave one squatting on the published port.

use std::process::{Command, Stdio};

use assert2::assert;

use super::ports::minio_port;

// ---------------------------------------------------------------------------
// MinIO-backed tiered-storage acceptance test (KIP-405 S3 backend).
//
// Spins up a real `mirror.gcr.io/minio/minio` container, points the broker at it via the
// S3-compatible `S3RemoteStorage` backend, then drives a JVM producer +
// consumer against a topic with `remote.storage.enable=true` and aggressive
// `segment.bytes` / `local.retention.bytes` overrides. We assert both that
// segment objects materialise in the MinIO bucket and that the JVM consumer
// reads back every record — including offsets whose local segments have
// already been evicted by `local_retention_pass`, forcing the read to come
// from the remote tier through `RemoteReader`.
// ---------------------------------------------------------------------------

pub(crate) const MINIO_IMAGE: &str = "mirror.gcr.io/minio/minio:RELEASE.2025-09-07T16-13-09Z";

pub(crate) const MINIO_CLIENT_IMAGE: &str = "mirror.gcr.io/minio/mc:RELEASE.2025-08-13T08-35-41Z";

pub(crate) const MINIO_ACCESS_KEY: &str = "minioadmin";

pub(crate) const MINIO_SECRET_KEY: &str = "minioadmin";

pub(crate) const MINIO_BUCKET: &str = "krabka-tiered";

/// Owns a `docker run -d` `MinIO` container and tears it down on drop.
pub(crate) struct MinioContainer {
    name: String,
}

impl MinioContainer {
    pub(crate) fn start() -> Self {
        // Unique name per test invocation so back-to-back runs don't see a
        // stale container squatting on port 9000.
        let minio_port = minio_port();
        let name = format!("krabka-minio-test-{}", uuid::Uuid::new_v4().simple());
        // Best-effort orphan reap from a prior aborted run.
        let _ = Command::new("docker")
            .args(["rm", "-f", &name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        let status = Command::new("docker")
            .args([
                "run",
                "-d",
                "--rm",
                "--name",
                &name,
                "-p",
                &format!("{minio_port}:9000"),
                "-e",
                &format!("MINIO_ROOT_USER={MINIO_ACCESS_KEY}"),
                "-e",
                &format!("MINIO_ROOT_PASSWORD={MINIO_SECRET_KEY}"),
                MINIO_IMAGE,
                "server",
                "/data",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .status()
            .expect("spawn docker run minio");
        assert!(status.success(), "docker run minio failed");
        wait_for_minio_ready();
        Self { name }
    }
}

/// Poll the published host port until `MinIO`'s HTTP listener answers. This
/// avoids a race with the first health check of the fast-starting image.
pub(crate) fn wait_for_minio_ready() {
    let minio_port = minio_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{minio_port}")
        .parse()
        .expect("static addr");
    for _ in 0..60 {
        if std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_millis(500))
            .is_ok()
        {
            // TCP accept != fully-initialised S3 server; give the
            // listenbuckets path a moment to come up.
            std::thread::sleep(std::time::Duration::from_millis(500));
            return;
        }
        // intentional: bounded readiness poll of the external MinIO process;
        // no krabka metric reflects its TCP/S3 listener coming up.
        std::thread::sleep(std::time::Duration::from_millis(500));
    }
    panic!("MinIO never accepted TCP on 127.0.0.1:{minio_port}");
}

pub(crate) fn minio_make_bucket(bucket: &str) {
    // `mc mb -p` is idempotent and creates parent prefixes; the inner
    // loop retries the `alias set` so a slow MinIO startup doesn't fail
    // the test on the first probe.
    let minio_port = minio_port();
    let script = format!(
        "for i in 1 2 3 4 5 6 7 8 9 10; do \
           mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && break; \
           sleep 1; \
         done && mc mb -p local/{bucket}"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc mb");
    assert!(
        out.status.success(),
        "mc mb failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Create a bucket with S3 Object Lock on and a compliance-mode default
/// retention, for the WORM archive suite.
///
/// `mc mb --with-lock` also turns on bucket versioning, which Object Lock
/// needs: a lock protects one object *version*, and an unversioned bucket has
/// none. `mc retention set --default COMPLIANCE 1d` is the part that makes
/// `MinIO` refuse a delete. Without it the bucket only *can* hold locked
/// objects, and no object is locked.
///
/// `1d` is the shortest retention the suite can use and still prove the point.
/// The bucket lives inside a container the test removes on drop, so nothing
/// outlives the run.
pub(crate) fn minio_make_locked_bucket(bucket: &str) {
    // Same `alias set` retry as `minio_make_bucket`, so a slow MinIO startup
    // doesn't fail the test on the first probe.
    let minio_port = minio_port();
    let script = format!(
        "for i in 1 2 3 4 5 6 7 8 9 10; do \
           mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null 2>&1 && break; \
           sleep 1; \
         done && mc mb --with-lock local/{bucket} \
         && mc retention set --default COMPLIANCE 1d local/{bucket}"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc mb --with-lock");
    assert!(
        out.status.success(),
        "mc mb --with-lock failed: stdout={}, stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// `mc ls --recursive local/<bucket>` for assertion-side bucket inspection.
pub(crate) fn minio_list_objects(bucket: &str) -> String {
    let minio_port = minio_port();
    let script = format!(
        "mc alias set local http://host.docker.internal:{minio_port} {MINIO_ACCESS_KEY} {MINIO_SECRET_KEY} >/dev/null && \
         mc ls --recursive local/{bucket}"
    );
    let out = Command::new("docker")
        .args([
            "run",
            "--rm",
            "--add-host=host.docker.internal:host-gateway",
            "--entrypoint",
            "/bin/sh",
            MINIO_CLIENT_IMAGE,
            "-c",
            &script,
        ])
        .output()
        .expect("spawn mc ls");
    assert!(
        out.status.success(),
        "mc ls failed: stderr={}",
        String::from_utf8_lossy(&out.stderr),
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

impl Drop for MinioContainer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "-f", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}
