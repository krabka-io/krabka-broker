//! Injecting each fault kind into one broker container, and undoing it.
//!
//! | kind | injected with | cleaned up with |
//! | --- | --- | --- |
//! | `pause` | `docker kill --signal=SIGSTOP` | `docker kill --signal=SIGCONT` |
//! | `restart`, graceful | `docker stop -t 60` (`SIGTERM`, controlled shutdown) | `docker start` |
//! | `restart`, not graceful | `docker kill` (`SIGKILL`) | `docker start` |
//! | `slow_follower`, `cpu_throttle` | `docker update --cpus <cpus>` | `docker update --cpu-quota -1` |
//! | `slow_follower`, `netem` | `tc qdisc add dev eth0 root netem delay ...` in the broker's network namespace | `tc qdisc del dev eth0 root` |
//! | `disk_pressure` | a ballast file in the broker's data root | removing it |
//! | `roll` | `docker stop -t 60`, then `docker rm`: the data root is a bind mount and stays | `docker run` of the same node from the other release's image |
//!
//! A `certificate_rotation` and a `reassignment` are not injected here: they
//! are requests to the running cluster, which the executor sends itself (see
//! the `transitions` module).
//!
//! `docker update --cpus 0` does not lift a quota: the daemon keeps the
//! `NanoCpus` it had and the cgroup keeps its CFS quota. `--cpu-quota -1`
//! writes the unlimited quota back to the cgroup, and a later `docker start`
//! keeps it. Where a daemon refuses that, the quota is raised to every CPU of
//! the host instead, which no broker can exceed.
//!
//! Every fault is held by a [`FaultGuard`] from before its injection command
//! runs, so a fault whose injection half-worked, and a test that panics while
//! a fault holds, still undo it when the guard drops: no broker is left
//! stopped, throttled, delayed or short of disk.

use {
    crate::{
        host::{docker_try, free_bytes},
        probe,
    },
    krabka_bench_driver::soak::contract::{FaultKind, HostResources, Slowdown},
    krabka_units::prelude::*,
    std::{
        fs::File,
        io::{ErrorKind, Write as _},
        path::{Path, PathBuf},
        process::Command,
    },
};

/// The name of the ballast file a disk-pressure fault writes into a broker's
/// data root.
const BALLAST: &str = "soak-ballast";

/// How a fault is undone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Cleanup {
    Resume {
        container: String,
    },
    Start {
        container: String,
    },
    Uncap {
        container: String,
        host_cpus: usize,
    },
    DropQdisc {
        container: String,
        image: String,
    },
    RemoveBallast {
        path: PathBuf,
    },
    /// Start node `container` again: `argv` is its whole `docker run`. A
    /// container left behind by a half-done roll is removed first.
    Relaunch {
        container: String,
        argv: Vec<String>,
    },
    /// Nothing to undo; `what` says why.
    Nothing {
        what: String,
    },
}

impl Cleanup {
    /// Undo the fault, and say how.
    fn run(&self) -> Result<String, String> {
        match self {
            Cleanup::Resume { container } => {
                docker_try(&["kill", "--signal=SIGCONT", container]).map(|_| "SIGCONT".to_owned())
            }
            Cleanup::Start { container } => docker_try(&["start", container]).map(|_| "docker start".to_owned()),
            Cleanup::Uncap { container, host_cpus } => match docker_try(&["update", "--cpu-quota", "-1", container]) {
                Ok(_) => Ok("CFS quota lifted with --cpu-quota -1".to_owned()),
                Err(refused) => docker_try(&["update", "--cpus", &host_cpus.to_string(), container]).map(|_| {
                    format!("--cpu-quota -1 refused ({refused}); quota raised to the host's {host_cpus} CPUs")
                }),
            },
            Cleanup::DropQdisc { container, image } => {
                let namespace = format!("container:{container}");
                docker_try(&[
                    "run", "--rm", "--network", &namespace, "--cap-add", "NET_ADMIN", image, "tc", "qdisc",
                    "del", "dev", "eth0", "root",
                ])
                .map(|_| "netem qdisc deleted".to_owned())
            }
            Cleanup::RemoveBallast { path } => match std::fs::remove_file(path) {
                Ok(()) => Ok(format!("removed {}", path.display())),
                Err(error) if error.kind() == ErrorKind::NotFound => {
                    Ok(format!("{} was never written", path.display()))
                }
                Err(error) => Err(format!("remove {}: {error}", path.display())),
            },
            Cleanup::Relaunch { container, argv } => {
                // Absent after a completed `docker rm`, which is the usual case.
                let _ = docker_try(&["rm", "--force", container]);
                let args: Vec<&str> = argv.iter().map(String::as_str).collect();
                docker_try(&args).map(|_| format!("docker {}", relaunched_image(argv)))
            }
            Cleanup::Nothing { what } => Ok(what.clone()),
        }
    }
}

/// The image a relaunch's `docker run` arguments start, for the fault log.
fn relaunched_image(argv: &[String]) -> String {
    argv.iter()
        .find(|arg| arg.contains("@sha256:") || arg.contains("krabka-broker:"))
        .map_or_else(|| "run".to_owned(), |image| format!("run {image}"))
}

/// A fault in place. [`FaultGuard::clear`] undoes it; dropping the guard
/// undoes it too, best effort.
#[derive(Debug)]
pub(crate) struct FaultGuard {
    cleanup: Option<Cleanup>,
}

impl FaultGuard {
    pub(crate) fn new(cleanup: Cleanup) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    /// Undo the fault.
    pub(crate) fn clear(mut self) -> Result<String, String> {
        self.cleanup
            .take()
            .map_or_else(|| Ok(String::new()), |cleanup| cleanup.run())
    }
}

impl Drop for FaultGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.run();
        }
    }
}

/// The ballast that brings a data root holding `used` bytes up to
/// `watermark` of `capacity`, or the reason it is refused: it would leave
/// less than `reserve` free on a host file system with `host_free` bytes
/// free. A fill is refused rather than shrunk, so a run on a smaller disk
/// fails instead of silently testing less pressure than it declares.
pub(crate) fn ballast_bytes(
    watermark: Ratio,
    capacity: ByteSize,
    used: u64,
    host_free: u64,
    reserve: ByteSize,
) -> Result<u64, String> {
    let target = ByteSize::from_bytes_f64(capacity.bytes_f64() * watermark.as_f64()).bytes_u64();
    let fill = target.saturating_sub(used);
    let left = host_free.saturating_sub(fill);
    if left < reserve.bytes_u64() {
        return Err(format!(
            "disk fill refused: {fill} bytes to reach {:.0}% of {} would leave {left} bytes free on the \
             host, below the {}-byte reserve",
            watermark.percent_f64(),
            capacity.bytes_u64(),
            reserve.bytes_u64()
        ));
    }
    Ok(fill)
}

/// Write `bytes` of ballast at `path`: `fallocate`, or zeros where the file
/// system does not support it.
fn write_ballast(path: &Path, bytes: u64) -> Result<&'static str, String> {
    let allocated = Command::new("fallocate")
        .args(["-l", &bytes.to_string()])
        .arg(path)
        .output()
        .is_ok_and(|output| output.status.success());
    if allocated {
        return Ok("fallocate");
    }
    let mut file =
        File::create(path).map_err(|error| format!("create {}: {error}", path.display()))?;
    let chunk = vec![0_u8; 1 << 20];
    let mut left = bytes;
    while left > 0 {
        let size = usize::try_from(left.min(1 << 20)).unwrap_or(chunk.len());
        file.write_all(&chunk[..size])
            .map_err(|error| format!("write {}: {error}", path.display()))?;
        left -= u64::try_from(size).unwrap_or(left);
    }
    file.sync_all()
        .map_err(|error| format!("sync {}: {error}", path.display()))?;
    Ok("written")
}

/// A fault in place and what it did.
pub(crate) struct Injected {
    pub(crate) guard: FaultGuard,
    pub(crate) detail: String,
}

/// Inject `kind` into the broker running as `container`, whose data root is
/// `root` on the host. `relaunch` is the `docker run` a `roll` starts the node
/// again with, from the image of the release it rolls to.
///
/// Blocking: it runs Docker and writes files, so callers run it off the async
/// workers.
pub(crate) fn inject(
    kind: &FaultKind,
    container: &str,
    root: &Path,
    host: &HostResources,
    relaunch: Option<Vec<String>>,
) -> Result<Injected, String> {
    let container = container.to_owned();
    match kind {
        FaultKind::Roll { to } => {
            let argv =
                relaunch.ok_or_else(|| format!("no image to roll {container} to the {to}"))?;
            docker_try(&["stop", "--time", "60", &container])?;
            // From here the node is down: the guard starts it again, on the
            // other image, whatever happens next.
            let guard = FaultGuard::new(Cleanup::Relaunch {
                container: container.clone(),
                argv,
            });
            docker_try(&["rm", &container])?;
            Ok(Injected {
                guard,
                detail: format!("docker stop --time 60 and docker rm, to roll to the {to}"),
            })
        }
        FaultKind::CertificateRotation | FaultKind::Reassignment { .. } => Err(format!(
            "a {} is a request to the cluster, which the executor sends",
            kind.name()
        )),
        FaultKind::Pause { .. } => {
            let guard = FaultGuard::new(Cleanup::Resume {
                container: container.clone(),
            });
            docker_try(&["kill", "--signal=SIGSTOP", &container])?;
            Ok(Injected {
                guard,
                detail: "SIGSTOP".to_owned(),
            })
        }
        FaultKind::Restart { graceful } => {
            let guard = FaultGuard::new(Cleanup::Start {
                container: container.clone(),
            });
            if *graceful {
                docker_try(&["stop", "--time", "60", &container])?;
            } else {
                docker_try(&["kill", "--signal=SIGKILL", &container])?;
            }
            Ok(Injected {
                guard,
                detail: if *graceful {
                    "docker stop --time 60"
                } else {
                    "docker kill --signal=SIGKILL"
                }
                .to_owned(),
            })
        }
        FaultKind::SlowFollower {
            slowdown: Slowdown::CpuThrottle { cpus },
        } => {
            let guard = FaultGuard::new(Cleanup::Uncap {
                container: container.clone(),
                host_cpus: std::thread::available_parallelism().map_or(1, std::num::NonZero::get),
            });
            docker_try(&["update", "--cpus", &cpus.to_string(), &container])?;
            Ok(Injected {
                guard,
                detail: format!("docker update --cpus {cpus}"),
            })
        }
        FaultKind::SlowFollower {
            slowdown:
                Slowdown::Netem {
                    delay,
                    jitter,
                    image,
                },
        } => {
            let guard = FaultGuard::new(Cleanup::DropQdisc {
                container: container.clone(),
                image: image.clone(),
            });
            let namespace = format!("container:{container}");
            let delay = format!("{}ms", delay.millis_i64());
            let jitter = format!("{}ms", jitter.millis_i64());
            docker_try(&[
                "run",
                "--rm",
                "--network",
                &namespace,
                "--cap-add",
                "NET_ADMIN",
                image,
                "tc",
                "qdisc",
                "add",
                "dev",
                "eth0",
                "root",
                "netem",
                "delay",
                &delay,
                &jitter,
            ])?;
            Ok(Injected {
                guard,
                detail: format!("netem delay {delay} {jitter}"),
            })
        }
        FaultKind::DiskPressure { watermark } => {
            let path = root.join(BALLAST);
            let used = probe::directory_usage(root).bytes;
            let free = free_bytes(root)
                .ok_or_else(|| format!("no free-space reading for {}", root.display()))?;
            let fill = ballast_bytes(
                *watermark,
                host.disk_capacity,
                used,
                free,
                host.disk_reserve,
            )?;
            let guard = FaultGuard::new(Cleanup::RemoveBallast { path: path.clone() });
            let how = write_ballast(&path, fill)?;
            Ok(Injected {
                guard,
                detail: format!(
                    "{fill} bytes of ballast ({how}) at {}: the data root held {used} bytes, {free} free on the host",
                    path.display()
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Cleanup, FaultGuard, ballast_bytes},
        assert2::{assert, check},
        krabka_units::prelude::*,
    };

    #[yare::parameterized(
        fills_to_the_watermark = { 100 << 20, 10 << 30, Ok(412 << 20) },
        already_past_it = { 600 << 20, 10 << 30, Ok(0) },
        exactly_the_reserve_left = { 0, (4 << 30) + (512 << 20), Ok(512 << 20) },
        refused_below_the_reserve = { 0, (4 << 30) + (511 << 20), Err(()) },
    )]
    fn a_disk_fill_reaches_its_watermark_or_is_refused(
        used: u64,
        host_free: u64,
        expected: Result<u64, ()>,
    ) {
        let fill = ballast_bytes(percent(25), gibibytes(2), used, host_free, gibibytes(4));
        let outcome = fill.clone().map_err(|_| ());
        check!(outcome == expected, "{fill:?}");
    }

    #[test]
    fn a_refusal_says_why() {
        let refusal =
            ballast_bytes(percent(80), gibibytes(500), 0, 100 << 30, gibibytes(50)).unwrap_err();
        assert!(refusal.starts_with("disk fill refused:"));
        check!(refusal.contains("reserve"));
    }

    #[test]
    fn a_dropped_guard_undoes_its_fault() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ballast");
        std::fs::write(&path, b"x").unwrap();
        drop(FaultGuard::new(Cleanup::RemoveBallast {
            path: path.clone(),
        }));
        check!(!path.exists());
    }

    #[test]
    fn a_cleared_guard_reports_its_cleanup() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ballast");
        std::fs::write(&path, b"x").unwrap();
        let guard = FaultGuard::new(Cleanup::RemoveBallast { path: path.clone() });
        check!(guard.clear().is_ok_and(|how| how.starts_with("removed")));
        check!(!path.exists());
        let never = FaultGuard::new(Cleanup::RemoveBallast { path });
        check!(
            never
                .clear()
                .is_ok_and(|how| how.ends_with("was never written"))
        );
    }
}
