//! The pprof HTTP routes and the sampling they drive.
//!
//! The module holds the `GET /debug/pprof/profile` CPU handler, the optional
//! `GET /debug/pprof/heap` handler behind the `heap-profiling` feature, the
//! gzip step that produces the pprof file format, and the router builders that
//! bind a `ProfilingConfig` to those handlers as axum state.

#[cfg(any(unix, test))]
use krabka_units::{Time, secs};
use {
    super::config::{ProfilingConfig, ProfilingError},
    axum::{
        Router,
        extract::{Query, State},
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::get,
    },
    flate2::write::GzEncoder,
    serde::Deserialize,
    tokio::{task, time},
};
#[cfg(unix)]
use {krabka_units::convert::TimeExt as _, std::io};

#[derive(Debug, Deserialize)]
struct CpuQuery {
    #[cfg_attr(not(unix), allow(dead_code))]
    seconds: Option<u64>,
}

#[cfg(all(unix, feature = "heap-profiling"))]
#[derive(Debug, Deserialize)]
struct HeapQuery {
    seconds: Option<u64>,
}

/// CPU profile in pprof protobuf, sampled for `?seconds=N`.
///
/// The default is 30 seconds, and the default configuration clamps the value
/// to `1..=60` seconds.
#[cfg(unix)]
async fn cpu_profile(State(config): State<ProfilingConfig>, Query(q): Query<CpuQuery>) -> Response {
    let duration = requested_duration(
        q.seconds,
        config.profiling_cpu_default_duration,
        config.profiling_cpu_max_duration,
    );
    let guard = match profile_work(move || start_cpu_profile(&config)).await {
        Ok(guard) => guard,
        Err(error) => return (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    };
    time::sleep(duration.to_std()).await;
    match profile_work(move || finish_cpu_profile(guard)).await {
        Ok(body) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            body,
        )
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

/// Profiler setup, symbolization, and encoding can take hundreds of milliseconds. Offload that synchronous work so other requests and timers can progress, including on a current-thread runtime.
#[cfg(unix)]
async fn profile_work<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    task::spawn_blocking(work)
        .await
        .map_err(|error| format!("profiling worker: {error}"))?
}

#[cfg(unix)]
fn start_cpu_profile(config: &ProfilingConfig) -> Result<pprof::ProfilerGuard<'static>, String> {
    let blocklist = config
        .profiling_native_frame_blocklist
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    pprof::ProfilerGuardBuilder::default()
        .frequency(config.profiling_cpu_sample_frequency.hertz())
        .blocklist(&blocklist)
        .build()
        .map_err(|error| format!("profiler: {error}"))
}

#[cfg(unix)]
fn finish_cpu_profile(guard: pprof::ProfilerGuard<'static>) -> Result<Vec<u8>, String> {
    // This is the prost Message trait used by pprof's generated Profile.
    use pprof::protos::Message as _;

    let report = guard
        .report()
        .build()
        .map_err(|error| format!("report: {error}"))?;
    drop(guard);
    let profile = report.pprof().map_err(|error| format!("pprof: {error}"))?;
    gzip(&profile.encode_to_vec()).map_err(|error| format!("gzip: {error}"))
}

/// Gzip a buffer.
///
/// The pprof file format is a gzipped `Profile` protobuf.
#[cfg(unix)]
fn gzip(raw: &[u8]) -> io::Result<Vec<u8>> {
    use std::io::Write as _;

    let mut encoder = GzEncoder::new(
        Vec::with_capacity(raw.len() / 2),
        flate2::Compression::fast(),
    );
    encoder.write_all(raw)?;
    encoder.finish()
}

/// Stub for non-Unix targets: CPU profiling is unavailable.
// cargo-mutants: non-Unix stub is not built or exercised on the default Linux mutation run.
#[cfg(not(unix))]
#[cfg_attr(test, mutants::skip)]
async fn cpu_profile(
    _config: State<ProfilingConfig>,
    _q: Query<CpuQuery>,
) -> axum::response::Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        "CPU profiling requires a Unix target",
    )
        .into_response()
}

// cargo-mutants: optional heap-profiling route is feature-gated out of the default mutation run.
#[cfg(all(unix, feature = "heap-profiling"))]
#[cfg_attr(test, mutants::skip)]
async fn heap_profile(
    State(config): State<ProfilingConfig>,
    Query(q): Query<HeapQuery>,
) -> Response {
    let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            "jemalloc profiling not enabled (build with --features heap-profiling and set MALLOC_CONF)",
        )
            .into_response();
    };
    let mut ctl = ctl.lock().await;
    let duration = requested_duration(
        q.seconds,
        config.profiling_heap_default_duration,
        config.profiling_heap_max_duration,
    );
    let dump = sample_heap(&mut *ctl, duration).await;
    drop(ctl);
    match dump {
        Ok(pprof) => (
            StatusCode::OK,
            [("content-type", "application/octet-stream")],
            pprof,
        )
            .into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

/// The jemalloc profiling controls the heap handler drives.
///
/// The trait is the seam that lets the unit tests run the sampling sequence
/// without a jemalloc allocator.
#[cfg(all(unix, any(feature = "heap-profiling", test)))]
trait HeapProfCtl {
    fn activated(&self) -> bool;
    fn activate(&mut self) -> Result<(), String>;
    fn deactivate(&mut self) -> Result<(), String>;
    fn dump_pprof(&mut self) -> Result<Vec<u8>, String>;
}

#[cfg(all(unix, feature = "heap-profiling"))]
impl HeapProfCtl for jemalloc_pprof::JemallocProfCtl {
    fn activated(&self) -> bool { Self::activated(self) }

    fn activate(&mut self) -> Result<(), String> {
        Self::activate(self).map_err(|error| error.to_string())
    }

    fn deactivate(&mut self) -> Result<(), String> {
        Self::deactivate(self).map_err(|error| error.to_string())
    }

    fn dump_pprof(&mut self) -> Result<Vec<u8>, String> {
        Self::dump_pprof(self).map_err(|error| error.to_string())
    }
}

/// Turns jemalloc profiling off when dropped.
///
/// The heap handler holds one while it sleeps, so a client that disconnects
/// mid-sample, which drops the handler future, still leaves profiling off.
#[cfg(all(unix, any(feature = "heap-profiling", test)))]
struct DeactivateOnDrop<'a, C: HeapProfCtl>(&'a mut C);

#[cfg(all(unix, any(feature = "heap-profiling", test)))]
impl<C: HeapProfCtl> Drop for DeactivateOnDrop<'_, C> {
    fn drop(&mut self) {
        if let Err(error) = self.0.deactivate() {
            tracing::warn!(%error, "could not deactivate jemalloc profiling after heap dump");
        }
    }
}

/// Sample the heap for `duration` and dump it as pprof.
///
/// When profiling is already on, the dump happens at once and profiling stays
/// on. Otherwise this activates profiling, sleeps, dumps, and deactivates,
/// and the deactivation also runs when the future is dropped mid-sleep.
#[cfg(all(unix, any(feature = "heap-profiling", test)))]
async fn sample_heap<C: HeapProfCtl>(ctl: &mut C, duration: Time) -> Result<Vec<u8>, String> {
    if ctl.activated() {
        return ctl
            .dump_pprof()
            .map_err(|error| format!("heap dump: {error}"));
    }
    ctl.activate()
        .map_err(|error| format!("jemalloc prof activate: {error}"))?;
    let guard = DeactivateOnDrop(ctl);
    time::sleep(duration.to_std()).await;
    guard
        .0
        .dump_pprof()
        .map_err(|error| format!("heap dump: {error}"))
}

/// The pprof routes with an explicit policy.
///
/// The router always has the CPU route, which returns 503 on non-Unix targets.
/// The router has the heap route only with the `heap-profiling` feature, and
/// only on Unix.
///
/// # Errors
/// Returns an error when related profiling duration bounds are invalid.
pub fn pprof_router_with_config(config: ProfilingConfig) -> Result<Router, ProfilingError> {
    config.validate().map_err(ProfilingError::Config)?;
    Ok(pprof_router_unchecked(config))
}

fn pprof_router_unchecked(config: ProfilingConfig) -> Router {
    let router = Router::new().route("/debug/pprof/profile", get(cpu_profile));
    #[cfg(all(unix, feature = "heap-profiling"))]
    let router = router.route("/debug/pprof/heap", get(heap_profile));
    router.with_state(config)
}

/// The pprof routes with the compatible default policy.
pub fn pprof_router() -> Router { pprof_router_unchecked(ProfilingConfig::default()) }

#[cfg(any(unix, test))]
fn requested_duration(seconds: Option<u64>, default: Time, maximum: Time) -> Time {
    seconds
        .map_or(default, |seconds| {
            secs(u32::try_from(seconds.max(1)).unwrap_or(u32::MAX))
        })
        .min(maximum)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_units::{millis, minutes},
        std::{sync::mpsc, time::Duration},
        tokio::sync::oneshot,
    };

    /// A blocked profiler worker must leave this single runtime thread free to run the task that releases it.
    #[cfg(unix)]
    #[tokio::test]
    async fn profiling_work_does_not_block_runtime_progress() {
        let (started, waiting) = oneshot::channel();
        let (release, released) = mpsc::channel();
        let work = profile_work(move || {
            started.send(()).unwrap();
            released
                .recv_timeout(Duration::from_secs(5))
                .map_err(|error| error.to_string())?;
            Ok(42)
        });
        let other_task = async move {
            waiting.await.unwrap();
            release.send(()).unwrap();
        };
        let (result, ()) = tokio::join!(work, other_task);
        assert!(result == Ok(42));
        assert!(
            profile_work(|| Err::<(), _>("report failed".into())).await
                == Err("report failed".into())
        );
    }

    /// Counts the profiling calls a heap sample makes.
    #[cfg(unix)]
    #[derive(Debug, Default, PartialEq, Eq)]
    struct FakeProfCtl {
        active: bool,
        activations: u32,
        deactivations: u32,
        dumps: u32,
    }

    #[cfg(unix)]
    impl HeapProfCtl for FakeProfCtl {
        fn activated(&self) -> bool { self.active }

        fn activate(&mut self) -> Result<(), String> {
            self.active = true;
            self.activations += 1;
            Ok(())
        }

        fn deactivate(&mut self) -> Result<(), String> {
            self.active = false;
            self.deactivations += 1;
            Ok(())
        }

        fn dump_pprof(&mut self) -> Result<Vec<u8>, String> {
            self.dumps += 1;
            Ok(vec![7])
        }
    }

    /// A client that disconnects mid-sample drops the future; profiling must still end up off.
    #[cfg(unix)]
    #[tokio::test]
    async fn cancelled_heap_sample_deactivates_profiling() {
        let mut ctl = FakeProfCtl::default();
        let cancelled =
            time::timeout(Duration::from_millis(10), sample_heap(&mut ctl, minutes(1))).await;
        assert!(cancelled.is_err());
        assert!(
            ctl == FakeProfCtl {
                active: false,
                activations: 1,
                deactivations: 1,
                dumps: 0,
            }
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn completed_heap_sample_dumps_then_deactivates() {
        let mut ctl = FakeProfCtl::default();
        assert!(sample_heap(&mut ctl, millis(1)).await == Ok(vec![7]));
        assert!(
            ctl == FakeProfCtl {
                active: false,
                activations: 1,
                deactivations: 1,
                dumps: 1,
            }
        );
    }

    /// Profiling someone else switched on is dumped at once and left on.
    #[cfg(unix)]
    #[tokio::test]
    async fn heap_sample_leaves_prior_activation_on() {
        let mut ctl = FakeProfCtl {
            active: true,
            ..FakeProfCtl::default()
        };
        assert!(sample_heap(&mut ctl, minutes(1)).await == Ok(vec![7]));
        assert!(
            ctl == FakeProfCtl {
                active: true,
                activations: 0,
                deactivations: 0,
                dumps: 1,
            }
        );
    }

    #[test]
    fn requested_profile_duration_uses_configured_default_floor_and_cap() {
        assert!(requested_duration(None, secs(2), secs(5)) == secs(2));
        assert!(requested_duration(Some(0), secs(2), secs(5)) == secs(1));
        assert!(requested_duration(Some(3), secs(2), secs(5)) == secs(3));
        assert!(requested_duration(Some(9), secs(2), secs(5)) == secs(5));
        assert!(
            (ProfilingConfig {
                profiling_cpu_max_duration: millis(500),
                ..ProfilingConfig::default()
            }
            .validate()
            .is_err())
        );
        assert!(
            (ProfilingConfig {
                profiling_heap_default_duration: minutes(1),
                ..ProfilingConfig::default()
            }
            .validate()
            .is_err())
        );
    }
}
