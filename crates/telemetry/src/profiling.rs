//! In-process profiling admin server.
//!
//! On Unix targets, the server always serves a CPU pprof profile at
//! `GET /debug/pprof/profile?seconds=N`. With the `heap-profiling` feature,
//! which needs jemalloc, the server also serves a heap pprof profile at
//! `GET /debug/pprof/heap`. Grafana Alloy `pyroscope.scrape` scrapes both. The
//! same admin server can carry more routes, for example `/metrics`.
//!
//! The bodies are gzipped `Profile` protobufs. This is the standard pprof file
//! format, and it is what Go's net/http/pprof serves. Alloy's
//! `pyroscope.scrape` forwards the scraped bytes without a change as the push
//! API's `raw_profile`, and the ingester gunzips them. An uncompressed
//! protobuf body makes that gunzip fail with "invalid gzip header".
//!
//! CPU profiling uses POSIX signals, so it is available only on Unix. On
//! non-Unix targets the server returns a 503 stub, and the crate thus compiles
//! on all platforms.

mod config;
mod routes;
mod server;

pub use self::{
    config::{ProfilingConfig, ProfilingError, ProfilingSampleFrequency},
    routes::{pprof_router, pprof_router_with_config},
    server::{
        await_admin_exit, serve_admin, serve_admin_from_env, serve_admin_from_env_with,
        serve_admin_from_env_with_config, serve_admin_with_config,
        spawn_admin_from_env_with_config, spawn_admin_with_config,
    },
};
