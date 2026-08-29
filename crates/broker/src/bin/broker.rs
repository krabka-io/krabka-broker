//! `krabka-broker`, the single-node Kafka-compatible broker daemon.

// Heap profiling: install jemalloc as the global allocator and enable the
// prof:true malloc_conf so jemalloc_pprof can dump heap profiles at runtime.
// Gated on both `unix` (tikv-jemallocator is Unix-only) and the
// `heap-profiling` feature so `cargo build` on Windows compiles cleanly.
#[cfg(all(unix, feature = "heap-profiling"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// `copy_plain_runtime!` and `copy_refined_runtime!` are textually scoped, and
// this file is a binary crate root, where `mod cli;` would name `src/bin/cli.rs`
// and Cargo would compile that file as a second binary. Every child therefore
// carries an explicit path, and the macro module precedes the module that
// expands its macros.
#[path = "broker/bootstrap.rs"]
mod bootstrap;
#[path = "broker/cli.rs"]
mod cli;
#[path = "broker/config.rs"]
mod config;
#[path = "broker/runtime_args.rs"]
mod runtime_args;
#[macro_use]
#[path = "broker/runtime_macros.rs"]
mod runtime_macros;
#[path = "broker/runtime_overlay.rs"]
mod runtime_overlay;
#[path = "broker/signals.rs"]
mod signals;
#[path = "broker/startup.rs"]
mod startup;
#[path = "broker/telemetry.rs"]
mod telemetry;
#[cfg(test)]
#[path = "broker/test_support.rs"]
mod test_support;

use self::startup::broker_main;

const BROKER_MAIN_STACK_BYTES: usize = 16 * 1024 * 1024;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let result = std::thread::Builder::new()
        .name("krabka-broker-main".into())
        .stack_size(BROKER_MAIN_STACK_BYTES)
        .spawn(|| broker_main().map_err(|error| error.to_string()))?
        .join()
        .map_err(|_| "krabka-broker main thread panicked")?;
    result.map_err(Into::into)
}
