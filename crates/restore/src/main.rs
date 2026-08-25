//! `krabka-restore` — rebuild a krabka log directory from a tiered-storage
//! archive.
//!
//! The `krabka` operator CLI spells this `krabka restore`. It resolves an
//! unknown subcommand to `krabka-<name>` on `PATH`, the way git resolves
//! `git foo` to `git-foo`, so the binary carries that name.

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    std::process::exit(crabka_restore::run_from_args(std::env::args_os()).await);
}
