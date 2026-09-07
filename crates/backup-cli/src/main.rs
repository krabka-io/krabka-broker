//! `krabka-backup` — copies the inputs a restore needs off a running cluster.
//!
//! The `krabka` operator CLI resolves an unknown subcommand to `krabka-<name>`
//! on `PATH`, the way git resolves `git foo` to `git-foo`, so this binary's
//! name is what makes `krabka backup` work.

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    std::process::exit(krabka_backup::run_from_args(std::env::args_os()).await);
}
