//! `krabka-format` — format a fresh broker log directory.
//!
//! The monorepo spells this `krabka format`, as a subcommand of the operator
//! CLI. Here it is its own binary, because the rest of that CLI stayed behind
//! with the gres layer. The arguments are the same either way.

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    std::process::exit(krabka_format::run_from_args(std::env::args_os()).await);
}
