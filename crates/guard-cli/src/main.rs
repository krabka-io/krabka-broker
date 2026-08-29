//! `krabka-guard` — one command for one incident.
//!
//! The monorepo spells an operator command as a subcommand of `crabka`. Here
//! this is its own binary, beside `krabka-barrier`, because the rest of that
//! CLI stayed behind with the gres layer. The arguments are the same either
//! way.

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    std::process::exit(krabka_guard::run_from_args(std::env::args_os()).await);
}
