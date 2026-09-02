//! The broker's startup sequence, from the parsed command line through to a
//! controlled shutdown.

use clap::Parser;
use krabka_broker::Broker;
use krabka_units::convert::TimeExt as _;

use crate::{
    bootstrap::detect_bootstrap_mode,
    cli::Args,
    config::{parse_optional_listen_addr, parse_roles_arg},
    signals::wait_for_termination_signal,
};

#[tokio::main]
// binary entrypoint: linear startup wiring
pub async fn broker_main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();

    // Install the tracing subscriber — stdout `fmt` plus an
    // optional OTLP export layer. OTLP stays off unless the environment
    // opts in (see `krabka_broker::telemetry`). Built here, inside the
    // tokio runtime, so the gRPC exporter captures the runtime handle.
    let otlp = krabka_broker::telemetry::OtlpConfig::from_env(
        |k| args.telemetry_value(k),
        &args.broker_id.to_string(),
        env!("CARGO_PKG_VERSION"),
        "krabka-broker",
    )?;
    let client_metrics_otlp_endpoint = otlp.as_ref().map(|cfg| cfg.endpoint.clone());
    let client_metrics_otlp_protocol = otlp
        .as_ref()
        .map_or(krabka_broker::telemetry::OtlpProtocol::Grpc, |cfg| {
            cfg.protocol
        });
    let telemetry = krabka_broker::telemetry::init(
        otlp,
        // The stdout filter. This is the one `BROKER_LOGGER` retargets, and
        // the one whose target directives seed the logger list.
        krabka_broker::config::DEFAULT_LOG_FILTER,
        "info,krabka_broker::request=debug,krabka_log=info",
        "krabka-broker",
    )?;
    // The handle behind the `BROKER_LOGGER` config resource. It drives the
    // stdout layer this call just installed, so `kafka-configs --entity-type
    // broker-loggers --alter` retargets the filter of the running process.
    let log_levels = telemetry.log_levels();
    let file_config: Option<krabka_broker::file_config::FileConfig> =
        match args.config_file.as_ref() {
            Some(p) => {
                let contents = std::fs::read_to_string(p)
                    .map_err(|e| format!("failed to read {}: {e}", p.display()))?;
                Some(
                    toml::from_str(&contents)
                        .map_err(|e| format!("failed to parse {}: {e}", p.display()))?,
                )
            }
            None => None,
        };
    let file_shutdown_timeout = file_config
        .as_ref()
        .and_then(|file| file.runtime.as_ref())
        .and_then(|runtime| runtime.controlled_shutdown_drain_timeout);
    let advertised = args
        .advertised_listener
        .take()
        .unwrap_or_else(|| args.listen_addr.to_string());
    let controller_addr: std::net::SocketAddr = {
        let mut a = args.listen_addr;
        a.set_port(9093);
        // Under `--config-file` (operator/StatefulSet mode), `--listen-addr`
        // conflicts_with the config file, so `args.listen_addr` keeps its
        // 127.0.0.1:9092 default. Peers dial this broker's controller via its
        // pod FQDN, so binding the controller listener to loopback would make
        // it unreachable across pods — bind all interfaces (0.0.0.0) instead.
        if args.config_file.is_some() {
            a.set_ip(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));
        }
        a
    };
    let node_id = u64::try_from(args.broker_id).unwrap_or_else(|_| {
        eprintln!("broker_id must be non-negative");
        std::process::exit(1);
    });
    let metrics_listen_addr = parse_optional_listen_addr(&args.metrics_listen_addr)?;
    let health_listen_addr = parse_optional_listen_addr(&args.health_listen_addr)?;
    let roles = if args.process_roles.is_empty() {
        None
    } else {
        Some(parse_roles_arg(&args.process_roles)?)
    };
    let mut config = args.base_broker_config(
        advertised,
        controller_addr,
        node_id,
        metrics_listen_addr,
        client_metrics_otlp_endpoint,
        client_metrics_otlp_protocol,
    );
    config.log_levels = log_levels;
    if let Some(roles) = roles {
        config.roles = roles;
    }
    if let Some(fc) = file_config {
        fc.apply_before_runtime_overlay(&mut config)?;
    }
    let controlled_shutdown_drain_timeout =
        args.apply_runtime_to(&mut config, file_shutdown_timeout)?;
    // Detect against the *resolved* log_dir so a TOML override picks up
    // its on-disk state rather than the CLI-default empty path. This is
    // the difference between a fresh-pod Bootstrap and a rolled-pod
    // Rejoin against an existing PVC.
    config.bootstrap_mode = detect_bootstrap_mode(&config.log_dir);
    // KIP-853: recover this replica's stable directory id, written by
    // `krabka format`. Required for every formatted node; absence means the
    // dir was never formatted, which is an operator error.
    config.directory_id = krabka_broker::bootstrap::read_directory_id(&config.log_dir)?;
    tracing::info!(
        bootstrap_mode = ?config.bootstrap_mode,
        directory_id = %config.directory_id,
        log_dir = %config.log_dir.display(),
        "selected bootstrap mode"
    );

    // Serve the probes before the broker starts, not after. Log-dir recovery
    // and metadata catch-up both run inside `Broker::start`, and those are
    // exactly the windows in which the orchestrator has to be able to ask
    // whether this pod is alive (yes) and ready (not yet). The state goes into
    // the broker by the same handle, so each startup phase marks its own
    // condition on the state these routes read.
    let health = krabka_broker::HealthState::new(
        args.readiness_max_metadata_lag
            .unwrap_or(krabka_broker::config::DEFAULT_READINESS_MAX_METADATA_LAG),
    );
    let health_shutdown = tokio_util::sync::CancellationToken::new();
    if let Some(addr) = health_listen_addr {
        krabka_broker::health::serve(addr, health.clone(), health_shutdown.child_token()).await?;
    }

    let handle = Broker::start_with_health(config, health).await?;
    tracing::info!(addr = %handle.listen_addr(), "krabka-broker listening");

    let mut shutdown_rx = handle.should_shutdown_rx();
    tokio::select! {
        signal = wait_for_termination_signal() => {
            tracing::info!(signal, "shutdown signal received");
        }
        () = async {
            // Wait until the self-shutdown flag flips true.
            loop {
                // Check first in case the flag was already set before we subscribed.
                if *shutdown_rx.borrow_and_update() { break; }
                if shutdown_rx.changed().await.is_err() { break; }
            }
        } => {
            tracing::error!("self-shutdown triggered (all log dirs offline); stopping broker");
        }
    }
    // KIP-500 controlled shutdown: ask the controller to move leadership of
    // every partition this broker leads onto its other in-sync replicas
    // BEFORE we stop. This is the difference between a near-seamless failover
    // and stranding producers on a dead leader until their request timeout —
    // `kubectl delete pod` sends SIGTERM, and without this hand-off the
    // partition has no leader until the controller fences us (~tens of
    // seconds). Bounded well under the pod's terminationGracePeriod (30s); on
    // timeout `controlled_shutdown` falls back to a hard stop internally.
    match handle
        .controlled_shutdown(controlled_shutdown_drain_timeout.to_std())
        .await
    {
        Ok(()) => tracing::info!("controlled shutdown complete (leadership drained)"),
        Err(e) => tracing::warn!(error = %e, "controlled shutdown incomplete; hard-stopped"),
    }
    // The probes outlive the broker's own drain deliberately: the kubelet is
    // still polling while `controlled_shutdown` hands leadership over, and a
    // refused connection there is indistinguishable from a crash.
    health_shutdown.cancel();
    tracing::info!("krabka-broker stopped");
    telemetry.shutdown();
    Ok(())
}
