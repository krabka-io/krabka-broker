//! The broker's startup sequence, from the parsed command line through to a
//! controlled shutdown.

use clap::Parser;
use krabka_broker::{Broker, BrokerError, BrokerHandle};
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

    if args.print_config_schema {
        let schema = krabka_broker::file_config::config_schema();
        println!("{}", serde_json::to_string_pretty(&schema)?);
        return Ok(());
    }

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
    let controller_addr = args.resolved_controller_listen_addr();
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
    let meta = krabka_broker::bootstrap::read_and_validate_meta_properties(
        &config.log_dir,
        config.cluster_id,
    )?;
    config.cluster_id = Some(meta.cluster_id);
    config.directory_id = meta.directory_id;
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

    let health_for_shutdown = health.clone();
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
            // Two things latch the flag: a fatal fault of the controller this
            // node hosts, and every log dir going offline (KIP-112).
            if handle.fatal_fault().is_some() {
                tracing::error!(
                    "self-shutdown triggered by a fatal fault of the metadata controller; \
                     stopping broker"
                );
            } else {
                tracing::error!("self-shutdown triggered (all log dirs offline); stopping broker");
            }
        }
    }
    // Flip /readyz to 503 so load balancers pull the broker out of rotation
    // before the leadership hand-off starts.
    health_for_shutdown.mark_shutting_down();

    let outcome = stop_broker(handle, controlled_shutdown_drain_timeout.to_std()).await;
    // The probes outlive the broker's own drain deliberately: the kubelet is
    // still polling while `controlled_shutdown` hands leadership over, and a
    // refused connection there is indistinguishable from a crash.
    health_shutdown.cancel();
    tracing::info!("krabka-broker stopped");
    telemetry.shutdown();
    Ok(outcome?)
}

/// Stops the broker and reports how the process should end.
///
/// A termination signal, or every log dir going offline (KIP-112), takes
/// Kafka's controlled shutdown (KIP-500): ask the controller to move
/// leadership of every partition this broker leads onto its other in-sync
/// replicas BEFORE we stop. This is the difference between a near-seamless
/// failover and stranding producers on a dead leader until their request
/// timeout -- `kubectl delete pod` sends SIGTERM, and without this hand-off the
/// partition has no leader until the controller fences us (~tens of seconds).
/// Bounded well under the pod's terminationGracePeriod (30s); on timeout
/// `controlled_shutdown` falls back to a hard stop internally.
///
/// A fatal fault of the controller takes none of that. Kafka handles it with
/// `ProcessTerminatingFaultHandler`, which halts the process with status 1: no
/// leadership hand-off, which a dead controller could not serve anyway, and no
/// orderly stop, so no clean-shutdown proof for the next start
/// (`BrokerHandle::shutdown` writes none after a fault). The stop that runs
/// here only closes what it can within `drain_timeout`, so a task that hangs
/// cannot keep the process alive, and the `FatalFault` it returns is what makes
/// the exit status non-zero.
pub(crate) async fn stop_broker(
    handle: BrokerHandle,
    drain_timeout: std::time::Duration,
) -> Result<(), BrokerError> {
    if let Some(fault) = handle.fatal_fault() {
        if tokio::time::timeout(drain_timeout, handle.shutdown())
            .await
            .is_err()
        {
            tracing::error!(
                ?drain_timeout,
                "the broker did not stop in time after a fatal fault; exiting anyway"
            );
        }
        return Err(BrokerError::FatalFault(fault));
    }
    match handle.controlled_shutdown(drain_timeout).await {
        Ok(()) => tracing::info!("controlled shutdown complete (leadership drained)"),
        Err(e) => tracing::warn!(error = %e, "controlled shutdown incomplete; hard-stopped"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use krabka_broker::BrokerConfig;
    use tempfile::tempdir;

    use super::*;

    // Kafka 4.3 supports `metadata.version` up to 30, and 33 (`4.4-IV2`) is one
    // of trunk's unstable levels.
    const FAULT: &str = "Tried to apply FeatureLevelRecord \
        FeatureLevelRecord(name='metadata.version', featureLevel=33), \
        but this controller only supports versions 7-30";

    const CLEAN_SHUTDOWN_PROOF: &str = "clean_shutdown";

    async fn start_broker(log_dir: &std::path::Path) -> BrokerHandle {
        Broker::start(BrokerConfig::for_tests(log_dir.to_path_buf()))
            .await
            .expect("broker start")
    }

    // The default config supports no unstable feature level, so committing one
    // makes the controller replay a level outside its range.
    async fn make_the_controller_fault(handle: &BrokerHandle) {
        handle
            .submit_metadata_record_for_test(krabka_metadata::MetadataRecord::V1FeatureLevel(
                krabka_metadata::FeatureLevelRecord {
                    name: "metadata.version".into(),
                    level: 33,
                },
            ))
            .await
            .expect("the unsupported level commits before the controller stops");
        let mut should_shutdown = handle.should_shutdown_rx();
        tokio::time::timeout(
            Duration::from_secs(30),
            should_shutdown.wait_for(|down| *down),
        )
        .await
        .expect("the fault did not latch the self-shutdown flag within 30s")
        .expect("the self-shutdown flag closed");
    }

    // The post-start path of `krabka-broker`: a controller fault after the
    // broker has come up ends the process with an error that carries Kafka's
    // message, and the stop it runs leaves no clean-shutdown proof, as a
    // halted Kafka process leaves none. The zero-length bound is a stop that
    // does not finish in time, and the error still comes back.
    #[tokio::test]
    async fn a_fault_after_start_ends_in_a_fatal_fault_error_without_a_clean_shutdown_proof() {
        for (name, bound) in [
            ("time to stop", Duration::from_secs(30)),
            ("no time to stop", Duration::ZERO),
        ] {
            let dir = tempdir().unwrap();
            let handle = start_broker(dir.path()).await;
            make_the_controller_fault(&handle).await;

            let outcome = stop_broker(handle, bound).await;

            assert!(
                matches!(&outcome, Err(BrokerError::FatalFault(fault)) if fault == FAULT),
                "{name}: {outcome:?}"
            );
            assert!(
                !dir.path().join(CLEAN_SHUTDOWN_PROOF).exists(),
                "{name}: a fault-driven stop left a clean-shutdown proof"
            );
        }
    }

    // The control for the test above: with no fault the same stop succeeds and
    // leaves the proof, so the absence above is the fault's doing.
    #[tokio::test]
    async fn a_stop_without_a_fault_succeeds_and_leaves_the_clean_shutdown_proof() {
        let dir = tempdir().unwrap();
        let handle = start_broker(dir.path()).await;

        let outcome = stop_broker(handle, Duration::from_secs(30)).await;

        assert!(outcome.is_ok(), "{outcome:?}");
        assert!(dir.path().join(CLEAN_SHUTDOWN_PROOF).exists());
    }
}
