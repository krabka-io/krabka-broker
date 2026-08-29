//! Outbound connections to the partition leader.
//!
//! The module holds the shared `ConnectionOptions` for every inter-broker call
//! this task makes, the exponential reconnect schedule, and the retry loop that
//! dials the leader until it succeeds or the task is cancelled.

use krabka_client_core::{Connection, ConnectionOptions};
use krabka_units::{Time, convert::TimeExt, fmt::Human as _};
use tracing::warn;

use super::Config;

pub(super) fn connection_options(client_id: &str) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_string(),
        ..ConnectionOptions::default()
    }
}

/// Opens a [`Connection`] against the leader of the partition.
///
/// The function retries with exponential backoff, with a cap from the
/// configured reconnect policy. It returns `Err` only if a shutdown starts
/// during the wait.
///
/// The connection routes through the shared [`InterBrokerClient`], which runs
/// TLS and SASL when the inter-broker listener needs them. It falls back to
/// plain TCP for `ListenerProtocol::Plaintext`.
pub(super) async fn connect_with_backoff(cfg: &Config) -> Result<Connection, String> {
    let mut delay = reconnect_delay(cfg, None);
    loop {
        let opts = connection_options(&cfg.client_id);
        let attempt = cfg.inter_broker_client.connect_as_connection(
            &cfg.leader_host,
            cfg.leader_port,
            cfg.inter_broker_listener_protocol,
            &cfg.inter_broker_server_name,
            opts,
        );
        let result = tokio::select! {
            () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
            r = attempt => r,
        };
        match result {
            Ok(c) => return Ok(c),
            Err(e) => {
                warn!(
                    host = %cfg.leader_host, port = cfg.leader_port, error = %e,
                    "replicator: connect failed; retrying after {}", delay.human()
                );
                tokio::select! {
                    () = cfg.shutdown.cancelled() => return Err("cancelled".into()),
                    () = tokio::time::sleep(delay.to_std()) => {}
                }
                delay = reconnect_delay(cfg, Some(delay));
            }
        }
    }
}

fn reconnect_delay(cfg: &Config, previous: Option<Time>) -> Time {
    previous.map_or(cfg.replication.reconnect_initial_delay, |delay| {
        // `Time` has no `Ord` — its `f64` storage is only `PartialOrd` — so the
        // cap is applied by comparison rather than `Ord::min`.
        let doubled = delay * 2.0;
        let cap = cfg.replication.reconnect_delay_cap;
        if doubled > cap { cap } else { doubled }
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::millis;

    use super::*;
    use crate::replicator::test_support::{LEADER_ID, image_with_leader, test_config};

    #[test]
    fn configured_reconnect_delay_doubles_until_cap() {
        let (mut cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.replication.reconnect_initial_delay = millis(37);
        cfg.replication.reconnect_delay_cap = millis(100);

        let first = reconnect_delay(&cfg, None);
        let second = reconnect_delay(&cfg, Some(first));
        let capped = reconnect_delay(&cfg, Some(second));

        assert!((first, second, capped) == (millis(37), millis(74), millis(100)));
    }
}
