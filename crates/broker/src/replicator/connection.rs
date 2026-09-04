//! Outbound connections to a leader.
//!
//! The module holds the shared `ConnectionOptions` for every inter-broker call
//! a fetcher makes, the exponential reconnect schedule, and the retry loop that
//! dials the leader until it succeeds or the fetcher is cancelled.
//!
//! One connection serves every partition the fetcher follows on that leader,
//! so a leader restart costs one redial per fetcher rather than one per
//! partition -- the reconnect storm an operator used to see during a roll.

use krabka_client_core::{Connection, ConnectionOptions};
use krabka_units::{Time, convert::TimeExt, fmt::Human as _};
use tracing::warn;

use super::FetcherConfig;

pub(super) fn connection_options(client_id: &str) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_string(),
        ..ConnectionOptions::default()
    }
}

/// Opens a [`Connection`] against a fetcher's leader.
///
/// The function retries with exponential backoff, with a cap from the
/// configured reconnect policy. It returns `Err` only if a shutdown starts
/// during the wait.
///
/// The connection routes through the shared [`InterBrokerClient`], which runs
/// TLS and SASL when the inter-broker listener needs them. It falls back to
/// plain TCP for `ListenerProtocol::Plaintext`.
pub(super) async fn connect_with_backoff(fetcher: &FetcherConfig) -> Result<Connection, String> {
    let mut delay = reconnect_delay(&fetcher.replication, None);
    loop {
        let opts = connection_options(&fetcher.client_id);
        let attempt = fetcher.inter_broker_client.connect_as_connection(
            &fetcher.leader_host,
            fetcher.leader_port,
            fetcher.inter_broker_listener_protocol,
            &fetcher.inter_broker_server_name,
            opts,
        );
        let result = tokio::select! {
            () = fetcher.shutdown.cancelled() => return Err("cancelled".into()),
            r = attempt => r,
        };
        match result {
            Ok(c) => return Ok(c),
            Err(e) => {
                warn!(
                    host = %fetcher.leader_host, port = fetcher.leader_port, error = %e,
                    "replicator: connect failed; retrying after {}", delay.human()
                );
                tokio::select! {
                    () = fetcher.shutdown.cancelled() => return Err("cancelled".into()),
                    () = tokio::time::sleep(delay.to_std()) => {}
                }
                delay = reconnect_delay(&fetcher.replication, Some(delay));
            }
        }
    }
}

fn reconnect_delay(
    replication: &crate::config::ReplicationRuntimeConfig,
    previous: Option<Time>,
) -> Time {
    previous.map_or(replication.reconnect_initial_delay, |delay| {
        // `Time` has no `Ord` — its `f64` storage is only `PartialOrd` — so the
        // cap is applied by comparison rather than `Ord::min`.
        let doubled = delay * 2.0;
        let cap = replication.reconnect_delay_cap;
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

        let first = reconnect_delay(&cfg.replication, None);
        let second = reconnect_delay(&cfg.replication, Some(first));
        let capped = reconnect_delay(&cfg.replication, Some(second));

        assert!((first, second, capped) == (millis(37), millis(74), millis(100)));
    }
}
