//! The follower's link to its WAL leader. It holds the reconnect loop with its
//! doubling backoff and the cancellable sleep that every retry path in the
//! follower uses, so a shutdown token always wins over a pending delay.

use krabka_client_core::{Connection, ConnectionOptions};
use krabka_units::{Time, convert::TimeExt as _, fmt::Human as _};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::Config;

pub(super) async fn connect_with_backoff(config: &Config) -> Result<Connection, String> {
    let mut delay = config.replication.reconnect_initial_delay;
    loop {
        let attempt = config.inter_broker_client.connect_as_connection(
            &config.leader_host,
            config.leader_port,
            config.inter_broker_listener_protocol,
            &config.inter_broker_server_name,
            follower_connection_options(&config.client_id),
        );
        let result = tokio::select! {
            () = config.shutdown.cancelled() => return Err("cancelled".into()),
            result = attempt => result,
        };
        match result {
            Ok(connection) => return Ok(connection),
            Err(error) => {
                warn!(
                    host = %config.leader_host,
                    port = config.leader_port,
                    error = %error,
                    "diskless WAL follower connect failed; retrying after {}",
                    delay.human()
                );
                sleep_or_cancel(&config.shutdown, delay).await?;
                delay = next_reconnect_delay(delay, config.replication.reconnect_delay_cap);
            }
        }
    }
}

fn follower_connection_options(client_id: &str) -> ConnectionOptions {
    ConnectionOptions {
        client_id: client_id.to_owned(),
        ..ConnectionOptions::default()
    }
}

fn next_reconnect_delay(delay: Time, cap: Time) -> Time {
    (delay + delay).min(cap)
}

pub(super) async fn sleep_or_cancel(
    shutdown: &CancellationToken,
    delay: Time,
) -> Result<(), String> {
    tokio::select! {
        () = shutdown.cancelled() => Err("cancelled".into()),
        () = tokio::time::sleep(delay.to_std()) => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{millis, secs};

    use super::*;

    #[test]
    fn follower_connection_and_backoff_policy_preserve_runtime_values() {
        let options = follower_connection_options("wal-client");
        assert!(options.client_id == "wal-client");
        assert!(next_reconnect_delay(millis(100), secs(1)) == millis(200));
        assert!(next_reconnect_delay(millis(600), secs(1)) == secs(1));
        assert!(next_reconnect_delay(secs(1), secs(1)) == secs(1));
    }

    #[tokio::test(start_paused = true)]
    async fn follower_sleep_completes_on_delay_or_cancellation() {
        let shutdown = CancellationToken::new();
        assert!(sleep_or_cancel(&shutdown, millis(10)).await.is_ok());

        shutdown.cancel();
        assert!(sleep_or_cancel(&shutdown, secs(1)).await.is_err());
    }
}
