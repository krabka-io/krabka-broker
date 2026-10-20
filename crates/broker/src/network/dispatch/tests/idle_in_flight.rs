//! `connections.max.idle.ms` while a request is in a handler or the
//! connection is muted.
//!
//! Kafka's `Selector` refreshes a channel's idle clock only when its key is
//! ready to read or write. A channel is muted from the moment a request is
//! received until its response is sent, and again for a KIP-219 throttle
//! window after it, so neither a slow handler nor a mute moves the clock, and
//! `maybeCloseOldestConnection` closes the channel once the window passes. A
//! pending response is lost with it.
//!
//! Each case drives a real socket through `serve_connection_stream` and
//! records what the client sees after it sends one request, and when. The
//! clock is paused once the broker is up, so every instant is exact: the
//! broker's own start-up timers need real time, as in the
//! `connections_max_idle` integration tests.
use {
    super::throttle_mute::{
        AnonymousQuota, FETCH_KEY, FETCH_VERSION, connect_to_serve_loop, consumer_fetch_request,
        create_topic, quota, seed_anonymous_quotas, send_api_versions, send_request,
    },
    crate::{
        handlers::test_support::StartedBroker, network::codec::KafkaCodec,
        test_support::start_broker_with,
    },
    bytes::BytesMut,
    futures_util::StreamExt as _,
    krabka_protocol::Encode as _,
    krabka_units::{Time, convert::TimeExt as _},
    std::time::Duration,
    tokio::{net::TcpStream, time::Instant},
    tokio_util::codec::Framed,
};

/// The idle window every case configures.
const IDLE: Duration = Duration::from_secs(1);
/// The topic a `Fetch` case long-polls. It is created empty, so a fetch on
/// it waits out its whole `max_wait_ms`.
const TOPIC: &str = "idle-in-flight";
/// A bound on how long one case may take on the paused clock. It is far
/// longer than any timeline a case expects, so a connection that is never
/// closed fails the case instead of hanging it.
const TIMELINE_BOUND: Duration = Duration::from_secs(60);

/// What the client sees after it sends the measured request, and when, in
/// milliseconds after the send, rounded down to a multiple of
/// [`RESOLUTION_MS`]. Tokio's timer wheel fires a deadline on the first
/// millisecond tick after it, so on a paused clock each timer an event waits
/// on lands it up to a millisecond late.
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    /// A response frame arrived.
    Response(u128),
    /// The broker closed the connection.
    Closed(u128),
}

/// The request whose timeline a case records.
#[derive(Clone, Copy)]
enum Measured {
    /// A `Fetch` on an empty partition, which its handler holds for
    /// `max_wait`.
    Fetch { max_wait: Duration },
    /// An `ApiVersions` that trips a request quota and so mutes the
    /// connection for `mute` once its response is written.
    Throttled { mute: Duration },
}

/// The resolution of a [`Seen`] timeline: coarse enough to absorb the
/// timer wheel's rounding, and far finer than any interval a case tells
/// apart.
const RESOLUTION_MS: u128 = 10;

const fn ms(millis: u64) -> Duration { Duration::from_millis(millis) }

/// Starts a broker whose listeners are held to [`IDLE`], prepares a
/// connection for `measured`, then pauses the clock and returns the
/// connection's timeline after `measured` is sent.
async fn timeline(measured: Measured) -> Vec<Seen> {
    let throttle_max = match measured {
        Measured::Throttled { mute } => mute,
        Measured::Fetch { .. } => IDLE,
    };
    let StartedBroker {
        broker: handle,
        dir: _dir,
    } = start_broker_with(|cfg| {
        cfg.connections_max_idle = Some(Time::from_std(IDLE));
        cfg.quota_throttle_max = Time::from_std(throttle_max);
    })
    .await;
    // A request quota of one microsecond of handler time per second makes
    // the first request earn the whole `quota_throttle_max`.
    let quotas: &[AnonymousQuota<'_>] = match measured {
        Measured::Throttled { .. } => &[quota("request_percentage", 0.0001)],
        Measured::Fetch { .. } => &[],
    };
    if !quotas.is_empty() {
        seed_anonymous_quotas(&handle, quotas).await;
    }
    let (server, mut framed) = connect_to_serve_loop(&handle).await;
    if let Measured::Fetch { .. } = measured {
        create_topic(&mut framed, TOPIC).await;
    }

    tokio::time::pause();
    let sent_at = Instant::now();
    match measured {
        Measured::Fetch { max_wait } => {
            let mut request = consumer_fetch_request(TOPIC);
            request.max_wait_ms = i32::try_from(max_wait.as_millis()).expect("max wait");
            let mut body = BytesMut::new();
            request
                .encode(&mut body, FETCH_VERSION)
                .expect("encode Fetch");
            send_request(&mut framed, FETCH_KEY, FETCH_VERSION, 1, &body).await;
        }
        Measured::Throttled { .. } => send_api_versions(&mut framed, 1).await,
    }
    let seen = tokio::time::timeout(TIMELINE_BOUND, record(&mut framed, sent_at))
        .await
        .expect("the broker must close the connection");
    tokio::time::resume();

    drop(framed);
    server.await.expect("serve loop joins");
    handle.shutdown().await;
    seen
}

/// Reads `framed` until the broker closes it.
async fn record(framed: &mut Framed<TcpStream, KafkaCodec>, sent_at: Instant) -> Vec<Seen> {
    let at = || Instant::now().duration_since(sent_at).as_millis() / RESOLUTION_MS * RESOLUTION_MS;
    let mut seen = Vec::new();
    while let Some(Ok(_)) = framed.next().await {
        seen.push(Seen::Response(at()));
    }
    seen.push(Seen::Closed(at()));
    seen
}

/// The window keeps running from the last socket activity while the request
/// is in its handler and while the connection is muted after it.
#[yare::parameterized(
    handler_slower_than_the_window = {
        Measured::Fetch { max_wait: ms(5000) },
        vec![Seen::Closed(1000)],
    },
    handler_faster_than_the_window = {
        Measured::Fetch { max_wait: ms(600) },
        vec![Seen::Response(600), Seen::Closed(1600)],
    },
    mute_longer_than_the_window = {
        Measured::Throttled { mute: ms(3000) },
        vec![Seen::Response(0), Seen::Closed(1000)],
    },
    mute_shorter_than_the_window = {
        Measured::Throttled { mute: ms(400) },
        vec![Seen::Response(0), Seen::Closed(1000)],
    },
)]
#[test_macro(tokio::test)]
async fn the_idle_window_runs_through_handler_and_mute_time(
    measured: Measured,
    expected: Vec<Seen>,
) {
    assert2::assert!(timeline(measured).await == expected);
}
