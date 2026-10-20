//! `connections.max.idle.ms` keeps running while a request is answered.
//! Kafka's `Processor` mutes a channel after each receive, so nothing
//! refreshes its idle clock until the response is queued (`Selector.send`) or
//! written. A handler that outlasts the window loses the connection and its
//! response, and a response that stops moving closes one window after its
//! last byte.

use {
    super::*,
    crate::{
        ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
        ControllerAdminRouter, ControllerApiVersion, server::api_versions,
    },
    krabka_protocol::owned::broker_heartbeat_request,
    tokio::{io::DuplexStream, time::Instant},
};

/// The window every test here runs with.
const WINDOW: Duration = Duration::from_millis(10);

/// The body length of every answer the router gives.
const ANSWER_BODY: usize = 32;

/// The bytes one answer takes on the wire: the size prefix, the correlation
/// id, the empty tagged fields of a v1 `ResponseHeader`, and the body.
const ANSWER_FRAME: usize = 4 + 4 + 1 + ANSWER_BODY;

/// An Admin router that answers `BrokerHeartbeat` after `delay`, standing in
/// for a handler that takes that long.
struct SlowRouter {
    delay: Duration,
}

impl ControllerAdminRouter for SlowRouter {
    fn api_versions(&self) -> &[ControllerApiVersion] { api_versions::BROKER_HEARTBEAT_ONLY }

    fn route(&self, _request: ControllerAdminRequest) -> ControllerAdminRouteFuture<'_> {
        let delay = self.delay;
        Box::pin(async move {
            tokio::time::sleep(delay).await;
            Ok(Some(ControllerAdminResponse {
                body: bytes::Bytes::from_static(&[0xab; ANSWER_BODY]),
                flexible: true,
            }))
        })
    }
}

/// A `BrokerHeartbeat` v0 request frame: a flexible v2 header with a null
/// `client_id`, and no body, which only the router sees.
fn heartbeat_request(correlation_id: i32) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(&broker_heartbeat_request::API_KEY.to_be_bytes());
    frame.extend_from_slice(&0_i16.to_be_bytes());
    frame.extend_from_slice(&correlation_id.to_be_bytes());
    frame.extend_from_slice(&(-1_i16).to_be_bytes());
    frame.push(0); // Flexible request header tags.
    let mut request = i32::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
    request.extend_from_slice(&frame);
    request
}

/// Serves one end of a pipe that holds `capacity` bytes each way, behind a
/// router whose answers take `delay`, under [`WINDOW`].
fn slow_connection(delay: Duration, capacity: usize) -> DuplexConnection {
    let (engine, dir) = single_voter_engine();
    let (client, server): (DuplexStream, DuplexStream) = tokio::io::duplex(capacity);
    let limits = ListenerLimits {
        max_idle: Some(WINDOW),
        ..ListenerLimits::default()
    };
    let connection = tokio::spawn(async move {
        let _dir = dir;
        handle_conn(
            server,
            engine,
            CancellationToken::new(),
            None,
            Some(Arc::new(SlowRouter { delay })),
            context(limits),
            KraftController::wall_clock_ms,
        )
        .await
    });
    DuplexConnection { client, connection }
}

/// Waits for the listener to close `connection` and returns when it did.
async fn closed_at(connection: &mut tokio::task::JoinHandle<Result<(), RaftError>>) -> Instant {
    let outcome = tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .expect("the listener closes the connection")
        .expect("the connection task does not panic");
    check!(outcome.is_ok());
    Instant::now()
}

/// A handler that answers inside the window is served. One that outlasts it
/// closes the connection one window after the request's last byte, and the
/// peer gets no response.
#[yare::parameterized(
    answers_inside_the_window = { Duration::from_millis(8), true },
    outlasts_the_window = { Duration::from_millis(15), false },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn the_idle_window_runs_while_a_handler_works(delay: Duration, served: bool) {
    let DuplexConnection {
        mut client,
        mut connection,
    } = slow_connection(delay, 1 << 16);

    client.write_all(&heartbeat_request(1)).await.unwrap();
    let last_byte = Instant::now();
    if served {
        check!(read_correlation_id(&mut client).await == 1);
        check!(!connection.is_finished());
    } else {
        check!(closed_at(&mut connection).await - last_byte == WINDOW);
        let mut answer = Vec::new();
        check!(client.read_to_end(&mut answer).await.unwrap() == 0);
    }
}

/// Each byte of a response that moves restarts the window, so a peer that
/// reads its answer slowly is served, and one that stops reading is closed
/// one window after the last byte it took.
#[tokio::test(start_paused = true)]
async fn the_idle_window_restarts_on_every_byte_of_a_response() {
    let gap = Duration::from_millis(6);
    let DuplexConnection {
        mut client,
        mut connection,
    } = slow_connection(Duration::ZERO, 1);

    // About a quarter of a second for the answer, many times the window.
    client.write_all(&heartbeat_request(1)).await.unwrap();
    let mut answer = Vec::new();
    for _ in 0..ANSWER_FRAME {
        tokio::time::sleep(gap).await;
        let mut byte = [0u8];
        client.read_exact(&mut byte).await.unwrap();
        answer.push(byte[0]);
    }
    check!(answer[4..8] == 1_i32.to_be_bytes());
    check!(!connection.is_finished());

    // Five bytes of the next answer, then the peer stops reading.
    client.write_all(&heartbeat_request(2)).await.unwrap();
    let mut last_byte = Instant::now();
    for _ in 0..5 {
        tokio::time::sleep(gap).await;
        let mut byte = [0u8];
        client.read_exact(&mut byte).await.unwrap();
        last_byte = Instant::now();
    }
    check!(closed_at(&mut connection).await - last_byte == WINDOW);
}

/// Queuing a response restarts the window before a byte of it is written.
/// The first answer fills the pipe, so the second cannot move at all: the
/// connection closes one window after the second handler finished, not one
/// window after the second request arrived.
#[tokio::test(start_paused = true)]
async fn a_queued_response_restarts_the_idle_window() {
    let delay = Duration::from_millis(8);
    let DuplexConnection {
        mut client,
        mut connection,
    } = slow_connection(delay, ANSWER_FRAME);

    client.write_all(&heartbeat_request(1)).await.unwrap();
    tokio::time::sleep(delay).await;
    client.write_all(&heartbeat_request(2)).await.unwrap();
    let request_arrived = Instant::now();
    check!(closed_at(&mut connection).await - request_arrived == delay + WINDOW);
}
