//! The broker-listener half of an admin RPC that Kafka's `KafkaApis` answers
//! with `forwardToController`: this module wraps the request in a KIP-590
//! `Envelope` under the caller's own principal and address, sends it to the
//! active controller, and unwraps the answer.
//!
//! The failure answers are `ForwardingManagerImpl`'s. An `Envelope` refused
//! with any code but `UNSUPPORTED_VERSION` becomes `UNKNOWN_SERVER_ERROR`, an
//! `UNSUPPORTED_VERSION` closes the client connection, and a controller that
//! cannot be reached becomes the `REQUEST_TIMED_OUT` of the forwarding
//! timeout.
//!
//! What runs on the active controller is the API's own handler, reached through
//! `controller_admin`. A handler that mutates state from what it reads of the
//! metadata image must run there: any other node's image can trail the
//! controller's, and a write built from a stale image rolls the newer state
//! back.

use {
    crate::{
        broker::Broker,
        codes,
        controller_admin::CONTROLLER_ADMIN_CONNECTION_ID,
        envelope::{self, ForwardedPrincipal, ForwardedRequest},
        error::BrokerError,
        handlers::{ApiKeyCode, DispatchResult, RequestContext, decode_request, encode_response},
        metadata_source::MetadataSource,
    },
    bytes::{Bytes, BytesMut},
    krabka_metadata::MetadataImage,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            envelope_request,
            envelope_response::{self, EnvelopeResponse},
        },
    },
    krabka_raft::RaftError,
    std::{future::Future, ops::ControlFlow, sync::Arc, time::Duration},
    tokio::time::{Instant, error::Elapsed, sleep, timeout_at},
};

/// `Errors.UNKNOWN_SERVER_ERROR.message()`, which the
/// `UnknownServerException` `ForwardingManagerImpl` builds carries.
const UNKNOWN_SERVER_ERROR_MESSAGE: &str =
    "The server experienced an unexpected error when processing the request.";

/// Retry a controller mutation after conflicting metadata or a pending tail.
///
/// `operation` must submit with `submit_change_if_image`; each attempt gets a
/// fresh authoritative image. The deadline covers admission and commit waits.
///
/// # Errors
/// Returns the deadline error outside the operation's own result.
pub async fn guarded_operation_on_source<T, F, Fut>(
    source: &dyn MetadataSource,
    deadline: Instant,
    mut operation: F,
) -> Result<Result<T, RaftError>, Elapsed>
where
    F: FnMut(Arc<MetadataImage>) -> Fut,
    Fut: Future<Output = Result<T, RaftError>> + Send,
{
    timeout_at(deadline, async {
        loop {
            let mut images = source.watch_image();
            let image = images.borrow_and_update().clone();
            let result = operation(image).await;
            match result {
                Err(RaftError::StaleMetadataImage | RaftError::UncommittedTail) => {
                    tokio::select! {
                        changed = images.changed() => {
                            changed.map_err(|_| RaftError::LeaderUnknown)?;
                        }
                        () = sleep(Duration::from_millis(10)) => {},
                    }
                }
                other => return other,
            }
        }
    })
    .await
}

/// Whether a `submit_change_if_image` failure ends the attempt rather than
/// the operation's own answer: a conflicting image or pending tail, which
/// [`guarded_operation_on_source`] retries, or a lost leadership, which the
/// caller answers `NOT_CONTROLLER`.
pub fn ends_guarded_attempt(error: &RaftError) -> bool {
    matches!(
        error,
        RaftError::StaleMetadataImage
            | RaftError::UncommittedTail
            | RaftError::NotLeader { .. }
            | RaftError::LeaderUnknown
    )
}

/// Route a typed controller-listener request through its own response codec.
/// Broker-listener requests continue to preprocessing and forwarding; a
/// controller listener refuses mutations after this node loses leadership.
pub(super) async fn controller_listener_dispatch<'a, Request, Response, Local, LocalFuture>(
    broker: &'a Broker,
    ctx: &'a RequestContext<'a>,
    request: Request,
    version: i16,
    local: Local,
    refusal: impl FnOnce(&Request, i16, Option<&str>) -> Response,
) -> ControlFlow<DispatchResult, Request>
where
    Response: Encode,
    Local: FnOnce(&'a Broker, Request, i16, &'a RequestContext<'a>) -> LocalFuture,
    LocalFuture: Future<Output = Result<Response, BrokerError>>,
{
    if ctx.connection_id != CONTROLLER_ADMIN_CONNECTION_ID {
        return ControlFlow::Continue(request);
    }
    if !is_active_controller(broker) {
        let known_leader = *broker.controller.watch_leader().borrow();
        return ControlFlow::Break(encode_response(
            &not_active_controller_refusal(&request, known_leader, refusal),
            version,
        ));
    }
    ControlFlow::Break(
        local(broker, request, version, ctx)
            .await
            .and_then(|response| encode_response(&response, version)),
    )
}

/// The answer of a node that is not the active controller to a request that
/// only the active controller runs, built by the API's own `refusal`.
///
/// Kafka's `QuorumController.ControllerWriteEvent.run` fails such a request
/// with `ControllerExceptions.newWrongControllerException(latestController())`,
/// which names `known_leader`, the controller leader this node knows of. The
/// `getErrorResponse` of each API that routes here keeps
/// `ApiError.fromThrowable(e).message()`, so the refusal carries that text.
pub(super) fn not_active_controller_refusal<Request, Response>(
    request: &Request,
    known_leader: Option<krabka_metadata::NodeId>,
    refusal: impl FnOnce(&Request, i16, Option<&str>) -> Response,
) -> Response {
    refusal(
        request,
        codes::NOT_CONTROLLER,
        Some(&wrong_controller_message(known_leader)),
    )
}

/// Forward a broker-listener mutation before running its controller handler.
pub(super) async fn forward_if_not_controller(
    broker: &Broker,
    ctx: &RequestContext<'_>,
    api_key: ApiKeyCode,
    req_bytes: &[u8],
    version: i16,
    refusal: impl Fn(i16, Option<&str>) -> DispatchResult,
) -> Option<DispatchResult> {
    if ctx.connection_id == CONTROLLER_ADMIN_CONNECTION_ID {
        None
    } else {
        to_active_controller(broker, api_key, req_bytes, version, ctx, refusal).await
    }
}
/// `ControllerExceptions.newWrongControllerException`'s message, the one a
/// request that only the active controller may run carries when it reaches
/// another node.
pub fn wrong_controller_message(leader: Option<krabka_metadata::NodeId>) -> String {
    leader.map_or_else(
        || "No controller appears to be active.".to_owned(),
        |leader| format!("The active controller appears to be node {leader}."),
    )
}

/// Whether this node is the active controller, the one node that may run a
/// request that only the active controller answers.
pub fn is_active_controller(broker: &Broker) -> bool {
    *broker.controller.watch_leader().borrow() == Some(broker.config.node_id)
}

/// Forwards the request of `api_key` to the active controller and returns the
/// answer, or `None` when this node is the active controller and answers it
/// itself.
///
/// `refusal` encodes the API's own response for an error code and message, for
/// the answers that `ForwardingManagerImpl` makes up when the controller gives
/// none.
pub async fn to_active_controller(
    broker: &Broker,
    api_key: ApiKeyCode,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
    refusal: impl Fn(i16, Option<&str>) -> Result<Bytes, BrokerError>,
) -> Option<Result<Bytes, BrokerError>> {
    let body = match build(broker, api_key, req_bytes, version, ctx) {
        Ok(body) => body,
        Err(error) => return Some(Err(error)),
    };
    let forwarded = broker
        .controller
        .forward_raw(
            envelope_request::API_KEY,
            envelope_request::MIN_VERSION,
            body,
        )
        .await?;
    Some(match forwarded {
        Ok(envelope_response) => unwrap(broker, api_key, &envelope_response, version, refusal),
        Err(_) => refusal(codes::REQUEST_TIMED_OUT, None),
    })
}

/// Forward a request to the active controller, or handle it locally when this
/// broker is the active controller.
pub async fn to_active_controller_or_handle<Request, Response, Refusal, Local, LocalFuture>(
    broker: &Broker,
    request: Request,
    api_key: ApiKeyCode,
    version: i16,
    ctx: &RequestContext<'_>,
    refusal: Refusal,
    local: Local,
) -> Result<Response, BrokerError>
where
    Request: Encode,
    Response: for<'a> Decode<'a> + Encode,
    Refusal: Fn(&Request, i16, Option<&str>) -> Response,
    Local: FnOnce(Request) -> LocalFuture,
    LocalFuture: Future<Output = Result<Response, BrokerError>>,
{
    let body = encode_response(&request, version)?;
    let refusal =
        |code, message: Option<&str>| encode_response(&refusal(&request, code, message), version);
    match to_active_controller(broker, api_key, &body, version, ctx, refusal).await {
        Some(answer) => decode_request(&answer?, version),
        None => local(request).await,
    }
}

/// Wrap `req_bytes` (an `api_key` request body at `version`) in a KIP-590
/// `EnvelopeRequest` carrying `ctx`'s principal and peer address, and encode
/// it at the envelope's one wire version.
///
/// # Errors
/// Returns an error if the generated codec rejects the assembled envelope.
pub fn build(
    broker: &Broker,
    api_key: ApiKeyCode,
    req_bytes: &[u8],
    version: i16,
    ctx: &RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let request_data = envelope::wrap_request(&ForwardedRequest {
        api_key,
        api_version: version,
        correlation_id: 0,
        client_id: ctx.client_id.map(ToOwned::to_owned),
        body: Bytes::copy_from_slice(req_bytes),
        body_flexible: broker.handlers().body_flexible(api_key, version),
    });
    let principal = ForwardedPrincipal {
        name: ctx.principal.name.clone(),
        token_authenticated: false,
    };
    let request = envelope::envelope_request(request_data, &principal, ctx.peer.ip())?;
    let mut out = BytesMut::with_capacity(request.encoded_len(envelope_request::MIN_VERSION));
    request.encode(&mut out, envelope_request::MIN_VERSION)?;
    Ok(out.freeze())
}

fn unwrap(
    broker: &Broker,
    api_key: ApiKeyCode,
    envelope_bytes: &[u8],
    version: i16,
    refusal: impl Fn(i16, Option<&str>) -> Result<Bytes, BrokerError>,
) -> Result<Bytes, BrokerError> {
    let mut cur = envelope_bytes;
    let resp = EnvelopeResponse::decode(&mut cur, envelope_response::MIN_VERSION)?;
    if resp.error_code == codes::UNSUPPORTED_VERSION {
        return Err(BrokerError::UnsupportedApi { api_key, version });
    }
    if resp.error_code != codes::NONE {
        return refusal(
            codes::UNKNOWN_SERVER_ERROR,
            Some(UNKNOWN_SERVER_ERROR_MESSAGE),
        );
    }
    let flexible = broker.handlers().body_flexible(api_key, version);
    let data = resp.response_data.unwrap_or_default();
    let (_, body) = envelope::unwrap_response(api_key, flexible, &data)?;
    Ok(body)
}

#[cfg(test)]
mod tests {
    use {
        super::{guarded_operation_on_source, not_active_controller_refusal},
        crate::{codes, test_support::FakeMetadataSource},
        krabka_raft::{NodeId, RaftError},
        std::{
            sync::atomic::{AtomicUsize, Ordering},
            time::Duration,
        },
        tokio::time::Instant,
    };

    /// A guarded operation never answers `UncommittedTail`. While a new leader
    /// has not committed its own epoch, the operation runs again, and it gets
    /// the deadline error if the tail is still there when the deadline passes.
    /// The callers answer that error with the retriable `REQUEST_TIMED_OUT`.
    #[tokio::test(start_paused = true)]
    async fn an_uncommitted_tail_retries_until_the_deadline() {
        let source = FakeMetadataSource::builder().build();
        let attempts = AtomicUsize::new(0);

        let result = guarded_operation_on_source(
            &source,
            Instant::now() + Duration::from_secs(5),
            |_image| {
                attempts.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(RaftError::UncommittedTail) }
            },
        )
        .await;

        assert2::assert!(let Err(_) = result);
        assert2::assert!(attempts.load(Ordering::SeqCst) > 1);
    }

    /// A controller-listener request that reaches a node that is not the
    /// active controller answers `NOT_CONTROLLER` with the message of
    /// Kafka's `ControllerExceptions.newWrongControllerException`, which
    /// `QuorumController.ControllerWriteEvent.run` builds from
    /// `latestController()`, the raft leader this node knows of. The
    /// `getErrorResponse` of `AlterConfigsRequest`,
    /// `IncrementalAlterConfigsRequest` and `ElectLeadersRequest` keeps
    /// `ApiError.fromThrowable(e).message()`, so the refusal carries that text.
    #[yare::parameterized(
        known_leader = {
            Some(NodeId(3)),
            (codes::NOT_CONTROLLER, Some("The active controller appears to be node 3.".to_owned())),
        },
        no_known_leader = {
            None,
            (codes::NOT_CONTROLLER, Some("No controller appears to be active.".to_owned())),
        },
    )]
    fn a_node_that_is_not_the_active_controller_names_the_leader_it_knows(
        known_leader: Option<NodeId>,
        expected: (i16, Option<String>),
    ) {
        let refusal = not_active_controller_refusal(&(), known_leader, |(), code, message| {
            (code, message.map(str::to_owned))
        });
        assert2::assert!(refusal == expected);
    }
}
