//! One send-and-decode helper per KIP-48 delegation-token RPC, each pinned to
//! the newest version the broker advertises.
//!
//! Every helper takes a stream that is already authenticated and a monotonic
//! correlation id, so the tests read as a sequence of protocol steps rather
//! than of encode/decode boilerplate.

use {
    crate::{
        support::wire::{
            CorrelationId as WireCorrelationId, HeaderEncoding as RequestHeaderEncoding,
            SocketExchangeSetup,
        },
        wire::round_trip,
    },
    bytes::BytesMut,
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{Decode, Encode, owned as protocol},
    std::io,
    tokio::{
        io::{AsyncRead, AsyncWrite},
        net::TcpStream,
    },
};

type CreateRequest = protocol::create_delegation_token_request::CreateDelegationTokenRequest;
type CreateResponse = protocol::create_delegation_token_response::CreateDelegationTokenResponse;
type DescribeRequest = protocol::describe_delegation_token_request::DescribeDelegationTokenRequest;
type DescribeResponse =
    protocol::describe_delegation_token_response::DescribeDelegationTokenResponse;
type ExpireRequest = protocol::expire_delegation_token_request::ExpireDelegationTokenRequest;
type ExpireResponse = protocol::expire_delegation_token_response::ExpireDelegationTokenResponse;
type RenewRequest = protocol::renew_delegation_token_request::RenewDelegationTokenRequest;
type RenewResponse = protocol::renew_delegation_token_response::RenewDelegationTokenResponse;

// ─────────────────────────────────────────────────────────────────────────────
// Delegation-token wire helpers — encode one request at the negotiated MAX
// version (the broker advertises 1..=3 for Create/Describe and 1..=2 for
// Renew/Expire). Each helper takes an already-authenticated stream and a
// monotonic `corr_id`.
// ─────────────────────────────────────────────────────────────────────────────

/// Newest `CreateDelegationToken` that Krabka supports: Apache Kafka v3,
/// flexible. `MAX_VERSION` here exercises the same wire shape that the JVM
/// admin client uses against a modern broker.
const CREATE_DT_VERSION: i16 = krabka_protocol::owned::create_delegation_token_request::MAX_VERSION;
const RENEW_DT_VERSION: i16 = krabka_protocol::owned::renew_delegation_token_request::MAX_VERSION;
const EXPIRE_DT_VERSION: i16 = krabka_protocol::owned::expire_delegation_token_request::MAX_VERSION;
const DESCRIBE_DT_VERSION: i16 =
    krabka_protocol::owned::describe_delegation_token_request::MAX_VERSION;

pub(crate) async fn send_create_delegation_token<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    corr_id: i32,
    req: &CreateRequest,
) -> Result<CreateResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("CreateDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(38),
            version: WireApiVersion(CREATE_DT_VERSION),
            correlation: WireCorrelationId(corr_id),
            header: RequestHeaderEncoding::Flexible,
        },
        &body,
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateResponse::decode(&mut cur, CREATE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("CreateDelegationToken decode: {e}")))
}

pub(crate) async fn send_renew_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &RenewRequest,
) -> Result<RenewResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, RENEW_DT_VERSION)
        .map_err(|e| io::Error::other(format!("RenewDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(39),
            version: WireApiVersion(RENEW_DT_VERSION),
            correlation: WireCorrelationId(corr_id),
            header: RequestHeaderEncoding::Flexible,
        },
        &body,
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    RenewResponse::decode(&mut cur, RENEW_DT_VERSION)
        .map_err(|e| io::Error::other(format!("RenewDelegationToken decode: {e}")))
}

pub(crate) async fn send_expire_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &ExpireRequest,
) -> Result<ExpireResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, EXPIRE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("ExpireDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(40),
            version: WireApiVersion(EXPIRE_DT_VERSION),
            correlation: WireCorrelationId(corr_id),
            header: RequestHeaderEncoding::Flexible,
        },
        &body,
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    ExpireResponse::decode(&mut cur, EXPIRE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("ExpireDelegationToken decode: {e}")))
}

pub(crate) async fn send_describe_delegation_token(
    stream: &mut TcpStream,
    corr_id: i32,
    req: &DescribeRequest,
) -> Result<DescribeResponse, io::Error> {
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeDelegationToken encode: {e}")))?;
    let resp_bytes = round_trip(
        stream,
        SocketExchangeSetup {
            api_key: WireApiKey(41),
            version: WireApiVersion(DESCRIBE_DT_VERSION),
            correlation: WireCorrelationId(corr_id),
            header: RequestHeaderEncoding::Flexible,
        },
        &body,
    )
    .await?;
    let mut cur: &[u8] = &resp_bytes;
    DescribeResponse::decode(&mut cur, DESCRIBE_DT_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeDelegationToken decode: {e}")))
}

/// Independent wire expectations for a freshly created token's identity and HMAC.
pub fn check_created_identity(response: &CreateResponse, owner: &str, requester: &str) {
    assert2::check!(response.principal_type == "User");
    assert2::check!(response.principal_name == owner);
    assert2::check!(response.token_requester_principal_type == "User");
    assert2::check!(response.token_requester_principal_name == requester);
    assert2::check!(!response.token_id.is_empty(), "token_id must be set");
    assert2::check!(response.hmac.len() == 64, "HMAC length must be 64 bytes");
}
