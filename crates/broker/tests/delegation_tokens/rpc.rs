//! One send-and-decode helper for every KIP-48 delegation-token RPC, pinned to
//! the newest version the broker advertises.
//!
//! The helper takes a stream that is already authenticated and a monotonic
//! correlation id, so the tests read as a sequence of protocol steps rather
//! than of encode/decode boilerplate.

use {
    crate::{
        kafka_wire::{decode_named, encode_named},
        support::wire::HeaderEncoding as RequestHeaderEncoding,
        wire::round_trip,
    },
    krabka_protocol::{ProtocolRequest, owned as protocol},
    std::io,
    tokio::io::{AsyncRead, AsyncWrite},
};

type CreateRequest = protocol::create_delegation_token_request::CreateDelegationTokenRequest;
type CreateResponse = protocol::create_delegation_token_response::CreateDelegationTokenResponse;
type DescribeRequest = protocol::describe_delegation_token_request::DescribeDelegationTokenRequest;
type ExpireRequest = protocol::expire_delegation_token_request::ExpireDelegationTokenRequest;
type RenewRequest = protocol::renew_delegation_token_request::RenewDelegationTokenRequest;

/// A delegation-token request and the operation name its codec errors carry.
pub trait DelegationTokenRpc: ProtocolRequest {
    const OPERATION: &'static str;
}

impl DelegationTokenRpc for CreateRequest {
    const OPERATION: &'static str = "CreateDelegationToken";
}

impl DelegationTokenRpc for RenewRequest {
    const OPERATION: &'static str = "RenewDelegationToken";
}

impl DelegationTokenRpc for ExpireRequest {
    const OPERATION: &'static str = "ExpireDelegationToken";
}

impl DelegationTokenRpc for DescribeRequest {
    const OPERATION: &'static str = "DescribeDelegationToken";
}

/// Send one delegation-token request at its `MAX_VERSION` and decode the
/// response.
///
/// The broker advertises 1..=3 for Create/Describe and 1..=2 for
/// Renew/Expire. `MAX_VERSION` exercises the same flexible wire shape that
/// the JVM admin client uses against a modern broker.
///
/// # Errors
/// Returns the operation-labelled codec error, or the framing error of the
/// round trip.
pub async fn send_delegation_token_rpc<S, Q>(
    stream: &mut S,
    corr_id: i32,
    req: &Q,
) -> io::Result<Q::Response>
where
    S: AsyncRead + AsyncWrite + Unpin,
    Q: DelegationTokenRpc,
{
    let body = encode_named(req, Q::MAX_VERSION, Q::OPERATION)?;
    let resp_bytes = round_trip()
        .stream(stream)
        .api_key(Q::API_KEY)
        .version(Q::MAX_VERSION)
        .correlation(corr_id)
        .header(RequestHeaderEncoding::Flexible)
        .body(&body)
        .call()
        .await?;
    decode_named(&resp_bytes, Q::MAX_VERSION, Q::OPERATION)
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
