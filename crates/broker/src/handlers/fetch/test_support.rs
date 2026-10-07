//! Fetch wire fixtures shared by version and replica-role tests.

use assert2::assert;
use krabka_protocol::{
    Decode,
    owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
};

pub(super) async fn fetch_wire(
    broker: &crate::broker::BrokerHandle,
    version: i16,
    user: &str,
    client_id: &str,
    request: &FetchRequest,
) -> FetchResponse {
    let shared = broker.broker_arc_for_test();
    let user = crate::test_support::principal(user);
    let peer = crate::test_support::peer();
    let context = crate::test_support::request_context(&user, &peer, client_id);
    let bytes = crate::test_support::encode_request(request, version);
    let (response, response_version) = super::handle(&shared, version, 7, &bytes, &context)
        .await
        .expect("handle fetch");
    let wire = super::encode_fetch_response(response, response_version).expect("encode response");
    let mut cursor: &[u8] = &wire;
    let decoded = FetchResponse::decode(&mut cursor, version).expect("decode response");
    assert!(cursor.is_empty(), "the decoder consumed every byte");
    decoded
}
