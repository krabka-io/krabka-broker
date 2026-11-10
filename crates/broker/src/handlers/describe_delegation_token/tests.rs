//! `DescribeDelegationToken` tests that drive [`super::handle`] against a live
//! single-voter controller.
//!
//! The visible-set rules (`filterToken`, the empty-list sentinel and
//! `allowTokenRequests`) are scenarios in `visibility.feature`; this file
//! pins the wire projection of a visible token.

use {
    super::handle,
    crate::{
        authorizer::SimpleAclAuthorizer,
        handlers::delegation_token_test_support::{TokenFixture, authed, kp, seed_token},
    },
    assert2::assert,
    krabka_protocol::owned::{
        describe_delegation_token_request::DescribeDelegationTokenRequest,
        describe_delegation_token_response::{
            DescribedDelegationToken, DescribedDelegationTokenRenewer,
        },
    },
    std::{collections::HashSet, net::SocketAddr},
};

/// The response names the requester that created the token, not the owner:
/// `DescribeDelegationTokenResponse` v3 `TokenRequesterPrincipalType` and
/// `TokenRequesterPrincipalName` (Kafka's `tokenInfo().tokenRequester()`).
#[tokio::test]
async fn describe_response_reports_the_requester_that_created_the_token() {
    let TokenFixture {
        directory: _dir,
        controller,
        secret,
    } = TokenFixture::new().await;
    seed_token()
        .controller(&controller)
        .token_id("t-minted")
        .maybe_requester(Some(kp("admin")))
        .renewers(vec![kp("bob")])
        .call()
        .await;
    let peer: SocketAddr = "127.0.0.1:0".parse().unwrap();

    let resp = handle(
        &DescribeDelegationTokenRequest {
            owners: None,
            ..Default::default()
        },
        &authed("alice"),
        Some(&secret),
        &*controller,
        &peer,
        &SimpleAclAuthorizer::new(HashSet::new()),
    );
    assert!(
        resp.tokens
            == vec![DescribedDelegationToken {
                principal_type: "User".into(),
                principal_name: "alice".into(),
                token_requester_principal_type: "User".into(),
                token_requester_principal_name: "admin".into(),
                issue_timestamp: 1_000,
                expiry_timestamp: 2_000,
                max_timestamp: 3_000,
                token_id: "t-minted".into(),
                hmac: bytes::Bytes::from(krabka_security::compute_token_hmac(b"k", "t-minted")),
                renewers: vec![DescribedDelegationTokenRenewer {
                    principal_type: "User".into(),
                    principal_name: "bob".into(),
                    ..Default::default()
                }],
                ..Default::default()
            }]
    );
    controller.cancel().await;
}
