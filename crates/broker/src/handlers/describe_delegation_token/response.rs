//! The `DescribeDelegationToken` response shapes: the wire projection of one
//! visible `krabka_metadata::DelegationToken`, and the error-only envelope the
//! handler returns before it has a caller to filter for.
//!
//! These are the response's contract with the JVM `AdminClient`, including the
//! KIP-48 token-requester fields that Krabka always fills from the owner, so
//! they sit apart from the code that decides which tokens are visible.

use krabka_protocol::owned::describe_delegation_token_response::{
    DescribeDelegationTokenResponse, DescribedDelegationToken, DescribedDelegationTokenRenewer,
};

pub(super) fn describe_token(t: krabka_metadata::DelegationToken) -> DescribedDelegationToken {
    DescribedDelegationToken {
        principal_type: t.owner.principal_type.clone(),
        principal_name: t.owner.name.clone(),
        // KIP-48 token-requester = owner; we don't support the
        // privileged "act-as" path so these are always equal.
        token_requester_principal_type: t.owner.principal_type,
        token_requester_principal_name: t.owner.name,
        issue_timestamp: t.issue_timestamp_ms,
        expiry_timestamp: t.expiry_timestamp_ms,
        max_timestamp: t.max_timestamp_ms,
        token_id: t.token_id,
        hmac: bytes::Bytes::from(t.hmac),
        renewers: t
            .renewers
            .into_iter()
            .map(|r| DescribedDelegationTokenRenewer {
                principal_type: r.principal_type,
                principal_name: r.name,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    }
}

pub(super) fn err_response(code: i16) -> DescribeDelegationTokenResponse {
    DescribeDelegationTokenResponse {
        error_code: code,
        ..Default::default()
    }
}
