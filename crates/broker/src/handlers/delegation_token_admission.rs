//! KIP-48 admission checks shared by the delegation-token handlers.
//!
//! `RenewDelegationToken` and `ExpireDelegationToken` share Kafka's order up
//! to the `hmac` lookup through [`admit_token_request`].

use {
    crate::{
        codes::{
            DELEGATION_TOKEN_AUTH_DISABLED, DELEGATION_TOKEN_NOT_FOUND,
            DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, UNSUPPORTED_VERSION,
        },
        features::{METADATA_VERSION, require_feature},
        metadata_source::MetadataSource,
        network::auth::ConnectionAuth,
    },
    krabka_metadata::{DelegationToken, metadata_version},
    krabka_security::{Principal, SecretBytes},
    krabka_verified::delegation_token::{TokenApi, TokenApiAdmission},
};

/// Kafka's `DelegationTokenManager.ERROR_TIMESTAMP`, the expiry the broker
/// answers with when it refuses the request before forwarding it.
const ERROR_TIMESTAMP: i64 = -1;

/// The checks `RenewDelegationToken` and `ExpireDelegationToken` share,
/// steps 1 and 2 of Kafka's order up to the `hmac` lookup.
///
/// Returns the authenticated principal and the token the `hmac` names, or the
/// refusal's error code and the expiry its response carries:
/// `ERROR_TIMESTAMP` for a broker-side refusal, the schema default of 0 for a
/// controller-side one.
pub fn admit_token_request<'a>(
    api: TokenApi,
    auth: &'a ConnectionAuth,
    secret_key: Option<&SecretBytes>,
    hmac: &[u8],
    controller: &dyn MetadataSource,
) -> Result<(&'a Principal, DelegationToken), (i16, i64)> {
    let refuse = |code| Err((code, 0));
    if auth.token_api_admission(api) == TokenApiAdmission::Reject {
        return Err((DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, ERROR_TIMESTAMP));
    }
    let ConnectionAuth::Authenticated { principal, .. } = auth else {
        return Err((DELEGATION_TOKEN_REQUEST_NOT_ALLOWED, ERROR_TIMESTAMP));
    };
    let Some(secret_key) = secret_key else {
        return refuse(DELEGATION_TOKEN_AUTH_DISABLED);
    };

    let image = controller.current_image();
    // KIP-48/KIP-778: KRaft delegation tokens require metadata.version >= 3.6-IV2.
    if require_feature(
        &image,
        METADATA_VERSION,
        metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
    )
    .is_err()
    {
        return refuse(UNSUPPORTED_VERSION);
    }
    let Some(token) = image
        .delegation_token_by_hmac(secret_key.as_bytes(), hmac)
        .cloned()
    else {
        return refuse(DELEGATION_TOKEN_NOT_FOUND);
    };
    Ok((principal, token))
}
