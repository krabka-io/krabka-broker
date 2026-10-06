//! Hand-written adapters for `AlterUserScramCredentials` and `UpdateFeatures`,
//! which decode the request body like the table's `decoded` adapters do but
//! whose handlers return a response struct that the adapter still has to
//! encode. The registry's `dispatch_table!` registers them as
//! `custom_context` entries.

use bytes::Bytes;
use futures_util::future::BoxFuture;

use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, RequestContext},
};

pub(super) fn alter_user_scram_credentials_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::alter_user_scram_credentials_request::AlterUserScramCredentialsRequest::decode(
            &mut cur,
            version,
        )?;
        let resp = crate::handlers::alter_user_scram_credentials::handle(broker, req, ctx).await;
        crate::handlers::encode_response(&resp, version)
    })
}

pub(super) fn update_features_adapter<'a>(
    broker: &'a Broker,
    version: ApiVersion,
    _correlation_id: CorrelationId,
    body: &'a [u8],
    ctx: &'a RequestContext<'a>,
) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
    Box::pin(async move {
        use krabka_protocol::Decode;

        let mut cur = body;
        let req = krabka_protocol::owned::update_features_request::UpdateFeaturesRequest::decode(
            &mut cur, version,
        )?;
        let resp = crate::handlers::update_features::handle(broker, req, version, ctx).await;
        crate::handlers::encode_response(&resp, version)
    })
}
