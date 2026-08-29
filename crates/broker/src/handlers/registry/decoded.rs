//! Registration tables for the apis whose adapter decodes the request body
//! first and hands the handler a typed message instead of a byte slice.
//!
//! `AlterUserScramCredentials` and `UpdateFeatures` get hand-written adapters
//! because their handlers return a response struct that the adapter still has
//! to encode, which the table macros do not do.

use bytes::Bytes;
use futures_util::future::BoxFuture;
use krabka_protocol::api_key::ApiKey;

use super::{DispatchEntry, DispatchRegistry};
use crate::{
    broker::Broker,
    error::BrokerError,
    handlers::{ApiVersion, CorrelationId, RequestContext},
};

decoded_context_dispatches!(register_decoded_context_dispatches;
    (create_acls_adapter, CreateAcls, create_acls_request, CreateAclsRequest, crate::handlers::create_acls::handle),
    (delete_acls_adapter, DeleteAcls, delete_acls_request, DeleteAclsRequest, crate::handlers::delete_acls::handle),
    (elect_leaders_adapter, ElectLeaders, elect_leaders_request, ElectLeadersRequest, crate::handlers::elect_leaders::handle),
    (alter_partition_reassignments_adapter, AlterPartitionReassignments, alter_partition_reassignments_request, AlterPartitionReassignmentsRequest, crate::handlers::alter_partition_reassignments::handle),
    (alter_client_quotas_adapter, AlterClientQuotas, alter_client_quotas_request, AlterClientQuotasRequest, crate::handlers::alter_client_quotas::handle),
);

decoded_sync_context_dispatches!(register_decoded_sync_context_dispatches;
    (describe_acls_adapter, DescribeAcls, describe_acls_request, DescribeAclsRequest, crate::handlers::describe_acls::handle),
    (list_partition_reassignments_adapter, ListPartitionReassignments, list_partition_reassignments_request, ListPartitionReassignmentsRequest, crate::handlers::list_partition_reassignments::handle),
    (describe_client_quotas_adapter, DescribeClientQuotas, describe_client_quotas_request, DescribeClientQuotasRequest, crate::handlers::describe_client_quotas::handle),
    (describe_user_scram_credentials_adapter, DescribeUserScramCredentials, describe_user_scram_credentials_request, DescribeUserScramCredentialsRequest, crate::handlers::describe_user_scram_credentials::handle),
);

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
