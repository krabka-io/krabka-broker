//! `AlterClientQuotas` (`api_key` 49, KIP-13/124/257).
//!
//! This file holds the wire entry point: the cluster `AlterConfigs` authorization
//! preamble, the loop that validates each entry, and the single metadata
//! submit that carries every accepted entry. Entry validation and the records
//! it produces live in `entries`; the response rows live in `response`.

use {
    crate::{
        broker::Broker,
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext, audit_admin_success, audit_resource,
            cluster_alter_configs_denied,
        },
    },
    krabka_protocol::owned::{
        alter_client_quotas_request::{self, AlterClientQuotasRequest},
        alter_client_quotas_response::AlterClientQuotasResponse,
    },
    krabka_raft::RaftError,
    std::{collections::HashSet, time::Duration},
    tokio::time::Instant,
};

mod entries;
mod response;

#[cfg(test)]
mod test_support;
#[cfg(test)]
mod tests;

use {
    self::{
        entries::{Alteration, alter_client_quotas},
        response::{apply_submit_error, err_entry, ok_entry, whole_request_error},
    },
    crate::{
        codes::{
            CLUSTER_AUTHORIZATION_FAILED, NONE, NOT_CONTROLLER, REQUEST_TIMED_OUT,
            UNKNOWN_SERVER_ERROR,
        },
        handlers::forward_to_controller::{
            ends_guarded_attempt, guarded_operation_on_source, to_active_controller_or_handle,
        },
        metadata_source::MetadataSource,
        quota::parse_ip_literal,
    },
    krabka_client_core::transport,
    krabka_protocol::owned::alter_client_quotas_response::EntryData,
};

pub async fn handle(
    broker: &Broker,
    req: AlterClientQuotasRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<AlterClientQuotasResponse, BrokerError> {
    // Kafka's `KafkaApis.handleAlterClientQuotasRequest` forwards the request
    // to the active controller, which plans and writes it against its own
    // image.
    to_active_controller_or_handle(
        broker,
        req,
        alter_client_quotas_request::API_KEY,
        version,
        ctx,
        request_refusal,
        |req| answer(broker, req, ctx),
    )
    .await
}

/// Runs the request on the active controller.
async fn answer(
    broker: &Broker,
    req: AlterClientQuotasRequest,
    ctx: &RequestContext<'_>,
) -> Result<AlterClientQuotasResponse, BrokerError> {
    let image = broker.controller.current_image();
    // Kafka's `ControllerApis.handleAlterClientQuotas` authorizes
    // `AlterConfigs` on the cluster. `Alter` does not imply it. A denial
    // answers `AlterClientQuotasRequest.getErrorResponse`: every entry
    // carries the error code and the default message of the error.
    if cluster_alter_configs_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return Ok(whole_request_error(
            &req,
            CLUSTER_AUTHORIZATION_FAILED,
            "Cluster authorization failed.",
        ));
    }

    let resolvable = resolve_ip_names(&req).await;
    let ip_is_valid = |name: &str| parse_ip_literal(name).is_some() || resolvable.contains(name);
    let entry_results = alter_and_submit(broker.controller.as_ref(), &req, &ip_is_valid).await;

    if !req.validate_only {
        audit_admin_success(
            broker.audit_log.as_ref(),
            ctx,
            "AlterClientQuotas",
            entry_results
                .iter()
                .filter(|entry| entry.error_code == NONE)
                .map(|entry| audit_resource("ClientQuotaEntity", quota_entity_name(entry)))
                .collect(),
        );
    }

    Ok(krabka_macros::unthrottled_wire!(
        AlterClientQuotasResponse {
            entries: entry_results,
        }
    ))
}

/// How long one request may wait for the controller to admit its write,
/// replanning after every intervening commit.
const SUBMIT_TIMEOUT: Duration = Duration::from_secs(5);

/// Plans `req`'s entries against the quota state of `source` and submits the
/// records they produce, returning one response row per distinct entity.
///
/// Kafka's `QuorumController.alterClientQuotas` plans and appends on the
/// controller event thread, so nothing commits between the two. Planning
/// drops a removal of a key that is not set and a set to the value already
/// set, so a plan built from an image that has since moved on can lose part
/// of the request. The plan and the append run in
/// [`guarded_operation_on_source`]: an append whose image is no longer
/// current is refused, and the request is planned again on the newer image.
async fn alter_and_submit(
    source: &dyn MetadataSource,
    req: &AlterClientQuotasRequest,
    ip_is_valid: &(dyn Fn(&str) -> bool + Sync),
) -> Vec<EntryData> {
    let result = guarded_operation_on_source(source, Instant::now() + SUBMIT_TIMEOUT, |image| {
        async move {
            let Alteration { results, records } =
                alter_client_quotas(&req.entries, image.client_quotas(), ip_is_valid);
            let mut entry_results: Vec<_> = results
                .iter()
                .map(|(entity, outcome)| match outcome {
                    Ok(()) => ok_entry(entity),
                    Err((code, msg)) => err_entry(entity, *code, msg.clone()),
                })
                .collect();
            // Kafka's `QuorumController.alterClientQuotas` drops the records
            // of a `validate_only` request and keeps its results.
            if req.validate_only || records.is_empty() {
                return Ok(entry_results);
            }
            match source.submit_change_if_image(records, image).await {
                Ok(_) => {}
                Err(error) if ends_guarded_attempt(&error) => return Err(error),
                Err(error) => {
                    tracing::warn!(%error, "alter-client-quotas submit failed");
                    apply_submit_error(&mut entry_results, error);
                }
            }
            Ok(entry_results)
        }
    })
    .await;
    // A controller failure fails the whole request, as
    // `AlterClientQuotasRequest.getErrorResponse` answers it.
    let code = match result {
        Ok(Ok(entry_results)) => return entry_results,
        Err(_) => REQUEST_TIMED_OUT,
        Ok(Err(RaftError::NotLeader { .. } | RaftError::LeaderUnknown)) => NOT_CONTROLLER,
        Ok(Err(error)) => {
            tracing::error!(%error, "alter-client-quotas submit failed");
            UNKNOWN_SERVER_ERROR
        }
    };
    request_refusal(req, code, None).entries
}

/// Every entry of `req` answered with `code` and `message`: the answer
/// `ForwardingManagerImpl` makes up when the controller gives none, and the
/// answer to a write the controller did not take.
fn request_refusal(
    req: &AlterClientQuotasRequest,
    code: i16,
    message: Option<&str>,
) -> AlterClientQuotasResponse {
    let mut response = whole_request_error(req, code, message.unwrap_or_default());
    for entry in &mut response.entries {
        entry.error_message = message.map(str::to_owned);
    }
    response
}

/// Resolves every named `ip` entity that is not an IP literal.
///
/// Kafka's `isValidIpEntity` accepts any name `InetAddress.getByName`
/// resolves, which covers host names as well as IPv4 and IPv6 literals. The
/// lookup runs here, before the synchronous validation, so that validation
/// stays a pure function of the request and the quota state.
async fn resolve_ip_names(req: &AlterClientQuotasRequest) -> HashSet<String> {
    let mut resolvable = HashSet::new();
    for entry in &req.entries {
        for component in &entry.entity {
            let Some(name) = component.entity_name.as_deref() else {
                continue;
            };
            if component.entity_type != "ip"
                || name.is_empty()
                || parse_ip_literal(name).is_some()
                || resolvable.contains(name)
            {
                continue;
            }
            // A literal address never reaches this lookup. A host name is
            // unresolvable on a platform without name resolution, WASI.
            if transport::resolve(&format!("{name}:0"))
                .await
                .is_ok_and(|addrs| !addrs.is_empty())
            {
                resolvable.insert(name.to_owned());
            }
        }
    }
    resolvable
}

/// Renders a quota entity as the audit record's resource name.
///
/// KIP-546 addresses a quota by a set of `(entity_type, entity_name)` pairs,
/// and a `None` name is the default entity for that type — what
/// `kafka-configs --entity-type users --entity-default` sets. `<default>` is
/// the spelling the JVM tools print for it.
fn quota_entity_name(entry: &EntryData) -> String {
    entry
        .entity
        .iter()
        .map(|e| {
            format!(
                "{}={}",
                e.entity_type,
                e.entity_name.as_deref().unwrap_or("<default>")
            )
        })
        .collect::<Vec<_>>()
        .join(",")
}
