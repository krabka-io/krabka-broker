//! The wire layer between the broker and the OPA decision endpoint: the
//! Strimzi-compatible JSON envelope, the mapping from Kafka's ACL vocabulary
//! onto the strings that a Rego policy matches on, and the single POST that
//! turns an OPA response into an [`AuthorizationResult`].

use krabka_authz::{AuthorizationRequest, AuthorizationResult};
use krabka_metadata::{AclOperation, ResourceType};
use serde::{Deserialize, Serialize};

use super::OpaAuthorizer;

/// Outer envelope of the Strimzi-compatible OPA request.
#[derive(Debug, Serialize)]
struct OpaRequest<'a> {
    input: OpaInput<'a>,
}

#[derive(Debug, Serialize)]
struct OpaInput<'a> {
    request: OpaRequestInner<'a>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaRequestInner<'a> {
    principal: String,
    operation: &'a str,
    resource: OpaResource<'a>,
    host: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpaResource<'a> {
    resource_type: &'a str,
    name: &'a str,
    pattern_type: &'a str,
}

/// Decision payload returned by OPA. Strimzi expects exactly
/// `{"result": true|false}`. Anything else parses as an error, and the
/// caller falls through to [`OpaAuthorizer::error_decision`].
#[derive(Debug, Deserialize)]
struct OpaResponse {
    result: bool,
}

impl OpaAuthorizer {
    /// POST the request to OPA and translate the boolean response into
    /// the binary decision. Any HTTP-level or JSON-level error falls through
    /// to [`Self::error_decision`], which honours `allow_on_error`.
    pub(super) async fn call_opa(&self, req: &AuthorizationRequest<'_>) -> AuthorizationResult {
        let body = OpaRequest {
            input: OpaInput {
                request: OpaRequestInner {
                    principal: format!("User:{}", req.principal.name),
                    operation: operation_str(req.operation),
                    resource: OpaResource {
                        resource_type: resource_type_str(req.resource_type),
                        name: req.resource_name,
                        pattern_type: "Literal",
                    },
                    host: req.host.ip().to_string(),
                },
            },
        };
        match self.http_client.post(&self.url).json(&body).send().await {
            Ok(resp) => match resp.json::<OpaResponse>().await {
                Ok(r) => {
                    if r.result {
                        AuthorizationResult::Allow
                    } else {
                        AuthorizationResult::Deny
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, url = %self.url, "OPA response parse failed");
                    self.error_decision()
                }
            },
            Err(e) => {
                tracing::warn!(error = %e, url = %self.url, "OPA HTTP call failed");
                self.error_decision()
            }
        }
    }
}

/// Map [`AclOperation`] to its Strimzi-compatible OPA wire string. The
/// vocabulary mirrors Kafka's `AclOperation.name()` exactly so existing
/// Strimzi Rego policies port unchanged.
fn operation_str(op: AclOperation) -> &'static str {
    match op {
        AclOperation::All => "All",
        AclOperation::Read => "Read",
        AclOperation::Write => "Write",
        AclOperation::Create => "Create",
        AclOperation::Delete => "Delete",
        AclOperation::Alter => "Alter",
        AclOperation::Describe => "Describe",
        AclOperation::ClusterAction => "ClusterAction",
        AclOperation::DescribeConfigs => "DescribeConfigs",
        AclOperation::AlterConfigs => "AlterConfigs",
        AclOperation::IdempotentWrite => "IdempotentWrite",
        AclOperation::TwoPhaseCommit => "TwoPhaseCommit",
    }
}

/// Map [`ResourceType`] to its Strimzi-compatible OPA wire string.
fn resource_type_str(t: ResourceType) -> &'static str {
    match t {
        ResourceType::Topic => "Topic",
        ResourceType::Group => "Group",
        ResourceType::Cluster => "Cluster",
        ResourceType::TransactionalId => "TransactionalId",
        ResourceType::DelegationToken => "DelegationToken",
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    /// The OPA input's `operation` string must be the Strimzi-compatible name,
    /// including KIP-939's `TwoPhaseCommit`. This pins the mapping, so the
    /// test catches a regression or a blanket mutation of `operation_str`.
    #[test]
    fn operation_str_maps_kafka_names() {
        for (op, want) in [
            (AclOperation::Read, "Read"),
            (AclOperation::Write, "Write"),
            (AclOperation::TwoPhaseCommit, "TwoPhaseCommit"),
        ] {
            assert!(operation_str(op) == want, "{op:?}");
        }
    }
}
