//! Authorizer decorator that counts every Deny decision and records it as an
//! audit event.

use std::sync::Arc;

use krabka_audit::{AuditEndpoint, AuditEvent, AuditLog, AuditPrincipal};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
    metrics::BrokerMetrics,
};

/// Wraps an [`Authorizer`]. It forwards decisions, bumps
/// `authorization_denied_total` and emits an audit record on every Deny. The
/// handlers audit Allow decisions for admin operations separately (Task 8),
/// so this decorator does not duplicate them.
///
/// The broker installs it whether or not audit is enabled: with
/// `audit.enabled=false` the [`AuditLog`] is the disabled one and drops the
/// emit, and the counter is the only record a Deny leaves behind.
#[derive(Debug)]
pub struct AuditingAuthorizer {
    inner: Arc<dyn Authorizer>,
    audit: Arc<AuditLog>,
    metrics: BrokerMetrics,
}

impl AuditingAuthorizer {
    #[must_use]
    pub fn new(inner: Arc<dyn Authorizer>, audit: Arc<AuditLog>, metrics: BrokerMetrics) -> Self {
        Self {
            inner,
            audit,
            metrics,
        }
    }
}

impl Authorizer for AuditingAuthorizer {
    fn authorize(
        &self,
        source: &dyn krabka_authz::AclSource,
        req: &AuthorizationRequest<'_>,
    ) -> AuthorizationResult {
        let result = self.inner.authorize(source, req);
        if result == AuthorizationResult::Deny {
            self.metrics.record_authorization_denied(
                &format!("{:?}", req.operation),
                &format!("{:?}", req.resource_type),
            );
            self.audit.emit(AuditEvent::AuthorizationDenied {
                principal: AuditPrincipal {
                    name: req.principal.name.clone(),
                    auth_method: format!("{:?}", req.principal.auth_method),
                },
                source: AuditEndpoint {
                    ip: req.host.ip().to_string(),
                    port: req.host.port(),
                },
                resource_type: format!("{:?}", req.resource_type),
                resource_name: req.resource_name.to_string(),
                operation: format!("{:?}", req.operation),
                time_ms: crate::time_util::now_ms(),
            });
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::check;
    use krabka_metadata::{AclOperation, ResourceType};
    use krabka_security::{AuthMethod, Principal};

    use super::*;
    use crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        test_support::DenyAll,
    };

    fn request_principal() -> Principal {
        Principal {
            name: "anonymous".into(),
            auth_method: AuthMethod::Anonymous,
            groups: vec![],
        }
    }

    fn denied_label() -> crate::metrics::AuthorizationDeniedLabel {
        crate::metrics::AuthorizationDeniedLabel {
            operation: "Write".to_string(),
            resource_type: "Topic".to_string(),
        }
    }

    #[tokio::test]
    async fn deny_decision_emits_audit_record() {
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let metrics = crate::metrics::BrokerMetrics::new();
        let authz = AuditingAuthorizer::new(Arc::new(DenyAll), log, metrics.clone());

        let principal = request_principal();
        let host: SocketAddr = "10.0.0.9:5555".parse().unwrap();
        let image = krabka_metadata::MetadataImage::default();
        let result = authz.authorize(
            &image,
            &AuthorizationRequest {
                principal: &principal,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "secrets",
                operation: AclOperation::Write,
            },
        );
        check!(result == AuthorizationResult::Deny);

        let ev = rx.try_recv().expect("an audit event was emitted");
        match ev {
            krabka_audit::AuditEvent::AuthorizationDenied {
                resource_name,
                operation,
                ..
            } => {
                check!(resource_name == "secrets");
                check!(operation == "Write");
            }
            other => panic!("unexpected event: {other:?}"),
        }
        let denied = metrics
            .authorization_denied
            .get_or_create(&denied_label())
            .get();
        check!(denied == 1);
    }

    /// With `audit.enabled=false` the broker still installs the decorator over
    /// a disabled [`AuditLog`]. The emit goes nowhere; the counter is what a
    /// Deny leaves behind.
    #[tokio::test]
    async fn deny_decision_counts_with_audit_disabled() {
        let metrics = crate::metrics::BrokerMetrics::new();
        let authz = AuditingAuthorizer::new(
            Arc::new(DenyAll),
            krabka_audit::AuditLog::disabled(),
            metrics.clone(),
        );

        let principal = request_principal();
        let host: SocketAddr = "10.0.0.9:5555".parse().unwrap();
        let image = krabka_metadata::MetadataImage::default();
        let result = authz.authorize(
            &image,
            &AuthorizationRequest {
                principal: &principal,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "secrets",
                operation: AclOperation::Write,
            },
        );

        check!(result == AuthorizationResult::Deny);
        let denied = metrics
            .authorization_denied
            .get_or_create(&denied_label())
            .get();
        check!(denied == 1);
    }
}
