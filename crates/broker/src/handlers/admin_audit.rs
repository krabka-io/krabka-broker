//! The audit hook that an admin handler calls once it has changed the cluster.
//!
//! The hook builds the `AdminOperation` event from the request context, so
//! every admin RPC reports the principal, the source endpoint, and the changed
//! resources in the same shape.

use super::RequestContext;

/// Emits an `AdminOperation` audit event for a completed admin request.
///
/// Call this on the SUCCESS path of each admin handler, after the broker
/// applies the operation and knows the set of resources that it changed
/// successfully. This function does nothing when `resources` is empty. The
/// caller guards with `if !resources.is_empty()`.
pub(crate) fn audit_admin(
    audit_log: &krabka_audit::AuditLog,
    ctx: &RequestContext<'_>,
    operation: &str,
    outcome: krabka_audit::AuditOutcome,
    resources: Vec<krabka_audit::AuditResource>,
) {
    audit_log.emit(krabka_audit::AuditEvent::AdminOperation {
        outcome,
        principal: krabka_audit::AuditPrincipal {
            name: ctx.principal.name.clone(),
            auth_method: format!("{:?}", ctx.principal.auth_method),
        },
        source: krabka_audit::AuditEndpoint {
            ip: ctx.peer.ip().to_string(),
            port: ctx.peer.port(),
        },
        operation: operation.to_string(),
        resources,
        time_ms: crate::time_util::now_ms(),
    });
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use assert2::assert;
    use krabka_security::{AuthMethod, Principal};

    use super::*;

    #[test]
    fn audit_admin_emits_admin_operation_event() {
        let (log, mut rx) = krabka_audit::AuditLog::new(8);
        let principal = Principal {
            name: "admin".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let peer: SocketAddr = "192.0.2.10:9092".parse().unwrap();
        let ctx = RequestContext {
            principal: &principal,
            peer: &peer,
            client_id: "admin-client",
            connection_id: "connection-a",
            sendfile_capable: false,
            connection_listener_name: "PLAINTEXT",
        };

        audit_admin(
            log.as_ref(),
            &ctx,
            "CreateTopics",
            krabka_audit::AuditOutcome::Success,
            vec![krabka_audit::AuditResource {
                resource_type: "Topic".into(),
                name: "orders".into(),
            }],
        );

        match rx.try_recv().expect("admin audit event") {
            krabka_audit::AuditEvent::AdminOperation {
                outcome,
                principal,
                source,
                operation,
                resources,
                ..
            } => {
                assert!(
                    (
                        outcome,
                        principal.name.as_str(),
                        principal.auth_method.as_str(),
                        source.ip.as_str(),
                        source.port,
                        operation.as_str(),
                        resources.len(),
                        resources[0].resource_type.as_str(),
                        resources[0].name.as_str()
                    ) == (
                        krabka_audit::AuditOutcome::Success,
                        "admin",
                        "SaslPlain",
                        "192.0.2.10",
                        9092,
                        "CreateTopics",
                        1,
                        "Topic",
                        "orders"
                    )
                );
            }
            other => panic!("expected admin operation event, got {other:?}"),
        }
    }
}
