//! The tail of the file-config apply pipeline.
//!
//! [`FileConfigTail`] bundles the sections that are applied after the
//! listeners, OAUTHBEARER, and remote storage: the authorizer and schema
//! registry clients, the `KRaft` roles, the Kerberos credentials, the quorum
//! and bootstrap endpoints, and the audit subsystem. They travel together
//! because each is a whole-broker singleton that `apply_to` writes exactly
//! once, after the values they may depend on are already resolved.

use std::sync::Arc;

use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt as _, TimeExt as _},
};

use super::{
    AuthzType, FileAuditConfig, FileAuthorizationConfig, FileConfig, FileConfigError,
    FileGssapiConfig, FileInterBrokerCredentials, FileProcessConfig, FileSchemaRegistryConfig,
    gssapi::DEFAULT_KERBEROS_SERVICE_NAME,
};

pub(super) struct FileConfigTail {
    pub(super) authorization: Option<FileAuthorizationConfig>,
    pub(super) schema_registry: Option<FileSchemaRegistryConfig>,
    pub(super) process: Option<FileProcessConfig>,
    pub(super) gssapi: Option<FileGssapiConfig>,
    pub(super) inter_broker_credentials: Option<FileInterBrokerCredentials>,
    pub(super) controller_quorum_voters: Vec<String>,
    pub(super) bootstrap_servers: Vec<String>,
    pub(super) auto_join: Option<bool>,
    pub(super) controller_server_name: Option<String>,
    pub(super) audit: Option<FileAuditConfig>,
}

pub(super) fn apply_config_tail(
    tail: FileConfigTail,
    cfg: &mut crate::config::BrokerConfig,
) -> Result<(), FileConfigError> {
    if let Some(authorization) = tail.authorization.as_ref() {
        let super_users = authorization.super_users.iter().cloned().collect();
        cfg.super_users.clone_from(&super_users);
        cfg.authorizer = match authorization.authz_type {
            AuthzType::AllowAll => Arc::new(crate::authorizer::AllowAllAuthorizer),
            AuthzType::Simple => Arc::new(crate::authorizer::SimpleAclAuthorizer::new(super_users)),
            AuthzType::Opa => {
                let opa = authorization
                    .opa
                    .as_ref()
                    .ok_or_else(|| FileConfigError::MissingSection("[authorization.opa]".into()))?;
                Arc::new(
                    crate::authorizer::opa::OpaAuthorizer::new(
                        super_users,
                        opa.url.clone(),
                        opa.allow_on_error,
                        opa.maximum_cache_size,
                        Time::from_millis(opa.expire_after_ms),
                        cfg.opa_http_timeout,
                    )
                    .map_err(|error| FileConfigError::OpaConfig(format!("{error:?}")))?,
                )
            }
        };
    }
    if let Some(sr) = tail.schema_registry.as_ref() {
        cfg.schema_validator = Some(std::sync::Arc::new(
            crate::schema_validation::SchemaValidator::new(
                sr.url.clone(),
                sr.fail_open,
                sr.maximum_cache_size,
                Time::from_millis(sr.expire_after_ms),
                cfg.schema_registry_http_timeout,
            )
            .map_err(|error| FileConfigError::SchemaRegistryConfig(format!("{error:?}")))?,
        ));
    }
    if let Some(process) = tail.process
        && !process.roles.is_empty()
    {
        cfg.roles = process
            .roles
            .iter()
            .map(|role| match role.to_ascii_lowercase().as_str() {
                "controller" => Ok(crate::config::NodeRole::Controller),
                "broker" => Ok(crate::config::NodeRole::Broker),
                "witness" => Ok(crate::config::NodeRole::Witness),
                other => Err(FileConfigError::InvalidConfig(format!(
                    "unknown process.role `{other}`"
                ))),
            })
            .collect::<Result<_, _>>()?;
    }
    if let Some(gssapi) = tail.gssapi {
        let max_time_skew = gssapi
            .max_time_skew
            .unwrap_or(krabka_security::gssapi::DEFAULT_GSSAPI_MAX_TIME_SKEW);
        if max_time_skew < Time::ZERO {
            return Err(FileConfigError::InvalidConfig(
                "gssapi.max_time_skew must be non-negative".to_owned(),
            ));
        }
        let rules = gssapi
            .principal_to_local_rules
            .iter()
            .map(|spec| {
                krabka_security::gssapi::name::Rule::parse(spec).map_err(|error| {
                    FileConfigError::InvalidConfig(format!(
                        "invalid GSSAPI principal rule {spec:?}: {error}"
                    ))
                })
            })
            .collect::<Result<_, _>>()?;
        cfg.gssapi = Some(krabka_security::gssapi::GssapiConfig {
            keytab_path: gssapi.keytab_path,
            service_name: gssapi
                .service_name
                .unwrap_or_else(|| DEFAULT_KERBEROS_SERVICE_NAME.to_owned()),
            principal_to_local_rules: rules,
            realm: gssapi.realm,
            kdc: gssapi.kdc,
            max_time_skew,
        });
    }
    if let Some(credentials) = tail.inter_broker_credentials {
        cfg.inter_broker_credentials = Some(match credentials {
            FileInterBrokerCredentials::Gssapi {
                keytab_path,
                client_principal,
                service_name,
                kdc_url,
            } => crate::config::InterBrokerCredentials::Gssapi {
                keytab_path,
                client_principal,
                service_name: service_name
                    .unwrap_or_else(|| DEFAULT_KERBEROS_SERVICE_NAME.to_owned()),
                kdc_url,
            },
            FileInterBrokerCredentials::OAuthBearer { token_path } => {
                let token = std::fs::read(&token_path).map_err(|error| {
                    FileConfigError::InvalidConfig(format!(
                        "cannot read inter-broker OAUTHBEARER token {}: {error}",
                        token_path.display()
                    ))
                })?;
                let token = token.trim_ascii();
                if token.is_empty() || token.contains(&b'\x01') {
                    return Err(FileConfigError::InvalidConfig(
                        "inter-broker OAUTHBEARER token must be non-empty and contain no RFC 7628 separator"
                            .into(),
                    ));
                }
                crate::config::InterBrokerCredentials::OAuthBearer { token_path }
            }
        });
    }
    if !tail.controller_quorum_voters.is_empty() {
        cfg.controller_quorum_voters = tail
            .controller_quorum_voters
            .iter()
            .map(|entry| FileConfig::parse_quorum_voter(entry))
            .collect::<Result<_, _>>()?;
    }
    if !tail.bootstrap_servers.is_empty() {
        cfg.bootstrap_servers = tail
            .bootstrap_servers
            .iter()
            .map(|entry| FileConfig::parse_bootstrap_server(entry))
            .collect::<Result<_, _>>()?;
    }
    if let Some(auto_join) = tail.auto_join {
        cfg.auto_join = auto_join;
    }
    if tail.controller_server_name.is_some() {
        cfg.controller_server_name = tail.controller_server_name;
    }
    let audit = tail.audit.unwrap_or_default();
    cfg.audit_enabled = audit.enabled;
    cfg.audit_failure_mode = audit.failure_mode;
    cfg.audit_topic = audit.topic;
    if let Some(signing) = audit.signing {
        cfg.audit_signing_key_path = Some(signing.key_path.into());
        cfg.audit_signing_key_id = Some(signing.key_id);
    }
    let checkpoint = audit.checkpoint.unwrap_or_default();
    cfg.audit_checkpoint_every_n = checkpoint.every_n;
    cfg.audit_checkpoint_every =
        Time::from_secs(i64::try_from(checkpoint.every_secs).unwrap_or(i64::MAX));
    let spool = audit.spool.unwrap_or_default();
    cfg.audit_spool_dir = spool.dir.into();
    cfg.audit_spool_max = ByteSize::from_bytes(spool.max_bytes);
    cfg.audit_spool_sync_every_n = spool.sync_every_n;
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::file_config::FileConfig;

    #[test]
    fn apply_to_absent_controller_server_name_leaves_default() {
        let src = r#"
controller_listener_protocol = "Ssl"
[tls_config]
cert_path = "/c"
key_path = "/k"
client_auth = "Required"
"#;
        let file: FileConfig = toml::from_str(src).expect("parse");
        let mut cfg = crate::config::BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert!(cfg.controller_server_name.is_none());
        assert!(cfg.tls_config.expect("tls").trust_roots_path.is_none());
    }
}
