//! Runtime security resources, built after configuration resolution and validation.

use {
    crate::{
        BrokerConfig,
        authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
        file_config::{AuthzType, FileConfigError, FileSchemaRegistryConfig},
        schema_validation::SchemaValidator,
    },
    krabka_units::{Time, convert::TimeExt as _},
    std::sync::Arc,
};

/// Acquire clients and secrets from the final settings. An absent section keeps
/// a caller's injected implementation. Errors abort startup before serving.
///
/// Call after resolving and validating every configuration source. A Tokio
/// runtime must be active when constructing OPA clients. Successfully assembled
/// sections are consumed so a subsequent broker start retains the clients.
///
/// # Errors
///
/// Returns an error when a security client cannot be built or a registry secret
/// cannot be read. A failed section remains pending so a retry cannot skip it.
pub fn assemble(cfg: &mut BrokerConfig) -> Result<(), FileConfigError> {
    if let Some(authorization) = cfg.authorization_config.as_ref() {
        cfg.authorizer = match authorization.authz_type {
            AuthzType::AllowAll => Arc::new(AllowAllAuthorizer),
            AuthzType::Simple => Arc::new(
                SimpleAclAuthorizer::new(cfg.super_users.clone())
                    .with_allow_everyone_if_no_acl_found(
                        authorization.allow_everyone_if_no_acl_found,
                    ),
            ),
            #[cfg(target_family = "wasm")]
            AuthzType::Opa => {
                return Err(FileConfigError::OpaConfig(
                    "the OPA authorizer is unavailable on this platform".to_owned(),
                ));
            }
            #[cfg(not(target_family = "wasm"))]
            AuthzType::Opa => {
                use crate::authorizer::opa::OpaAuthorizer;

                let opa = authorization
                    .opa
                    .as_ref()
                    .ok_or_else(|| FileConfigError::MissingSection("[authorization.opa]".into()))?;
                Arc::new(
                    OpaAuthorizer::new(
                        cfg.super_users.clone(),
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
        cfg.authorization_config = None;
    }
    if let Some(sr) = cfg.schema_registry_config.as_ref() {
        cfg.schema_validator = Some(Arc::new(schema_validator(
            sr,
            cfg.schema_registry_http_timeout,
        )?));
        cfg.schema_registry_config = None;
    }
    Ok(())
}

/// Load a registry secret and build the validator from resolved settings.
fn schema_validator(
    sr: &FileSchemaRegistryConfig,
    http_timeout: Time,
) -> Result<SchemaValidator, FileConfigError> {
    let basic_auth = match (&sr.basic_auth_username, &sr.basic_auth_password_path) {
        (None, None) => None,
        (Some(username), Some(path)) => Some((
            username.clone(),
            std::fs::read_to_string(path)
                .map_err(|error| {
                    FileConfigError::InvalidConfig(format!(
                        "[schema_registry]: failed to read basic_auth_password_path {}: {error}",
                        path.display()
                    ))
                })?
                .trim_end_matches(['\n', '\r'])
                .to_owned(),
        )),
        _ => {
            return Err(FileConfigError::InvalidConfig(
                "[schema_registry]: basic_auth_username and basic_auth_password_path must be set together"
                    .to_owned(),
            ));
        }
    };
    basic_auth
        .map_or_else(
            || {
                SchemaValidator::new(
                    sr.url.clone(),
                    sr.fail_open,
                    sr.maximum_cache_size,
                    Time::from_millis(sr.expire_after_ms),
                    http_timeout,
                )
            },
            |(username, password)| {
                SchemaValidator::new_with_basic_auth(
                    sr.url.clone(),
                    sr.fail_open,
                    sr.maximum_cache_size,
                    Time::from_millis(sr.expire_after_ms),
                    http_timeout,
                    username,
                    password,
                )
            },
        )
        .map_err(|error| FileConfigError::SchemaRegistryConfig(format!("{error:?}")))
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use {
        super::*,
        crate::{
            authorizer::{AuthorizationRequest, AuthorizationResult},
            file_config::FileConfig,
            test_support::{peer, sasl_principal},
        },
        krabka_metadata::{AclOperation, MetadataImage, ResourceType},
        std::collections::HashSet,
    };

    #[yare::parameterized(
        missing = { false },
        invalid_utf8 = { true },
    )]
    fn unreadable_secret_is_deferred_and_repeated_assembly_fails_closed(invalid_utf8: bool) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("password");
        if invalid_utf8 {
            std::fs::write(&path, [0xff]).unwrap();
        }
        let file: FileConfig = toml::from_str(&format!(
            "[schema_registry]\nurl = 'http://localhost:8081'\nbasic_auth_username = 'broker'\nbasic_auth_password_path = '{}'\n",
            path.display(),
        ))
        .unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        assert2::assert!(cfg.schema_validator.is_none());
        for _ in 0..2 {
            let error = assemble(&mut cfg).unwrap_err().to_string();
            assert2::assert!(error.contains("failed to read basic_auth_password_path"));
            assert2::assert!(cfg.schema_validator.is_none());
            assert2::assert!(cfg.schema_registry_config.is_some());
        }
    }

    #[yare::parameterized(
        default_deny = { false, AuthorizationResult::Deny },
        allow_without_acls = { true, AuthorizationResult::Allow },
    )]
    fn simple_authorization_uses_the_final_super_users(
        allow_everyone: bool,
        expected: AuthorizationResult,
    ) {
        let file: FileConfig = toml::from_str(&format!(
            "[authorization]\ntype = 'simple'\nsuper_users = ['User:file-admin']\nallow_everyone_if_no_acl_found = {allow_everyone}"
        ))
        .unwrap();
        let mut cfg = BrokerConfig::default();
        file.apply_to(&mut cfg).unwrap();
        cfg.super_users = ["final-admin".to_owned()].into();
        cfg.validate().unwrap();
        assemble(&mut cfg).unwrap();

        let image = MetadataImage::new(uuid::Uuid::nil());
        let host = peer();
        for (name, want) in [
            ("final-admin", AuthorizationResult::Allow),
            ("file-admin", expected),
            ("alice", expected),
        ] {
            let principal = sasl_principal(name);
            let request = AuthorizationRequest {
                principal: &principal,
                host: &host,
                resource_type: ResourceType::Topic,
                resource_name: "t",
                operation: AclOperation::Read,
            };
            assert2::assert!(cfg.authorizer.authorize(&image, &request) == want, "{name}");
        }
        assert2::assert!(cfg.authorizer.is_configured());
        let authorizer = cfg.authorizer.clone();
        assemble(&mut cfg).unwrap();
        assert2::assert!(Arc::ptr_eq(&authorizer, &cfg.authorizer));
    }

    #[test]
    fn absent_sections_preserve_injected_clients() {
        let mut cfg = BrokerConfig {
            authorizer: Arc::new(SimpleAclAuthorizer::new(HashSet::default())),
            schema_validator: Some(Arc::new(
                SchemaValidator::new(
                    "http://registry.invalid".to_owned(),
                    false,
                    8,
                    Time::from_millis(1_000),
                    Time::from_millis(1_000),
                )
                .unwrap(),
            )),
            ..BrokerConfig::default()
        };
        let authorizer = cfg.authorizer.clone();
        let validator = cfg.schema_validator.clone().unwrap();
        assemble(&mut cfg).unwrap();
        assert2::assert!(Arc::ptr_eq(&authorizer, &cfg.authorizer));
        assert2::assert!(Arc::ptr_eq(
            &validator,
            cfg.schema_validator.as_ref().unwrap()
        ));
    }
}
