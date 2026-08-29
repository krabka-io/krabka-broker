//! `BrokerConfig` builders for the ACL suite. Each one declares a single
//! `SASL_PLAINTEXT` listener with PLAIN credentials and installs the
//! enforcing `SimpleAclAuthorizer` in place of the permissive default, so
//! every test in the suite starts from a cluster that actually checks ACLs.

use krabka_broker::{BrokerConfig, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use krabka_security::{ListenerProtocol, SaslMechanism};

/// Build a `BrokerConfig` with a single `SASL_PLAINTEXT` listener, PLAIN
/// enabled, and the given super-user. The non-super-user case still
/// declares a super-user so the cluster-Alter gate applies. It also
/// installs `SimpleAclAuthorizer` explicitly so the broker enforces ACLs.
/// The default is `AllowAllAuthorizer`, which would silently let
/// every test through.
pub fn sasl_plain_broker_config(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_user: Option<&str>,
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (u, p) in creds {
        cfg.plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    cfg.super_users = super_user.map(str::to_string).into_iter().collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));
    cfg
}

/// Like `sasl_plain_broker_config`, but it accepts multiple super-users. The
/// `multi_super_user_both_can_provision` test uses it to verify that
/// any principal in the `super_users` set can drive privileged admin APIs.
pub fn sasl_plain_broker_config_multi_super(
    log_dir: &std::path::Path,
    creds: &[(&str, &str)],
    super_users: &[&str],
) -> BrokerConfig {
    let mut cfg = BrokerConfig::for_tests(log_dir.to_path_buf());
    cfg.listeners = vec![ListenerSpec {
        name: "SASL_PLAINTEXT".to_string(),
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        advertised: "127.0.0.1:0".to_string(),
        protocol: ListenerProtocol::SaslPlaintext,
        tls_config: None,
        sasl_mechanisms: None,
    }];
    cfg.inter_broker_listener_name = "SASL_PLAINTEXT".to_string();
    cfg.enabled_sasl_mechanisms = vec![SaslMechanism::Plain];
    for (u, p) in creds {
        cfg.plain_credentials
            .insert((*u).to_string(), (*p).to_string());
    }
    cfg.super_users = super_users.iter().map(|s| (*s).to_string()).collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));
    cfg
}
