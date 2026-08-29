//! Boot of the single-broker `SASL_PLAINTEXT` cluster the tests describe
//! credentials against, together with the metadata records they seed into it.
//!
//! The seeding helpers go through `submit_metadata_record_for_test` rather than
//! the `AlterUserScramCredentials` wire path, which keeps every test in this
//! suite focused on the read half of KIP-554.

use std::net::SocketAddr;

use krabka_broker::{Broker, BrokerHandle, authorizer::SimpleAclAuthorizer, config::ListenerSpec};
use krabka_metadata::{
    AclEntry, AclOperation, MetadataRecord, PatternType, PermissionType, ResourceType,
};
use krabka_security::{ListenerProtocol, SaslMechanism};
use tempfile::TempDir;

pub(crate) fn admin_test_password() -> String {
    ['a', 'd', 'm', 'i', 'n', '-', 's', 'e', 'c', 'r', 'e', 't']
        .iter()
        .collect()
}

pub(crate) fn alice_test_password() -> String {
    ['a', 'l', 'i', 'c', 'e', '-', 's', 'e', 'c', 'r', 'e', 't']
        .iter()
        .collect()
}

/// Start a single-broker SASL/PLAINTEXT cluster.
/// Returns `(handle, _dir, addr)`.
pub(crate) type BrokerStartup =
    std::pin::Pin<Box<dyn std::future::Future<Output = (BrokerHandle, TempDir, SocketAddr)>>>;

pub(crate) fn start_single_broker_sasl_plaintext_with_users(
    super_user: &str,
    users: &[(&str, &str)],
) -> BrokerStartup {
    let super_users = [super_user];
    start_single_broker_sasl_plaintext_with_acl_authorizer(&super_users, users)
}

pub(crate) fn start_single_broker_sasl_plaintext_with_acl_authorizer(
    super_users: &[&str],
    users: &[(&str, &str)],
) -> BrokerStartup {
    let log_dir = tempfile::tempdir().unwrap();
    let mut cfg = krabka_broker::BrokerConfig::for_tests(log_dir.path().to_path_buf());
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
    for (name, pass) in users {
        cfg.plain_credentials
            .insert((*name).to_string(), (*pass).to_string());
    }
    cfg.super_users = super_users.iter().map(|user| (*user).to_string()).collect();
    cfg.authorizer = std::sync::Arc::new(SimpleAclAuthorizer::new(cfg.super_users.clone()));

    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker must start");
        let addr = handle.listen_addr();
        (handle, log_dir, addr)
    })
}

pub(crate) async fn seed_cluster_acl(
    handle: &BrokerHandle,
    principal: &str,
    operation: AclOperation,
) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1AccessControlEntry(AclEntry {
            resource_type: ResourceType::Cluster,
            resource_name: "kafka-cluster".into(),
            pattern_type: PatternType::Literal,
            principal: format!("User:{principal}"),
            host: "*".into(),
            operation,
            permission_type: PermissionType::Allow,
        }))
        .await
        .expect("seed cluster ACL");
    handle
        .wait_for_image(|img| {
            img.matching_acls(ResourceType::Cluster, "kafka-cluster")
                .any(|entry| entry.principal == format!("User:{principal}"))
        })
        .await;
}

pub(crate) async fn seed_scram_credential(
    handle: &BrokerHandle,
    user: &str,
    mechanism: SaslMechanism,
    iterations: u32,
) {
    handle
        .submit_metadata_record_for_test(MetadataRecord::V1ScramCredential(
            krabka_metadata::ScramCredentialRecord {
                user: user.into(),
                mechanism,
                iterations,
                salt: vec![1, 2, 3, 4],
                server_key: vec![5; 64],
                stored_key: vec![6; 64],
            },
        ))
        .await
        .expect("seed SCRAM credential");
    handle
        .wait_for_image(|img| !img.scram_credentials_for_user(user).is_empty())
        .await;
}
