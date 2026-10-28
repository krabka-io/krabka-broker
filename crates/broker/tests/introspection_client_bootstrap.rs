//! Regression test for the rustls `CryptoProvider` install order.
//!
//! When an operator-rendered broker config sets
//! `[oauthbearer] introspection_endpoint_uri`, `FileConfig::apply_to` builds
//! the introspection client before `Broker::start` can install the rustls
//! process-level `CryptoProvider`. The `reqwest::Client::builder().build()`
//! path inside `ReqwestIntrospectionClient::new` reaches into
//! `rustls::ClientConfig::builder` → `CryptoProvider::get_default`. That call
//! panics with "Could not automatically determine the process-level
//! `CryptoProvider` from Rustls crate features" when no provider is
//! installed.
//!
//! Each integration-test file is its own process, so this file does not
//! install the provider anywhere else. If anyone removes the eager install
//! inside the client constructor, this test panics on startup, exactly as the
//! broker binary did in the kind-oauth-introspection e2e job.
#![cfg(test)]

use {
    assert2::assert,
    krabka_broker::{config::BrokerConfig, file_config::FileConfig},
    krabka_security::OAuthBearerValidator,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::fs,
    tempfile::{TempDir, tempdir},
};

/// A self-contained CA cert. It does not depend on the security crate's
/// fixtures, which live behind `#[cfg(test)]`-only paths.
const DEV_CERT_PEM: &str = "\
-----BEGIN CERTIFICATE-----\n\
MIIB4zCCAYmgAwIBAgIUcSDwFlx+8XhU+aAAtS17F6TnHQgwCgYIKoZIzj0EAwIw\n\
FTETMBEGA1UEAwwKY3JhYmthLWRldjAgFw0yNjA1MTUwNTA2MzNaGA8yMTI2MDQy\n\
MTA1MDYzM1owFTETMBEGA1UEAwwKY3JhYmthLWRldjBZMBMGByqGSM49AgEGCCqG\n\
SM49AwEHA0IABIzQW9UMYH1u0MSki3EBCJ4qIrjV67hAJv79lFJCGZCbl+pwVhYS\n\
wLP4u7jqwoM8qFg68ZBGFVrcGulQs3UYTCujgbQwgbEwHQYDVR0OBBYEFGZZjSyx\n\
/Lc2SUwBiFQ/VcKLRSTJMB8GA1UdIwQYMBaAFGZZjSyx/Lc2SUwBiFQ/VcKLRSTJ\n\
MAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgWgMBMGA1UdJQQMMAoGCCsGAQUF\n\
BwMBMDwGA1UdEQQ1MDOCCmNyYWJrYS1kZXaCFGhvc3QuZG9ja2VyLmludGVybmFs\n\
gglsb2NhbGhvc3SHBH8AAAEwCgYIKoZIzj0EAwIDSAAwRQIgcO9PwkwNCPt589FM\n\
OnP9WAj0vBqSLWmYcm6N6hNz8KcCIQDDXGEYKtNkfUzKrICS2v40ybJqzyZ9cbaR\n\
Shzd0RKUbQ==\n\
-----END CERTIFICATE-----\n";

struct IntrospectionBootstrap {
    directory: Option<TempDir>,
    file: Option<FileConfig>,
    broker: BrokerConfig,
}

#[fixture]
fn introspection() -> IntrospectionBootstrap {
    // Start this scenario with fresh inputs and observations.

    IntrospectionBootstrap {
        directory: None,
        file: None,
        broker: BrokerConfig::default(),
    }
}

#[given("a fresh process has an introspection client configuration with a secret and trusted CA")]
fn client_configuration(introspection: &mut IntrospectionBootstrap) {
    let directory = tempdir().expect("tempdir");
    let secret_path = directory.path().join("client-secret");
    fs::write(&secret_path, "the-secret").expect("write secret");
    let ca_path = directory.path().join("idp-ca.pem");
    fs::write(&ca_path, DEV_CERT_PEM).expect("write idp-ca");
    let toml = format!(
        r#"
[oauthbearer]
introspection_endpoint_uri       = "https://idp.example/introspect"
introspection_client_id          = "kafka-broker"
introspection_client_secret_path = '{secret}'
idp_tls_trust                    = '{ca}'
"#,
        secret = secret_path.display(),
        ca = ca_path.display()
    );
    introspection.file = Some(toml::from_str(&toml).expect("parse FileConfig"));
    introspection.directory = Some(directory);
}

#[when("the listener configuration builds its introspection client before broker startup")]
fn apply_client_configuration(introspection: &mut IntrospectionBootstrap) {
    introspection
        .file
        .take()
        .expect("an introspection configuration was supplied")
        .apply_to(&mut introspection.broker)
        .expect("apply_to should not panic");
}

#[then(
    "the broker uses an introspection validator without an externally installed crypto provider"
)]
fn introspection_validator(introspection: &IntrospectionBootstrap) {
    assert!(matches!(
        introspection.broker.oauthbearer_validator,
        OAuthBearerValidator::Introspection(_)
    ));
}

scenarios!("tests/features/migrated/tests/introspection_client_bootstrap.feature", fixtures = [introspection: IntrospectionBootstrap]);
