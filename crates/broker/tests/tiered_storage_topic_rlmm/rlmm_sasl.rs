//! The copy-then-fetch round trip over a broker whose only listener is
//! `SASL_PLAINTEXT/PLAIN`.
//!
//! This is the one test that proves the manager's internal metadata client
//! authenticates as the inter-broker PLAIN principal, so it keeps the SASL
//! client-security setup to itself.

use {
    crate::{
        TopicRlmmScenario,
        rlmm_cluster::{await_activation, build_client_secured, start_sasl_broker_with_topic_rlmm},
        topic_rlmm,
    },
    krabka_client_core::security::{ClientSecurity, SaslCredentials},
    krabka_security::ListenerProtocol,
};

#[rstest_bdd_macros::given(
    "the topic metadata manager and its client authenticate over SASL_PLAINTEXT as rlmm with password rlmm-secret"
)]
fn authenticated_topic_manager(topic_rlmm: &mut TopicRlmmScenario) {
    topic_rlmm.cluster = Some(
        topic_rlmm
            .runtime
            .block_on(start_sasl_broker_with_topic_rlmm()),
    );
    topic_rlmm
        .runtime
        .block_on(await_activation(topic_rlmm.broker()));
    let security = ClientSecurity {
        protocol: ListenerProtocol::SaslPlaintext,
        tls: None,
        sasl: Some(SaslCredentials::Plain {
            username: "rlmm".into(),
            password: "rlmm-secret".into(),
        }),
        sasl_host: None,
    };
    topic_rlmm.client = Some(
        topic_rlmm
            .runtime
            .block_on(build_client_secured(topic_rlmm.broker(), Some(security))),
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/tiered_storage_topic_rlmm/rlmm_sasl.feature",
    name = "topic rlmm sasl loopback copy then fetch round trip"
)]
fn topic_rlmm_sasl_loopback_copy_then_fetch_round_trip(topic_rlmm: TopicRlmmScenario) {
    topic_rlmm.shutdown();
}
