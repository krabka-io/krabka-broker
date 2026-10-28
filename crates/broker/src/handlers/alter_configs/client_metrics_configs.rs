//! The authoritative `V1ClientMetricsConfig` record an `AlterConfigs`
//! `CLIENT_METRICS` resource becomes.
//!
//! `AlterConfigs` replaces a subscription's whole override map, so unlike
//! `IncrementalAlterConfigs`'
//! [`super::super::incremental_alter_configs::client_metrics_scope`] (which
//! merges per-key SET/DELETE operations onto the current map), this builder
//! never reads the subscription's stored overrides: the request carries the
//! complete set of non-default values. Every value is validated per KIP-714.

use {
    crate::client_metrics,
    krabka_metadata::{ClientMetricsConfigRecord, MetadataRecord},
    krabka_protocol::owned::alter_configs_request::AlterConfigsResource,
    std::collections::BTreeMap,
};

/// Build the authoritative `V1ClientMetricsConfig` record for a
/// `CLIENT_METRICS` resource. The request carries the *complete* set of
/// non-default values, so the map this builds is the whole override map.
pub(super) fn client_metrics_config_record(
    resource: &AlterConfigsResource,
) -> Result<MetadataRecord, (i16, String)> {
    // Kafka's `legacyAlterConfigResource` turns a null value into a
    // deletion, so it is left out of the map `ClientMetricsConfigs.validate`
    // checks.
    let overrides: BTreeMap<String, String> = resource
        .configs
        .iter()
        .filter_map(|cfg| Some((cfg.name.clone(), cfg.value.clone()?)))
        .collect();
    client_metrics::config::validate(&resource.resource_name, &overrides)
        .map_err(|error| (error.code(), error.message().to_owned()))?;
    Ok(MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: resource.resource_name.clone(),
            configs: overrides,
        },
    ))
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::handlers::alter_configs::test_support::client_metrics_resource,
        assert2::assert,
    };

    #[test]
    fn client_metrics_replacement_builds_authoritative_override_map() {
        let record = client_metrics_config_record(&client_metrics_resource("sub-a", &[
            ("interval.ms", "60000"),
            ("metrics", "org.apache.kafka.consumer."),
        ]))
        .expect("valid client-metrics replacement");

        let expected = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: maplit::btreemap! {
                "interval.ms".to_owned() => "60000".to_owned(),
                "metrics".to_owned() => "org.apache.kafka.consumer.".to_owned(),
            },
        });
        assert!(record == expected);
    }

    #[derive(Default)]
    struct ClientMetricsReplacementScenario {
        subscription: String,
        key: String,
        value: String,
        result: Option<Result<MetadataRecord, (i16, String)>>,
    }
    #[rstest::fixture]
    fn client_metrics_replacement() -> ClientMetricsReplacementScenario {
        let subscription = String::new();
        ClientMetricsReplacementScenario {
            subscription,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "client metrics subscription {subscription} is replaced with key {key} value {value}"
    )]
    fn client_metrics_replacement_input(
        client_metrics_replacement: &mut ClientMetricsReplacementScenario,
        subscription: String,
        key: String,
        value: String,
    ) {
        client_metrics_replacement.subscription = if subscription == "empty" {
            String::new()
        } else {
            subscription
        };
        client_metrics_replacement.key = key;
        client_metrics_replacement.value = value;
    }
    #[rstest_bdd_macros::when("the authoritative client metrics replacement record is built")]
    fn build_client_metrics_replacement(
        client_metrics_replacement: &mut ClientMetricsReplacementScenario,
    ) {
        let configs = if client_metrics_replacement.key == "none" {
            Vec::new()
        } else {
            vec![(
                client_metrics_replacement.key.as_str(),
                client_metrics_replacement.value.as_str(),
            )]
        };
        client_metrics_replacement.result = Some(client_metrics_config_record(
            &client_metrics_resource(&client_metrics_replacement.subscription, &configs),
        ));
    }
    #[rstest_bdd_macros::then(
        "the client metrics replacement fails with code {code} and message {message}"
    )]
    fn check_client_metrics_replacement(
        client_metrics_replacement: &ClientMetricsReplacementScenario,
        code: i16,
        message: String,
    ) {
        assert2::check!(
            client_metrics_replacement.result.as_ref().unwrap() == &Err((code, message))
        );
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_client_metrics_replacement",fixtures=[client_metrics_replacement:ClientMetricsReplacementScenario]);
}
