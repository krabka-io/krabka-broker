//! The authoritative `V1ClientMetricsConfig` record an `AlterConfigs`
//! `CLIENT_METRICS` resource becomes.
//!
//! `AlterConfigs` replaces a subscription's whole override map, so unlike
//! `IncrementalAlterConfigs`'
//! [`super::super::incremental_alter_configs::client_metrics_scope`] (which
//! merges per-key SET/DELETE operations onto the current map), this builder
//! never reads the subscription's stored overrides: the request carries the
//! complete set of non-default values. Every value is validated per KIP-714.

use krabka_metadata::{ClientMetricsConfigRecord, MetadataRecord};
use krabka_protocol::owned::alter_configs_request::AlterConfigsResource;

use crate::{client_metrics, codes};

/// Build the authoritative `V1ClientMetricsConfig` record for a
/// `CLIENT_METRICS` resource. The request carries the *complete* set of
/// non-default values, so the map this builds is the whole override map.
pub(super) fn client_metrics_config_record(
    resource: &AlterConfigsResource,
) -> Result<MetadataRecord, (i16, String)> {
    if resource.resource_name.is_empty() {
        return Err((
            codes::INVALID_REQUEST,
            "client-metrics subscription name must not be empty".into(),
        ));
    }
    let mut overrides = std::collections::BTreeMap::new();
    for cfg in &resource.configs {
        let value = cfg.value.clone().unwrap_or_default();
        client_metrics::config::validate(&cfg.name, &value)
            .map_err(|reason| (codes::INVALID_CONFIG, reason))?;
        overrides.insert(cfg.name.clone(), value);
    }
    Ok(MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: resource.resource_name.clone(),
            configs: overrides,
        },
    ))
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers::alter_configs::test_support::client_metrics_resource;

    #[test]
    fn client_metrics_replacement_builds_authoritative_override_map() {
        let record = client_metrics_config_record(&client_metrics_resource(
            "sub-a",
            &[
                ("interval.ms", "60000"),
                ("metrics", "org.apache.kafka.consumer."),
            ],
        ))
        .expect("valid client-metrics replacement");

        let expected = MetadataRecord::V1ClientMetricsConfig(ClientMetricsConfigRecord {
            name: "sub-a".into(),
            configs: maplit::btreemap! {
                "interval.ms".to_string() => "60000".to_string(),
                "metrics".to_string() => "org.apache.kafka.consumer.".to_string(),
            },
        });
        assert!(record == expected);
    }

    #[test]
    fn client_metrics_replacement_rejects_bad_interval() {
        let error = client_metrics_config_record(&client_metrics_resource(
            "sub-a",
            &[("interval.ms", "5")],
        ))
        .expect_err("interval below the minimum must be rejected");
        assert!(error.0 == codes::INVALID_CONFIG);
    }

    #[test]
    fn client_metrics_replacement_rejects_unknown_key() {
        let error =
            client_metrics_config_record(&client_metrics_resource("sub-a", &[("bogus.key", "x")]))
                .expect_err("unknown client-metrics key must be rejected");
        assert!(error.0 == codes::INVALID_CONFIG);
    }

    #[test]
    fn client_metrics_replacement_rejects_empty_subscription_name() {
        let error = client_metrics_config_record(&client_metrics_resource("", &[]))
            .expect_err("empty subscription name must be rejected");
        assert!(error.0 == codes::INVALID_REQUEST);
    }
}
