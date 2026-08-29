//! Client-metrics resources for `IncrementalAlterConfigs`, the KIP-714
//! subscription configs. The handler merges the per-key operations onto the
//! subscription's current override map and stages a `V1ClientMetricsConfig`
//! record with the merged map.

use krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord};
use krabka_protocol::owned::{
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{OP_DELETE, OP_SET};
use crate::codes;

/// Merge per-key ops into a client-metrics subscription's override map and
/// stage a `V1ClientMetricsConfig` record. SET validates per KIP-714.
/// DELETE drops the override, so the effective value reverts to its default at
/// read time. APPEND and SUBTRACT are rejected.
pub(super) fn handle_client_metrics_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    if resource.resource_name.is_empty() {
        out.error_code = codes::INVALID_REQUEST;
        out.error_message = Some("client-metrics subscription name must not be empty".into());
        return;
    }
    let mut merged = image
        .client_metrics_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    for cfg in &resource.configs {
        match cfg.config_operation {
            OP_SET => {
                let value = cfg.value.clone().unwrap_or_default();
                if let Err(reason) = crate::client_metrics::config::validate(&cfg.name, &value) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(reason);
                    return;
                }
                merged.insert(cfg.name.clone(), value);
            }
            OP_DELETE => {
                if !crate::client_metrics::config::is_recognized(&cfg.name) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(format!("unrecognized config key `{}`", cfg.name));
                    return;
                }
                merged.remove(&cfg.name);
            }
            op => {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(format!(
                    "config_operation={op} (APPEND/SUBTRACT) not supported for client-metrics key `{}`",
                    cfg.name
                ));
                return;
            }
        }
    }
    to_submit.push(MetadataRecord::V1ClientMetricsConfig(
        ClientMetricsConfigRecord {
            name: resource.resource_name.clone(),
            configs: merged,
        },
    ));
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig;

    use super::*;
    use crate::handlers::incremental_alter_configs::RESOURCE_TYPE_CLIENT_METRICS;

    #[test]
    fn client_metrics_set_produces_record() {
        let img = MetadataImage::new(uuid::Uuid::nil());
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![
                AlterableConfig {
                    name: "interval.ms".into(),
                    config_operation: OP_SET,
                    value: Some("60000".into()),
                    ..Default::default()
                },
                AlterableConfig {
                    name: "metrics".into(),
                    config_operation: OP_SET,
                    value: Some("org.apache.kafka.consumer.".into()),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        assert!(to_submit.len() == 1);
        match &to_submit[0] {
            MetadataRecord::V1ClientMetricsConfig(rec) => {
                assert!(rec.name == "sub-a");
                assert!(rec.configs.get("interval.ms").map(String::as_str) == Some("60000"));
            }
            other => panic!("expected V1ClientMetricsConfig, got {other:?}"),
        }
    }

    #[test]
    fn client_metrics_bad_interval_rejected() {
        let img = MetadataImage::new(uuid::Uuid::nil());
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![AlterableConfig {
                name: "interval.ms".into(),
                config_operation: OP_SET,
                value: Some("5".into()),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn client_metrics_delete_drops_key() {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        let mut existing = std::collections::BTreeMap::new();
        existing.insert("interval.ms".to_string(), "60000".to_string());
        existing.insert("metrics".to_string(), "a.".to_string());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "sub-a".into(),
                configs: existing,
            },
        ));
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: "sub-a".into(),
            configs: vec![AlterableConfig {
                name: "interval.ms".into(),
                config_operation: OP_DELETE,
                value: None,
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        match &to_submit[0] {
            MetadataRecord::V1ClientMetricsConfig(rec) => {
                assert!(!rec.configs.contains_key("interval.ms"));
                assert!(rec.configs.get("metrics").map(String::as_str) == Some("a."));
            }
            other => panic!("expected V1ClientMetricsConfig, got {other:?}"),
        }
    }
}
