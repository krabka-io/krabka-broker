//! Broker-scoped resources for `IncrementalAlterConfigs`. This module holds
//! the whitelist of broker config keys, the per-key value validators, the
//! mapping from a resource name to a `NodeId`, and the op merge that stages
//! one `V1BrokerConfig` record per altered key.

use krabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord, NodeId};
use krabka_protocol::owned::{
    incremental_alter_configs_request::AlterConfigsResource,
    incremental_alter_configs_response::AlterConfigsResourceResponse,
};

use super::{OP_DELETE, OP_SET};
use crate::{codes, config_keys};

/// Returns `true` if `name` is a broker-scoped config key accepted by this
/// broker.
pub(in crate::handlers) fn is_known_broker_config(name: &str) -> bool {
    matches!(
        name,
        crate::throttle::LEADER_THROTTLED_RATE_KEY
            | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
            | crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY
            | config_keys::UNCLEAN_LEADER_ELECTION_ENABLE
            | config_keys::UNCLEAN_RECOVERY_STRATEGY
            | config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS
    )
}

/// Returns `true` for a topic setting that the controller may inherit from
/// the cluster-wide default broker-config resource. Per-broker values would
/// have no deterministic meaning for controller policy, so handlers reject
/// them.
pub(in crate::handlers) fn is_cluster_default_topic_config(name: &str) -> bool {
    matches!(
        name,
        config_keys::UNCLEAN_LEADER_ELECTION_ENABLE | config_keys::UNCLEAN_RECOVERY_STRATEGY
    )
}

/// Validate the value for a broker-scoped config key.
/// Returns `Err` if the key is unknown or if the value does not parse as an
/// `i64`.
pub(in crate::handlers) fn validate_broker_config_value(
    name: &str,
    value: &str,
) -> Result<(), String> {
    match name {
        crate::throttle::LEADER_THROTTLED_RATE_KEY
        | crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
        | crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY => value
            .parse::<i64>()
            .map(|_| ())
            .map_err(|e| format!("invalid rate: {e}")),
        config_keys::UNCLEAN_LEADER_ELECTION_ENABLE | config_keys::UNCLEAN_RECOVERY_STRATEGY => {
            config_keys::validate_topic_config(name, value)
        }
        config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS => {
            config_keys::parse_remote_list_offsets_timeout(value).map(|_| ())
        }
        _ => Err(format!("unknown broker config {name}")),
    }
}

pub(in crate::handlers) fn broker_config_node_id(
    resource_name: &str,
    image: &MetadataImage,
) -> Result<NodeId, (i16, String)> {
    if resource_name.is_empty() {
        return Ok(krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID);
    }
    let node_id = resource_name.parse::<u64>().map(NodeId).map_err(|_| {
        (
            codes::INVALID_REQUEST,
            format!("invalid broker id {resource_name:?}"),
        )
    })?;
    if image.broker(node_id).is_none() {
        return Err((codes::INVALID_REQUEST, format!("unknown broker {node_id}")));
    }
    Ok(node_id)
}

pub(super) fn handle_broker_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    let node_id = match broker_config_node_id(&resource.resource_name, image) {
        Ok(node_id) => node_id,
        Err((code, message)) => {
            out.error_code = code;
            out.error_message = Some(message);
            return;
        }
    };
    for cfg in &resource.configs {
        if config_keys::is_controller_managed_broker_config(&cfg.name) {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(format!(
                "broker config {} is controller-managed and read-only",
                cfg.name
            ));
            return; // halt processing this resource
        }
        if !is_known_broker_config(&cfg.name) {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(format!("unknown broker config {}", cfg.name));
            return; // halt processing this resource
        }
        if node_id != krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID
            && is_cluster_default_topic_config(&cfg.name)
        {
            out.error_code = codes::INVALID_CONFIG;
            out.error_message = Some(format!(
                "broker config {} is valid only on the cluster-default resource",
                cfg.name
            ));
            return;
        }
        let new_value = match cfg.config_operation {
            OP_SET => {
                let v = cfg.value.clone().unwrap_or_default();
                if let Err(e) = validate_broker_config_value(&cfg.name, &v) {
                    out.error_code = codes::INVALID_CONFIG;
                    out.error_message = Some(e);
                    return;
                }
                Some(v)
            }
            OP_DELETE => None,
            _ => {
                out.error_code = codes::INVALID_REQUEST;
                out.error_message = Some(format!(
                    "unsupported config_operation {}",
                    cfg.config_operation
                ));
                return;
            }
        };
        to_submit.push(MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: cfg.name.clone(),
            config_value: new_value,
        }));
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;
    use crate::handlers::incremental_alter_configs::test_support::{
        make_del_cfg, make_image_with_broker, make_resource, make_set_cfg,
    };

    #[test]
    fn broker_scoped_configs_recognized() {
        assert!(is_known_broker_config(
            crate::throttle::LEADER_THROTTLED_RATE_KEY
        ));
        assert!(is_known_broker_config(
            crate::throttle::FOLLOWER_THROTTLED_RATE_KEY
        ));
        assert!(is_known_broker_config(
            crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY
        ));
        assert!(is_known_broker_config(
            config_keys::UNCLEAN_LEADER_ELECTION_ENABLE
        ));
        assert!(is_known_broker_config(
            config_keys::UNCLEAN_RECOVERY_STRATEGY
        ));
        assert!(is_known_broker_config(
            config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS
        ));
    }

    #[test]
    fn broker_scoped_unknown_config_rejected() {
        assert!(!is_known_broker_config("not.a.real.config"));
        assert!(validate_broker_config_value("not.a.real.config", "1024").is_err());
    }

    #[test]
    fn broker_scoped_invalid_value_rejected() {
        assert!(
            validate_broker_config_value(
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                "not-a-number"
            )
            .is_err()
        );
        assert!(
            validate_broker_config_value(crate::throttle::LEADER_THROTTLED_RATE_KEY, "1024")
                .is_ok()
        );
        assert!(
            validate_broker_config_value(
                config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
                "30000"
            )
            .is_ok()
        );
        for invalid in ["", "0", "-1", "2147483648", "not-a-number"] {
            assert!(
                validate_broker_config_value(
                    config_keys::REMOTE_LIST_OFFSETS_REQUEST_TIMEOUT_MS,
                    invalid
                )
                .is_err(),
                "{invalid}"
            );
        }
    }

    #[test]
    fn broker_scoped_empty_name_targets_cluster_default() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource(
            "",
            vec![
                make_set_cfg(crate::throttle::LEADER_THROTTLED_RATE_KEY, "2048"),
                make_set_cfg(config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
            ],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        assert!(
            to_submit
                == vec![
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                        config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.to_string(),
                        config_value: Some("2048".to_string()),
                    }),
                    MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                        config_name: config_keys::UNCLEAN_RECOVERY_STRATEGY.to_string(),
                        config_value: Some("Balanced".to_string()),
                    }),
                ]
        );
    }

    #[test]
    fn recovery_settings_reject_per_broker_scope() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        for (key, value) in [
            (config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "true"),
            (config_keys::UNCLEAN_RECOVERY_STRATEGY, "Balanced"),
        ] {
            let resource = make_resource("1", vec![make_set_cfg(key, value)]);
            let mut out = AlterConfigsResourceResponse::default();
            let mut to_submit = Vec::new();

            handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);

            assert!(out.error_code == codes::INVALID_CONFIG, "key {key}");
            assert!(to_submit.is_empty(), "key {key}");
        }
    }

    #[test]
    fn recovery_settings_validate_cluster_default_values() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        for (key, value) in [
            (config_keys::UNCLEAN_LEADER_ELECTION_ENABLE, "yes"),
            (config_keys::UNCLEAN_RECOVERY_STRATEGY, "fast"),
        ] {
            let resource = make_resource("", vec![make_set_cfg(key, value)]);
            let mut out = AlterConfigsResourceResponse::default();
            let mut to_submit = Vec::new();

            handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);

            assert!(out.error_code == codes::INVALID_CONFIG, "key {key}");
            assert!(to_submit.is_empty(), "key {key}");
        }
    }

    #[test]
    fn controller_managed_broker_configs_are_rejected_as_read_only() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        for key in config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS {
            for cfg in [make_set_cfg(key, "true"), make_del_cfg(key)] {
                for resource_name in ["1", ""] {
                    let resource = make_resource(resource_name, vec![cfg.clone()]);
                    let mut out = AlterConfigsResourceResponse::default();
                    let mut to_submit = Vec::new();

                    handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);

                    check!(out.error_code == codes::INVALID_CONFIG, "key {key}");
                    check!(
                        out.error_message
                            == Some(format!(
                                "broker config {key} is controller-managed and read-only"
                            )),
                        "key {key}"
                    );
                    check!(to_submit.is_empty(), "key {key}");
                }
            }
        }
    }

    #[test]
    fn broker_scoped_unknown_broker_returns_invalid_request() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource("99", vec![]);
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_REQUEST);
    }

    #[test]
    fn broker_scoped_unknown_config_key_returns_invalid_config() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource("1", vec![make_set_cfg("some.unknown.key", "123")]);
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }

    #[test]
    fn broker_scoped_set_produces_broker_config_record() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource(
            "1",
            vec![make_set_cfg(
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                "2048",
            )],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        let expected = vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_audit::NodeId(1),
            config_name: crate::throttle::LEADER_THROTTLED_RATE_KEY.to_string(),
            config_value: Some("2048".to_string()),
        })];
        assert!(to_submit == expected);
    }

    #[test]
    fn broker_scoped_log_dir_rate_is_validated_and_persisted() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource(
            "1",
            vec![make_set_cfg(
                crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY,
                "4096",
            )],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);

        assert!(out.error_code == codes::NONE);
        assert!(
            to_submit
                == vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                    node_id: krabka_audit::NodeId(1),
                    config_name: crate::throttle::ALTER_LOG_DIRS_THROTTLED_RATE_KEY.to_string(),
                    config_value: Some("4096".to_string()),
                })]
        );
    }

    #[test]
    fn broker_scoped_delete_produces_broker_config_record_none_value() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource(
            "1",
            vec![make_del_cfg(crate::throttle::FOLLOWER_THROTTLED_RATE_KEY)],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::NONE);
        let expected = vec![MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_audit::NodeId(1),
            config_name: crate::throttle::FOLLOWER_THROTTLED_RATE_KEY.to_string(),
            config_value: None,
        })];
        assert!(to_submit == expected);
    }

    #[test]
    fn broker_scoped_invalid_rate_value_returns_invalid_config() {
        let img = make_image_with_broker(krabka_audit::NodeId(1));
        let resource = make_resource(
            "1",
            vec![make_set_cfg(
                crate::throttle::LEADER_THROTTLED_RATE_KEY,
                "not-a-number",
            )],
        );
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_broker_scoped(&resource, &img, &mut out, &mut to_submit);
        assert!(out.error_code == codes::INVALID_CONFIG);
        assert!(to_submit.is_empty());
    }
}
