//! Client-metrics resources for `IncrementalAlterConfigs`, the KIP-714
//! subscription configs. The handler merges the per-key operations onto the
//! subscription's current override map and stages a `V1ClientMetricsConfig`
//! record with the merged map.

use {
    super::{OP_APPEND, OP_DELETE, OP_SET, OP_SUBTRACT},
    crate::{client_metrics::config::validate, codes},
    krabka_metadata::{ClientMetricsConfigRecord, MetadataImage, MetadataRecord},
    krabka_protocol::owned::{
        incremental_alter_configs_request::AlterConfigsResource,
        incremental_alter_configs_response::AlterConfigsResourceResponse,
    },
};

/// Merge per-key ops into a client-metrics subscription's override map and
/// stage a `V1ClientMetricsConfig` record, as Kafka's
/// `ConfigurationControlManager.incrementalAlterConfigResource` does. SET
/// puts the value and DELETE drops the override, so the effective value
/// reverts to its default at read time. APPEND and SUBTRACT answer
/// `INVALID_CONFIG`, because the controller's `KafkaConfigSchema` holds no
/// `CLIENT_METRICS` definitions, so no key is splittable. The merged map is
/// then checked by `ClientMetricsConfigs.validate`. A request that changes
/// nothing, such as a DELETE of a key with no value, stages no record.
pub(super) fn handle_client_metrics_scoped(
    resource: &AlterConfigsResource,
    image: &MetadataImage,
    out: &mut AlterConfigsResourceResponse,
    to_submit: &mut Vec<MetadataRecord>,
) {
    let current = image
        .client_metrics_config(&resource.resource_name)
        .cloned()
        .unwrap_or_default();
    let mut merged = current.clone();
    for cfg in &resource.configs {
        match cfg.config_operation {
            OP_SET => {
                merged.insert(cfg.name.clone(), cfg.value.clone().unwrap_or_default());
            }
            OP_DELETE => {
                merged.remove(&cfg.name);
            }
            op => {
                out.error_code = codes::INVALID_CONFIG;
                out.error_message = Some(match op {
                    OP_APPEND => format!(
                        "Can't APPEND to key {} because its type is not LIST.",
                        cfg.name
                    ),
                    OP_SUBTRACT => format!(
                        "Can't SUBTRACT to key {} because its type is not LIST.",
                        cfg.name
                    ),
                    op => format!(
                        "config_operation={op} not supported for client-metrics key `{}`",
                        cfg.name
                    ),
                });
                return;
            }
        }
    }
    if let Err(error) = validate(&resource.resource_name, &merged) {
        out.error_code = error.code();
        out.error_message = Some(error.message().to_owned());
        return;
    }
    if merged != current {
        to_submit.push(MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: resource.resource_name.clone(),
                configs: merged,
            },
        ));
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::incremental_alter_configs::{
            RESOURCE_TYPE_CLIENT_METRICS, test_support::Handled,
        },
        assert2::check,
        krabka_protocol::owned::incremental_alter_configs_request::AlterableConfig,
        std::collections::BTreeMap,
    };

    /// One alteration: key, operation and value.
    #[derive(Clone, Copy)]
    struct Op<'a> {
        key: &'a str,
        operation: i8,
        value: Option<&'a str>,
    }

    /// One accepted alteration and the records it stages.
    struct AcceptedCase<'a> {
        name: &'a str,
        op: Op<'a>,
        expected: Vec<MetadataRecord>,
    }

    /// An image whose subscription `sub-a` holds `interval.ms=60000` and
    /// `metrics=a.`.
    fn image_with_sub_a() -> MetadataImage {
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            ClientMetricsConfigRecord {
                name: "sub-a".into(),
                configs: maplit::btreemap! {
                    "interval.ms".to_owned() => "60000".to_owned(),
                    "metrics".to_owned() => "a.".to_owned(),
                },
            },
        ));
        img
    }

    fn run(name: &str, ops: &[Op<'_>]) -> Handled {
        let resource = AlterConfigsResource {
            resource_type: RESOURCE_TYPE_CLIENT_METRICS,
            resource_name: name.into(),
            configs: ops
                .iter()
                .map(
                    |&Op {
                         key,
                         operation,
                         value,
                     }| AlterableConfig {
                        name: key.into(),
                        config_operation: operation,
                        value: value.map(Into::into),
                        ..Default::default()
                    },
                )
                .collect(),
            ..Default::default()
        };
        let mut out = AlterConfigsResourceResponse::default();
        let mut to_submit = Vec::new();
        handle_client_metrics_scoped(&resource, &image_with_sub_a(), &mut out, &mut to_submit);
        Handled {
            response: out,
            records: to_submit,
        }
    }

    fn refused(code: i16, message: &str) -> AlterConfigsResourceResponse {
        AlterConfigsResourceResponse {
            error_code: code,
            error_message: Some(message.into()),
            ..Default::default()
        }
    }

    #[derive(Default)]
    struct ClientMetricsAlterationScenario {
        key: String,
        operation: i8,
        value: String,
        handled: Option<Handled>,
    }
    #[rstest::fixture]
    fn client_metrics_alteration() -> ClientMetricsAlterationScenario {
        let key = String::new();
        ClientMetricsAlterationScenario {
            key,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given(
        "subscription sub-a alters key {key} with operation {operation} and value {value}"
    )]
    fn client_metrics_alteration_input(
        client_metrics_alteration: &mut ClientMetricsAlterationScenario,
        key: String,
        operation: i8,
        value: String,
    ) {
        client_metrics_alteration.key = key;
        client_metrics_alteration.operation = operation;
        client_metrics_alteration.value = value;
    }
    #[rstest_bdd_macros::when("the alteration merges with interval 60000 and metrics a.")]
    fn merge_client_metrics_alteration(
        client_metrics_alteration: &mut ClientMetricsAlterationScenario,
    ) {
        client_metrics_alteration.handled = Some(run("sub-a", &[Op {
            key: &client_metrics_alteration.key,
            operation: client_metrics_alteration.operation,
            value: Some(&client_metrics_alteration.value),
        }]));
    }
    #[rstest_bdd_macros::then(
        "the client metrics alteration returns code {code} message {message} and stages no record"
    )]
    fn check_client_metrics_alteration(
        client_metrics_alteration: &ClientMetricsAlterationScenario,
        code: i16,
        message: String,
    ) {
        let handled = client_metrics_alteration.handled.as_ref().unwrap();
        check!(handled.response == refused(code, &message));
        check!(handled.records.is_empty());
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_client_metrics_alteration",fixtures=[client_metrics_alteration:ClientMetricsAlterationScenario]);

    /// Accepted alterations stage the whole merged map, and one that changes
    /// nothing stages no record, as Kafka writes a `ConfigRecord` only for a
    /// key whose value changes.
    #[test]
    fn accepted_alterations_stage_the_merged_map() {
        let record = |configs: BTreeMap<String, String>| {
            vec![MetadataRecord::V1ClientMetricsConfig(
                ClientMetricsConfigRecord {
                    name: "sub-a".into(),
                    configs,
                },
            )]
        };
        let cases: Vec<AcceptedCase<'_>> = vec![
            AcceptedCase {
                name: "interval with spaces is trimmed before parsing",
                op: Op {
                    key: "interval.ms",
                    operation: OP_SET,
                    value: Some(" 1000 "),
                },
                expected: record(maplit::btreemap! {
                    "interval.ms".to_owned() => " 1000 ".to_owned(),
                    "metrics".to_owned() => "a.".to_owned(),
                }),
            },
            AcceptedCase {
                name: "delete of a key with no value changes nothing",
                op: Op {
                    key: "bogus",
                    operation: OP_DELETE,
                    value: None,
                },
                expected: vec![],
            },
            AcceptedCase {
                name: "delete of a set key drops it",
                op: Op {
                    key: "interval.ms",
                    operation: OP_DELETE,
                    value: None,
                },
                expected: record(maplit::btreemap! {"metrics".to_owned() => "a.".to_owned()}),
            },
            AcceptedCase {
                name: "java lookahead pattern",
                op: Op {
                    key: "match",
                    operation: OP_SET,
                    value: Some("client_id=(?!test).*"),
                },
                expected: record(maplit::btreemap! {
                    "interval.ms".to_owned() => "60000".to_owned(),
                    "match".to_owned() => "client_id=(?!test).*".to_owned(),
                    "metrics".to_owned() => "a.".to_owned(),
                }),
            },
            AcceptedCase {
                name: "java named group and backreference",
                op: Op {
                    key: "match",
                    operation: OP_SET,
                    value: Some("client_id=(?<n>a)\\k<n>"),
                },
                expected: record(maplit::btreemap! {
                    "interval.ms".to_owned() => "60000".to_owned(),
                    "match".to_owned() => "client_id=(?<n>a)\\k<n>".to_owned(),
                    "metrics".to_owned() => "a.".to_owned(),
                }),
            },
        ];
        for AcceptedCase { name, op, expected } in cases {
            let Handled {
                response,
                records: to_submit,
            } = run("sub-a", &[op]);
            check!(
                response == AlterConfigsResourceResponse::default(),
                "case {name}"
            );
            check!(to_submit == expected, "case {name}");
        }
    }

    #[test]
    fn empty_subscription_name_is_an_invalid_request() {
        let Handled {
            response,
            records: to_submit,
        } = run("", &[Op {
            key: "interval.ms",
            operation: OP_SET,
            value: Some("60000"),
        }]);
        check!(response == refused(codes::INVALID_REQUEST, "Subscription name can't be empty"));
        check!(to_submit.is_empty());
    }
}
