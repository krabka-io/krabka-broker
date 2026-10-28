//! `ListConfigResources` (`api_key=74`). KIP-1142 enumeration of every
//! resource an admin client can target with `DescribeConfigs` /
//! `AlterConfigs`. The same RPC at v0 was historically called
//! `ListClientMetricsResources` (KIP-714) and returned only client-metrics
//! subscriptions. v1 generalises it with a `resource_types` filter.
//!
//! Krabka surfaces `TOPIC` (2), `BROKER` (4), `BROKER_LOGGER` (8),
//! `CLIENT_METRICS` (16), and `GROUP` (32), which is the set the JVM
//! `ListConfigResourcesRequest.supportedResourceTypes()` reports at v1.
//! `BROKER_LOGGER` enumerates the same names `BROKER` does — one per node —
//! because a logger resource is addressed by the id of the node whose loggers
//! it names.
//!
//! `CLIENT_METRICS` enumerates configured subscription names from the
//! metadata image (see `MetadataImage::client_metrics_subscriptions`).

use {
    crate::{
        broker::Broker,
        codes,
        handlers::{
            RequestContext, acl_denied,
            acl_wire::CLUSTER_RESOURCE_NAME,
            describe_configs::{
                RESOURCE_TYPE_BROKER, RESOURCE_TYPE_BROKER_LOGGER, RESOURCE_TYPE_CLIENT_METRICS,
                RESOURCE_TYPE_GROUP, RESOURCE_TYPE_TOPIC,
            },
        },
    },
    krabka_metadata::AclOperation,
    krabka_protocol::owned::{
        list_config_resources_request::ListConfigResourcesRequest,
        list_config_resources_response::{ConfigResource, ListConfigResourcesResponse},
    },
};

/// Default set returned when v1 callers omit `resource_types` (KIP-1142).
/// Mirrors the JVM admin client's expectation: every supported type the
/// broker can describe configs for.
const DEFAULT_RESOURCE_TYPES: [i8; 5] = [
    RESOURCE_TYPE_TOPIC,
    RESOURCE_TYPE_BROKER,
    RESOURCE_TYPE_BROKER_LOGGER,
    RESOURCE_TYPE_CLIENT_METRICS,
    RESOURCE_TYPE_GROUP,
];

pub fn handle(
    broker: &Broker,
    req: &ListConfigResourcesRequest,
    version: i16,
    ctx: &RequestContext<'_>,
) -> ListConfigResourcesResponse {
    let image = broker.controller.current_image();

    // Whole-request gate. Kafka's `KafkaApis.handleListConfigResources`
    // authorizes `DescribeConfigs` on the cluster. Only `AlterConfigs` and
    // `All` imply it, so a principal with only `Describe` (or `Read`,
    // `Write`, `Delete`, `Alter`) gets `CLUSTER_AUTHORIZATION_FAILED`.
    if acl_denied(
        broker.config.authorizer.as_ref(),
        &image,
        ctx,
        krabka_metadata::ResourceType::Cluster,
        CLUSTER_RESOURCE_NAME,
        AclOperation::DescribeConfigs,
    ) {
        return ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
    }

    // Kafka's `KafkaApis.handleListConfigResources`: if any requested type is
    // not in `ListConfigResourcesRequest.supportedResourceTypes()`, the whole
    // request fails with `UNSUPPORTED_VERSION` and no resources, rather than
    // silently dropping the type it does not recognize.
    if version >= 1 && req.resource_types.iter().any(|rt| !is_supported_type(*rt)) {
        return ListConfigResourcesResponse {
            throttle_time_ms: 0,
            error_code: codes::UNSUPPORTED_VERSION,
            config_resources: vec![],
            ..Default::default()
        };
    }

    let resources = collect_resources(
        &image,
        version,
        &req.resource_types,
        ctx.connection_listener_name,
    );

    ListConfigResourcesResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        config_resources: resources,
        ..Default::default()
    }
}

/// Whether `rt` is one of the types `ListConfigResourcesRequest.
/// supportedResourceTypes()` reports at v1: `TOPIC`, `BROKER`,
/// `BROKER_LOGGER`, `CLIENT_METRICS`, `GROUP`.
fn is_supported_type(rt: i8) -> bool { DEFAULT_RESOURCE_TYPES.contains(&rt) }

/// Resolve the effective filter and enumerate each requested type from the
/// image, as Kafka's `KafkaApis.handleListConfigResources` does. v0 gives
/// client-metrics only. v1 with an empty list gives every supported type. v1
/// with explicit types gives the caller's filter, tested by membership, so a
/// repeated type lists its resources once. The rows follow Kafka's fixed type
/// order: `GROUP`, `CLIENT_METRICS`, `BROKER_LOGGER`, `BROKER`, then `TOPIC`.
/// `BROKER` and `BROKER_LOGGER` list only the brokers with an endpoint on
/// `listener` (`KRaftMetadataCache.getBrokerNodes(listenerName)`). Within one
/// type the rows are sorted (names lexicographically, broker ids numerically)
/// so the wire payload does not depend on `MetadataImage`'s hash order. Callers
/// at v1 must have already rejected any unsupported requested type with
/// `UNSUPPORTED_VERSION`.
fn collect_resources(
    image: &krabka_metadata::MetadataImage,
    version: i16,
    requested: &[i8],
    listener: &str,
) -> Vec<ConfigResource> {
    let wants = |rt: i8| {
        if version < 1 {
            // v0 (legacy ListClientMetricsResources): always client metrics.
            rt == RESOURCE_TYPE_CLIENT_METRICS
        } else {
            requested.is_empty() || requested.contains(&rt)
        }
    };
    let rows = |resource_type: i8, mut names: Vec<String>| {
        names.sort();
        names.into_iter().map(move |resource_name| ConfigResource {
            resource_name,
            resource_type,
            ..Default::default()
        })
    };

    let mut broker_ids: Vec<u64> = image
        .brokers()
        .filter(|b| b.endpoints.iter().any(|endpoint| endpoint.name == listener))
        .map(|b| b.node_id.0)
        .collect();
    broker_ids.sort_unstable();
    let broker_rows = |resource_type: i8| {
        broker_ids.iter().map(move |id| ConfigResource {
            resource_name: id.to_string(),
            resource_type,
            ..Default::default()
        })
    };

    let mut out: Vec<ConfigResource> = Vec::new();
    if wants(RESOURCE_TYPE_GROUP) {
        out.extend(rows(
            RESOURCE_TYPE_GROUP,
            image.group_configs().map(|(id, _)| id.clone()).collect(),
        ));
    }
    if wants(RESOURCE_TYPE_CLIENT_METRICS) {
        out.extend(rows(
            RESOURCE_TYPE_CLIENT_METRICS,
            image
                .client_metrics_subscriptions()
                .map(|(name, _)| name.clone())
                .collect(),
        ));
    }
    if wants(RESOURCE_TYPE_BROKER_LOGGER) {
        out.extend(broker_rows(RESOURCE_TYPE_BROKER_LOGGER));
    }
    if wants(RESOURCE_TYPE_BROKER) {
        out.extend(broker_rows(RESOURCE_TYPE_BROKER));
    }
    if wants(RESOURCE_TYPE_TOPIC) {
        out.extend(rows(
            RESOURCE_TYPE_TOPIC,
            image.topics().map(|t| t.name.clone()).collect(),
        ));
    }
    out
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{authorizer::AllowAllAuthorizer, broker::BrokerHandle},
        assert2::assert,
        krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord, TopicRecord},
        std::sync::Arc,
        uuid::Uuid,
    };

    const VERSION: i16 = 1;

    const LISTENER: &str = "PLAINTEXT";

    fn broker_on(id: u64, listener: &str) -> MetadataRecord {
        MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
            endpoints: vec![krabka_metadata::BrokerEndpoint {
                name: listener.into(),
                host: "127.0.0.1".into(),
                port: 9092,
                protocol: krabka_security::ListenerProtocol::Plaintext,
            }],
            ..broker_registration(krabka_raft::NodeId(id))
        })
    }

    /// Topics `t-b` and `t-a`, brokers 10 and 2 on `PLAINTEXT` and broker 3
    /// on `SSL` only, group config `g-1` and subscription `sub-1`.
    fn populated_image() -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        for name in ["t-b", "t-a"] {
            img.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }));
        }
        img.apply(&broker_on(10, LISTENER));
        img.apply(&broker_on(2, LISTENER));
        img.apply(&broker_on(3, "SSL"));
        img.apply(&MetadataRecord::V1GroupConfig(
            krabka_metadata::GroupConfigRecord {
                group_id: "g-1".into(),
                configs: maplit::btreemap! {"streams.num.standby.replicas".into() => "1".into()},
            },
        ));
        img.apply(&MetadataRecord::V1ClientMetricsConfig(
            krabka_metadata::ClientMetricsConfigRecord {
                name: "sub-1".into(),
                configs: maplit::btreemap! {"interval.ms".into() => "60000".into()},
            },
        ));
        img
    }

    fn listed_topics(broker: &Broker, topic: &str) -> Vec<ConfigResource> {
        let topics = collect_resources(
            &broker.controller.current_image(),
            VERSION,
            &[RESOURCE_TYPE_TOPIC],
            LISTENER,
        );
        assert!(
            topics
                .iter()
                .any(|resource| resource.resource_name == topic)
        );
        topics
    }

    #[derive(Default)]
    struct ConfigResourceListingScenario {
        image: Option<MetadataImage>,
        version: i16,
        requested: Vec<i8>,
        resources: Option<Vec<ConfigResource>>,
    }
    #[rstest::fixture]
    fn config_resource_listing() -> ConfigResourceListingScenario {
        let image = None;
        ConfigResourceListingScenario {
            image,
            ..Default::default()
        }
    }
    fn requested_resource_type(name: &str) -> i8 {
        match name {
            "topic" => RESOURCE_TYPE_TOPIC,
            "broker" => RESOURCE_TYPE_BROKER,
            "broker_logger" => RESOURCE_TYPE_BROKER_LOGGER,
            "client_metrics" => RESOURCE_TYPE_CLIENT_METRICS,
            "group" => RESOURCE_TYPE_GROUP,
            _ => panic!("unknown configuration resource type: {name}"),
        }
    }
    #[rstest_bdd_macros::given(
        "metadata has topics t-b and t-a group g-1 subscription sub-1 plaintext brokers 10 and 2 and SSL-only broker 3"
    )]
    fn available_config_resources(config_resource_listing: &mut ConfigResourceListingScenario) {
        config_resource_listing.image = Some(populated_image());
    }
    #[rstest_bdd_macros::given("ListConfigResources version {version} requests types {types}")]
    fn config_resource_type_filter(
        config_resource_listing: &mut ConfigResourceListingScenario,
        version: i16,
        types: String,
    ) {
        config_resource_listing.version = version;
        config_resource_listing.requested = if types == "all" {
            Vec::new()
        } else {
            types.split(',').map(requested_resource_type).collect()
        };
    }
    #[rstest_bdd_macros::when("configuration resources are collected on the PLAINTEXT listener")]
    fn collect_requested_config_resources(
        config_resource_listing: &mut ConfigResourceListingScenario,
    ) {
        config_resource_listing.resources = Some(collect_resources(
            config_resource_listing.image.as_ref().unwrap(),
            config_resource_listing.version,
            &config_resource_listing.requested,
            LISTENER,
        ));
    }
    #[rstest_bdd_macros::then("the complete configuration resource rows in order are {expected}")]
    fn check_config_resource_listing(
        config_resource_listing: &ConfigResourceListingScenario,
        expected: String,
    ) {
        let expected: Vec<_> = expected
            .split(',')
            .map(|spec| {
                let (kind, name) = spec.split_once(':').unwrap();
                krabka_macros::tagged_wire!(ConfigResource {
                    resource_name: name.into(),
                    resource_type: requested_resource_type(kind)
                })
            })
            .collect();
        assert2::check!(config_resource_listing.resources.as_ref().unwrap() == &expected);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_list_config_resources",fixtures=[config_resource_listing:ConfigResourceListingScenario]);

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    use crate::{
        handlers::test_support::start_empty_acl_broker,
        test_support::{
            StartedBroker as HandlerStartedBroker, broker_registration, describe_configs_acl_cases,
            grant_optional_cluster_operation,
            start_broker_with_authorizer_no_audit as start_broker,
        },
    };

    /// Request the live topic enumeration, including broker-owned topics.
    fn topic_listing_request() -> ListConfigResourcesRequest {
        ListConfigResourcesRequest {
            resource_types: vec![RESOURCE_TYPE_TOPIC],
            ..Default::default()
        }
    }
    /// Start the no-audit listing broker, then commit its topic before creating a caller.
    async fn listing_broker(topic: &str) -> HandlerStartedBroker {
        let started = start_broker(Arc::new(AllowAllAuthorizer)).await;
        seed_topic(&started.broker, topic).await;
        started
    }
    async fn seed_topic(handle: &BrokerHandle, name: &str) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
                name: name.into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            })])
            .await
            .expect("seed topic");
    }

    /// Kafka's `KafkaApis.handleListConfigResources`: a request naming a type
    /// outside `ListConfigResourcesRequest.supportedResourceTypes()` answers
    /// `UNSUPPORTED_VERSION` (35) for the whole request, with no resources —
    /// even when the request also names types krabka does support. Type 64 is
    /// past every type KIP-1142 defines.
    #[tokio::test]
    async fn v1_unsupported_resource_type_fails_the_whole_request() {
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, ctx);
            boot [listing_broker("t-a")];
            user ["admin"];
        );

        let unsupported = krabka_macros::unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            config_resources: vec![],
        });

        for (name, resource_types) in [
            ("unsupported type alone", vec![64]),
            ("supported type mixed with unsupported", vec![
                RESOURCE_TYPE_TOPIC,
                64,
                RESOURCE_TYPE_BROKER,
            ]),
        ] {
            let req = ListConfigResourcesRequest {
                resource_types,
                ..Default::default()
            };

            let resp = handle(&broker, &req, VERSION, &ctx);

            assert2::check!(resp == unsupported, "case {name}");
        }
        broker_handle.shutdown().await;
    }

    /// The all-supported case in the same table Kafka's issue laid out: a
    /// request naming only `TOPIC` still succeeds and returns the topic.
    /// Real Kafka's `handleListConfigResources` enumerates every topic in the
    /// metadata cache for `TOPIC`, internal topics included, so the expected
    /// set is built from the live image (`collect_resources`) rather than
    /// hardcoded to just the seeded topic — a running broker also carries
    /// `__consumer_offsets`, seeded at startup like real Kafka does. Same
    /// pattern as `cluster_describe_configs_gates_the_enumeration`.
    #[tokio::test]
    async fn v1_all_supported_types_still_succeed() {
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, ctx);
            boot [listing_broker("t-a")];
            user ["admin"];
        );

        let req = topic_listing_request();
        let resp = handle(&broker, &req, VERSION, &ctx);

        let topics = listed_topics(&broker, "t-a");
        let expected = krabka_macros::unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::NONE,
            config_resources: topics,
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn denied_handler_response_preserves_error_fields() {
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker] deny_all;
            user ["alice"];
        };
        let req = topic_listing_request();

        let resp = handle(&broker, &req, VERSION, &ctx);

        let expected = krabka_macros::unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            config_resources: vec![],
        });
        assert!(resp == expected);
        broker_handle.shutdown().await;
    }

    /// Kafka's `KafkaApis.handleListConfigResources` authorizes
    /// `DescribeConfigs` on the cluster (#660). Only `AlterConfigs` and `All`
    /// imply it. `Describe` and the operations that imply `Describe` get
    /// `CLUSTER_AUTHORIZATION_FAILED` and no resources.
    #[tokio::test]
    async fn cluster_describe_configs_gates_the_enumeration() {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_empty_acl_broker().await;
        seed_topic(&broker_handle, "orders").await;
        let broker = broker_handle.broker_arc_for_test();
        let req = topic_listing_request();
        let topics = listed_topics(&broker, "orders");
        let allowed = krabka_macros::unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::NONE,
            config_resources: topics,
        });
        let denied = krabka_macros::unthrottled_wire!(ListConfigResourcesResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            config_resources: vec![],
        });

        let mut cases = describe_configs_acl_cases(&allowed, &denied);
        cases.insert(3, ("read", Some(AclOperation::Read), &denied));
        for (user, grant, expected) in cases {
            grant_optional_cluster_operation(&broker_handle, user, grant).await;
            krabka_macros::request_test_identity! {
                bind(ctx);
                user [user];
            };

            let resp = handle(&broker, &req, VERSION, &ctx);

            assert2::check!(resp == *expected, "user {user} with grant {grant:?}");
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn successful_handler_response_preserves_resource_fields() {
        krabka_macros::broker_test_setup!(
            bind(broker_handle, _dir, broker, ctx);
            boot [listing_broker("orders")];
            user ["admin"];
        );
        let req = topic_listing_request();

        let resp = handle(&broker, &req, VERSION, &ctx);

        assert!(resp.error_code == codes::NONE);
        assert!(resp.throttle_time_ms == 0);
        let resource = resp
            .config_resources
            .iter()
            .find(|r| r.resource_name == "orders")
            .expect("seeded topic resource");
        assert!(resource.resource_type == RESOURCE_TYPE_TOPIC);
        broker_handle.shutdown().await;
    }
}
