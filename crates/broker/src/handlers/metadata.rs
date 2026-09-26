//! `Metadata` (`api_key=3`). It returns registered broker endpoints and the
//! partitions of the requested topics, or of all topics when `topics` is
//! `None` (or, at version 0, empty).
//!
//! The metadata comes from `controller.current_image()`, the
//! quorum-replicated snapshot, and not from a local in-memory struct.
//!
//! The topic rows follow Kafka's `KafkaApis.handleTopicMetadataRequest`:
//!
//! 1. The request's topics become a set: the distinct names, or the names of
//!    the distinct ids that resolve. An id that does not resolve answers
//!    `UNKNOWN_TOPIC_ID` without authorization.
//! 2. `Describe` on each topic splits the set into described and denied
//!    topics.
//! 3. When the request allows auto-creation and
//!    [`AUTO_CREATE_TOPICS_ENABLE`] holds, a described topic that does not
//!    exist needs `Create` on the cluster or, failing that, on the topic. A
//!    topic denied both leaves the described set.
//! 4. The described set answers full metadata for the topics that exist and a
//!    partitionless row for the rest; see [`missing_topics`].
//!
//! The response lists the unknown ids, then the described set, then the
//! topics denied `Create`, then the topics denied `Describe`. An all-topics
//! request lists no denied topic, so that it does not disclose one.
//!
//! `MetadataResponsePartition` carries no KIP-966 eligible-leader-replica
//! field in any version of Kafka's schema, `0-13` included, so a partition row
//! here stops at `offline_replicas`. `DescribeTopicPartitions` is the only API
//! that reports ELR; see [`crate::handlers::elr`].
//!
//! Leader, ISR and `offline_replicas` are decided together, by
//! `crate::handlers::offline_replicas::partition_availability`, so this API and
//! `DescribeTopicPartitions` cannot report a replica offline in one column and
//! leading in another.

use bytes::Bytes;
use krabka_metadata::{AclOperation, ResourceType};
use krabka_protocol::{
    Decode,
    owned::{
        metadata_request::MetadataRequest,
        metadata_response::{
            MetadataResponse, MetadataResponseBroker, MetadataResponsePartition,
            MetadataResponseTopic,
        },
    },
    primitives::uuid::Uuid as WireUuid,
};

use crate::{
    authorizer::{AuthorizationRequest, AuthorizationResult, authorize_topics},
    broker::Broker,
    codes,
    error::BrokerError,
    handlers::{
        acl_wire::CLUSTER_RESOURCE_NAME, authorized_operations::authorized_operations_bits,
        offline_replicas::NO_LEADER_ID,
    },
};

mod missing_topics;

#[cfg(test)]
mod authorization_tests;
#[cfg(test)]
mod topic_resolution_tests;

/// The first `Metadata` version whose topic rows may carry a null name or a
/// non-zero topic id. Versions 10 and 11 have both fields on the wire, but
/// Kafka refuses a request that uses them.
const FIRST_TOPIC_ID_VERSION: i16 = 12;

/// The versions that carry `cluster_authorized_operations` (KIP-430).
const CLUSTER_AUTHORIZED_OPERATIONS_VERSIONS: std::ops::RangeInclusive<i16> = 8..=10;

/// The first version that carries `topic_authorized_operations` (KIP-430).
const FIRST_TOPIC_AUTHORIZED_OPERATIONS_VERSION: i16 = 8;

/// Kafka's `auto.create.topics.enable`, at Kafka's default.
///
/// The broker has no `auto.create.topics.enable` key yet, so auto-creation
/// runs whenever the request allows it, as it does on a Kafka broker that
/// leaves the key unset.
const AUTO_CREATE_TOPICS_ENABLE: bool = true;

#[tracing::instrument(
    name = "handle_metadata",
    level = "info",
    skip_all,
    fields(api = "Metadata", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    let inter_broker_name = broker.config.inter_broker_listener_name.clone();

    let mut cur: &[u8] = req_bytes;
    let req = MetadataRequest::decode(&mut cur, version)?;

    let image = controller.current_image();

    let requested = match lookup_requested_topics(&image, &req, version) {
        Ok(requested) => requested,
        Err(error_code) => {
            return crate::handlers::encode_response(&error_response(&req, error_code), version);
        }
    };

    // Brokers: enumerate registered nodes from the metadata image. Keep
    // endpoint discovery separate from replica-placement eligibility: a
    // partition can still name a fenced/dead broker as leader until failover
    // commits, and clients need its endpoint to route or receive the broker's
    // protocol error. DescribeCluster v2 exposes authoritative fencing state
    // to placement clients.
    // Each broker's `host:port` is projected from the endpoint matching the
    // listener this request arrived on (Kafka returns the connection
    // listener's advertised address), falling back to the inter-broker
    // endpoint when the connection listener isn't recorded on that broker.
    let brokers = image
        .brokers()
        .map(|broker| project_broker(broker, ctx.connection_listener_name, &inter_broker_name))
        .collect();

    // KIP-112 / KIP-858 `offline_replicas` needs the fenced-broker set as well
    // as the image; see `handlers::offline_replicas`.
    let unavailable = crate::handlers::offline_replicas::unavailable_brokers(broker, &image).await;
    let topics_out = build_topic_rows(
        broker,
        &image,
        ctx,
        &TopicRowInputs {
            request: &req,
            version,
            requested: &requested,
            unavailable: &unavailable,
        },
    )
    .await;

    // controller_id: an unfenced registered broker, not the quorum leader.
    // See `handlers::controller_id`.
    let controller_id =
        crate::handlers::controller_id::advertised_controller_id(&image, &unavailable);

    // KIP-430: the cluster-level field only exists on the wire for v8-10.
    // Kafka answers 0 rather than the bit field when `Describe` on the
    // cluster is denied, and leaves the default `i32::MIN` when the request
    // does not ask.
    let cluster_authorized_operations = if CLUSTER_AUTHORIZED_OPERATIONS_VERSIONS.contains(&version)
        && req.include_cluster_authorized_operations
    {
        if cluster_allows(broker, &image, ctx, AclOperation::Describe) {
            authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &image,
                ctx.principal,
                ctx.peer,
                ResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
            )
        } else {
            0
        }
    } else {
        i32::MIN
    };

    let resp = MetadataResponse {
        throttle_time_ms: 0,
        brokers,
        // Kafka's `Uuid.toString()` is URL-safe unpadded base64 of the 16 raw
        // bytes, not `java.util.UUID`'s hyphenated form. See #1042.
        cluster_id: Some(crate::cluster_id::encode(image.cluster_id())),
        controller_id,
        topics: topics_out,
        cluster_authorized_operations,
        ..Default::default()
    };
    tracing::debug!(
        version,
        req_topics = ?req.topics.as_ref().map(|ts| ts.iter().filter_map(|t| t.name.clone()).collect::<Vec<_>>()),
        resp_brokers = ?resp.brokers.iter().map(|b| format!("{}@{}:{}", b.node_id, b.host, b.port)).collect::<Vec<_>>(),
        resp_controller_id = resp.controller_id,
        resp_cluster_id = ?resp.cluster_id,
        resp_topics = ?resp.topics.iter().map(|t| format!("{}={:?}/p{}", t.name.as_deref().unwrap_or("?"), t.error_code, t.partitions.len())).collect::<Vec<_>>(),
        "metadata response"
    );
    crate::handlers::encode_response(&resp, version)
}

/// Whether the principal of `ctx` holds `operation` on the cluster.
fn cluster_allows(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    operation: AclOperation,
) -> bool {
    broker.config.authorizer.authorize(
        image,
        &AuthorizationRequest {
            principal: ctx.principal,
            host: ctx.peer,
            resource_type: ResourceType::Cluster,
            resource_name: CLUSTER_RESOURCE_NAME,
            operation,
        },
    ) == AuthorizationResult::Allow
}

/// The topics a request asks for, as Kafka's handler sees them before
/// authorization.
#[derive(Debug, Default, PartialEq, Eq)]
struct RequestedTopics {
    /// The request asks for every topic: `topics` is null, or empty at
    /// version 0 (`MetadataRequest.isAllTopics`).
    all: bool,
    /// The request names its topics by id.
    by_id: bool,
    /// The distinct requested ids that name no topic.
    unknown_ids: Vec<WireUuid>,
    /// The distinct topic names to describe: every topic for an all-topics
    /// request, the names of the resolved ids for an id request, and the
    /// requested names otherwise. First occurrence decides the order.
    names: Vec<String>,
}

/// Applies Kafka's version rules for topic ids to a request.
///
/// The rules come from `KafkaApis.handleTopicMetadataRequest`:
///
/// - Versions 10 and 11: a null name or a non-zero id throws
///   `InvalidRequestException`, so the whole request fails with
///   `INVALID_REQUEST`.
/// - Version 12 and later: when any row has a non-zero id, Kafka describes the
///   set of those ids and ignores every name, and every row with the zero id.
/// - Version 12 and later with only zero ids: Kafka describes the set of names.
///   A null name among them makes Kafka throw a `NullPointerException`, so the
///   whole request fails with `UNKNOWN_SERVER_ERROR`. A `cp-kafka` 8.3.1 broker
///   answers that way.
fn lookup_requested_topics(
    image: &krabka_metadata::MetadataImage,
    request: &MetadataRequest,
    version: i16,
) -> Result<RequestedTopics, i16> {
    let topics = match &request.topics {
        Some(topics) if !(version == 0 && topics.is_empty()) => topics,
        _ => {
            return Ok(RequestedTopics {
                all: true,
                names: image.topics().map(|topic| topic.name.clone()).collect(),
                ..Default::default()
            });
        }
    };
    let uses_ids = topics
        .iter()
        .any(|topic| topic.name.is_none() || topic.topic_id != WireUuid::ZERO);
    if version < FIRST_TOPIC_ID_VERSION && uses_ids {
        return Err(codes::INVALID_REQUEST);
    }
    let mut ids: Vec<WireUuid> = Vec::new();
    for topic in topics {
        if topic.topic_id != WireUuid::ZERO && !ids.contains(&topic.topic_id) {
            ids.push(topic.topic_id);
        }
    }
    let mut requested = RequestedTopics {
        by_id: !ids.is_empty(),
        ..Default::default()
    };
    if requested.by_id {
        for id in ids {
            match image.topic_by_id(&uuid::Uuid::from_bytes(id.0)) {
                Some(record) => push_distinct(&mut requested.names, &record.name),
                None => requested.unknown_ids.push(id),
            }
        }
        return Ok(requested);
    }
    for topic in topics {
        let name = topic.name.as_deref().ok_or(codes::UNKNOWN_SERVER_ERROR)?;
        push_distinct(&mut requested.names, name);
    }
    Ok(requested)
}

/// Appends `name` to `names` unless it is already there.
fn push_distinct(names: &mut Vec<String>, name: &str) {
    if !names.iter().any(|known| known == name) {
        names.push(name.to_owned());
    }
}

/// The response that Kafka's `MetadataRequest.getErrorResponse` builds when
/// the handler throws.
///
/// Every requested topic gets a row with `error_code`, its requested name (an
/// empty name for a null one) and its requested id. The response carries no
/// broker, no cluster id and no controller.
fn error_response(request: &MetadataRequest, error_code: i16) -> MetadataResponse {
    MetadataResponse {
        topics: request
            .topics
            .iter()
            .flatten()
            .map(|topic| MetadataResponseTopic {
                error_code,
                name: Some(topic.name.clone().unwrap_or_default()),
                topic_id: topic.topic_id,
                is_internal: false,
                ..Default::default()
            })
            .collect(),
        error_code,
        ..Default::default()
    }
}

/// The per-request inputs every topic row shares. They travel as one struct so
/// the row builders keep a readable arity as the response gains fields.
struct TopicRowInputs<'a> {
    request: &'a MetadataRequest,
    version: i16,
    requested: &'a RequestedTopics,
    /// Brokers the controller currently treats as fenced or dead, from
    /// [`crate::handlers::offline_replicas::unavailable_brokers`].
    unavailable: &'a std::collections::HashSet<u64>,
}

/// The topic rows, in Kafka's order: unknown ids, the described topics, the
/// topics denied `Create`, and the topics denied `Describe`.
async fn build_topic_rows(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    ctx: &crate::handlers::RequestContext<'_>,
    inputs: &TopicRowInputs<'_>,
) -> Vec<MetadataResponseTopic> {
    let requested = inputs.requested;
    let authorizer = broker.config.authorizer.as_ref();
    let allowed = |results: &std::collections::HashMap<&str, AuthorizationResult>, name: &str| {
        results.get(name).copied() == Some(AuthorizationResult::Allow)
    };

    let describe = authorize_topics(
        authorizer,
        image,
        ctx.principal,
        ctx.peer,
        AclOperation::Describe,
        requested.names.iter().map(String::as_str),
    );
    let (mut described, denied_describe): (Vec<&str>, Vec<&str>) = requested
        .names
        .iter()
        .map(String::as_str)
        .partition(|name| allowed(&describe, name));

    let auto_create =
        AUTO_CREATE_TOPICS_ENABLE && inputs.request.allow_auto_topic_creation && !requested.all;
    let mut denied_create: Vec<&str> = Vec::new();
    if auto_create {
        let missing: Vec<&str> = described
            .iter()
            .copied()
            .filter(|name| image.topic(name).is_none())
            .collect();
        if !missing.is_empty() && !cluster_allows(broker, image, ctx, AclOperation::Create) {
            let create = authorize_topics(
                authorizer,
                image,
                ctx.principal,
                ctx.peer,
                AclOperation::Create,
                missing.iter().copied(),
            );
            denied_create = missing
                .into_iter()
                .filter(|name| !allowed(&create, name))
                .collect();
            described.retain(|name| !denied_create.contains(name));
        }
    }

    let mut described_rows: Vec<MetadataResponseTopic> = described
        .iter()
        .filter_map(|name| image.topic(name))
        .map(|record| success_topic_row(broker, image, inputs, record))
        .collect();
    if !requested.all {
        let missing: Vec<&str> = described
            .iter()
            .copied()
            .filter(|name| image.topic(name).is_none())
            .collect();
        if !missing.is_empty() {
            described_rows.extend(
                missing_topics::missing_topic_rows(broker, ctx, &missing, auto_create).await,
            );
        }
    }
    if inputs.version >= FIRST_TOPIC_AUTHORIZED_OPERATIONS_VERSION
        && inputs.request.include_topic_authorized_operations
    {
        for row in &mut described_rows {
            row.topic_authorized_operations = authorized_operations_bits(
                authorizer,
                image,
                ctx.principal,
                ctx.peer,
                ResourceType::Topic,
                row.name.as_deref().unwrap_or_default(),
            );
        }
    }

    let unknown_id_rows = requested
        .unknown_ids
        .iter()
        .map(|id| MetadataResponseTopic {
            error_code: codes::UNKNOWN_TOPIC_ID,
            name: None,
            topic_id: *id,
            ..Default::default()
        });
    // Kafka never creates a topic it denies `Create` on, so the row carries
    // the zero id.
    let denied_create_rows = denied_create.iter().map(|name| MetadataResponseTopic {
        error_code: codes::TOPIC_AUTHORIZATION_FAILED,
        name: Some((*name).to_owned()),
        topic_id: WireUuid::ZERO,
        is_internal: crate::internal_topics::is_internal_topic(&broker.config, name),
        ..Default::default()
    });
    // An all-topics request does not disclose a topic denied `Describe`.
    let denied_describe: &[&str] = if requested.all { &[] } else { &denied_describe };
    let denied_describe_rows = denied_describe.iter().map(|name| {
        if requested.by_id {
            // Kafka does not treat a topic id as secret, so a denied id row
            // carries the real id and a null name.
            MetadataResponseTopic {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                name: None,
                topic_id: image.topic(name).map_or(WireUuid::ZERO, |record| {
                    WireUuid(record.topic_id.into_bytes())
                }),
                ..Default::default()
            }
        } else {
            // A denied name row carries the zero id, so it does not disclose
            // the id, or whether the topic exists.
            MetadataResponseTopic {
                error_code: codes::TOPIC_AUTHORIZATION_FAILED,
                name: Some((*name).to_owned()),
                topic_id: WireUuid::ZERO,
                ..Default::default()
            }
        }
    });
    unknown_id_rows
        .chain(described_rows)
        .chain(denied_create_rows)
        .chain(denied_describe_rows)
        .collect()
}

fn success_topic_row(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
    inputs: &TopicRowInputs<'_>,
    record: &krabka_metadata::TopicRecord,
) -> MetadataResponseTopic {
    let name = record.name.as_str();
    let partitions = image
        .partitions_of(name)
        .map(|partition| {
            let availability = crate::handlers::offline_replicas::partition_availability(
                image,
                partition,
                inputs.unavailable,
            );
            MetadataResponsePartition {
                // Kafka's `KRaftMetadataCache.partitionMetadata` answers a
                // partition it can find no live leader endpoint for with
                // `LEADER_NOT_AVAILABLE` beside the `-1`, and with the
                // replica, ISR and offline lists filled in as usual. Read out
                // of `kafka-metadata-4.3.1.jar`; `DescribeTopicPartitions`
                // takes the same `-1` with no error code, which is why only
                // this API sets one.
                error_code: if availability.leader_id == NO_LEADER_ID {
                    codes::LEADER_NOT_AVAILABLE
                } else {
                    codes::NONE
                },
                partition_index: partition.partition,
                leader_id: availability.leader_id,
                leader_epoch: partition.leader_epoch.0,
                replica_nodes: partition
                    .replicas
                    .iter()
                    .map(|replica| i32::try_from(replica.0).unwrap_or(i32::MAX))
                    .collect(),
                isr_nodes: availability.isr_nodes,
                offline_replicas: availability.offline_replicas,
                ..Default::default()
            }
        })
        .collect();
    MetadataResponseTopic {
        error_code: codes::NONE,
        name: Some(record.name.clone()),
        topic_id: WireUuid(record.topic_id.into_bytes()),
        partitions,
        is_internal: crate::internal_topics::is_internal_topic(&broker.config, name),
        ..Default::default()
    }
}

/// Projects a stored [`krabka_metadata::BrokerRegistrationRecord`] into one
/// wire-format [`MetadataResponseBroker`].
///
/// The Kafka `MetadataResponse` wire format, v0 to v12 at the time of writing,
/// carries exactly one `host:port` and `rack` tuple per broker.
/// `MetadataResponseBroker` has no `endpoints[]` array. Apache Kafka returns
/// the advertised address **of the listener that the request arrived on**, so
/// a TLS client gets the TLS endpoint and a plaintext client gets the
/// plaintext endpoint. This function follows that rule and selects, in order:
///   1. the endpoint whose name matches the connection's listener
///      (`connection_listener_name`), which is the correct, Kafka-faithful
///      choice;
///   2. the inter-broker endpoint, matched by name, as a defensive fallback
///      when this broker has no record of the connection listener, for example
///      in a cluster with heterogeneous listeners;
///   3. the first recorded endpoint;
///   4. the legacy top-level `host` and `port` when `endpoints` is empty.
///
/// This function clamps `node_id` to `i32::MAX` when the openraft `u64`
/// overflows. Broker ids are small in practice, so that clamp is purely
/// defensive.
fn project_broker(
    b: &krabka_metadata::BrokerRegistrationRecord,
    connection_listener_name: &str,
    inter_broker_name: &str,
) -> MetadataResponseBroker {
    let (host, port) = pick_endpoint_host_port(b, connection_listener_name, inter_broker_name);
    MetadataResponseBroker {
        node_id: i32::try_from(b.node_id.0).unwrap_or(i32::MAX),
        host,
        port,
        rack: b.rack.clone(),
        ..Default::default()
    }
}

/// Selects the `(host, port)` to advertise for a registered broker, from the
/// listener that the request arrived on.
///
/// Every handler that projects a broker address into a wire response, such as
/// `Metadata` and `DescribeCluster`, shares this function, so they all treat
/// the connection listener the same way. The selection order is:
///   1. the endpoint whose name matches `connection_listener_name`, because
///      Kafka returns the connection listener's advertised address;
///   2. the inter-broker endpoint, matched by name;
///   3. the first recorded endpoint;
///   4. the legacy top-level `host` and `port` when `endpoints` is empty.
pub(crate) fn pick_endpoint_host_port(
    b: &krabka_metadata::BrokerRegistrationRecord,
    connection_listener_name: &str,
    inter_broker_name: &str,
) -> (String, i32) {
    let primary = b
        .endpoints
        .iter()
        .find(|e| e.name == connection_listener_name)
        .or_else(|| b.endpoints.iter().find(|e| e.name == inter_broker_name))
        .or_else(|| b.endpoints.first());
    match primary {
        Some(e) => (e.host.clone(), i32::from(e.port)),
        None => (b.host.clone(), i32::from(b.port)),
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    fn endpoint(name: &str, host: &str, port: u16) -> krabka_metadata::BrokerEndpoint {
        krabka_metadata::BrokerEndpoint {
            name: name.to_string(),
            host: host.to_string(),
            port,
            protocol: krabka_security::ListenerProtocol::Plaintext,
        }
    }

    fn record(
        endpoints: Vec<krabka_metadata::BrokerEndpoint>,
    ) -> krabka_metadata::BrokerRegistrationRecord {
        krabka_metadata::BrokerRegistrationRecord {
            node_id: krabka_metadata::NodeId(7),
            broker_epoch: 0,
            incarnation_id: uuid::Uuid::nil(),
            host: "legacy-host".to_string(),
            port: 1000,
            rack: Some("rack-a".to_string()),
            log_dirs: vec![],
            endpoints,
            features: std::collections::BTreeMap::new(),
        }
    }

    /// The connection-listener endpoint wins when it is present. A request
    /// that arrived on the `"tls"` listener gets the tls endpoint's host and
    /// port, even though `"plain"` is the inter-broker listener.
    #[test]
    fn project_broker_picks_connection_listener_endpoint() {
        let rec = record(vec![
            endpoint("plain", "plain-host", 9092),
            endpoint("tls", "tls-host", 9094),
        ]);
        let out = project_broker(&rec, "tls", "plain");
        let expected = MetadataResponseBroker {
            node_id: 7,
            host: "tls-host".to_string(),
            port: 9094,
            rack: Some("rack-a".to_string()),
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(out == expected);
    }

    /// A plaintext client on the `"plain"` listener gets the plain endpoint.
    /// This is a regression guard against the behaviour before the fix.
    #[test]
    fn project_broker_picks_plain_for_plain_connection() {
        let rec = record(vec![
            endpoint("plain", "plain-host", 9092),
            endpoint("tls", "tls-host", 9094),
        ]);
        let out = project_broker(&rec, "plain", "plain");
        assert!(out.host == "plain-host");
        assert!(out.port == 9092);
    }

    /// When the broker has no record of the connection listener, it falls back
    /// to the inter-broker endpoint. That keeps the previous behaviour.
    #[test]
    fn project_broker_falls_back_to_inter_broker() {
        let rec = record(vec![
            endpoint("plain", "plain-host", 9092),
            endpoint("tls", "tls-host", 9094),
        ]);
        let out = project_broker(&rec, "external", "plain");
        assert!(out.host == "plain-host");
        assert!(out.port == 9092);
    }

    /// When neither the connection listener nor the inter-broker listener is
    /// present, the broker falls back to the first recorded endpoint.
    #[test]
    fn project_broker_falls_back_to_first_endpoint() {
        let rec = record(vec![
            endpoint("other-a", "host-a", 5000),
            endpoint("other-b", "host-b", 5001),
        ]);
        let out = project_broker(&rec, "tls", "plain");
        assert!(out.host == "host-a");
        assert!(out.port == 5000);
    }

    /// With no endpoint at all, the broker falls back to the legacy top-level
    /// host and port.
    #[test]
    fn project_broker_falls_back_to_legacy_host_port() {
        let rec = record(vec![]);
        let out = project_broker(&rec, "tls", "plain");
        assert!(out.host == "legacy-host");
        assert!(out.port == 1000);
    }

    /// `MetadataResponse.ClusterId` reports Kafka's base64 `Uuid` form, not
    /// `java.util.UUID`'s hyphenated form (#1042). The expected string is what
    /// `org.apache.kafka.common.Uuid(0x0102030405060708L, 0x090a0b0c0d0e0f10L)
    /// .toString()` produces for the same 16 bytes.
    #[tokio::test]
    async fn reports_cluster_id_in_kafka_base64_form() {
        let known_cluster_id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|cfg| {
            cfg.cluster_id = Some(known_cluster_id);
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let p = crate::test_support::principal("describer");
        let peer = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&p, &peer, "metadata-client");
        let req = crate::test_support::encode_request(&MetadataRequest::default(), 9);

        let bytes = handle(&broker, 9, 1, &req, &ctx).await.expect("handle");
        let resp: MetadataResponse = crate::test_support::decode_response(&bytes, 9);

        assert!(resp.cluster_id.as_deref() == Some("AQIDBAUGBwgJCgsMDQ4PEA"));
        broker_handle.shutdown().await;
    }
}
