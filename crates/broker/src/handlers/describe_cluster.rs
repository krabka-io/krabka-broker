//! `DescribeCluster` (`api_key=60`).
//!
//! This handler projects registrations from the metadata image and broker
//! fencing from the controller's heartbeat registry. Unlike most admin
//! handlers, it never refuses the whole request for a missing ACL: any
//! authenticated principal can read the cluster id, controller id and broker
//! list. Kafka has no `Describe` gate in
//! `KafkaApis.handleDescribeCluster`, and clients such as a Kafka Connect
//! worker rely on `Admin.describeCluster()` succeeding under least-privilege
//! ACLs (#704).
//!
//! KIP-430: when the request sets the
//! `include_cluster_authorized_operations` flag, the response carries a
//! bitfield of the cluster operations the principal is authorized for. If the
//! flag is not set, the field stays at `i32::MIN`, which is Kafka's "not
//! present" sentinel. If the flag is set but the principal lacks `Describe`
//! on `Cluster("kafka-cluster")`, the bitfield is `0` rather than absent --
//! `Describe` gates only this field, not the rest of the response.

#[cfg(test)]
use crate::authorizer::SimpleAclAuthorizer;
use {
    crate::{
        broker::Broker,
        cluster_id::encode as encode_cluster_id,
        codes,
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext,
            acl_wire::CLUSTER_RESOURCE_NAME,
            authorized_operations::authorized_operations_bits,
            cluster_describe_denied,
            controller_id::{NO_CONTROLLER_ID, advertised_controller_id_among},
            offline_replicas::{listener_endpoint, unavailable_brokers},
        },
    },
    krabka_metadata::ResourceType,
    krabka_protocol::owned::{
        describe_cluster_request::DescribeClusterRequest,
        describe_cluster_response::{DescribeClusterBroker, DescribeClusterResponse},
    },
};

/// `DescribeCluster` `endpoint_type` (KIP-919): `1` = BROKER.
const ENDPOINT_TYPE_BROKER: i8 = 1;
/// `DescribeCluster` `endpoint_type` (KIP-919): `2` = CONTROLLER.
const ENDPOINT_TYPE_CONTROLLER: i8 = 2;

/// The `broker_id` a registration's [`NodeId`](krabka_metadata::NodeId) projects
/// to on the wire.
///
/// `DescribeClusterResponse.brokers[].brokerId` is an int32, and so is the
/// `nodeId` every registration path validates, so the conversion cannot fail
/// for a registration the controller accepted. `-1` is the sentinel a client
/// reads as "no such node" if one ever did.
// cargo-mutants: an unobservable sentinel. No registration this broker can hold
// carries a node id above `i32::MAX`, so nothing a test constructs reaches the
// `-1` arm and no mutation of it changes an observable byte. Only the fallback
// is skipped -- `handle` itself stays in the sweep, because the branches around
// it (endpoint type, authorization, fenced filtering, KIP-430 opt-in) are all
// wire-visible and are asserted by the tests below.
#[cfg_attr(test, mutants::skip)]
fn wire_broker_id(node_id: u64) -> i32 { i32::try_from(node_id).unwrap_or(-1) }

pub async fn handle(
    broker: &Broker,
    req: DescribeClusterRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<DescribeClusterResponse, BrokerError> {
    let image = broker.controller.current_image();

    // KIP-919 endpoint-type check, matching Kafka's `AuthHelper` exactly. A
    // broker listener only ever serves `BROKER`; the requested type decides
    // which error the whole response carries. Neither branch echoes the
    // requested `endpoint_type` back -- the error response leaves it at the
    // schema default (`1`), same as Kafka's `AuthHelper.computeDescribeClusterResponse`.
    if req.endpoint_type == ENDPOINT_TYPE_CONTROLLER {
        let resp = DescribeClusterResponse {
            error_code: codes::MISMATCHED_ENDPOINT_TYPE,
            error_message: Some(
                "The request was sent to an endpoint of type BROKER, but we wanted an endpoint \
                 of type CONTROLLER"
                    .into(),
            ),
            ..Default::default()
        };
        return Ok(resp);
    }
    if req.endpoint_type != ENDPOINT_TYPE_BROKER {
        // Anything other than BROKER or CONTROLLER is EndpointType.UNKNOWN.
        // Kafka's v0 schema predates KIP-919's endpoint_type field and has no
        // `UNSUPPORTED_ENDPOINT_TYPE` in its error-code table, so v0 answers
        // INVALID_REQUEST instead.
        let error_code = if version == 0 {
            codes::INVALID_REQUEST
        } else {
            codes::UNSUPPORTED_ENDPOINT_TYPE
        };
        let resp = DescribeClusterResponse {
            error_code,
            error_message: Some(format!("Unsupported endpoint type {}", req.endpoint_type)),
            ..Default::default()
        };
        return Ok(resp);
    }

    // KIP-919: a broker listener serves only BROKERS. KIP-1073 excludes known
    // dead/fenced brokers unless the request opts in, and marks included
    // unavailable rows as fenced. Unknown liveness entries remain eligible
    // while a newly elected controller seeds its heartbeat registry.
    let unavailable = unavailable_brokers(broker, &image).await;

    // Kafka lists only the brokers with an endpoint on the request's listener
    // (`KRaftMetadataCache.getBrokerNodes(listenerName)`); a broker without
    // one is left out rather than advertised at another listener's address.
    let listener = ctx.connection_listener_name;
    let brokers: Vec<DescribeClusterBroker> = image
        .brokers()
        .filter(|b| req.include_fenced_brokers || !unavailable.contains(&b.node_id.0))
        .filter_map(|b| {
            let endpoint = listener_endpoint(b, listener)?;
            Some(DescribeClusterBroker {
                broker_id: wire_broker_id(b.node_id.0),
                host: endpoint.host.clone(),
                port: i32::from(endpoint.port),
                rack: b.rack.clone(),
                is_fenced: unavailable.contains(&b.node_id.0),
                ..Default::default()
            })
        })
        .collect();

    // controller_id: an unfenced registered broker, not the quorum leader.
    // `Metadata` answers from the same helper. See `handlers::controller_id`.
    // `AuthHelper.computeDescribeClusterResponse` answers -1 for an id that
    // the broker list does not hold, so the id is drawn from the brokers with
    // an endpoint on this listener, and checked against the list.
    let controller_id = advertised_controller_id_among(&image, &unavailable, |b| {
        listener_endpoint(b, listener).is_some()
    });
    let controller_id = if brokers.iter().any(|b| b.broker_id == controller_id) {
        controller_id
    } else {
        NO_CONTROLLER_ID
    };

    // KIP-430: only populate the bitfield when the client asked for it;
    // otherwise leave the wire-default `i32::MIN` ("not present") sentinel.
    // `Describe` gates only this field (matching Kafka's
    // `AuthHelper.computeDescribeClusterResponse`), never the rest of the
    // response: without `Describe` the bitfield reads `0` even though the
    // principal may hold other Cluster operations.
    let cluster_authorized_operations = if req.include_cluster_authorized_operations {
        if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            0
        } else {
            authorized_operations_bits(
                broker.config.authorizer.as_ref(),
                &image,
                ctx,
                ResourceType::Cluster,
                CLUSTER_RESOURCE_NAME,
            )
        }
    } else {
        i32::MIN
    };

    Ok(DescribeClusterResponse {
        error_code: codes::NONE,
        error_message: None,
        // Echo the requested endpoint type (KIP-919). v0 has no such field; the
        // request default of `1` keeps the response byte-identical there.
        endpoint_type: req.endpoint_type,
        // Kafka's `Uuid.toString()` is URL-safe unpadded base64 of the 16 raw
        // bytes, not `java.util.UUID`'s hyphenated form. See #1042.
        cluster_id: encode_cluster_id(image.cluster_id()),
        controller_id,
        brokers,
        cluster_authorized_operations,
        throttle_time_ms: 0,
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {

    use {
        super::*,
        crate::{
            authorizer::{AllowAllAuthorizer, Authorizer},
            broker::{Broker, BrokerHandle},
            test_support::{
                self as broker_fixture_support, ControllerPeerAllowed, DenyAll,
                StartedBroker as HandlerStartedBroker, broker_registration,
            },
        },
        assert2::assert,
        krabka_metadata::{AclOperation, BrokerEndpoint, BrokerRegistrationRecord, MetadataRecord},
        krabka_security::ListenerProtocol,
        std::sync::Arc,
    };

    const VERSION: i16 = 2;

    krabka_macros::wire_codec_helpers! {
        test_context ["admin-client"];
    }

    use {
        crate::{
            cluster_id::encode,
            test_support::{allow_acl, start_broker_with_authorizer_no_audit as start_broker},
        },
        std::collections::HashSet,
    };

    fn request(include_ops: bool) -> DescribeClusterRequest {
        DescribeClusterRequest {
            include_cluster_authorized_operations: include_ops,
            endpoint_type: 1,
            ..Default::default()
        }
    }

    async fn seed_broker(handle: &BrokerHandle) {
        handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1BrokerRegistration(
                BrokerRegistrationRecord {
                    broker_epoch: -1,
                    host: "legacy-host".into(),
                    port: 19092,
                    rack: Some("rack-a".into()),
                    endpoints: vec![BrokerEndpoint {
                        name: "PLAINTEXT".into(),
                        host: "broker-a".into(),
                        port: 29092,
                        protocol: ListenerProtocol::Plaintext,
                    }],
                    ..broker_registration(krabka_raft::NodeId(42))
                },
            )])
            .await
            .expect("seed broker registration");
    }

    async fn seeded_broker(authorizer: Arc<dyn Authorizer>) -> (HandlerStartedBroker, Arc<Broker>) {
        let started = start_broker(authorizer).await;
        seed_broker(&started.broker).await;
        let broker = started.broker.broker_arc_for_test();
        (started, broker)
    }

    macro_rules! admin_request_identity {
        ($context:ident) => {
            krabka_macros::request_test_identity! {
                bind($context);
                user ["admin"];
            };
        };
    }

    /// #704: a principal without cluster `Describe` is never refused the
    /// whole request. Kafka's `handleDescribeCluster` has no `authorize` call
    /// at all; the missing grant only zeroes `cluster_authorized_operations`,
    /// gated below by `the_authorized_operations_bitfield` table.
    #[tokio::test]
    async fn a_principal_without_describe_still_gets_full_cluster_data() {
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(ControllerPeerAllowed(DenyAll))).await;
        seed_broker(&broker_handle).await;
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["alice"];
        };

        let resp = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("handle");

        // `start_broker` self-registers as broker 1 with a dynamic
        // 127.0.0.1 host/port, so the broker list is compared by content
        // (as `broker_endpoint_response_preserves_non_default_fields` does)
        // rather than as a whole struct against a literal.
        check_cluster_data(&broker, &resp);
        assert!(resp.brokers.len() == 2);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn broker_endpoint_response_preserves_non_default_fields() {
        let (broker_handle, broker) = seeded_broker(Arc::new(AllowAllAuthorizer)).await;
        admin_request_identity!(ctx);

        let resp = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("handle");

        check_cluster_data(&broker, &resp);
        broker_handle.broker.shutdown().await;
    }

    #[derive(Default)]
    struct DescribeClusterScenario {
        started: Option<HandlerStartedBroker>,
        version: i16,
        endpoint_type: i8,
        principal: String,
        response: Option<DescribeClusterResponse>,
    }
    #[rstest::fixture]
    fn describe_cluster() -> DescribeClusterScenario {
        let started = None;
        DescribeClusterScenario {
            started,
            ..Default::default()
        }
    }
    #[rstest_bdd_macros::given("a broker listener denies all authorization")]
    async fn denied_describe_cluster(describe_cluster: &mut DescribeClusterScenario) {
        describe_cluster.started = Some(start_broker(Arc::new(DenyAll)).await);
        describe_cluster.principal = "alice".into();
    }
    #[rstest_bdd_macros::given("the broker cluster UUID has bytes 1 through 16")]
    async fn fixed_describe_cluster_id(describe_cluster: &mut DescribeClusterScenario) {
        let known_cluster_id = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        describe_cluster.started = Some(
            broker_fixture_support::start_broker_with(|config| {
                config.cluster_id = Some(known_cluster_id);
            })
            .await,
        );
        describe_cluster.principal = "describer".into();
        describe_cluster.version = VERSION;
        describe_cluster.endpoint_type = 1;
    }
    #[rstest_bdd_macros::given(
        "a DescribeCluster version {version} request asks for endpoint type {endpoint}"
    )]
    fn describe_cluster_endpoint_request(
        describe_cluster: &mut DescribeClusterScenario,
        version: i16,
        endpoint: i8,
    ) {
        describe_cluster.version = version;
        describe_cluster.endpoint_type = endpoint;
    }
    #[rstest_bdd_macros::when("the broker listener handles DescribeCluster")]
    async fn handle_describe_cluster(describe_cluster: &mut DescribeClusterScenario) {
        let broker = describe_cluster
            .started
            .as_ref()
            .unwrap()
            .broker
            .broker_arc_for_test();
        let principal = describe_cluster.principal.as_str();
        krabka_macros::request_test_identity! {bind(ctx);user [principal];}
        let req = DescribeClusterRequest {
            endpoint_type: describe_cluster.endpoint_type,
            ..Default::default()
        };
        describe_cluster.response = Some(
            handle(&broker, req, describe_cluster.version, &ctx)
                .await
                .expect("handle"),
        );
    }
    #[rstest_bdd_macros::then(
        "the endpoint refusal is code {code} message {message} with the default broker endpoint and no cluster data"
    )]
    async fn check_describe_cluster_endpoint(
        describe_cluster: &mut DescribeClusterScenario,
        code: i16,
        message: String,
    ) {
        assert!(
            describe_cluster.response.as_ref().unwrap()
                == &krabka_macros::unthrottled_wire!(DescribeClusterResponse {
                    error_code: code,
                    error_message: Some(message),
                    endpoint_type: 1,
                    cluster_id: String::new(),
                    controller_id: -1,
                    brokers: vec![],
                    cluster_authorized_operations: i32::MIN
                })
        );
        describe_cluster
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::then("the reported cluster ID is AQIDBAUGBwgJCgsMDQ4PEA")]
    async fn check_describe_cluster_base64(describe_cluster: &mut DescribeClusterScenario) {
        assert!(
            describe_cluster
                .response
                .as_ref()
                .unwrap()
                .cluster_id
                .as_str()
                == "AQIDBAUGBwgJCgsMDQ4PEA"
        );
        describe_cluster
            .started
            .take()
            .unwrap()
            .broker
            .shutdown()
            .await;
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_describe_cluster/cluster.feature",
        name = "Endpoint type errors precede authorization and retain Kafka response defaults"
    )]
    #[tokio::test]
    async fn endpoint_type_errors_match_kafka(
        describe_cluster: DescribeClusterScenario,
        version: i16,
        endpoint: i8,
        code: i16,
        message: String,
    ) {
        let _ = (describe_cluster, version, endpoint, code, message);
    }
    #[rstest_bdd_macros::scenario(
        path = "tests/features/handler_describe_cluster/cluster.feature",
        name = "The cluster identifier uses Kafka unpadded base64"
    )]
    #[tokio::test]
    async fn reports_cluster_id_in_kafka_base64_form(describe_cluster: DescribeClusterScenario) {
        let _ = describe_cluster;
    }

    /// KIP-430: the bitfield is filled only when the request opts in. Without
    /// the flag the field keeps the `i32::MIN` "not present" sentinel, which
    /// `broker_endpoint_response_preserves_non_default_fields` pins; with it,
    /// the response carries the operations the principal actually holds on the
    /// singleton `Cluster` resource.
    #[tokio::test]
    async fn the_authorized_operations_bitfield_is_filled_only_on_opt_in() {
        let authorizer = Arc::new(AllowAllAuthorizer);
        krabka_macros::broker_test_setup! {
            bind(broker_handle, _dir, broker, ctx);
            boot [start_broker(Arc::<AllowAllAuthorizer>::clone(&authorizer))];
            user ["admin"];
        };

        let resp = handle(&broker, request(true), VERSION, &ctx)
            .await
            .expect("handle");

        let expected = authorized_operations_bits(
            authorizer.as_ref(),
            &broker.controller.current_image(),
            &ctx,
            ResourceType::Cluster,
            CLUSTER_RESOURCE_NAME,
        );
        assert!(expected != i32::MIN);
        assert!(resp.cluster_authorized_operations == expected);
        broker_handle.shutdown().await;
    }

    /// #704: `Describe` gates `cluster_authorized_operations` only, and it
    /// gates the *whole* bitfield, not per-operation. A principal that holds
    /// some other Cluster ACL (here `AlterConfigs`) but not `Describe` still
    /// reads `0`, exactly as `AuthHelper.computeDescribeClusterResponse`
    /// does -- it never falls through to computing a partial mask.
    #[tokio::test]
    async fn cluster_authorized_operations_reads_zero_without_describe_even_with_other_acls() {
        let authorizer = Arc::new(SimpleAclAuthorizer::new(HashSet::new()));
        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::clone(&authorizer) as Arc<dyn Authorizer>).await;
        broker_handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![MetadataRecord::V1AccessControlEntry(
                allow_acl()
                    .resource_type(ResourceType::Cluster)
                    .resource_name(CLUSTER_RESOURCE_NAME)
                    .operation(AclOperation::AlterConfigs)
                    .call(),
            )])
            .await
            .expect("seed ACL");
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(ctx);
            user ["alice"];
        };

        let resp = handle(&broker, request(true), VERSION, &ctx)
            .await
            .expect("handle");

        assert!(resp.error_code == codes::NONE);
        assert!(resp.cluster_authorized_operations == 0);
        broker_handle.shutdown().await;
    }

    /// `wire_broker_id` is the `-1` sentinel fallback that the sweep skips: an
    /// id the controller can register always projects to itself.
    #[test]
    fn a_registered_node_id_projects_to_itself() {
        assert!(wire_broker_id(42) == 42);
    }

    #[tokio::test]
    async fn fenced_brokers_require_explicit_opt_in() {
        let (broker_handle, broker) = seeded_broker(Arc::new(AllowAllAuthorizer)).await;
        broker.liveness.record_fenced_heartbeat(42).await;
        assert!(broker.liveness.apply_fencing(42, true, true).await);
        admin_request_identity!(ctx);

        let response = handle(&broker, request(false), VERSION, &ctx)
            .await
            .expect("exclude fenced broker");
        assert!(response.brokers.iter().all(|row| row.broker_id != 42));

        let mut include_fenced = request(false);
        include_fenced.include_fenced_brokers = true;
        let response = handle(&broker, include_fenced, VERSION, &ctx)
            .await
            .expect("include fenced broker");
        let fenced = response
            .brokers
            .iter()
            .find(|row| row.broker_id == 42)
            .expect("fenced broker row");
        assert!(fenced.is_fenced);

        broker_handle.broker.shutdown().await;
    }

    fn registration(node_id: u64, listeners: &[&str]) -> BrokerRegistrationRecord {
        BrokerRegistrationRecord {
            broker_epoch: -1,
            host: "legacy-host".into(),
            port: 19092,
            endpoints: listeners
                .iter()
                .enumerate()
                .map(|(index, name)| BrokerEndpoint {
                    name: (*name).into(),
                    host: format!("{}-{node_id}", name.to_lowercase()),
                    port: 30_000 + u16::try_from(index).expect("port"),
                    protocol: ListenerProtocol::Plaintext,
                })
                .collect(),
            ..broker_registration(krabka_raft::NodeId(node_id))
        }
    }

    /// #778: Kafka lists only the brokers with an endpoint on the request's
    /// listener (`KRaftMetadataCache.getBrokerNodes(listenerName)`), and
    /// `AuthHelper.computeDescribeClusterResponse` answers -1 for a
    /// `controller_id` the list does not hold. Broker 1 is the test broker
    /// itself, which serves `PLAINTEXT` only.
    #[tokio::test]
    async fn brokers_without_an_endpoint_on_the_request_listener_are_left_out() {
        /// A listener name, the broker rows it must answer with, and the controller ids.
        struct ListenerCase {
            listener: &'static str,
            expected_brokers: Vec<DescribeClusterBroker>,
            controllers: &'static [i32],
        }

        let broker_row = |node_id: i32, listener: &str, port: i32| {
            krabka_macros::tagged_wire!(DescribeClusterBroker {
                broker_id: node_id,
                host: format!("{}-{node_id}", listener.to_lowercase()),
                port,
                rack: None,
                is_fenced: false,
            })
        };
        let cases: [ListenerCase; 3] = [
            ListenerCase {
                listener: "EXTERNAL",
                expected_brokers: vec![broker_row(43, "EXTERNAL", 30_001)],
                controllers: &[43],
            },
            ListenerCase {
                listener: "INTERNAL",
                expected_brokers: vec![],
                controllers: &[-1],
            },
            ListenerCase {
                listener: "PLAINTEXT",
                expected_brokers: vec![
                    broker_row(42, "PLAINTEXT", 30_000),
                    broker_row(43, "PLAINTEXT", 30_000),
                ],
                controllers: &[1, 42, 43],
            },
        ];

        let HandlerStartedBroker {
            broker: broker_handle,
            dir: _dir,
        } = start_broker(Arc::new(AllowAllAuthorizer)).await;
        broker_handle
            .broker_arc_for_test()
            .controller
            .submit_change(vec![
                MetadataRecord::V1BrokerRegistration(registration(42, &["PLAINTEXT"])),
                MetadataRecord::V1BrokerRegistration(registration(43, &["PLAINTEXT", "EXTERNAL"])),
            ])
            .await
            .expect("seed broker registrations");
        let broker = broker_handle.broker_arc_for_test();
        krabka_macros::request_test_identity! {
            bind(p, peer);
            user ["admin"];
        };

        for ListenerCase {
            listener,
            expected_brokers,
            controllers,
        } in cases
        {
            let ctx = RequestContext::new(
                &p,
                &peer,
                "admin-client",
                "test-connection",
                false,
                listener,
            );
            let mut response = handle(&broker, request(false), VERSION, &ctx)
                .await
                .expect("handle");
            // The test broker's own row carries an ephemeral port; the seeded
            // rows are compared whole.
            let lists_self = response.brokers.iter().any(|row| row.broker_id == 1);
            assert!(lists_self == (listener == "PLAINTEXT"), "{listener}");
            response.brokers.retain(|row| row.broker_id != 1);
            response.brokers.sort_by_key(|row| row.broker_id);
            assert!(response.brokers == expected_brokers, "{listener}");
            assert!(
                controllers.contains(&response.controller_id),
                "{listener}: {}",
                response.controller_id
            );
        }
        broker_handle.shutdown().await;
    }
    fn check_cluster_data(broker: &Broker, resp: &DescribeClusterResponse) {
        assert!(
            (
                resp.error_code,
                resp.error_message.clone(),
                resp.endpoint_type,
                resp.cluster_id.clone(),
                // Not requested: stays at the "not present" sentinel even
                // though Describe is denied (#704 gates only this field,
                // never the rest of the response).
                resp.cluster_authorized_operations,
                resp.throttle_time_ms
            ) == (
                codes::NONE,
                None,
                1,
                encode(broker.controller.current_image().cluster_id()),
                i32::MIN,
                0
            )
        );
        let seeded_row = resp
            .brokers
            .iter()
            .find(|b| b.broker_id == 42)
            .expect("seeded broker row");
        let expected_seeded_row = krabka_macros::tagged_wire!(DescribeClusterBroker {
            broker_id: 42,
            host: "broker-a".into(),
            port: 29092,
            rack: Some("rack-a".into()),
            is_fenced: false,
        });
        assert!(*seeded_row == expected_seeded_row);
    }
}
