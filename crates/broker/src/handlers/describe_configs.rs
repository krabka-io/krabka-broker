//! `DescribeConfigs` (`api_key=32`). It answers with a resource's effective
//! configuration and the provenance of every value in it.
//!
//! - `resource_type=2` (TOPIC): the handler reports every key in
//!   [`crate::config_keys::registry`], not only the ones the topic overrides.
//!   An override reports `DYNAMIC_TOPIC_CONFIG (1)`; a key the cluster-default
//!   broker config supplies reports `DYNAMIC_DEFAULT_BROKER_CONFIG (3)`; the
//!   rest report `DEFAULT_CONFIG (5)`.
//! - `resource_type=4` (BROKER): a numeric name returns the effective dynamic
//!   per-broker and cluster-default overrides, plus the settings read from
//!   the serving process itself. An empty name returns the cluster-wide
//!   defaults. Sources distinguish `DYNAMIC_BROKER_CONFIG (2)` from
//!   `DYNAMIC_DEFAULT_BROKER_CONFIG (3)` and `STATIC_BROKER_CONFIG (4)`. A
//!   numeric name that is not this node is refused with `INVALID_REQUEST`,
//!   which is what `ConfigHelper` in the pinned image does; the JVM
//!   `AdminClient` never sends one, because it routes a broker resource to
//!   the node it names.
//! - `resource_type=16` (`CLIENT_METRICS`) and `resource_type=32` (GROUP) report
//!   their own effective values the same way.
//! - `resource_type=8` (`BROKER_LOGGER`) reports this node's live `tracing`
//!   targets and their effective levels at `DYNAMIC_BROKER_LOGGER_CONFIG (6)`.
//!   The resource is node-local, so its name must be this broker's id.
//! - Every other resource type receives an empty configs list and no error.
//!   The JVM `AdminClient` accepts that.
//!
//! Each entry carries the typed metadata `ConfigEntry` exposes: the
//! `ConfigDef` type byte the client parses the value with, the documentation
//! when the request set `include_documentation`, the synonym chain when it set
//! `include_synonyms`, and `is_sensitive` with the value withheld when the key
//! is one the broker must not disclose. All of it comes from the registry, so
//! `kafka-configs --describe --all` reads the same facts the validator
//! enforces. [`entry`] holds the shaping and the chain order.
//!
//! A broker config that only the controller writes comes back with
//! `read_only` set, next to the static `node.id` entry. See
//! [`crate::config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS`].
//!
//! A numeric broker resource carries five more synthesised read-only keys
//! beside `node.id`: KIP-211's
//! [`offsets.retention.minutes`](crate::config_keys::OFFSETS_RETENTION_MINUTES)
//! and
//! [`offsets.retention.check.interval.ms`](crate::config_keys::OFFSETS_RETENTION_CHECK_INTERVAL_MS),
//! KIP-98's
//! [`transactional.id.expiration.ms`](crate::config_keys::TRANSACTIONAL_ID_EXPIRATION_MS)
//! and
//! [`transaction.remove.expired.transaction.cleanup.interval.ms`](crate::config_keys::TRANSACTION_REMOVE_EXPIRED_CLEANUP_INTERVAL_MS),
//! and the idle window in [`static_configs`] —
//! [`connections.max.idle.ms`](crate::config_keys::CONNECTIONS_MAX_IDLE_MS)
//! together with its per-listener overrides, which are a key per listener
//! rather than a registry row. The process reads all of them once at startup,
//! so no alter can change them and `kafka-configs` must say so. The
//! cluster-default broker resource (an empty `resource_name`) reports dynamic
//! defaults only, which is what Kafka does.
//!
//! The `config_source` of each is provenance and not a value comparison: a key
//! an operator wrote reads `STATIC_BROKER_CONFIG` above the built-in default
//! even when what they wrote is Kafka's own default; one left alone reports
//! that default alone.
//!
//! One stored topic override is read-only too:
//! [`crate::config_keys::DISKLESS`]. A partition reads the data-path flag once,
//! when it is opened, so both alter paths refuse to change it and
//! `kafka-configs` must say so.
//!
//! A topic resource carries one synthesised key beside its stored overrides:
//! KFC-9's [`crate::config_keys::WRITE_FREEZE`]. It is read-only for the same
//! reason. The handler reads it from the freeze registry and not from the
//! topic's override map, because a freeze is never stored as a topic config.
//! The value names the state:
//!
//! - A frozen topic reads `frozen:` and then the registry scope that matched,
//!   for example `frozen:prefixed:tenant-a.` or `frozen:literal:orders`. The
//!   part after `frozen:` is [`crate::freeze::freeze_target`]. The operator
//!   reads one vocabulary here, in the produce-path refusal, in the audit
//!   events, and in a break-glass target. The scope also says whether the
//!   freeze names this topic or covers it through a namespace prefix.
//! - Any other topic reads `false`, at `DEFAULT_CONFIG`. The handler emits the
//!   key when no freeze covers the topic, and for an internal topic, which is
//!   never freezable. An absent key reads the same as a broker that has no
//!   write-freeze feature. The operator cannot separate those two states.
//!
//! The handler honors the `configuration_keys` filter on the request. When the
//! client supplies a non-empty key list, the response holds only those keys. A
//! null list and an empty list both ask for every key, which is what Kafka's
//! `ConfigHelperUtils.toDescribeConfigsResult` does. The synthesised key obeys
//! that filter like every stored key.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        describe_configs_request::DescribeConfigsRequest,
        describe_configs_response::{DescribeConfigsResponse, DescribeConfigsResult},
    },
};
use krabka_units::convert::TimeExt as _;

mod authz;
mod entry;
mod resources;
mod static_configs;
mod wire;

use self::{
    authz::{denied_result, resource_authz_failure},
    entry::EntryOptions,
    resources::{
        BrokerLoggers, ServingBroker, StaticBrokerConfigs, StaticBrokerSetting, describe_one,
    },
};
use crate::{broker::Broker, error::BrokerError};

#[tracing::instrument(
    name = "handle_describe_configs",
    level = "info",
    skip_all,
    fields(api = "DescribeConfigs", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();

    {
        let mut cur: &[u8] = req_bytes;
        let req = DescribeConfigsRequest::decode(&mut cur, version)?;

        let image = controller.current_image();
        // KIP-226: the chain of sources behind each value goes out only when
        // the client asks for it. `req.resources` is consumed below, so the
        // flags are read off the request first.
        let options = EntryOptions::from_request(&req);
        // The node answering the request. Kafka refuses a broker resource
        // that names any other node, because everything a broker resource
        // reports beyond the dynamic overrides is read out of the serving
        // process.
        let serving_node = krabka_metadata::NodeId(broker.config.node_id.0);
        // This node's own static settings, which a named broker resource
        // reports beside its dynamic overrides. The two KIP-98 keys are
        // `ConfigDef.Type::INT` in Kafka, and the broker's config validation
        // already refused a value wider than that. The two KIP-211 keys travel
        // as what the operator named, not as what the broker runs: the source
        // a key reports is provenance, so a key set to its own default is
        // still `STATIC_BROKER_CONFIG`.
        let origins = broker.config.static_config_origins;
        let static_broker = StaticBrokerConfigs {
            txn_id_expiration: StaticBrokerSetting {
                value_ms: broker.config.txn_id_expiration.millis_i32(),
                supplied: origins.txn_id_expiration,
            },
            txn_id_expiration_cleanup_interval: StaticBrokerSetting {
                value_ms: broker
                    .config
                    .txn_id_expiration_cleanup_interval
                    .millis_i32(),
                supplied: origins.txn_id_expiration_cleanup_interval,
            },
            offsets_retention: broker.config.offsets_retention_override,
            offsets_retention_check_interval: broker
                .config
                .offsets_retention_check_interval_override,
            connections_max_idle: broker.config.connections_max_idle,
            connections_max_idle_overrides: &broker.config.connections_max_idle_overrides,
        };
        // ── ACL preamble ────────────────────────────────────────────
        // Per-resource `DescribeConfigs`: Topic → `Topic(name)`; Broker →
        // `Cluster("kafka-cluster")`. On Deny stamp the result entry with
        // the matching authorization-failed code; authorized resources
        // resolve normally.
        let results: Vec<DescribeConfigsResult> = req
            .resources
            .into_iter()
            .map(|r| {
                if let Some(code) = resource_authz_failure(
                    broker.config.authorizer.as_ref(),
                    &image,
                    ctx.principal,
                    ctx.peer,
                    r.resource_type,
                    &r.resource_name,
                ) {
                    denied_result(r.resource_type, r.resource_name, code)
                } else {
                    describe_one(
                        &image,
                        r,
                        ServingBroker {
                            node: serving_node,
                            static_broker,
                            loggers: BrokerLoggers {
                                node_id: broker.config.broker_id,
                                levels: &broker.config.log_levels,
                            },
                        },
                        broker.config.client_metrics_default_interval.millis_i32(),
                        &broker.config.streams_group,
                        options,
                    )
                }
            })
            .collect();

        let resp = DescribeConfigsResponse {
            throttle_time_ms: 0,
            results,
            ..Default::default()
        };
        crate::handlers::encode_response(&resp, version)
    }
}
