//! `DescribeConfigs` (`api_key=32`). It returns the dynamic override configs
//! that the metadata image holds plus the broker's static node id.
//!
//! - `resource_type=2` (TOPIC): the handler reads the per-topic override map
//!   and emits entries with `config_source = DYNAMIC_TOPIC_CONFIG (1)`.
//! - `resource_type=4` (BROKER): a numeric name returns the effective dynamic
//!   per-broker and cluster-default overrides. An empty name returns the
//!   cluster-wide defaults. Sources distinguish `DYNAMIC_BROKER_CONFIG (2)`
//!   from `DYNAMIC_DEFAULT_BROKER_CONFIG (3)` and `STATIC_BROKER_CONFIG (4)`.
//! - Every other resource type receives an empty configs list and no error.
//!   The JVM `AdminClient` accepts that.
//!
//! A broker config that only the controller writes comes back with
//! `read_only` set, next to the static `node.id` entry. See
//! [`crate::config_keys::CONTROLLER_MANAGED_BROKER_CONFIGS`].
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
//! client supplies an explicit key list, the response holds only those keys.
//! The synthesised key obeys that filter like every stored key.

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
mod resources;
mod wire;

use self::{
    authz::{denied_result, resource_authz_failure},
    resources::{StaticBrokerConfigs, describe_one},
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
        // This node's own static settings, which a named broker resource
        // reports beside its dynamic overrides.
        let static_broker = StaticBrokerConfigs {
            txn_id_expiration_ms: broker.config.txn_id_expiration.millis_i64(),
            txn_id_expiration_cleanup_interval_ms: broker
                .config
                .txn_id_expiration_cleanup_interval
                .millis_i64(),
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
                        broker.config.client_metrics_default_interval.millis_i32(),
                        &broker.config.streams_group,
                        static_broker,
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
