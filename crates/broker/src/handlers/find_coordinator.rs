//! `FindCoordinator` (`api_key=10`). Supports:
//!   - `key_type=0` (GROUP): hashes the group id to its
//!     `__consumer_offsets` partition and returns that partition's leader.
//!   - `key_type=1` (TRANSACTION): ensures `__transaction_state` exists,
//!     hashes the transaction-id to a partition, resolves the leader, and
//!     returns that broker's address.
//!
//! The handler populates the response fields in both the legacy
//! single-coordinator form, v0-v3, and the per-key `coordinators` array, v4+.

use std::sync::Arc;

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        find_coordinator_request::FindCoordinatorRequest, find_coordinator_response::Coordinator,
    },
};

mod authz;
mod listener;
mod resolve;
mod response;

#[cfg(test)]
mod tests;

use self::{
    authz::authorize_keys,
    listener::local_advertised_for_listener,
    resolve::{
        local_coordinators, parse_share_key, resolve_partition_coordinator,
        resolve_transaction_keys,
    },
    response::{encode_coordinators, encode_error_response},
};
use crate::{broker::Broker, codes, error::BrokerError};

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

// cargo-mutants: the surviving mutant here flips the `-1` fallback in
// `i32::try_from(leader.0).unwrap_or(-1)` (a coordinator broker's node id).
// Kafka broker ids are int32 on the wire, so `try_from` from the u64 NodeId
// never fails and the `-1` branch is unreachable with realistic inputs. The
// live-broker TXN/GROUP coordinator-resolution behaviour is covered by the
// integration suite, not this in-file module.
#[cfg_attr(test, mutants::skip)]
#[tracing::instrument(
    name = "handle_find_coordinator",
    level = "info",
    skip_all,
    fields(api = "FindCoordinator", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let broker_id = broker.config.broker_id;
    // The local broker's advertised `host:port` for the listener this request
    // arrived on (Kafka returns the connection listener's address). Falls back
    // to the legacy top-level `advertised_listener` when the connection
    // listener isn't among this broker's configured listeners.
    let advertised = local_advertised_for_listener(&broker.config, ctx.connection_listener_name);
    let controller = Arc::clone(&broker.controller);
    {
        let mut cur: &[u8] = req_bytes;
        let req = FindCoordinatorRequest::decode(&mut cur, version)?;

        // For v4+, requests carry `coordinator_keys`. For v0-v3 the single
        // `key` field is what the client cares about — populate the legacy
        // top-level fields and also emit a single `Coordinator` entry for
        // that key so the encode path is uniform.
        let keys: Vec<String> = if req.coordinator_keys.is_empty() {
            vec![req.key.clone()]
        } else {
            req.coordinator_keys.clone()
        };

        // ── ACL preamble ────────────────────────────────────────────
        // Per-key `Describe`: GROUP → `Group(key)`; TRANSACTION →
        // `TransactionalId(key)`. Denied keys are emitted with the
        // authorization-failed code (per-entry for the v4+ multi-key
        // array; the v0-v3 top-level fields are derived from the first
        // entry below). Authorized keys resolve normally — so we split
        // `keys` into denied entries + the still-to-resolve list.
        let (mut denied_entries, keys) =
            authorize_keys(broker, &controller.current_image(), ctx, req.key_type, keys);

        let mut coordinators: Vec<Coordinator> = match req.key_type {
            KEY_TYPE_GROUP => {
                let image = controller.current_image();
                keys.into_iter()
                    .map(|key| {
                        let partition =
                            crate::coordinator::partitioner::partition_for_group(&image, &key);
                        resolve_partition_coordinator(
                            broker,
                            &image,
                            crate::coordinator::bootstrap::OFFSETS_TOPIC,
                            partition,
                            key,
                            &advertised,
                            ctx,
                        )
                    })
                    .collect()
            }
            KEY_TYPE_TRANSACTION => {
                // Ensure __transaction_state topic exists before we try to
                // look up partitions in it.
                if let Err(e) = crate::txn::bootstrap::ensure_topic(
                    &controller,
                    broker.config.transaction_state_num_partitions,
                    broker.config.transaction_state_replication_factor,
                )
                .await
                {
                    tracing::warn!(
                        error = %e,
                        "txn bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                    );
                    return encode_error_response(
                        broker_id,
                        &advertised,
                        version,
                        codes::COORDINATOR_NOT_AVAILABLE,
                        Some("txn topic bootstrap failed"),
                    );
                }

                resolve_transaction_keys(broker, keys, &advertised, ctx)
            }
            KEY_TYPE_SHARE => {
                // Ensure __share_group_state exists before resolving its
                // partitions' leaders.
                if let Err(e) = crate::share_coordinator::bootstrap::ensure_topic(
                    &controller,
                    broker.config.share_coordinator.state_topic_num_partitions,
                    broker
                        .config
                        .share_coordinator
                        .state_topic_replication_factor,
                )
                .await
                {
                    tracing::warn!(
                        error = %e,
                        "share-state bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                    );
                    return encode_error_response(
                        broker_id,
                        &advertised,
                        version,
                        codes::COORDINATOR_NOT_AVAILABLE,
                        Some("share-state topic bootstrap failed"),
                    );
                }

                let mut result = Vec::with_capacity(keys.len());
                for k in keys {
                    // Kafka's share-coordinator key is `group:topicId:partition`.
                    // Group ids may contain ':', so split from the right: the
                    // last segment is the partition, the next is the topic id,
                    // and everything before that is the group.
                    let Some((group, topic_uuid, partition)) = parse_share_key(&k) else {
                        result.push(Coordinator {
                            key: k,
                            node_id: -1,
                            host: String::new(),
                            port: -1,
                            error_code: codes::COORDINATOR_NOT_AVAILABLE,
                            error_message: Some("malformed share-state key".into()),
                            ..Default::default()
                        });
                        continue;
                    };

                    let p = crate::share_coordinator::partitioner::partition_for_share_key(
                        group,
                        &topic_uuid,
                        partition,
                        broker.config.share_coordinator.state_topic_num_partitions,
                    );
                    let image = controller.current_image();
                    result.push(resolve_partition_coordinator(
                        broker,
                        &image,
                        crate::share_coordinator::bootstrap::TOPIC,
                        p,
                        k,
                        &advertised,
                        ctx,
                    ));
                }
                result
            }
            unknown => {
                tracing::warn!(key_type = unknown, "unknown FindCoordinator key_type");
                local_coordinators(keys, broker_id, &advertised)
            }
        };

        // Re-attach the authorization-denied entries. They lead the list so
        // a v0-v3 single-key request whose only key was denied surfaces the
        // authorization-failed code in the derived top-level fields below.
        if !denied_entries.is_empty() {
            denied_entries.extend(coordinators);
            coordinators = denied_entries;
        }

        encode_coordinators(broker_id, &advertised, version, coordinators)
    }
}
