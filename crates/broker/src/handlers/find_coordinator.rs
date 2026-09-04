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
    authz::{KeySlot, authorize_keys},
    listener::local_advertised_for_listener,
    resolve::{
        parse_share_key, resolve_partition_coordinator, resolve_transaction_keys,
        unavailable_coordinator,
    },
    response::encode_coordinators,
};
use crate::{broker::Broker, codes, error::BrokerError};

const KEY_TYPE_GROUP: i8 = 0;
const KEY_TYPE_TRANSACTION: i8 = 1;
const KEY_TYPE_SHARE: i8 = 2;

fn unavailable_for_keys(keys: Vec<String>, message: &str) -> Vec<Coordinator> {
    keys.into_iter()
        .map(|key| unavailable_coordinator(key, message))
        .collect()
}

fn merge_key_slots(key_slots: Vec<KeySlot>, coordinators: Vec<Coordinator>) -> Vec<Coordinator> {
    let mut resolved = coordinators.into_iter();
    key_slots
        .into_iter()
        .map(|slot| match slot {
            KeySlot::Rejected(coordinator) => coordinator,
            KeySlot::Resolve(_) => resolved
                .next()
                .expect("one coordinator result per admitted key"),
        })
        .collect()
}

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
        let keys: Vec<String> = if version < 4 {
            vec![req.key.clone()]
        } else {
            req.coordinator_keys.clone()
        };

        // ── ACL preamble ────────────────────────────────────────────
        // Per-key authorization: GROUP and TRANSACTION require `Describe` on
        // their keyed resources; SHARE v6+ requires `ClusterAction` on the
        // singleton Cluster resource. Denied or malformed keys retain their
        // original response slots while admitted keys resolve normally.
        let key_slots = authorize_keys(
            broker,
            &controller.current_image(),
            ctx,
            version,
            req.key_type,
            keys,
        );
        let keys: Vec<String> = key_slots
            .iter()
            .filter_map(|slot| match slot {
                KeySlot::Resolve(key) => Some(key.clone()),
                KeySlot::Rejected(_) => None,
            })
            .collect();

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
            KEY_TYPE_TRANSACTION | KEY_TYPE_SHARE if keys.is_empty() => Vec::new(),
            KEY_TYPE_TRANSACTION => {
                // Ensure __transaction_state topic exists before we try to
                // look up partitions in it.
                match crate::txn::bootstrap::ensure_topic(
                    &controller,
                    broker.config.transaction_state_num_partitions,
                    broker.config.transaction_state_replication_factor,
                )
                .await
                {
                    Ok(()) => resolve_transaction_keys(broker, keys, &advertised, ctx),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "txn bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                        );
                        unavailable_for_keys(keys, "txn topic bootstrap failed")
                    }
                }
            }
            KEY_TYPE_SHARE => {
                // Ensure __share_group_state exists before resolving its
                // partitions' leaders.
                let topic_ready = crate::share_coordinator::bootstrap::ensure_topic(
                    &controller,
                    broker.config.share_coordinator.state_topic_num_partitions,
                    broker
                        .config
                        .share_coordinator
                        .state_topic_replication_factor,
                )
                .await;
                if let Err(error) = topic_ready {
                    tracing::warn!(
                        %error,
                        "share-state bootstrap failed; replying COORDINATOR_NOT_AVAILABLE"
                    );
                    unavailable_for_keys(keys, "share-state topic bootstrap failed")
                } else {
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
                                error_code: codes::INVALID_REQUEST,
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
            }
            _ => Vec::new(),
        };

        // Re-attach rejected entries in their original request slots. Kafka's
        // batched response preserves input order even when authorization and
        // resolution produce different errors for adjacent keys.
        coordinators = merge_key_slots(key_slots, coordinators);

        encode_coordinators(broker_id, &advertised, version, coordinators)
    }
}
