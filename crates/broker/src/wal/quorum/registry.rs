//! WAL shard registry.

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use dashmap::DashMap;
use krabka_ids::PartitionIndex;

use super::{
    engine::WalShardEngine,
    wire::{
        OFFSET_OUT_OF_RANGE, QuorumGroup, WalFetchRequest, decode_fetch, decode_fetch_request,
        encode_fetch_response_struct, fetch_response, unknown_shard_fetch_response,
    },
};

/// Per-partition WAL shard identity for Slice 6a.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ShardId {
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: PartitionIndex,
}

/// Registry from shard identity to its in-process WAL engine.
#[derive(Debug)]
pub(crate) struct WalShardRegistry {
    local_node_id: krabka_raft::NodeId,
    engines: DashMap<ShardId, Arc<WalShardEngine>>,
    placements: RwLock<HashMap<ShardId, Vec<krabka_raft::NodeId>>>,
    #[cfg(any(test, feature = "test-helpers"))]
    follower_fetchers: DashMap<ShardId, std::collections::BTreeSet<krabka_raft::NodeId>>,
}

impl WalShardRegistry {
    #[must_use]
    pub(crate) fn new(local_node_id: krabka_raft::NodeId) -> Self {
        Self {
            local_node_id,
            engines: DashMap::new(),
            placements: RwLock::new(HashMap::new()),
            #[cfg(any(test, feature = "test-helpers"))]
            follower_fetchers: DashMap::new(),
        }
    }

    pub(crate) fn insert(&self, shard_id: ShardId, engine: Arc<WalShardEngine>) {
        let placements = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let voters = placements.get(&shard_id).map_or(&[][..], Vec::as_slice);
        engine.configure_distributed(self.local_node_id, voters);
        self.engines.insert(shard_id, engine);
    }

    /// Atomically install the voter placement derived from one metadata image.
    /// Replacing the map also removes deleted topics and superseded topic IDs.
    pub(crate) fn replace_placements(
        &self,
        placements: &HashMap<ShardId, Vec<krabka_raft::NodeId>>,
    ) {
        let mut current = self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        current.clone_from(placements);
        for entry in &self.engines {
            let voters = current.get(entry.key()).map_or(&[][..], Vec::as_slice);
            entry
                .value()
                .configure_distributed(self.local_node_id, voters);
        }
    }

    #[cfg(any(test, feature = "test-helpers"))]
    #[must_use]
    pub(crate) fn placement(&self, shard_id: ShardId) -> Option<Vec<krabka_raft::NodeId>> {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .cloned()
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub(crate) fn follower_fetcher_count(&self, shard_id: ShardId) -> usize {
        self.follower_fetchers
            .get(&shard_id)
            .map_or(0, |fetchers| fetchers.len())
    }

    pub(crate) fn get(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        self.engines
            .get(&shard_id)
            .map(|entry| entry.value().clone())
    }

    #[must_use]
    pub(crate) fn local_node_id(&self) -> krabka_raft::NodeId {
        self.local_node_id
    }

    #[must_use]
    pub(crate) fn local_is_leader(&self, shard_id: ShardId) -> bool {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .and_then(|voters| voters.first())
            == Some(&self.local_node_id)
    }

    #[must_use]
    pub(crate) fn local_is_voter(&self, shard_id: ShardId) -> bool {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .is_some_and(|voters| voters.contains(&self.local_node_id))
    }

    pub(crate) fn remove(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        let mut placements = self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        placements.remove(&shard_id);
        self.engines.remove(&shard_id).map(|(_, engine)| engine)
    }

    pub(crate) fn route_fetch_request(
        &self,
        request: &krabka_protocol::owned::fetch_request::FetchRequest,
        authenticated_from: krabka_raft::NodeId,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, crate::BrokerError>>
    {
        self.route_decoded_fetch(decode_fetch_request(request)?, Some(authenticated_from))
    }

    fn route_decoded_fetch(
        &self,
        request: WalFetchRequest,
        authenticated_from: Option<krabka_raft::NodeId>,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, crate::BrokerError>>
    {
        let QuorumGroup::DisklessWal {
            topic_id,
            partition,
        } = request.group
        else {
            return None;
        };
        let shard = ShardId {
            topic_id,
            partition,
        };
        let placement = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard)
            .cloned();
        let authorized = placement.as_ref().is_some_and(|voters| {
            Some(request.from) == authenticated_from
                && voters.first() == Some(&self.local_node_id)
                && voters.contains(&request.from)
        });
        if !authorized {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        }
        #[cfg(any(test, feature = "test-helpers"))]
        self.follower_fetchers
            .entry(shard)
            .or_default()
            .insert(request.from);
        let Some(engine) = self.get(shard) else {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        };
        engine.record_follower_ack(request.from, krabka_ids::Offset(request.fetch_offset));
        Some(
            engine
                .serve_fetch(krabka_ids::Offset(request.fetch_offset), request.max_size)
                .map(|fetch| {
                    let error_code = if fetch.offset_out_of_range {
                        OFFSET_OUT_OF_RANGE
                    } else {
                        0
                    };
                    fetch_response(
                        request.group,
                        fetch.high_watermark.0,
                        fetch.log_end_offset.0,
                        fetch.log_start_offset.0,
                        fetch.records,
                        error_code,
                    )
                }),
        )
    }
}

/// Routes shard-addressed KIP-595 Fetch requests to the registered diskless
/// WAL engines.
#[derive(Debug, Clone)]
pub(crate) struct WalShardRouter {
    registry: Arc<WalShardRegistry>,
}

impl WalShardRouter {
    #[must_use]
    pub(crate) fn new(registry: Arc<WalShardRegistry>) -> Self {
        Self { registry }
    }
}

impl krabka_raft::RaftShardRouter for WalShardRouter {
    fn route(
        &self,
        api_key: i16,
        body: bytes::Bytes,
        principal: Option<&krabka_security::Principal>,
    ) -> krabka_raft::ShardRouteFuture<'_> {
        let authenticated_from = principal.and_then(super::wire::authenticated_node_id);
        Box::pin(async move {
            if api_key != krabka_raft::kraft::transport::api_key::FETCH {
                return Ok(None);
            }
            let Some(request) = decode_fetch(&body) else {
                return Ok(None);
            };
            let Some(response) = self
                .registry
                .route_decoded_fetch(request, authenticated_from)
            else {
                return Ok(None);
            };
            let response =
                response.map_err(|err| krabka_raft::RaftError::ChangeRejected(err.to_string()))?;
            Ok(Some(encode_fetch_response_struct(&response)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use bytes::Bytes;
    use krabka_ids::Offset;
    use krabka_log::{Log, LogConfig};
    use krabka_protocol::{
        Decode,
        owned::fetch_response::FetchResponse,
        records::{Record, RecordBatch},
    };
    use krabka_raft::RaftShardRouter;
    use tempfile::tempdir;

    use super::*;
    use crate::wal::quorum::{engine::WalShardEngine, wire::encode_fetch_for_group};

    fn broker_principal(id: u64) -> krabka_security::Principal {
        krabka_security::Principal {
            name: format!("broker-{id}"),
            auth_method: krabka_security::AuthMethod::SaslPlain,
            groups: Vec::new(),
        }
    }

    #[tokio::test]
    async fn wal_shard_router_serves_registered_fetch() {
        let dir = tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let mut batch = RecordBatch {
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(b"a")),
                headers: vec![],
            }],
            ..Default::default()
        };
        source
            .lock()
            .unwrap()
            .append_at(&mut batch, Offset(0))
            .unwrap();
        source.lock().unwrap().sync().unwrap();
        let engine = Arc::new(WalShardEngine::for_logs(
            maplit::btreemap! {krabka_raft::NodeId(1) => source.clone()},
        ));
        engine.replicate_and_sync(&source, Offset(1)).await.unwrap();

        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(9)));
        let topic_id = uuid::Uuid::from_u128(17);
        let partition = PartitionIndex(2);
        registry.insert(
            ShardId {
                topic_id,
                partition,
            },
            engine,
        );
        registry.replace_placements(&maplit::hashmap! {ShardId {
            topic_id,
            partition,
        } => vec![krabka_raft::NodeId(9)]});
        let router = WalShardRouter::new(registry);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            krabka_raft::NodeId(9),
            0,
            0,
        );
        let principal = broker_principal(9);

        let response = router
            .route(
                krabka_raft::kraft::transport::api_key::FETCH,
                body,
                Some(&principal),
            )
            .await
            .unwrap()
            .expect("diskless WAL fetch response");
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.high_watermark) == (1));
        assert2::assert!((partition.last_stable_offset) == (1));
        assert2::assert!((partition.log_start_offset) == (0));
        assert2::assert!(
            partition
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() > 0)
        );
    }

    #[tokio::test]
    async fn wal_shard_router_reports_offset_out_of_range_with_log_bounds() {
        let dir = tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        {
            let mut log = source.lock().unwrap();
            for offset in 0..6 {
                let mut batch = RecordBatch {
                    records: vec![Record {
                        attributes: 0,
                        offset_delta: 0,
                        timestamp_delta: 0,
                        key: None,
                        value: Some(Bytes::from_static(b"a")),
                        headers: vec![],
                    }],
                    ..Default::default()
                };
                log.append_at(&mut batch, Offset(offset)).unwrap();
            }
            log.sync().unwrap();
            log.trim_to_offset(Offset(5)).unwrap();
        }
        let engine = Arc::new(WalShardEngine::for_logs(
            maplit::btreemap! {krabka_raft::NodeId(1) => source},
        ));

        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(9)));
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(18),
            partition: PartitionIndex(3),
        };
        registry.insert(shard, engine);
        registry.replace_placements(&maplit::hashmap! {shard => vec![krabka_raft::NodeId(9)]});
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            krabka_raft::NodeId(9),
            0,
            4,
        );

        let router = WalShardRouter::new(registry);
        let principal = broker_principal(9);
        let response = router
            .route(
                krabka_raft::kraft::transport::api_key::FETCH,
                body,
                Some(&principal),
            )
            .await
            .unwrap()
            .unwrap();
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.error_code) == (OFFSET_OUT_OF_RANGE));
        assert2::assert!((partition.log_start_offset) == (5));
        assert2::assert!((partition.last_stable_offset) == (6));
        assert2::assert!(partition.records.is_none());

        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            krabka_raft::NodeId(9),
            0,
            7,
        );
        let response = router
            .route(
                krabka_raft::kraft::transport::api_key::FETCH,
                body,
                Some(&principal),
            )
            .await
            .unwrap()
            .unwrap();
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.error_code) == (OFFSET_OUT_OF_RANGE));
        assert2::assert!((partition.log_start_offset) == (5));
        assert2::assert!((partition.last_stable_offset) == (6));
        assert2::assert!(partition.records.is_none());
    }

    #[tokio::test]
    async fn wal_shard_router_rejects_a_broker_outside_the_placement() {
        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(2)));
        let topic_id = uuid::Uuid::from_u128(17);
        let partition = PartitionIndex(2);
        let shard = ShardId {
            topic_id,
            partition,
        };
        let dir = tempdir().unwrap();
        let log = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        registry.insert(
            shard,
            Arc::new(WalShardEngine::for_logs(
                maplit::btreemap! {krabka_raft::NodeId(1) => log},
            )),
        );
        registry.replace_placements(&maplit::hashmap! {shard => vec![krabka_raft::NodeId(2)]});
        let router = WalShardRouter::new(registry);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            krabka_raft::NodeId(9),
            0,
            0,
        );
        let principal = broker_principal(9);

        let response = router
            .route(
                krabka_raft::kraft::transport::api_key::FETCH,
                body,
                Some(&principal),
            )
            .await
            .unwrap()
            .expect("diskless WAL fetch response");
        let decoded = FetchResponse::decode(&mut response.as_ref(), 17).unwrap();
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.error_code) == (3));
        assert2::assert!(partition.records.is_none());
    }

    #[test]
    fn claimed_voter_must_match_authenticated_node() {
        let dir = tempdir().unwrap();
        let source = Arc::new(Mutex::new(
            Log::open(dir.path(), LogConfig::default()).unwrap(),
        ));
        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..Default::default()
        };
        source.lock().unwrap().append(&mut batch).unwrap();
        let engine = Arc::new(WalShardEngine::new_distributed(source, 3).unwrap());
        let registry = WalShardRegistry::new(krabka_raft::NodeId(1));
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(19),
            partition: PartitionIndex(0),
        };
        registry.insert(shard, Arc::clone(&engine));
        registry.replace_placements(
            &maplit::hashmap! {shard => vec![krabka_raft::NodeId(1), krabka_raft::NodeId(2), krabka_raft::NodeId(3)]},
        );
        let request = super::super::wire::fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            krabka_raft::NodeId(2),
            0,
            1,
            krabka_units::mebibytes(1),
        );

        let response = registry
            .route_fetch_request(&request, krabka_raft::NodeId(3))
            .unwrap()
            .unwrap();

        assert2::assert!((response.responses[0].partitions[0].error_code) == (3));
        assert2::assert!((engine.durable_watermark()) == (Offset(0)));
    }

    #[test]
    fn replacing_placements_removes_stale_shards() {
        let registry = WalShardRegistry::new(krabka_raft::NodeId(1));
        let stale = ShardId {
            topic_id: uuid::Uuid::from_u128(1),
            partition: PartitionIndex(0),
        };
        let current = ShardId {
            topic_id: uuid::Uuid::from_u128(2),
            partition: PartitionIndex(0),
        };
        registry.replace_placements(&maplit::hashmap! {stale => vec![krabka_raft::NodeId(1)]});

        registry.replace_placements(&maplit::hashmap! {current => vec![krabka_raft::NodeId(2)]});

        assert2::assert!(registry.placement(stale).is_none());
        assert2::assert!((registry.placement(current)) == (Some(vec![krabka_raft::NodeId(2)])));
    }
}
