//! WAL shard registry.

use {
    super::{engine::WalShardEngine, wire::QuorumGroup},
    crate::metrics::BrokerMetrics,
    dashmap::DashMap,
    krabka_ids::PartitionIndex,
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    },
};

/// Per-partition WAL shard identity for distributed WAL registry work stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct ShardId {
    pub(crate) topic_id: uuid::Uuid,
    pub(crate) partition: PartitionIndex,
}

impl ShardId {
    pub(super) fn from_group(group: QuorumGroup) -> Option<Self> {
        match group {
            QuorumGroup::Metadata => None,
            QuorumGroup::DisklessWal {
                topic_id,
                partition,
            } => Some(Self {
                topic_id,
                partition,
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WalPlacement {
    pub(crate) voters: Vec<krabka_raft::NodeId>,
    pub(crate) leader_epoch: i32,
}

/// Registry from shard identity to its in-process WAL engine.
#[derive(Debug)]
pub(crate) struct WalShardRegistry {
    local_node_id: krabka_raft::NodeId,
    engines: DashMap<ShardId, Arc<WalShardEngine>>,
    placements: RwLock<HashMap<ShardId, WalPlacement>>,
    metrics: BrokerMetrics,
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
            metrics: BrokerMetrics::new(),
            #[cfg(any(test, feature = "test-helpers"))]
            follower_fetchers: DashMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_metrics(mut self, metrics: BrokerMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    pub(crate) fn insert(&self, shard_id: ShardId, engine: Arc<WalShardEngine>) {
        let placements = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let voters = placements
            .get(&shard_id)
            .map_or(&[][..], |placement| placement.voters.as_slice());
        engine.configure_distributed(self.local_node_id, voters);
        engine.attach_observability(shard_id, self.metrics.clone());
        self.engines.insert(shard_id, engine);
    }

    /// Atomically install the voter placement derived from one metadata image.
    /// Replacing the map also removes deleted topics and superseded topic IDs.
    pub(crate) fn replace_placements(&self, placements: &HashMap<ShardId, WalPlacement>) {
        let mut current = self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for entry in &self.engines {
            if current.get(entry.key()) != placements.get(entry.key()) {
                entry.value().wake_fetches();
            }
        }
        current.clone_from(placements);
        for entry in &self.engines {
            let voters = current
                .get(entry.key())
                .map_or(&[][..], |placement| placement.voters.as_slice());
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
            .map(|placement| placement.voters.clone())
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
    pub(crate) fn local_node_id(&self) -> krabka_raft::NodeId { self.local_node_id }

    #[must_use]
    pub(crate) fn local_is_leader(&self, shard_id: ShardId) -> bool {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .and_then(|placement| placement.voters.first())
            == Some(&self.local_node_id)
    }

    #[must_use]
    pub(crate) fn local_is_voter(&self, shard_id: ShardId) -> bool {
        self.placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&shard_id)
            .is_some_and(|placement| placement.voters.contains(&self.local_node_id))
    }

    pub(crate) fn remove(&self, shard_id: ShardId) -> Option<Arc<WalShardEngine>> {
        let mut placements = self
            .placements
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        placements.remove(&shard_id);
        self.engines.remove(&shard_id).map(|(_, engine)| {
            engine.clear_observability();
            engine
        })
    }

    /// Read placement and engine under the same placement lock as registration.
    pub(super) fn fetch_target(
        &self,
        shard: ShardId,
    ) -> (Option<WalPlacement>, Option<Arc<WalShardEngine>>) {
        let placements = self
            .placements
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (placements.get(&shard).cloned(), self.get(shard))
    }

    #[cfg(any(test, feature = "test-helpers"))]
    pub(super) fn record_fetcher(&self, shard: ShardId, node: krabka_raft::NodeId) {
        self.follower_fetchers
            .entry(shard)
            .or_default()
            .insert(node);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{
            super::{
                router::WalShardRouter,
                wire::{FENCED_LEADER_EPOCH, OFFSET_OUT_OF_RANGE, UNKNOWN_LEADER_EPOCH},
            },
            *,
        },
        crate::{
            test_support::sasl_principal,
            wal::quorum::{
                engine::WalShardEngine,
                test_support::open_log,
                wire::{encode_fetch_for_group, fetch_request},
            },
        },
        bytes::Bytes,
        krabka_ids::Offset,
        krabka_protocol::{
            Decode,
            owned::fetch_response::FetchResponse,
            records::{Record, RecordBatch},
        },
        krabka_raft::RaftShardRouter,
        std::{future::Future, pin::Pin},
        tempfile::tempdir,
    };

    async fn assert_pending<F: Future>(future: F) -> Pin<Box<F>> {
        let mut future = Box::pin(future);
        assert2::assert!(futures_util::poll!(future.as_mut()).is_pending());
        future
    }

    fn value_batch() -> RecordBatch {
        RecordBatch {
            records: vec![Record {
                value: Some(Bytes::from_static(b"a")),
                ..Record::default()
            }],
            ..RecordBatch::default()
        }
    }

    fn placement(voters: Vec<krabka_raft::NodeId>, leader_epoch: i32) -> WalPlacement {
        WalPlacement {
            voters,
            leader_epoch,
        }
    }

    async fn route_fetch(
        router: &WalShardRouter,
        body: Bytes,
        principal: &krabka_security::Principal,
    ) -> FetchResponse {
        let response = router
            .route(
                krabka_raft::kraft::transport::api_key::FETCH,
                body,
                Some(principal),
            )
            .await
            .unwrap()
            .expect("diskless WAL fetch response");
        FetchResponse::decode(&mut response.as_ref(), 17).unwrap()
    }

    fn broker_principal(id: u64) -> krabka_security::Principal {
        sasl_principal(&format!("broker-{id}"))
    }

    #[tokio::test]
    async fn wal_shard_router_serves_registered_fetch() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        let mut batch = value_batch();
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
        } => placement(vec![krabka_raft::NodeId(9)], 0)});
        let router = WalShardRouter::new(registry).with_principal_node_ids(
            maplit::hashmap! {"admin".to_string() => krabka_raft::NodeId(9)},
        );
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            krabka_raft::NodeId(9),
            0,
            0,
        );
        let principal = sasl_principal("admin");

        let decoded = route_fetch(&router, body, &principal).await;
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
        let source = open_log(dir.path());
        {
            let mut log = source.lock().unwrap();
            for offset in 0..6 {
                let mut batch = value_batch();
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
        registry.replace_placements(
            &maplit::hashmap! {shard => placement(vec![krabka_raft::NodeId(9)], 0)},
        );
        let router = WalShardRouter::new(registry);
        let principal = broker_principal(9);
        for fetch_offset in [4, 7] {
            let body = encode_fetch_for_group(
                QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
                krabka_raft::NodeId(9),
                0,
                fetch_offset,
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
            assert2::assert!(
                partition
                    .records
                    .as_ref()
                    .is_some_and(|records| records.payload_len() == 0)
            );
        }
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
        let log = open_log(dir.path());
        registry.insert(
            shard,
            Arc::new(WalShardEngine::for_logs(
                maplit::btreemap! {krabka_raft::NodeId(1) => log},
            )),
        );
        registry.replace_placements(
            &maplit::hashmap! {shard => placement(vec![krabka_raft::NodeId(2)], 0)},
        );
        let router = WalShardRouter::new(registry);
        let body = encode_fetch_for_group(
            QuorumGroup::diskless_wal(topic_id, partition),
            krabka_raft::NodeId(9),
            0,
            0,
        );
        let principal = broker_principal(9);

        let decoded = route_fetch(&router, body, &principal).await;
        let partition = &decoded.responses[0].partitions[0];
        assert2::assert!((partition.error_code) == (3));
        assert2::assert!(
            partition
                .records
                .as_ref()
                .is_some_and(|records| records.payload_len() == 0)
        );
    }

    #[test]
    fn claimed_voter_must_match_authenticated_node() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        let mut batch = RecordBatch {
            records: vec![Record::default()],
            ..Default::default()
        };
        source.lock().unwrap().append(&mut batch).unwrap();
        let engine = Arc::new(WalShardEngine::new_distributed(source, 3).unwrap());
        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(1)));
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(19),
            partition: PartitionIndex(0),
        };
        registry.insert(shard, Arc::clone(&engine));
        registry.replace_placements(&maplit::hashmap! {shard => placement(
            vec![krabka_raft::NodeId(1), krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
            0,
        )});
        let router = WalShardRouter::new(Arc::clone(&registry));
        let request = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            krabka_raft::NodeId(2),
            0,
            0,
            1,
            krabka_units::mebibytes(1),
        );

        let response = router
            .route_fetch_request(&request, krabka_raft::NodeId(3))
            .unwrap()
            .unwrap();

        assert2::assert!((response.responses[0].partitions[0].error_code) == (3));
        assert2::assert!((engine.durable_watermark()) == (Offset(0)));
    }

    #[test]
    fn wal_shard_registry_fences_mismatched_leader_epochs_before_acknowledging() {
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        source
            .lock()
            .unwrap()
            .append(&mut RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .unwrap();
        let engine = Arc::new(WalShardEngine::new_distributed(source, 3).unwrap());
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(19),
            partition: PartitionIndex(4),
        };
        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(1)));
        registry.replace_placements(&maplit::hashmap! {shard => placement(
            vec![
                krabka_raft::NodeId(1),
                krabka_raft::NodeId(2),
                krabka_raft::NodeId(3),
            ],
            8,
        )});
        registry.insert(shard, Arc::clone(&engine));
        for (epoch, expected) in [(7, FENCED_LEADER_EPOCH), (9, UNKNOWN_LEADER_EPOCH)] {
            let router = WalShardRouter::new(Arc::clone(&registry));
            let request = fetch_request(
                QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
                krabka_raft::NodeId(2),
                epoch,
                epoch,
                1,
                krabka_units::mebibytes(1),
            );
            let response = router
                .route_fetch_request(&request, krabka_raft::NodeId(2))
                .unwrap()
                .unwrap();

            assert2::assert!((response.responses[0].partitions[0].error_code) == (expected));
            assert2::assert!((engine.durable_watermark()) == (Offset(0)));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn wal_fetch_waits_for_append_or_deadline_and_rechecks_placement() {
        use {krabka_raft::NodeId, std::time::Duration};
        let dir = tempdir().unwrap();
        let source = open_log(dir.path());
        let engine = Arc::new(WalShardEngine::new_distributed(Arc::clone(&source), 3).unwrap());
        let registry = Arc::new(WalShardRegistry::new(NodeId(1)));
        let shard = ShardId {
            topic_id: uuid::Uuid::from_u128(199),
            partition: PartitionIndex(0),
        };
        let voters = vec![NodeId(1), NodeId(2), NodeId(3)];
        registry.replace_placements(&HashMap::from([(shard, placement(voters.clone(), 0))]));
        registry.insert(shard, Arc::clone(&engine));
        let router = WalShardRouter::new(Arc::clone(&registry));
        let mut request = fetch_request(
            QuorumGroup::diskless_wal(shard.topic_id, shard.partition),
            NodeId(2),
            0,
            -1,
            0,
            krabka_units::mebibytes(1),
        );
        request.max_wait_ms = 500;
        request.min_bytes = 1;
        {
            let waiting =
                assert_pending(router.route_fetch_request_wait(&request, NodeId(2))).await;
            source.lock().unwrap().append(&mut value_batch()).unwrap();
            let syncing = engine.replicate_and_sync(&source, Offset(1));
            tokio::pin!(syncing);
            // The real durability path announces data before waiting for the remote fsync vote.
            assert2::assert!(futures_util::poll!(syncing.as_mut()).is_pending());
            let response = waiting.await.unwrap().unwrap();
            assert2::assert!(
                response.responses[0].partitions[0]
                    .records
                    .as_ref()
                    .unwrap()
                    .payload_len()
                    > 0
            );
            assert2::assert!(engine.durable_watermark() == Offset(0));
            let mut ack = request.clone();
            ack.topics[0].partitions[0].fetch_offset = 1;
            ack.min_bytes = 0;
            router
                .route_fetch_request_wait(&ack, NodeId(2))
                .await
                .unwrap()
                .unwrap();
            assert2::assert!(syncing.await.unwrap() == Offset(1));
        }
        // Existing records do not wait when they meet the requested minimum.
        {
            let immediate = router.route_fetch_request_wait(&request, NodeId(2));
            tokio::pin!(immediate);
            assert2::assert!(futures_util::poll!(immediate.as_mut()).is_ready());
            let mut larger_minimum = request.clone();
            larger_minimum.min_bytes = i32::MAX;
            let waiting =
                assert_pending(router.route_fetch_request_wait(&larger_minimum, NodeId(2))).await;
            tokio::time::advance(Duration::from_millis(500)).await;
            assert2::assert!(
                waiting.await.unwrap().unwrap().responses[0].partitions[0]
                    .records
                    .as_ref()
                    .unwrap()
                    .payload_len()
                    > 0
            );
        }

        request.topics[0].partitions[0].fetch_offset = 1;
        {
            let mut waiting =
                assert_pending(router.route_fetch_request_wait(&request, NodeId(2))).await;
            tokio::time::advance(Duration::from_millis(400)).await;
            engine.wake_fetches();
            assert2::assert!(futures_util::poll!(waiting.as_mut()).is_pending());
            tokio::time::advance(Duration::from_millis(100)).await;
            let response = waiting.await.unwrap().unwrap();
            assert2::assert!(
                response.responses[0].partitions[0]
                    .records
                    .as_ref()
                    .unwrap()
                    .payload_len()
                    == 0
            );
        }
        for (next_voters, epoch, code) in [
            (voters.clone(), 1, FENCED_LEADER_EPOCH),
            (vec![NodeId(2), NodeId(1), NodeId(3)], 0, 3),
        ] {
            registry.replace_placements(&HashMap::from([(shard, placement(voters.clone(), 0))]));
            let waiting =
                assert_pending(router.route_fetch_request_wait(&request, NodeId(2))).await;
            registry.replace_placements(&HashMap::from([(shard, placement(next_voters, epoch))]));
            let response = waiting.await.unwrap().unwrap();
            assert2::assert!(response.responses[0].partitions[0].error_code == code);
        }
        for field in ["max_wait_ms", "min_bytes"] {
            let mut invalid = request.clone();
            if field == "max_wait_ms" {
                invalid.max_wait_ms = -1;
            } else {
                invalid.min_bytes = -1;
            }
            registry.replace_placements(&HashMap::from([(shard, placement(voters.clone(), 0))]));
            let response = router
                .route_fetch_request_wait(&invalid, NodeId(2))
                .await
                .unwrap()
                .unwrap();
            assert2::assert!(
                response.responses[0].partitions[0].error_code == crate::codes::INVALID_REQUEST
            );
        }
    }

    #[test]
    fn replacing_placements_removes_stale_shards() {
        let registry = Arc::new(WalShardRegistry::new(krabka_raft::NodeId(1)));
        let stale = ShardId {
            topic_id: uuid::Uuid::from_u128(1),
            partition: PartitionIndex(0),
        };
        let current = ShardId {
            topic_id: uuid::Uuid::from_u128(2),
            partition: PartitionIndex(0),
        };
        registry.replace_placements(
            &maplit::hashmap! {stale => placement(vec![krabka_raft::NodeId(1)], 1)},
        );

        registry.replace_placements(
            &maplit::hashmap! {current => placement(vec![krabka_raft::NodeId(2)], 2)},
        );

        assert2::assert!(registry.placement(stale).is_none());
        assert2::assert!((registry.placement(current)) == (Some(vec![krabka_raft::NodeId(2)])));
    }

    #[test]
    fn authenticated_node_id_honors_listener_and_map() {
        use krabka_security::{AuthMethod, Principal};

        let node3 = krabka_raft::NodeId(3);
        let mapped = maplit::hashmap! {"wal-fetcher".to_string() => node3};
        let scram = AuthMethod::SaslScramSha256;
        // (configured map, principal name, auth method, inter-broker listener, expected node)
        let cases = vec![
            (HashMap::new(), "broker-3", scram, false, None),
            (HashMap::new(), "broker-3", scram, true, Some(node3)),
            (HashMap::new(), "CN=broker-3,O=krabka", scram, false, None),
            (
                HashMap::new(),
                "CN=broker-3,O=krabka",
                scram,
                true,
                Some(node3),
            ),
            (
                HashMap::new(),
                "broker-3",
                AuthMethod::Anonymous,
                true,
                None,
            ),
            (mapped.clone(), "wal-fetcher", scram, false, Some(node3)),
            (mapped.clone(), "wal-fetcher", scram, true, Some(node3)),
            (mapped, "broker-3", scram, true, None),
        ];

        for (principal_node_ids, name, auth_method, inter_broker, expected) in cases {
            let router =
                WalShardRouter::new(Arc::new(WalShardRegistry::new(krabka_raft::NodeId(1))))
                    .with_principal_node_ids(principal_node_ids);
            let principal = Principal {
                name: name.to_string(),
                auth_method,
                groups: Vec::new(),
            };

            let actual = router.authenticated_node_id(&principal, inter_broker);

            assert2::check!(actual == expected, "{name} inter_broker={inter_broker}");
        }
    }
}
