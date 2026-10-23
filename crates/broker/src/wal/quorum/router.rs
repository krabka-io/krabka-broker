//! Authenticated WAL Fetch service for broker and Raft listeners.

use {
    super::{
        registry::{ShardId, WalShardRegistry},
        wire::{
            FENCED_LEADER_EPOCH, OFFSET_OUT_OF_RANGE, UNKNOWN_LEADER_EPOCH, WalFetchRequest,
            decode_fetch, decode_fetch_request, encode_fetch_response_struct, fetch_response,
            unknown_shard_fetch_response,
        },
    },
    crate::{BrokerError, codes::INVALID_REQUEST},
    krabka_protocol::records::RecordsPayload,
    krabka_verified::wal::{WalFetchAdmission, wal_fetch_admission},
    std::{collections::HashMap, sync::Arc},
};

/// Routes shard-addressed KIP-595 Fetch requests to the registered diskless
/// WAL engines.
#[derive(Debug, Clone)]
pub(crate) struct WalShardRouter {
    registry: Arc<WalShardRegistry>,
    principal_node_ids: HashMap<String, krabka_raft::NodeId>,
}

impl WalShardRouter {
    #[must_use]
    pub(crate) fn new(registry: Arc<WalShardRegistry>) -> Self {
        Self {
            registry,
            principal_node_ids: HashMap::new(),
        }
    }

    #[must_use]
    pub(crate) fn with_principal_node_ids(
        mut self,
        principal_node_ids: HashMap<String, krabka_raft::NodeId>,
    ) -> Self {
        self.principal_node_ids = principal_node_ids;
        self
    }

    /// Maps an authenticated principal to the WAL node id it may fetch as.
    ///
    /// `inter_broker_principal_node_ids` is authoritative: when it is
    /// populated, only the names it lists map to a node id. The built-in
    /// `broker-<id>` / `CN=broker-<id>` convention is a fallback for an empty
    /// map, and only on the inter-broker listener -- otherwise a principal
    /// created on a client listener could name itself into a voter slot.
    pub(crate) fn authenticated_node_id(
        &self,
        principal: &krabka_security::Principal,
        on_inter_broker_listener: bool,
    ) -> Option<krabka_raft::NodeId> {
        if principal.auth_method == krabka_security::AuthMethod::Anonymous {
            return None;
        }
        if let Some(node_id) = self.principal_node_ids.get(&principal.name) {
            return Some(*node_id);
        }
        if !on_inter_broker_listener || !self.principal_node_ids.is_empty() {
            return None;
        }
        super::wire::conventional_node_id(&principal.name)
    }

    #[cfg(test)]
    pub(crate) fn route_fetch_request(
        &self,
        request: &krabka_protocol::owned::fetch_request::FetchRequest,
        authenticated_from: krabka_raft::NodeId,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, BrokerError>> {
        self.route_decoded_fetch(decode_fetch_request(request)?, Some(authenticated_from))
    }

    pub(crate) async fn route_fetch_request_wait(
        &self,
        request: &krabka_protocol::owned::fetch_request::FetchRequest,
        authenticated_from: krabka_raft::NodeId,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, BrokerError>> {
        self.route_decoded_fetch_wait(decode_fetch_request(request)?, Some(authenticated_from))
            .await
    }

    async fn route_decoded_fetch_wait(
        &self,
        request: WalFetchRequest,
        authenticated_from: Option<krabka_raft::NodeId>,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, BrokerError>> {
        let Some(engine) = self.registry.get(ShardId::from_group(request.group)?) else {
            return self.route_decoded_fetch(request, authenticated_from);
        };
        let deadline = tokio::time::Instant::now()
            + std::time::Duration::from_millis(u64::try_from(request.max_wait_ms).unwrap_or(0));
        loop {
            let progress = engine.fetch_progress_changed();
            tokio::pin!(progress);
            progress.as_mut().enable();
            let response = self.route_decoded_fetch(request, authenticated_from)?;
            let Ok(response) = response else {
                return Some(response);
            };
            let row = &response.responses[0].partitions[0];
            let bytes = row.records.as_ref().map_or(0, RecordsPayload::payload_len);
            if row.error_code != 0
                || row.diverging_epoch.end_offset >= 0
                || bytes >= usize::try_from(request.min_bytes).unwrap_or(0)
                || tokio::time::Instant::now() >= deadline
            {
                return Some(Ok(response));
            }
            tokio::select! {
                () = &mut progress => {},
                () = tokio::time::sleep_until(deadline) => {
                    return self.route_decoded_fetch(request, authenticated_from);
                }
            }
        }
    }

    fn route_decoded_fetch(
        &self,
        request: WalFetchRequest,
        authenticated_from: Option<krabka_raft::NodeId>,
    ) -> Option<Result<krabka_protocol::owned::fetch_response::FetchResponse, BrokerError>> {
        let shard = ShardId::from_group(request.group)?;
        let (placement, engine) = self.registry.fetch_target(shard);
        let voters = placement.as_ref().map_or_else(Vec::new, |placement| {
            placement.voters.iter().map(|voter| voter.0).collect()
        });
        let admission = wal_fetch_admission(
            authenticated_from.map(|node| node.0),
            request.from.0,
            self.registry.local_node_id().0,
            &voters,
            request.current_leader_epoch,
            placement
                .as_ref()
                .map_or(0, |placement| placement.leader_epoch),
        );
        if admission == WalFetchAdmission::Denied {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        }
        if request.max_wait_ms < 0 || request.min_bytes < 0 {
            return Some(Ok(fetch_response(
                request.group,
                0,
                0,
                0,
                bytes::Bytes::new(),
                INVALID_REQUEST,
                None,
            )));
        }
        #[cfg(any(test, feature = "test-helpers"))]
        self.registry.record_fetcher(shard, request.from);
        let Some(engine) = engine else {
            return Some(Ok(unknown_shard_fetch_response(request.group)));
        };
        let epoch_error = match admission {
            WalFetchAdmission::FencedLeaderEpoch => Some(FENCED_LEADER_EPOCH),
            WalFetchAdmission::UnknownLeaderEpoch => Some(UNKNOWN_LEADER_EPOCH),
            WalFetchAdmission::Denied | WalFetchAdmission::Serve => None,
        };
        if let Some(error_code) = epoch_error {
            return Some(Ok(fetch_response(
                request.group,
                0,
                0,
                0,
                bytes::Bytes::new(),
                error_code,
                None,
            )));
        }
        Some(
            engine
                .serve_fetch(
                    krabka_ids::Offset(request.fetch_offset),
                    request.last_fetched_epoch,
                    request.max_size,
                )
                .map(|fetch| {
                    if !fetch.offset_out_of_range && fetch.diverging_epoch.is_none() {
                        engine.record_follower_ack(
                            request.from,
                            krabka_ids::Offset(request.fetch_offset),
                        );
                    }
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
                        fetch
                            .diverging_epoch
                            .map(|(epoch, offset)| (epoch.0, offset.0)),
                    )
                }),
        )
    }
}

impl krabka_raft::RaftShardRouter for WalShardRouter {
    fn route(
        &self,
        api_key: i16,
        body: bytes::Bytes,
        principal: Option<&krabka_security::Principal>,
    ) -> krabka_raft::ShardRouteFuture<'_> {
        // The raft transport only ever carries inter-broker traffic, so the
        // `broker-<id>` convention stays available on it.
        let authenticated_from =
            principal.and_then(|principal| self.authenticated_node_id(principal, true));
        Box::pin(async move {
            if api_key != krabka_raft::kraft::transport::api_key::FETCH {
                return Ok(None);
            }
            let Some(request) = decode_fetch(&body) else {
                return Ok(None);
            };
            let Some(response) = self
                .route_decoded_fetch_wait(request, authenticated_from)
                .await
            else {
                return Ok(None);
            };
            let response =
                response.map_err(|err| krabka_raft::RaftError::ChangeRejected(err.to_string()))?;
            Ok(Some(encode_fetch_response_struct(&response)))
        })
    }
}
