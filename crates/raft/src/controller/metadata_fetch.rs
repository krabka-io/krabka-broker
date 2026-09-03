//! Committed `__cluster_metadata` reads for observers: the local record-batch
//! slice this node serves out of its own log, and the one-shot outbound fetch a
//! broker-only observer issues against a controller listener.

use std::net::SocketAddr;

use krabka_units::prelude::{ByteSize, ByteSizeExt as _};

use super::ControllerHandle;
use crate::{error::RaftError, types::NodeId};

impl ControllerHandle {
    /// Read committed `__cluster_metadata` entries starting at `fetch_offset`,
    /// encoded as Kafka record batches for an observer.
    #[must_use]
    pub async fn metadata_records(
        &self,
        fetch_offset: u64,
        max_size: ByteSize,
    ) -> crate::kraft::MetadataFetchSlice {
        let off = i64::try_from(fetch_offset).unwrap_or(i64::MAX);
        self.engine.metadata_fetch(off, max_size).await.unwrap_or(
            crate::kraft::MetadataFetchSlice {
                records: bytes::Bytes::new(),
                snapshot_id: None,
                log_start_offset: 0,
                high_watermark: 0,
                quorum_high_watermark: 0,
            },
        )
    }

    /// Dial a controller-listener `addr` and issue one `API_KEY_METADATA_FETCH`.
    /// Used by broker-only observers to pull committed `__cluster_metadata`.
    ///
    /// # Errors
    /// - [`RaftError::Network`] if the dial or request fails.
    /// - [`RaftError::Protocol`] if the response cannot be decoded.
    pub async fn fetch_metadata_from(
        &self,
        addr: SocketAddr,
        fetch_offset: u64,
        max_size: ByteSize,
    ) -> Result<crate::wire::KrabkaMetadataFetchResponse, RaftError> {
        let req = crate::wire::KrabkaMetadataFetchRequest {
            fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
            // `max_bytes` is the KIP-595-shaped `int32` on the Krabka observer
            // wire; the quantity converts here and nowhere deeper.
            max_bytes: max_size.bytes_i32(),
        };
        let mut body = Vec::with_capacity(12);
        req.encode_v0(&mut body);

        let opts = krabka_client_core::ConnectionOptions {
            client_id: self.client_id.clone(),
            dispatch_queue_capacity: self.client_dispatch_queue_capacity,
            frame_max: self.client_frame_max,
            ..krabka_client_core::ConnectionOptions::default()
        };
        let conn = self
            .dialer
            .dial(NodeId(1), &addr.to_string(), opts)
            .await
            .map_err(RaftError::Network)?;
        let resp_body = conn
            .raw_request(
                crate::wire::API_KEY_METADATA_FETCH,
                0,
                bytes::Bytes::from(body),
            )
            .await
            .map_err(RaftError::Network)?;
        conn.close();

        let mut cur: &[u8] = &resp_body;
        crate::wire::KrabkaMetadataFetchResponse::decode_v0(&mut cur).map_err(RaftError::Protocol)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use krabka_units::prelude::{TimeExt as _, mebibytes};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::{
        config::{BootstrapMode, ControllerConfig},
        controller::{
            Controller,
            test_support::{
                FAST_ELECTION_TIMEOUT, RecordingDialer, TEST_OP_TIMEOUT, UNBOUNDED_FETCH,
                committable_topic_record, submit_change_with_timeout, wait_for_leader,
            },
        },
    };

    #[tokio::test]
    async fn metadata_records_serves_committed_topic() {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord, from_kraft_value};
        use krabka_protocol::records::RecordBatch;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            })],
            "metadata_records seed",
        )
        .await
        .expect("submit");

        let slice = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.metadata_records(0, UNBOUNDED_FETCH),
        )
        .await
        .expect("metadata_records timed out");
        assert2::assert!(slice.high_watermark >= 1);
        let image = MetadataImage::new(Uuid::nil());
        let mut buf: &[u8] = &slice.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            if batch.attributes.is_control_batch() {
                continue;
            }
            for r in &batch.records {
                let Some(value) = r.value.as_ref() else {
                    continue;
                };
                if let Ok(MetadataRecord::V1Topic(t)) = from_kraft_value(value, &image)
                    && t.name == "t"
                {
                    found = true;
                }
            }
        }
        assert2::assert!(found);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_returns_committed_records() {
        use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord, from_kraft_value};
        use krabka_protocol::records::RecordBatch;

        let dir = TempDir::new().unwrap();
        let cfg = ControllerConfig {
            bootstrap_mode: BootstrapMode::Bootstrap,
            ..ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf())
        };
        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![MetadataRecord::V1Topic(TopicRecord {
                name: "fetched".into(),
                topic_id: Uuid::new_v4(),
                partitions: 1,
                replication_factor: 1,
            })],
            "fetch_metadata seed",
        )
        .await
        .expect("submit");

        let addr = ctrl.controller_bound_addr();
        let resp = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.fetch_metadata_from(addr, 0, mebibytes(1)),
        )
        .await
        .expect("fetch_metadata_from timed out")
        .expect("fetch");
        assert2::assert!(resp.error_code == 0);
        assert2::assert!(resp.high_watermark >= 1);

        let image = MetadataImage::new(Uuid::nil());
        let mut buf: &[u8] = &resp.records;
        let mut found = false;
        while !buf.is_empty() {
            let batch = RecordBatch::decode(&mut buf).expect("decode");
            if batch.attributes.is_control_batch() {
                continue;
            }
            for r in &batch.records {
                let Some(value) = r.value.as_ref() else {
                    continue;
                };
                if let Ok(MetadataRecord::V1Topic(t)) = from_kraft_value(value, &image)
                    && t.name == "fetched"
                {
                    found = true;
                }
            }
        }
        assert2::assert!(found);
        ctrl.shutdown().await;
    }

    #[tokio::test]
    async fn fetch_metadata_from_passes_configured_client_id_to_dialer() {
        let dir = TempDir::new().unwrap();
        let client_ids = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dialer = RecordingDialer {
            client_ids: Arc::clone(&client_ids),
        };
        let mut cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
        cfg.election_timeout = FAST_ELECTION_TIMEOUT;
        cfg.client_id = "metadata-fetch-client".into();
        cfg.dialer = Some(Arc::new(dialer));

        let ctrl = Controller::start(cfg).await.expect("bootstrap");
        wait_for_leader(&ctrl).await;
        submit_change_with_timeout(
            &ctrl,
            vec![committable_topic_record("client-id-check")],
            "client-id fetch seed",
        )
        .await
        .expect("submit");

        let resp = tokio::time::timeout(
            TEST_OP_TIMEOUT.to_std(),
            ctrl.fetch_metadata_from(ctrl.controller_bound_addr(), 0, mebibytes(1)),
        )
        .await
        .expect("fetch_metadata_from timed out")
        .expect("fetch");

        assert2::assert!(resp.error_code == 0);
        assert2::assert!(client_ids.lock().unwrap().as_slice() == ["metadata-fetch-client"]);
        ctrl.shutdown().await;
    }
}
