//! The fixtures more than one of this module's unit-test modules needs: the
//! per-fetch soft byte cap every test `ObserverConfig` is built with, the
//! config itself with everything a test does not care about already filled in,
//! and the `ApiVersions` handshake every `MockBroker` an observer dials has to
//! answer before it sees a request.

use std::{path::PathBuf, sync::Arc};

use krabka_units::{ByteSize, mebibytes, minutes};

use super::ObserverConfig;

/// Per-fetch soft byte cap for every observer fixture: 1 MiB.
pub(crate) const TEST_MAX_FETCH_BYTES: ByteSize = mebibytes(1);

/// An observer config a fixture can build on: no voters, the plaintext dialer,
/// a real timer, and no self-written checkpoints. Tests override the fields
/// they are actually about with struct-update syntax, so a new field in
/// [`ObserverConfig`] does not have to be spelled out in every fixture.
///
/// `data_dir` is where the observer keeps its metadata checkpoints, so it must
/// outlive the observer — pass the path of a `TempDir` the test still holds.
pub(crate) fn observer_config(cluster_id: uuid::Uuid, data_dir: PathBuf) -> ObserverConfig {
    ObserverConfig {
        client_dispatch_queue_capacity:
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        client_frame_max: krabka_client_core::ClientFrameMax::default(),
        voters: vec![],
        dialer: Arc::new(krabka_raft::PlaintextDialer),
        client_id: "test-observer".into(),
        cluster_id,
        // Fixtures put the controller at node 1, so the observer is node 2.
        node_id: krabka_raft::NodeId(2),
        data_dir,
        // Off by default: a fixture that is about resuming from disk turns it
        // on, and every other one is spared the image serialization.
        snapshot_interval_records: 0,
        snapshot_fetch_max: krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax::default(),
        max_bytes: TEST_MAX_FETCH_BYTES,
        poll_interval: minutes(1),
        timer: Arc::new(qubit_clock::StdTimer::new()),
    }
}

/// The `ApiVersions` response body a [`krabka_client_core::MockBroker`] must
/// answer the dial handshake with before the observer can issue anything.
pub(crate) fn api_versions_response_v0() -> Vec<u8> {
    use krabka_protocol::{
        Encode as _,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
        },
    };

    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![ApiVersion {
            api_key: api_versions_request::API_KEY,
            min_version: 0,
            max_version: 3,
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    resp.encode(&mut buf, 0).expect("encode ApiVersions");
    buf.to_vec()
}
