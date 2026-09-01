//! Metadata Raft quorum for Krabka.
//!
//! `krabka-raft` runs a hand-rolled KIP-595 `KRaft` consensus engine, the
//! [`kraft::KraftController`], over Krabka's storage ([`krabka_log`]) and
//! transport ([`krabka_client_core`]). The public entry point is
//! [`Controller::start`]. It spawns the engine and opens a TCP listener. That
//! listener serves the real KIP-595 RPCs (Fetch=1, Vote=52,
//! BeginQuorumEpoch=53, EndQuorumEpoch=54) and the Krabka-private observer and
//! forward RPCs. [`Controller::start`] returns a [`ControllerHandle`], which
//! submits metadata changes and reads the current
//! [`krabka_metadata::MetadataImage`].
//!
//! ## Quick start
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use krabka_metadata::{MetadataRecord, TopicRecord};
//! use krabka_raft::{Controller, ControllerConfig, NodeId};
//! use uuid::Uuid;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let dir = tempfile::tempdir()?;
//! let cfg = ControllerConfig::for_tests(NodeId(1), dir.path().to_path_buf());
//! let controller = Controller::start(cfg).await?;
//!
//! controller
//!     .submit_change(vec![MetadataRecord::V1Topic(TopicRecord {
//!         name: "my-topic".into(),
//!         topic_id: Uuid::new_v4(),
//!         partitions: 3,
//!         replication_factor: 1,
//!     })])
//!     .await?;
//!
//! assert2::assert!(controller.current_image().topic("my-topic").is_some());
//! controller.shutdown().await;
//! # Ok(())
//! # }
//! ```
//!
//! ## Capabilities and boundaries
//!
//! The controller persists and recovers `KRaft` metadata records, and it serves
//! and installs KIP-630 snapshots through `FetchSnapshot`. It also publishes
//! the current metadata image to broker tasks and exposes Krabka-private submit
//! and fetch RPCs for broker and observer integration.
//!
//! KIP-853-style observer bootstrap and auto-join are wired through the broker
//! and controller configuration. The older handle-level `add_learner` and
//! `change_membership` compatibility methods still return
//! [`RaftError::Unsupported`]. Mixed JVM and Krabka controller quorums are
//! outside this crate's compatibility target.

#![doc(html_root_url = "https://docs.rs/krabka-raft/0.5.2")]

mod config;
mod controller;
mod error;
pub mod handshake;
pub mod kraft;
mod network;
pub mod reconfig;
/// The deterministic `KRaft` failure-scenario simulator with trace recording,
/// re-exported from the leaf [`krabka_kraft_core::sim`] module. `crabka-docgen`
/// runs [`scenarios::scenarios`] in-process to render the failure-scenario
/// slideshow.
#[cfg(feature = "scenarios")]
pub use krabka_kraft_core::sim as scenarios;
mod server;
mod snapshot;
mod types;
mod wire;

pub use config::{
    BootstrapMode, ControllerAdminRequest, ControllerAdminResponse, ControllerAdminRouteFuture,
    ControllerAdminRouter, ControllerApiVersion, ControllerConfig, ControllerFetchMissLimit,
    MetadataRaftCommandQueueCapacity, MetadataRaftFetchMax, RaftShardRouter, ShardRouteFuture,
};
pub use controller::{
    Controller, ControllerHandle, QuorumState, SnapshotRange, SnapshotSlice, metadata_log_nonempty,
};
pub use error::RaftError;
pub use handshake::{RaftConnection, RaftHandshakeError, RaftListenerHandshake};
pub use kraft::MetadataFetchSlice;
pub use network::{OutboundDialer, PlaintextDialer};
pub use reconfig::{AddVoter, ReconfigOutcome, RemoveVoter, UpdateVoter};
pub use types::{
    AppData, AppDataResponse, DelegationTokenMutation, Node, NodeId, OffsetReservation,
    SubmitChangeResult,
};

/// Serialize a Kafka metadata snapshot, including KIP-853 control state.
///
/// # Errors
/// Returns an error if a metadata or control record cannot be encoded.
pub fn serialize_metadata_snapshot(
    image: &krabka_metadata::MetadataImage,
    last_contained_log_timestamp: i64,
) -> Result<bytes::Bytes, RaftError> {
    snapshot::SnapshotWriter::serialize(image, last_contained_log_timestamp)
}

/// Decode the KIP-630 metadata records from a Kafka metadata snapshot.
///
/// KIP-853 quorum controls are intentionally omitted: a restore formats a new
/// quorum, while the returned records describe the cluster state it recovers.
///
/// # Errors
/// Returns an error when the snapshot framing, ordering, or a metadata record
/// is invalid.
pub fn deserialize_metadata_snapshot(
    bytes: &[u8],
) -> Result<Vec<krabka_metadata::MetadataRecord>, RaftError> {
    Ok(snapshot::SnapshotReader::read(bytes)?.metadata_records)
}
pub use wire::{
    API_KEY_DELEGATION_TOKEN_MUTATION, API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE,
    KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest,
    KrabkaSubmitChangeResponse,
};
