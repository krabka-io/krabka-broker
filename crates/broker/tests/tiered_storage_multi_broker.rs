//! In-process multi-broker tiered-storage metadata-sharing test.
//!
//! Proves that a broker which NEVER ran the RLM copy task itself can serve a
//! remote-tier read using segment metadata it consumed from the replicated
//! `__remote_log_metadata` topic.
//!
//! ## Design
//!
//! Three in-process Krabka brokers boot with:
//! - advertised listeners on `127.0.0.1:<port>`, with no Docker and no
//!   host.docker.internal
//! - a **shared** `Local` remote-storage backend, the same temp dir, on all
//!   three
//! - topic-backed RLMM. All clients bootstrap to broker 1's loopback port with
//!   `num_partitions=1, replication=3`, so topic creation waits until every
//!   broker is registered and broker 1 deterministically leads partition 0.
//!
//! The test needs a **3-broker quorum**. After it kills the partition leader,
//! broker 1, the surviving quorum of 2 out of 3 is still a majority and can
//! commit the partition-leader-election record for broker 2. A 2-voter cluster
//! leaves 1 out of 2, which is not a majority, so the raft quorum would break
//! and the partition leader could never move.
//!
//! The metadata-sharing claim is this. Broker 2's RLMM consumer reads
//! `CopySegment*` events from broker 1's `__remote_log_metadata` partition 0
//! over loopback and caches the segment metadata locally. When broker 1 shuts
//! down, broker 2 serves remote reads from that already-consumed cached
//! metadata. The leader-epoch fallback in `remote_reader.rs`, the
//! `list_remote_log_segments` scan, handles the change from broker 1's epoch
//! to broker 2's new leader epoch.
//!
//! Scenario:
//! 1. Three brokers boot concurrently (3-voter static bootstrap).
//! 2. Wait for all 3 to see each other + topic-backed RLMM active on all.
//! 3. Create a rf=2, tiered-storage topic with tiny `segment.bytes` and
//!    `local.retention.bytes=1` so every sealed segment is evicted locally.
//!    With 3 registered brokers and rf=2, the round-robin assignment places
//!    the partition on broker 1 (leader) + broker 2 (follower).
//! 4. Produce 160 records through broker 1, then wait until several segments
//!    land in the shared remote dir. The leader has then run the copy task and
//!    published `CopySegment*` events to `__remote_log_metadata`.
//! 5. Wait 8s for broker 2's RLMM consumer to read the `CopySegment` events.
//! 6. Shut down broker 1, the partition leader. The surviving quorum of 2 out
//!    of 3 commits a new partition leader record, and broker 2 wins the
//!    election.
//! 7. Consume ALL records from broker 2 at offset 0. They can come only from
//!    the shared remote tier, because broker 2's local log is evicted and
//!    broker 2 never ran the copy task. Broker 2 serves them from the cached
//!    metadata.
//!
//! ## Discriminating property
//!
//! The survivor never ran the copy task for these segments, and its local copy
//! is evicted. It can serve them only through the shared Local tier and the
//! shared RLMM metadata. With a per-broker in-memory RLMM, the survivor would
//! have no metadata and the consume would fail. Do NOT weaken the assertion; it
//! must require all records back.
//!
//! The binary root carries only the module tree and the two constants every
//! part shares. [`multi_cluster`] boots the three brokers and waits for them,
//! [`multi_workload`] creates the tiered topic and produces into it,
//! [`multi_client`] reads back over the wire from a broker the test holds no
//! handle for, and [`multi_failover`] is the test that orders them.

mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `tiered_storage_multi_broker/` directory, which keeps the parts out of
// `tests/` where every `.rs` file would become another test binary.
#[path = "tiered_storage_multi_broker/multi_client.rs"]
mod multi_client;
#[path = "tiered_storage_multi_broker/multi_cluster.rs"]
mod multi_cluster;
#[path = "tiered_storage_multi_broker/multi_failover.rs"]
mod multi_failover;
#[path = "tiered_storage_multi_broker/multi_workload.rs"]
mod multi_workload;

const TOPIC: &str = "tiered-multi-broker-itest";
const RECORDS: usize = 160;
