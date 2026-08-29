//! End-to-end integration tests for the KIP-932 Slice D admin offset RPCs.
//!
//! The RPCs are `DescribeShareGroupOffsets` (`api_key` 90),
//! `AlterShareGroupOffsets` (91), and `DeleteShareGroupOffsets` (92).
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 90/91/92
//! and all three requests impl `ProtocolRequest`. `client.send(req)` thus
//! exercises the real wire path: frame parse, then handler, then a
//! version-negotiated encode.
//!
//! These tests prove:
//! - Describe reflects the durable SPSO after a consume and an Accept advance
//!   it. Describe also reports lag = HWM − SPSO for a locally-led partition.
//! - Alter on an *empty* group resets the SPSO. It bumps the state epoch,
//!   re-initializes, AND invalidates the share-partition leader cache, so a
//!   later `ShareFetch` acquires from the new offset.
//! - Alter on a *non-empty* group with a live member is rejected with
//!   `NON_EMPTY_GROUP`.
//! - Delete removes the durable share-state for a topic. Describe then reads
//!   the partition as missing and reports `start_offset` -1.
//! - Describe of an unknown topic returns `UNKNOWN_TOPIC_OR_PARTITION` per
//!   partition.
//!
//! The suite is split by admin surface. `harness` holds the cluster and
//! share-consume helpers that every surface shares, `describe`, `alter`, and
//! `delete` hold the tests of one RPC each, and `state_restore` holds the
//! restart-durability test of the share-state summary.

#[path = "share_admin_offsets/harness.rs"]
mod harness;

#[path = "share_admin_offsets/alter.rs"]
mod alter;
#[path = "share_admin_offsets/delete.rs"]
mod delete;
#[path = "share_admin_offsets/describe.rs"]
mod describe;
#[path = "share_admin_offsets/state_restore.rs"]
mod state_restore;
