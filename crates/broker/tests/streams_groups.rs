//! End-to-end integration tests for KIP-1071 streams-group membership (the
//! Streams Rebalance Protocol), driven against an in-process Krabka broker
//! through `krabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 88/89.
//! `StreamsGroupHeartbeatRequest` and `StreamsGroupDescribeRequest` implement
//! `ProtocolRequest`, so `client.send(req)` returns the typed response and
//! exercises the real wire path. Both streams RPCs are MIN=MAX=0, so the client
//! negotiates v0.
//!
//! Unlike share groups, the streams heartbeat handler gates on BOTH the
//! finalized `streams.version >= 1` feature (KIP-1071 early access) AND the
//! `streams_group.enable` config kill-switch, which is true by default in
//! `BrokerConfig::for_tests`. Every test therefore finalizes `streams.version`
//! to level 1 with `UpdateFeatures` before it issues streams RPCs.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `streams_groups/` directory, which keeps the parts out of `tests/` where
// every `.rs` file would become another test binary.
#[path = "streams_groups/streams_admin.rs"]
mod streams_admin;
#[path = "streams_groups/streams_harness.rs"]
mod streams_harness;
#[path = "streams_groups/streams_internal_topics.rs"]
mod streams_internal_topics;
#[path = "streams_groups/streams_membership.rs"]
mod streams_membership;
