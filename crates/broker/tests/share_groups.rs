//! End-to-end integration tests for KIP-932 share-group membership, driven
//! against an in-process Krabka broker through `krabka-client-core`.
//!
//! The typed client works because `ApiVersions` advertises `api_keys` 76 and
//! 77. `ShareGroupHeartbeatRequest` and `ShareGroupDescribeRequest` implement
//! `ProtocolRequest`, so `client.send(req)` returns the typed response and
//! exercises the real wire path. Version negotiation goes through
//! `ApiVersions`, and both share RPCs have MIN=MAX=1, so the client negotiates
//! v1.

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `share_groups/` directory, which keeps the parts out of `tests/` where every
// `.rs` file would become another test binary.
#[path = "share_groups/share_group_harness.rs"]
mod share_group_harness;
#[path = "share_groups/share_group_listing.rs"]
mod share_group_listing;
#[path = "share_groups/share_group_membership.rs"]
mod share_group_membership;
#[path = "share_groups/share_group_state.rs"]
mod share_group_state;
