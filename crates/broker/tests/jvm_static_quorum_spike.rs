//! KIP-595 Slice 6 ACCEPTANCE TEST, Docker-gated and `#[ignore]`.
//!
//! One `mirror.gcr.io/apache/kafka:4.0.0` controller and two Krabka
//! controllers form one STATIC metadata quorum, with
//! `controller.quorum.voters` and kraft.version=0. The quorum elects a
//! cross-implementation leader AND replicates committed metadata. The JVM
//! joins as a follower of the Krabka leader, never fatal-faults, catches its
//! high-watermark up to the leader's, and builds a `FeaturesImage` that
//! carries `metadata.version=25` from the Krabka-committed log. This is the
//! program's end goal for the leader-to-follower direction. KIP-853 dynamic
//! voters are not needed for it.
//!
//! This test is not a default-CI gate, because it needs Docker and a published
//! controller port. Run it explicitly.
//!
//! Run:
//! ```text
//! cargo test -p krabka-broker --test jvm_static_quorum_spike -- --ignored --nocapture
//! ```
//!
//! ## Topology
//!
//! - Krabka voters id 1, 2: in-process, real TCP controller listeners bound to
//!   `0.0.0.0:p1` / `0.0.0.0:p2` on the host. They hold the 2/3 majority and
//!   elect among themselves immediately.
//! - JVM voter id 3: `mirror.gcr.io/apache/kafka:4.0.0`, `process.roles=controller`, in a
//!   container publishing `-p p3:p3`, dialing the Krabka voters at
//!   `host.docker.internal:p1` / `:p2`.
//! - Shared cluster id: a `uuid::Uuid` whose 16 bytes are the same bytes the JVM
//!   sees as the base64-url-no-pad `--cluster-id` string.
//!
//! The binary root carries only the module tree: [`static_quorum_harness`]
//! boots the topology, and each of the other two children holds one scenario.

mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `jvm_static_quorum_spike/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "jvm_static_quorum_spike/contested_election.rs"]
mod contested_election;
#[path = "jvm_static_quorum_spike/mixed_quorum.rs"]
mod mixed_quorum;
#[path = "jvm_static_quorum_spike/static_quorum_harness.rs"]
mod static_quorum_harness;
