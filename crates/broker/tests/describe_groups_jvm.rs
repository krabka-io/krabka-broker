//! JVM Kafka cross-validation for `DescribeGroups` (`api_key=15`) metadata.
//!
//! The test boots two single-node real Kafka brokers in Docker, one at a time.
//! `mirror.gcr.io/confluentinc/cp-kafka:7.4.0` forms a CLASSIC consumer group
//! with the `RangeAssignor`. `mirror.gcr.io/apache/kafka:4.0.0` forms a
//! next-generation consumer group with `group.protocol=consumer`. The host
//! sends `DescribeGroupsRequest` to each broker with
//! `krabka_client_core::Client` and captures the responses. JVM Kafka is the
//! authority. This proves the spec premise that real Kafka populates the fields
//! Krabka's handler now surfaces. See `describe_groups_metadata.rs` for the
//! in-process byte-exact echo, and the calibration cross-check below:
//!
//!   * `protocol_type == "consumer"` for an active classic consumer group;
//!   * `protocol_data == "range"`, the SELECTED assignor name, NON-empty;
//!   * `member_metadata` NON-empty: the encoded `ConsumerProtocolSubscription`
//!     the consumer sent in its `JoinGroup`;
//!   * a TYPELESS group, which only commits offsets and never had a protocol,
//!     reports `protocol_type == ""`. This settles the `unwrap_or_default()`
//!     projection in `handlers/describe_groups.rs`.
//!   * the classic API rejects a live next-generation group with
//!     `GROUP_ID_NOT_FOUND`: its state is `Dead` and its classic protocol and
//!     member projections are empty. Next-generation clients must use
//!     `ConsumerGroupDescribe` (`api_key=69`).
//!
//! The test writes the capture to
//! `tests/fixtures/describe_groups/real_kafka_{classic,next_gen}.json`. String
//! fields are verbatim and byte fields are hex plus UTF-8-lossy. A new run
//! regenerates both files.
//!
//! ```text
//! cargo test -p krabka-broker --test describe_groups_jvm -- --ignored --nocapture
//! ```
//!
//! Networking: each Kafka container publishes two PLAINTEXT listeners. `PLAINTEXT` on
//! `9092` is advertised as `localhost:9092` for the in-container admin and
//! consumer CLI. `EXTERNAL` on `19092` is advertised as `localhost:19092` and
//! published to host port 19092, so the host `Client` reaches it on
//! `127.0.0.1:19092`. The sole broker serves a single `DescribeGroups` because
//! it is the group coordinator, so no `FindCoordinator` redirect is needed.
//!
//! The binary root carries only the constants the whole capture shares and the
//! module tree below. `groups_docker` runs the container, `groups_setup` drives
//! the in-container admin tools that create the groups, `groups_fixture` writes
//! the JSON, and `groups_capture` holds the host-side call, its assertions, and
//! the test.

mod support;

// Cargo compiles this file as its own test binary, so a plain `mod` here
// resolves against `tests/`. `#[path]` re-bases each declaration onto the
// sibling `describe_groups_jvm/` directory, which keeps the parts out of
// `tests/`, where every `.rs` file would become another test binary.
#[path = "describe_groups_jvm/groups_capture.rs"]
mod groups_capture;
#[path = "describe_groups_jvm/groups_docker.rs"]
mod groups_docker;
#[path = "describe_groups_jvm/groups_fixture.rs"]
mod groups_fixture;
#[path = "describe_groups_jvm/groups_setup.rs"]
mod groups_setup;

const CLASSIC_IMAGE: &str = "mirror.gcr.io/confluentinc/cp-kafka:7.4.0";
const NEXT_GEN_IMAGE: &str = "mirror.gcr.io/apache/kafka:4.0.0";
const CONTAINER: &str = "krabka-describe-groups-jvm";
/// Fixed host port the `EXTERNAL` listener is published on.
const HOST_PORT: u16 = 19092;
const HOST_BOOTSTRAP: &str = "127.0.0.1:19092";
/// Stable classic consumer group.
const GROUP: &str = "g";
/// Next-generation consumer-protocol group.
const NEXT_GEN_GROUP: &str = "g-next";
/// Offset-commit-only group. It never carries a protocol type.
const TYPELESS_GROUP: &str = "simple-typeless";
const TOPIC: &str = "t";
