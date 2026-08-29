//! KIP-1071 heartbeat-response status codes.
//!
//! A non-empty status list keeps the member in a `NotReady` state until the
//! condition that caused it clears.

// Byte values for the Kafka StreamsGroupHeartbeatResponse.Status enum.
pub const STALE_TOPOLOGY: i8 = 0;
pub const MISSING_SOURCE_TOPICS: i8 = 1;
pub const INCORRECTLY_PARTITIONED_TOPICS: i8 = 2;
pub const MISSING_INTERNAL_TOPICS: i8 = 3;
pub const SHUTDOWN_APPLICATION: i8 = 4;
