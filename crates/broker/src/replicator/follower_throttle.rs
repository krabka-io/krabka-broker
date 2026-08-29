//! KIP-73 follower-side fetch throttling.
//!
//! The module decides whether a fetch round may run at all, and with how large
//! a `partition_max_bytes` budget, by reading the topic's
//! `follower.replication.throttled.replicas` list and drawing from the
//! broker-wide follower-in token bucket.

use krabka_units::{
    ByteRate, ByteSize,
    convert::{ByteRateExt, ByteSizeExt},
};

use super::Config;
use crate::throttle::TopicThrottle;

/// Whether this round may fetch, and with how large a per-partition budget.
///
/// This enum is not `Eq`. The budget is a [`ByteSize`], and its `f64` storage
/// is only `PartialEq`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum FetchThrottleDecision {
    Fetch(ByteSize),
    Sleep,
}

pub(super) fn follower_partition_fetch_cap(cfg: &Config) -> FetchThrottleDecision {
    let image = cfg.controller.current_image();
    let throttle = TopicThrottle::for_topic(&image, &cfg.topic);
    let throttled = throttle.follower.contains(cfg.partition.get(), cfg.node_id);
    if !throttled || cfg.throttle_state.follower_in.byte_rate() == <ByteRate as ByteRateExt>::ZERO {
        return FetchThrottleDecision::Fetch(cfg.replication.fetch_max);
    }

    // The bucket seam counts raw bytes, so the budget crosses into `u64` here
    // and back on the granted amount. `try_consume` never grants more than it
    // was asked for, so the result is bounded by the configured maximum.
    let granted = cfg
        .throttle_state
        .follower_in
        .try_consume(cfg.replication.fetch_max.bytes_u64());
    if granted == 0 {
        FetchThrottleDecision::Sleep
    } else {
        FetchThrottleDecision::Fetch(ByteSize::from_bytes(granted))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{bytes, bytes_per_sec};

    use super::*;
    use crate::replicator::test_support::{
        LEADER_ID, image_with_follower_throttle, image_with_leader, test_config,
    };

    #[test]
    fn follower_partition_fetch_cap_ignores_unthrottled_partitions() {
        let (cfg, _log_dir) = test_config(image_with_leader(LEADER_ID));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1234), bytes(0));

        assert!(
            follower_partition_fetch_cap(&cfg)
                == FetchThrottleDecision::Fetch(cfg.replication.fetch_max)
        );
    }

    #[test]
    fn follower_partition_fetch_cap_ignores_zero_rate_throttle() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));

        assert!(
            follower_partition_fetch_cap(&cfg)
                == FetchThrottleDecision::Fetch(cfg.replication.fetch_max)
        );
    }

    #[test]
    fn follower_partition_fetch_cap_sleeps_when_throttled_bucket_is_empty() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1024), bytes(0));

        assert!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Sleep);
    }

    #[test]
    fn follower_partition_fetch_cap_uses_granted_bucket_size() {
        let (cfg, _log_dir) = test_config(image_with_follower_throttle("*"));
        cfg.throttle_state
            .follower_in
            .set_byte_rate_with_burst(bytes_per_sec(1234), bytes(1234));

        assert!(follower_partition_fetch_cap(&cfg) == FetchThrottleDecision::Fetch(bytes(1234)));
    }
}
