//! Background task that subscribes to `MetadataImage` changes and updates the
//! bounds of the throttled-replication quotas, as Kafka's
//! `BrokerConfigHandler` calls `ReplicationQuotaManager.updateQuota`. The task
//! runs unconditionally on every broker. A quota without a bound is never
//! exceeded.

use {
    super::{ReplicationQuota, ThrottleState},
    crate::metadata_source::watch_image_loop,
    krabka_metadata::{MetadataImage, NodeId, ThrottleKind},
    krabka_units::{ByteRate, convert::ByteRateExt},
    std::sync::Arc,
    tokio::sync::watch,
    tokio_util::sync::CancellationToken,
    tracing::debug,
};

pub async fn run(
    images: watch::Receiver<Arc<MetadataImage>>,
    node_id: NodeId,
    throttle: Arc<ThrottleState>,
    shutdown: CancellationToken,
) {
    watch_image_loop(images, "throttle refresh", shutdown, |image| {
        apply_image(image, node_id, &throttle);
    })
    .await;
}

/// Sets the bound of each quota to the rate the image holds for it.
///
/// A configured rate, `0` included, is the bound, as Kafka's `atLeast(0)`
/// rate configs become `Quota.upperBound(rate)`. An unset rate falls back to
/// the default-broker entity, and with neither the quota has no bound and the
/// traffic is unthrottled.
pub fn apply_image(image: &MetadataImage, node_id: NodeId, throttle: &ThrottleState) {
    for (kind, quota, name) in [
        (ThrottleKind::Leader, &throttle.leader_out, "leader-out"),
        (ThrottleKind::Follower, &throttle.follower_in, "follower-in"),
        (
            ThrottleKind::AlterLogDirs,
            &throttle.alter_log_dirs,
            "alter-log-dirs",
        ),
    ] {
        update_bound(
            quota,
            image.broker_throttle_rate(node_id, kind),
            node_id,
            name,
        );
    }
}

/// Sets `bound` on `quota` when it differs from the bound in force.
fn update_bound(quota: &ReplicationQuota, bound: Option<ByteRate>, node_id: NodeId, name: &str) {
    if quota.bound() == bound {
        return;
    }
    debug!(
        node_id = node_id.0,
        quota = name,
        bytes_per_sec = bound.map(ByteRateExt::bytes_per_sec_i64),
        "throttle: replication quota bound update"
    );
    quota.set_bound(bound);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord},
        krabka_units::bytes_per_sec,
        uuid::Uuid,
    };

    fn set_rate(image: &mut MetadataImage, node_id: NodeId, name: &str, value: Option<&str>) {
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id,
            config_name: name.into(),
            config_value: value.map(str::to_owned),
        }));
    }

    fn set_broker_rate(image: &mut MetadataImage, name: &str, value: Option<&str>) {
        set_rate(image, NodeId(1), name, value);
    }

    fn image_with_leader_and_directory_rates() -> MetadataImage {
        let mut img = MetadataImage::new(Uuid::nil());
        set_broker_rate(&mut img, "leader.replication.throttled.rate", Some("2048"));
        set_broker_rate(
            &mut img,
            "replica.alter.log.dirs.io.max.bytes.per.second",
            Some("512"),
        );
        img
    }

    /// The bounds of the leader-out, follower-in and alter-log-dirs quotas.
    fn bounds(throttle: &ThrottleState) -> [Option<ByteRate>; 3] {
        [
            throttle.leader_out.bound(),
            throttle.follower_in.bound(),
            throttle.alter_log_dirs.bound(),
        ]
    }

    #[test]
    fn configured_rates_become_bounds_and_an_unset_rate_none() {
        let img = image_with_leader_and_directory_rates();
        let throttle = ThrottleState::new();
        apply_image(&img, NodeId(1), &throttle);
        assert!(bounds(&throttle) == [Some(bytes_per_sec(2048)), None, Some(bytes_per_sec(512))]);
    }

    #[test]
    fn a_deleted_config_removes_the_bound() {
        let mut img = image_with_leader_and_directory_rates();
        let throttle = ThrottleState::new();
        apply_image(&img, NodeId(1), &throttle);
        set_broker_rate(&mut img, "leader.replication.throttled.rate", None);
        set_broker_rate(
            &mut img,
            "replica.alter.log.dirs.io.max.bytes.per.second",
            None,
        );
        apply_image(&img, NodeId(1), &throttle);
        assert!(bounds(&throttle) == [None, None, None]);
    }

    /// Kafka's rate configs are `atLeast(0)` LONGs and the handler sets
    /// `Quota.upperBound(0)` for `0`: a zero bound that throttles every byte,
    /// not an unthrottled quota.
    #[test]
    fn a_configured_zero_is_a_zero_bound() {
        let mut img = MetadataImage::new(Uuid::nil());
        set_broker_rate(&mut img, "follower.replication.throttled.rate", Some("0"));
        let throttle = ThrottleState::new();
        apply_image(&img, NodeId(1), &throttle);
        throttle.follower_in.record(1);
        assert!(
            (
                throttle.follower_in.bound(),
                throttle.follower_in.quota_exceeded()
            ) == (Some(bytes_per_sec(0)), true)
        );
    }

    /// A broker that sets no rate of its own takes the default-broker
    /// entity's, and its own setting wins over the default.
    #[test]
    fn the_default_broker_entity_supplies_an_unset_bound() {
        let mut img = MetadataImage::new(Uuid::nil());
        for name in [
            "leader.replication.throttled.rate",
            "follower.replication.throttled.rate",
        ] {
            set_rate(&mut img, DEFAULT_BROKER_CONFIG_NODE_ID, name, Some("4096"));
        }
        set_broker_rate(
            &mut img,
            "follower.replication.throttled.rate",
            Some("1024"),
        );
        let throttle = ThrottleState::new();
        apply_image(&img, NodeId(1), &throttle);
        assert!(bounds(&throttle) == [Some(bytes_per_sec(4096)), Some(bytes_per_sec(1024)), None]);
    }
}
