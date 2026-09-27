//! Background task that subscribes to `MetadataImage` changes and pushes new
//! quota rates to the `QuotaBuckets` cache.
//!
//! The subscription itself is [`watch_image_loop`], shared with
//! `throttle::refresh`; only the per-image work differs.

use std::sync::Arc;

use krabka_metadata::{EntityKey, MetadataImage};
use krabka_units::convert::ByteRateExt as _;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::buckets::{BucketEntry, QuotaBuckets};
use crate::metadata_source::watch_image_loop;

pub async fn run(
    images: watch::Receiver<Arc<MetadataImage>>,
    buckets: Arc<QuotaBuckets>,
    shutdown: CancellationToken,
) {
    watch_image_loop(images, "quota refresh", shutdown, |image| {
        refresh_buckets(image, &buckets);
    })
    .await;
}

/// The token rate a bucket for `quota_key` runs at, in the unit its consumer
/// meters: the same conversion the consumer used when it created the bucket.
///
/// `request_percentage` meters microseconds of handler time per second. Every
/// other quota meters its own unit, and keeps its fractional part: Kafka
/// holds each quota as a double, and the bucket enforces a fractional rate as
/// configured.
fn token_rate(quota_key: &str, rate: f64) -> f64 {
    match quota_key {
        "request_percentage" => super::request::request_percentage_token_rate(rate),
        _ => rate,
    }
}

fn refresh_buckets(image: &MetadataImage, buckets: &QuotaBuckets) {
    let window = buckets.quota_window();
    for ((quota_key, entity_key), entry) in buckets.iter() {
        let new_rate = configured_rate(image, &quota_key, &entity_key, &entry)
            .map_or(0.0, |rate| token_rate(&quota_key, rate));

        let new_rate = super::bucket_rate(new_rate);
        // Compared as the bucket stores it, so a rate finer than the bucket
        // resolves does not reset the bucket, and refill it, on every image.
        if !entry.bucket.runs_at_byte_rate(new_rate) {
            debug!(
                quota_key,
                ?entity_key,
                principal = %entry.principal,
                client_id = %entry.client_id,
                new_rate = new_rate.bytes_per_sec_f64(),
                "quota refresh: rate update"
            );
            let burst = (new_rate * window).into();
            entry.bucket.set_byte_rate_with_burst(new_rate, burst);
        }
    }
}

/// The rate the image configures for one bucket.
///
/// An accept-path bucket is keyed by `[("ip", Some(peer))]` and has no
/// principal or client id, so it is looked up with the `ip` precedence. The
/// user and client-id lookup would find nothing for it and would remove the
/// `connection_creation_rate` limit at the next image change.
///
/// The rate comes back in the unit the image configures it in; [`token_rate`]
/// converts it to the bucket's token unit.
fn configured_rate(
    image: &MetadataImage,
    quota_key: &str,
    entity_key: &EntityKey,
    entry: &BucketEntry,
) -> Option<f64> {
    if let [(entity_type, Some(peer))] = entity_key.as_slice()
        && entity_type == "ip"
    {
        let peer_ip = peer.parse().ok()?;
        return super::lookup::lookup_ip_quota_with_key(image, peer_ip, quota_key)
            .map(|(_, rate)| rate);
    }
    super::lookup::lookup_quota_with_key(image, &entry.principal, &entry.client_id, quota_key)
        .map(|(_, rate)| rate)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::EntityKey;

    use super::{super::bucket_rate, *};
    use crate::quota::test_support::image_with_quota as quota_image;

    fn img_with_quota(
        entity: Vec<(&str, Option<&str>)>,
        key: &str,
        value: f64,
    ) -> Arc<MetadataImage> {
        Arc::new(quota_image(entity, key, value))
    }

    /// A refresh keeps each bucket at the rate its consumer created it with
    /// (#692). A `request_percentage` of 50 is 500 000 microseconds of handler
    /// time per second, not 50, and a fractional one does not round down to the
    /// unlimited rate 0.
    #[test]
    fn refresh_keeps_the_rate_unit_each_quota_meters() {
        for (quota_key, value, created_at, expected) in [
            ("request_percentage", 50.0, 500_000.0, 500_000.0),
            ("request_percentage", 0.0001, 1.0, 1.0),
            ("request_percentage", 0.000_05, 0.5, 0.5),
            ("connection_creation_rate", 0.5, 0.5, 0.5),
            ("producer_byte_rate", 2048.0, 2048.0, 2048.0),
        ] {
            let buckets = Arc::new(QuotaBuckets::new());
            let key: EntityKey = vec![("user".into(), Some("alice".into()))];
            let bucket = buckets.get_or_create(quota_key, &key, "alice", "", created_at);

            refresh_buckets(
                &img_with_quota(vec![("user", Some("alice"))], quota_key, value),
                &buckets,
            );

            assert2::check!(
                bucket.byte_rate() == bucket_rate(expected),
                "{quota_key}={value}"
            );
        }
    }

    #[test]
    fn refresh_updates_existing_bucket_rate() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, "alice", "", 0.0);
        assert!(b.byte_rate() == bucket_rate(0.0));

        let img = img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 2048.0);
        refresh_buckets(&img, &buckets);
        assert!(b.byte_rate() == bucket_rate(2048.0));
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, "alice", "", 1024.0);
        assert!(b.byte_rate() == bucket_rate(1024.0));

        let empty = Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        refresh_buckets(&empty, &buckets);
        assert!(b.byte_rate() == bucket_rate(0.0));
    }

    /// An image change keeps the `connection_creation_rate` of an `ip` bucket.
    /// The rate comes from the exact `ip` entity, or else from the default
    /// `ip` entity, as on the accept path.
    #[test]
    fn refresh_keeps_the_ip_connection_creation_rate() {
        let cases: [(&str, Option<&str>, f64, f64); 4] = [
            ("the exact ip entity", Some("127.0.0.1"), 3.0, 3.0),
            ("the default ip entity", None, 3.0, 3.0),
            ("another ip entity", Some("10.0.0.1"), 3.0, 0.0),
            ("a rate under one per second", Some("127.0.0.1"), 0.5, 0.5),
        ];
        for (case, entity_name, rate, expected) in cases {
            let buckets = Arc::new(QuotaBuckets::new());
            let key: EntityKey = vec![("ip".into(), Some("127.0.0.1".into()))];
            let b = buckets.get_or_create("connection_creation_rate", &key, "", "", 1.0);

            let img = img_with_quota(vec![("ip", entity_name)], "connection_creation_rate", rate);
            refresh_buckets(&img, &buckets);
            assert!(b.byte_rate() == bucket_rate(expected), "{case}");
        }
    }

    /// An image change sets a byte-rate bucket to the configured rate,
    /// fractional part included: Kafka holds the quota as a double, and
    /// neither rounds it to a whole byte per second nor to no limit.
    #[test]
    fn refresh_keeps_a_fractional_byte_rate() {
        let cases: [(&str, f64, f64); 6] = [
            ("consumer_byte_rate", 2048.0, 2048.0),
            ("consumer_byte_rate", 1.5, 1.5),
            ("consumer_byte_rate", 0.5, 0.5),
            ("producer_byte_rate", 2048.0, 2048.0),
            ("producer_byte_rate", 1.5, 1.5),
            ("producer_byte_rate", 0.5, 0.5),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (quota_key, rate, want) in cases {
            let buckets = Arc::new(QuotaBuckets::new());
            let key: EntityKey = vec![("user".into(), Some("alice".into()))];
            let b = buckets.get_or_create(quota_key, &key, "alice", "", 1024.0);

            let img = img_with_quota(vec![("user", Some("alice"))], quota_key, rate);
            refresh_buckets(&img, &buckets);
            actual.push((quota_key, rate.to_string(), b.byte_rate()));
            expected.push((quota_key, rate.to_string(), bucket_rate(want)));
        }
        assert!(actual == expected);
    }

    /// Re-applying a rate finer than the bucket stores does not reset the
    /// bucket: a reset refills it to its burst, which would hand a drained
    /// client a fresh window on every image change.
    #[test]
    fn refresh_does_not_refill_a_bucket_whose_rate_is_unchanged() {
        let rate = 0.123_456_7;
        let buckets = Arc::new(QuotaBuckets::with_window(krabka_units::secs(10)));
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, "alice", "", rate);
        let drained = b.try_consume(5);

        refresh_buckets(
            &img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", rate),
            &buckets,
        );

        assert!((drained, b.try_consume(1)) == (1, 0));
    }
}
