//! Background task that subscribes to `MetadataImage` changes and pushes new
//! quota rates to the `QuotaBuckets` cache.
//!
//! The subscription itself is [`watch_image_loop`], shared with
//! `throttle::refresh`; only the per-image work differs.

use std::sync::Arc;

use krabka_metadata::MetadataImage;
use krabka_units::convert::ByteRateExt as _;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{buckets::QuotaBuckets, positive_f64_to_u64};
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
/// `request_percentage` meters microseconds of handler time per second, and
/// the accept loop never lets a positive `connection_creation_rate` round down
/// to the unlimited rate 0. The byte rates are whole bytes per second.
fn token_rate(quota_key: &str, rate: f64) -> u64 {
    match quota_key {
        "request_percentage" => super::request::request_percentage_token_rate(rate),
        "connection_creation_rate" if rate > 0.0 => positive_f64_to_u64(rate).max(1),
        _ => positive_f64_to_u64(rate),
    }
}

fn refresh_buckets(image: &MetadataImage, buckets: &QuotaBuckets) {
    let window = buckets.quota_window();
    for ((quota_key, entity_key), entry) in buckets.iter() {
        let new_rate: u64 = super::lookup::lookup_quota_with_key(
            image,
            &entry.principal,
            &entry.client_id,
            &quota_key,
        )
        .map_or(0, |(_, rate)| token_rate(&quota_key, rate));

        let new_rate = super::bucket_rate(new_rate);
        if entry.bucket.byte_rate() != new_rate {
            debug!(
                quota_key,
                ?entity_key,
                principal = %entry.principal,
                client_id = %entry.client_id,
                new_rate = new_rate.bytes_per_sec_i64(),
                "quota refresh: rate update"
            );
            let burst = (new_rate * window).into();
            entry.bucket.set_byte_rate_with_burst(new_rate, burst);
        }
    }
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
            ("request_percentage", 50.0, 500_000, 500_000),
            ("request_percentage", 0.0001, 1, 1),
            ("connection_creation_rate", 0.5, 1, 1),
            ("producer_byte_rate", 2048.0, 2048, 2048),
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
        let b = buckets.get_or_create("producer_byte_rate", &key, "alice", "", 0);
        assert!(b.byte_rate() == bucket_rate(0));

        let img = img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 2048.0);
        refresh_buckets(&img, &buckets);
        assert!(b.byte_rate() == bucket_rate(2048));
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, "alice", "", 1024);
        assert!(b.byte_rate() == bucket_rate(1024));

        let empty = Arc::new(MetadataImage::new(uuid::Uuid::nil()));
        refresh_buckets(&empty, &buckets);
        assert!(b.byte_rate() == bucket_rate(0));
    }

    #[test]
    fn refresh_updates_qos_tier_bucket_from_base_quota_entity() {
        let buckets = Arc::new(QuotaBuckets::new());
        let tiered_key: EntityKey = vec![
            ("client-id".into(), Some("app".into())),
            ("user".into(), Some("alice".into())),
            ("qos-tier".into(), Some("gold".into())),
        ];
        let b = buckets.get_or_create("producer_byte_rate", &tiered_key, "alice", "app", 128);
        assert!(b.byte_rate() == bucket_rate(128));

        let img = img_with_quota(
            vec![("user", Some("alice")), ("client-id", Some("app"))],
            "producer_byte_rate",
            2048.0,
        );
        refresh_buckets(&img, &buckets);
        assert!(b.byte_rate() == bucket_rate(2048));
    }
}
