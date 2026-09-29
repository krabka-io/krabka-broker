//! Background task that subscribes to `MetadataImage` changes and pushes new
//! quota rates to the `QuotaBuckets` cache.
//!
//! The subscription itself is [`watch_image_loop`], shared with
//! `throttle::refresh`; only the per-image work differs.

use std::{sync::Arc, time::Instant};

use krabka_metadata::{EntityKey, MetadataImage};
use krabka_units::convert::ByteRateExt as _;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use super::{IpNames, buckets::QuotaBuckets};
use crate::metadata_source::watch_image_loop;

pub async fn run(
    images: watch::Receiver<Arc<MetadataImage>>,
    buckets: Arc<QuotaBuckets>,
    shutdown: CancellationToken,
) {
    let latest = images.clone();
    watch_image_loop(images, "quota refresh", shutdown, |image| {
        // Kafka resolves an `ip` entity's name when it applies the record
        // (#1214). Literals resolve here, and a host name is looked up off
        // this loop and applied to the buckets when it has an address. A name
        // that a lookup is running for, or that failed a moment ago, is not
        // looked up again by this image.
        let unresolved = buckets.ip_names().update(image);
        refresh_buckets(image, &buckets);
        let due = buckets.ip_names().claim(unresolved, Instant::now());
        if !due.is_empty() {
            let buckets = Arc::clone(&buckets);
            let latest = latest.clone();
            tokio::spawn(async move {
                buckets.ip_names().resolve(&due).await;
                let image = Arc::clone(&latest.borrow());
                buckets.ip_names().update(&image);
                refresh_buckets(&image, &buckets);
            });
        }
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
        let new_rate = configured_rate(image, buckets.ip_names(), &quota_key, &entity_key)
            .map_or(0.0, |rate| token_rate(&quota_key, rate));

        let new_rate = super::bucket_rate(new_rate);
        // Compared as the bucket stores it, so a rate finer than the bucket
        // resolves does not reset the bucket on every image.
        if !entry.bucket.runs_at_byte_rate(new_rate) {
            debug!(
                quota_key,
                ?entity_key,
                new_rate = new_rate.bytes_per_sec_f64(),
                "quota refresh: rate update"
            );
            let burst = (new_rate * window).into();
            entry.bucket.set_byte_rate_with_burst(new_rate, burst);
        }
    }
}

/// The rate the image configures for one bucket, resolved from the bucket's
/// own entity key.
///
/// A bucket is shared by every client that resolves to its key, so it is
/// re-rated from the key and not from whichever client created it (#1213):
/// Kafka re-rates each sensor from its own metric tags.
///
/// An accept-path bucket is keyed by `[("ip", Some(peer))]`, so it is looked
/// up with the `ip` precedence. The user and client-id lookup would find
/// nothing for it and would remove the `connection_creation_rate` limit at the
/// next image change.
///
/// The rate comes back in the unit the image configures it in; [`token_rate`]
/// converts it to the bucket's token unit.
fn configured_rate(
    image: &MetadataImage,
    ip_names: &IpNames,
    quota_key: &str,
    entity_key: &EntityKey,
) -> Option<f64> {
    if let [(entity_type, Some(peer))] = entity_key.as_slice()
        && entity_type == "ip"
    {
        let peer_ip = peer.parse().ok()?;
        return super::lookup::lookup_ip_quota_with_key(image, ip_names, peer_ip, quota_key)
            .map(|(_, rate)| rate);
    }
    super::lookup::lookup_bucket_rate(image, entity_key, quota_key)
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
            let bucket = buckets.get_or_create(quota_key, &key, created_at);

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
        let b = buckets.get_or_create("producer_byte_rate", &key, 0.0);
        assert!(b.byte_rate() == bucket_rate(0.0));

        let img = img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", 2048.0);
        refresh_buckets(&img, &buckets);
        assert!(b.byte_rate() == bucket_rate(2048.0));
    }

    #[test]
    fn refresh_zeroes_bucket_when_quota_removed_from_image() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, 1024.0);
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
            let b = buckets.get_or_create("connection_creation_rate", &key, 1.0);

            let img = img_with_quota(vec![("ip", entity_name)], "connection_creation_rate", rate);
            refresh_buckets(&img, &buckets);
            assert!(b.byte_rate() == bucket_rate(expected), "{case}");
        }
    }

    /// An accept-path bucket is keyed by the peer's canonical address, and a
    /// refresh keeps the rate of an `ip` entity that spells that address
    /// another way (#1214).
    #[test]
    fn refresh_keeps_the_rate_an_alias_ip_entity_configures() {
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("ip".into(), Some("::1".into()))];
        let b = buckets.get_or_create("connection_creation_rate", &key, 1.0);
        let img = img_with_quota(
            vec![("ip", Some("0:0:0:0:0:0:0:1"))],
            "connection_creation_rate",
            3.0,
        );

        let _ = buckets.ip_names().update(&img);
        refresh_buckets(&img, &buckets);

        assert!(b.byte_rate() == bucket_rate(3.0));
    }

    /// The refresh task resolves an `ip` entity that names a host, and applies
    /// its rate to the bucket of the address the host resolved to (#1214).
    #[tokio::test]
    async fn run_applies_a_host_name_entity_to_its_address() {
        let peer = tokio::net::lookup_host("localhost:0")
            .await
            .expect("localhost resolves")
            .next()
            .expect("localhost has an address")
            .ip()
            .to_canonical();
        let buckets = Arc::new(QuotaBuckets::new());
        let key: EntityKey = vec![("ip".into(), Some(peer.to_string()))];
        let b = buckets.get_or_create("connection_creation_rate", &key, 1.0);
        let img = img_with_quota(
            vec![("ip", Some("localhost"))],
            "connection_creation_rate",
            3.0,
        );
        let (_tx, rx) = watch::channel(img);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(run(rx, Arc::clone(&buckets), shutdown.clone()));

        let mut rate = b.byte_rate();
        for _ in 0..200 {
            rate = b.byte_rate();
            if rate == bucket_rate(3.0) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        shutdown.cancel();
        task.await.expect("the refresh task stops on cancellation");

        assert!(rate == bucket_rate(3.0));
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
            let b = buckets.get_or_create(quota_key, &key, 1024.0);

            let img = img_with_quota(vec![("user", Some("alice"))], quota_key, rate);
            refresh_buckets(&img, &buckets);
            actual.push((quota_key, rate.to_string(), b.byte_rate()));
            expected.push((quota_key, rate.to_string(), bucket_rate(want)));
        }
        assert!(actual == expected);
    }

    /// A bucket is shared by every client that resolves to its key, so a
    /// refresh rates it from that key and not from the client that created it
    /// (#1213). Adding a quota to a more specific or an overlapping level
    /// changes what the creating client resolves to, and used to change the
    /// rate every other client of the bucket draws on.
    #[test]
    fn refresh_rates_a_shared_bucket_by_its_own_key() {
        use crate::quota::test_support::{image_with_quotas, quota_record};

        let user: EntityKey = vec![("user".into(), Some("alice".into()))];
        let client: EntityKey = vec![("client-id".into(), Some("app1".into()))];
        let pair: EntityKey = vec![
            ("client-id".into(), Some("app1".into())),
            ("user".into(), Some("alice".into())),
        ];
        let alice = || quota_record(vec![("user", Some("alice"))], "producer_byte_rate", 1_000.0);
        let app1 = || {
            quota_record(
                vec![("client-id", Some("app1"))],
                "producer_byte_rate",
                200.0,
            )
        };
        let alice_app1 = || {
            quota_record(
                vec![("user", Some("alice")), ("client-id", Some("app1"))],
                "producer_byte_rate",
                5_000.0,
            )
        };
        // (label, bucket key, quotas the bucket was made under, quotas after
        // the alter, the rate the bucket keeps)
        let cases = [
            (
                "the user bucket, after the creating client got a pair quota",
                user.clone(),
                vec![alice()],
                vec![alice(), alice_app1()],
                1_000.0,
            ),
            (
                "the client bucket, after the creating client's user got a quota",
                client,
                vec![app1()],
                vec![app1(), alice()],
                200.0,
            ),
            (
                "the pair bucket, after its user got a quota",
                pair,
                vec![alice_app1()],
                vec![alice_app1(), alice()],
                5_000.0,
            ),
            (
                "the user bucket, after a client quota appeared",
                user,
                vec![alice()],
                vec![alice(), app1()],
                1_000.0,
            ),
        ];
        for (label, key, before, after, expected) in cases {
            let buckets = QuotaBuckets::new();
            let bucket = buckets.get_or_create(
                "producer_byte_rate",
                &key,
                configured_rate(
                    &image_with_quotas(before),
                    buckets.ip_names(),
                    "producer_byte_rate",
                    &key,
                )
                .expect("the bucket was made under a quota"),
            );

            refresh_buckets(&image_with_quotas(after), &buckets);

            assert2::check!(bucket.byte_rate() == bucket_rate(expected), "{label}");
        }
    }

    /// Re-applying a rate finer than the bucket stores does not reset the
    /// bucket: a reset refills it to its burst, which would hand a drained
    /// client a fresh window on every image change.
    #[test]
    fn refresh_does_not_refill_a_bucket_whose_rate_is_unchanged() {
        let rate = 0.123_456_7;
        let buckets = Arc::new(QuotaBuckets::with_window(krabka_units::secs(10)));
        let key: EntityKey = vec![("user".into(), Some("alice".into()))];
        let b = buckets.get_or_create("producer_byte_rate", &key, rate);
        let drained = b.try_consume(5);

        refresh_buckets(
            &img_with_quota(vec![("user", Some("alice"))], "producer_byte_rate", rate),
            &buckets,
        );

        assert!((drained, b.try_consume(1)) == (1, 0));
    }
}
