//! Metadata-load accounting on a broker-only node: the count of committed
//! metadata records the observer could not decode and skipped.

use super::BrokerMetrics;

impl BrokerMetrics {
    /// Publish the observer's running count of committed metadata records it
    /// could not decode. Kafka's `metadata-load-error-count` is a gauge over
    /// an `AtomicLong` that the "metadata loading" fault handler bumps; the
    /// broker gauge updater samples the observer's count into this one the
    /// same way.
    pub fn set_metadata_load_error_count(&self, count: u64) {
        self.metadata_load_error_count
            .set(i64::try_from(count).unwrap_or(i64::MAX));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The count scrapes under the broker prefix and the Kafka-mirrored name,
    /// at zero on a fresh broker and at the sampled count after.
    #[tokio::test]
    async fn the_metadata_load_error_count_scrapes_under_its_name() {
        let cases = [(None, "0"), (Some(0), "0"), (Some(3), "3")];
        for (sampled, want) in cases {
            let metrics = BrokerMetrics::new();
            if let Some(count) = sampled {
                metrics.set_metadata_load_error_count(count);
            }

            let mut scraped = String::new();
            let registry = metrics.registry.lock().await;
            prometheus_client::encoding::text::encode(&mut scraped, &registry).expect("encode");
            drop(registry);

            let line = scraped
                .lines()
                .find(|line| line.starts_with("krabka_broker_metadata_load_error_count "))
                .map(|line| line.rsplit(' ').next().unwrap_or_default().to_owned());
            assert2::check!(line.as_deref() == Some(want), "{sampled:?}");
        }
    }
}
