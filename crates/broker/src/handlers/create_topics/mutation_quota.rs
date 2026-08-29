//! The KIP-599 controller-mutation accounting of a `CreateTopics` request.
//! The handler charges the quota before it runs any topic logic, so a
//! malformed or rejected request still consumes the budget it asked for.

use krabka_protocol::owned::create_topics_request::CreateTopicsRequest;

pub(super) fn mutation_count(request: &CreateTopicsRequest) -> u64 {
    request
        .topics
        .iter()
        .map(|topic| {
            if topic.assignments.is_empty() {
                u64::try_from(topic.num_partitions.max(1)).expect("mutation count is positive")
            } else {
                u64::try_from(topic.assignments.len()).unwrap_or(u64::MAX)
            }
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_units::{Time, convert::TimeExt, secs};

    #[test]
    fn consume_controller_mutation_quota_tuple_match_overage_throttles() {
        use krabka_metadata::{ClientQuotaRecord, MetadataImage, MetadataRecord, QuotaEntity};
        let mut img = MetadataImage::new(uuid::Uuid::nil());
        img.apply(&MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: vec![
                QuotaEntity {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                },
                QuotaEntity {
                    entity_type: "client-id".into(),
                    entity_name: Some("app-x".into()),
                },
            ],
            config_key: "controller_mutation_rate".into(),
            config_value: Some(1.0),
        }));
        let cases = [
            // Exact (user, client-id) tuple match should throttle on overage.
            ("app-x", true),
            // Non-matching client_id should not throttle.
            ("other", false),
        ];
        for (client_id, want_throttle) in cases {
            let buckets = crate::quota::QuotaBuckets::new();
            let delay = crate::quota::consume_controller_mutation_quota(
                &img,
                &buckets,
                "alice",
                client_id,
                10,
                secs(1),
            );
            assert!(
                (delay > <Time as TimeExt>::ZERO) == want_throttle,
                "client_id {client_id}, delay {delay:?}"
            );
        }
    }
}
