//! KIP-516 strict topic resolution.
//!
//! This module is the one place that maps a request's `(name, topic_id)` pair
//! to a `TopicRecord` under the KIP-516 strictness rules. The `Err` carries
//! the wire error code to report for the offending topic or partition.

use {
    crate::codes,
    krabka_metadata::{MetadataImage, TopicRecord},
    krabka_protocol::primitives::uuid::Uuid as WireUuid,
};

/// Resolves a requested topic to its record.
///
/// - a non-zero `id` that is unknown gives `UNKNOWN_TOPIC_ID`
/// - a non-zero `id` with a non-empty `name` whose stored name differs gives
///   `INCONSISTENT_TOPIC_ID`
/// - a zero `id` with a `name` that resolves takes the name path. An unknown
///   name gives `UNKNOWN_TOPIC_OR_PARTITION`.
pub fn resolve<'a>(
    image: &'a MetadataImage,
    name: &str,
    id: WireUuid,
) -> Result<&'a TopicRecord, i16> {
    if id == WireUuid::ZERO {
        image.topic(name).ok_or(codes::UNKNOWN_TOPIC_OR_PARTITION)
    } else {
        let uuid = uuid::Uuid::from_bytes(id.0);
        match image.topic_by_id(&uuid) {
            None => Err(codes::UNKNOWN_TOPIC_ID),
            Some(t) if !name.is_empty() && t.name != name => Err(codes::INCONSISTENT_TOPIC_ID),
            Some(t) => Ok(t),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, crate::test_support::single_partition_topic, assert2::assert, krabka_bdd::Given,
        krabka_metadata::MetadataRecord,
    };

    /// The error code a scenario expects a request to fail with.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
    enum ErrorCode {
        #[step_param("UNKNOWN_TOPIC_ID")]
        UnknownTopicId,
        #[step_param("INCONSISTENT_TOPIC_ID")]
        InconsistentTopicId,
        #[step_param("UNKNOWN_TOPIC_OR_PARTITION")]
        UnknownTopicOrPartition,
    }

    impl ErrorCode {
        const fn code(self) -> i16 {
            match self {
                Self::UnknownTopicId => codes::UNKNOWN_TOPIC_ID,
                Self::InconsistentTopicId => codes::INCONSISTENT_TOPIC_ID,
                Self::UnknownTopicOrPartition => codes::UNKNOWN_TOPIC_OR_PARTITION,
            }
        }
    }

    /// The cluster's image and what one request resolved against it: the
    /// resolved topic's name, or the error code.
    struct TopicResolution {
        image: MetadataImage,
        resolved: Given<Result<String, i16>>,
    }

    #[rstest::fixture]
    fn topic_resolution() -> TopicResolution {
        TopicResolution {
            image: MetadataImage::new(uuid::Uuid::nil()),
            resolved: Given::new(
                "resolved",
                "When a request resolves topic id <id> without a name",
            ),
        }
    }

    impl TopicResolution {
        fn resolve(&mut self, name: &str, id: WireUuid) {
            let resolved = resolve(&self.image, name, id).map(|topic| topic.name.clone());
            self.resolved.set(resolved);
        }
    }

    fn wire_id(id: u128) -> WireUuid { WireUuid(uuid::Uuid::from_u128(id).into_bytes()) }

    #[krabka_bdd::given("the cluster holds topic {name} with id {id}")]
    fn cluster_topic(topic_resolution: &mut TopicResolution, name: String, id: u128) {
        let topic = single_partition_topic(&name, uuid::Uuid::from_u128(id));
        topic_resolution
            .image
            .apply(&MetadataRecord::V1Topic(topic));
    }

    #[krabka_bdd::when("a request resolves topic id {id} without a name")]
    fn resolve_by_id(topic_resolution: &mut TopicResolution, id: u128) {
        topic_resolution.resolve("", wire_id(id));
    }

    #[krabka_bdd::when("a request resolves topic id {id} named {name}")]
    fn resolve_by_id_and_name(topic_resolution: &mut TopicResolution, id: u128, name: String) {
        topic_resolution.resolve(&name, wire_id(id));
    }

    #[krabka_bdd::when("a request resolves topic name {name} without an id")]
    fn resolve_by_name(topic_resolution: &mut TopicResolution, name: String) {
        topic_resolution.resolve(&name, WireUuid::ZERO);
    }

    #[krabka_bdd::then("the request resolves to topic {name}")]
    fn resolved_to(topic_resolution: &TopicResolution, name: String) {
        assert!(*topic_resolution.resolved.get() == Ok(name));
    }

    #[krabka_bdd::then("the request fails with {error}")]
    fn failed_with(topic_resolution: &TopicResolution, error: ErrorCode) {
        assert!(*topic_resolution.resolved.get() == Err(error.code()));
    }

    krabka_bdd::scenarios!(
        "tests/features/migrated/src/topic_resolve",
        fixtures = [topic_resolution: TopicResolution],
    );
}
