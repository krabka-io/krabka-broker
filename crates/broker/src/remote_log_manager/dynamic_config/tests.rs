//! Dynamic quota bounds and cache sizes follow broker, cluster and static defaults.

use {
    super::*,
    assert2::check,
    krabka_metadata::{BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataRecord},
    uuid::Uuid,
};

/// A dynamic broker config record for broker 1, or a cluster default when
/// `node` is `None`.
fn config(image: &mut MetadataImage, node: Option<u64>, name: &str, value: Option<&str>) {
    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
        node_id: node.map_or(DEFAULT_BROKER_CONFIG_NODE_ID, NodeId),
        config_name: name.into(),
        config_value: value.map(str::to_owned),
    }));
}

/// The values [`apply_image`] left in force.
#[derive(Debug, PartialEq, Eq)]
struct Applied {
    copy: i64,
    fetch: i64,
    index_cache: u64,
}

/// One image's dynamic configs: `(node, key, value)`.
type Configs = &'static [(Option<u64>, &'static str, &'static str)];

#[yare::parameterized(
    nothing_set_keeps_the_defaults = { &[], Applied { copy: i64::MAX, fetch: i64::MAX, index_cache: 4096 } },
    the_cluster_default_applies = {
        &[(None, COPY_MAX_BYTES_PER_SECOND, "100"), (None, FETCH_MAX_BYTES_PER_SECOND, "200"), (None, INDEX_CACHE_TOTAL_SIZE_BYTES, "300")],
        Applied { copy: 100, fetch: 200, index_cache: 300 },
    },
    the_broker_value_wins_over_the_cluster_default = {
        &[(None, FETCH_MAX_BYTES_PER_SECOND, "200"), (Some(1), FETCH_MAX_BYTES_PER_SECOND, "50")],
        Applied { copy: i64::MAX, fetch: 50, index_cache: 4096 },
    },
    another_brokers_value_is_ignored = {
        &[(Some(2), COPY_MAX_BYTES_PER_SECOND, "7")],
        Applied { copy: i64::MAX, fetch: i64::MAX, index_cache: 4096 },
    },
    an_unusable_value_falls_back = {
        &[(None, COPY_MAX_BYTES_PER_SECOND, "fast"), (None, FETCH_MAX_BYTES_PER_SECOND, "0")],
        Applied { copy: i64::MAX, fetch: i64::MAX, index_cache: 4096 },
    },
)]
fn the_image_resolves_broker_then_cluster_then_static(configs: Configs, expected: Applied) {
    let mut image = MetadataImage::new(Uuid::nil());
    for (node, key, value) in configs {
        config(&mut image, *node, key, Some(value));
    }
    let quotas = RemoteLogQuotas::default();
    let cache = RemoteIndexCache::disabled();
    cache.resize(4096);

    apply_image(&image, NodeId(1), &DynamicRemoteLogTargets {
        quotas: &quotas,
        index_cache: Some((&cache, 4096)),
    });

    check!(
        Applied {
            copy: quotas.copy.bytes_per_second(),
            fetch: quotas.fetch.bytes_per_second(),
            index_cache: cache.max_bytes(),
        } == expected
    );
}

/// Deleting a dynamic value returns the quota to its default, and the cache
/// to its static size, as Kafka's `reconfigure` does when the new config
/// falls back to the static one.
#[test]
fn deleting_the_dynamic_value_restores_the_default() {
    let mut image = MetadataImage::new(Uuid::nil());
    config(&mut image, Some(1), FETCH_MAX_BYTES_PER_SECOND, Some("10"));
    config(
        &mut image,
        Some(1),
        INDEX_CACHE_TOTAL_SIZE_BYTES,
        Some("10"),
    );
    let quotas = RemoteLogQuotas::default();
    let cache = RemoteIndexCache::disabled();
    let targets = DynamicRemoteLogTargets {
        quotas: &quotas,
        index_cache: Some((&cache, 4096)),
    };
    apply_image(&image, NodeId(1), &targets);
    check!(quotas.fetch.bytes_per_second() == 10);
    check!(cache.max_bytes() == 10);

    config(&mut image, Some(1), FETCH_MAX_BYTES_PER_SECOND, None);
    config(&mut image, Some(1), INDEX_CACHE_TOTAL_SIZE_BYTES, None);
    apply_image(&image, NodeId(1), &targets);

    check!(quotas.fetch.bytes_per_second() == i64::MAX);
    check!(cache.max_bytes() == 4096);
}
