//! The iteration order of a `java.util.HashMap<String, _>`.
//!
//! Kafka's `QuorumController.updateFeatures` puts the request rows into a
//! `HashMap` and `FeatureControlManager.updateFeatures` stops at the first
//! row that fails in that map's iteration order. Which row's error the
//! response names therefore depends on Java's bucket layout, and this module
//! reproduces it.

/// `HashMap`'s initial table size.
const INITIAL_CAPACITY: usize = 16;

/// Java's `String.hashCode` over UTF-16 code units.
fn java_string_hash(key: &str) -> i32 {
    key.encode_utf16().fold(0_i32, |hash, unit| {
        hash.wrapping_mul(31).wrapping_add(i32::from(unit))
    })
}

/// `HashMap.hash`: the hash code with its high half folded into the low half.
fn spread(key: &str) -> u32 {
    let hash = u32::from_ne_bytes(java_string_hash(key).to_ne_bytes());
    hash ^ (hash >> 16)
}

/// The distinct `keys`, in the order a `new HashMap<>()` filled by `put` in
/// `keys` order iterates them.
///
/// The table starts at 16 buckets and doubles whenever the size passes three
/// quarters of it. Iteration walks the buckets in index order, and a bucket
/// keeps its keys in first-insertion order: a resize splits each bucket
/// without reordering it, and putting an existing key again leaves it where
/// it is. A bucket only turns into a tree at 8 keys in a table of 64 or more,
/// far past any `UpdateFeatures` request, so that case is not modelled.
pub(super) fn hash_map_order<'a>(keys: impl IntoIterator<Item = &'a str>) -> Vec<&'a str> {
    let mut distinct: Vec<&str> = Vec::new();
    for key in keys {
        if !distinct.contains(&key) {
            distinct.push(key);
        }
    }
    let mut capacity = INITIAL_CAPACITY;
    while distinct.len() > capacity / 4 * 3 {
        capacity *= 2;
    }
    let mask = u32::try_from(capacity - 1).unwrap_or(u32::MAX);
    // A stable sort keeps first-insertion order inside each bucket.
    distinct.sort_by_key(|key| spread(key) & mask);
    distinct
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest_bdd::datatable::Rows};

    #[yare::parameterized(
        empty = { "", 0 },
        hello = { "hello", 99_162_322 },
        overflow = { "polygenelubricants", i32::MIN },
    )]
    fn java_string_hash_matches_the_jvm(key: &str, want: i32) {
        assert2::assert!(java_string_hash(key) == want, "{key}");
    }

    struct FeatureValidationOrder {
        requested: Vec<String>,
        validated: Option<Vec<String>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct FeatureName {
        name: String,
    }

    #[rstest::fixture]
    fn feature_order() -> FeatureValidationOrder {
        // Start this scenario with fresh inputs and observations.

        FeatureValidationOrder {
            requested: Vec::new(),
            validated: None,
        }
    }

    #[rstest_bdd_macros::given("an UpdateFeatures request lists these features in order:")]
    fn requested_feature_order(
        feature_order: &mut FeatureValidationOrder,
        #[datatable] rows: Rows<FeatureName>,
    ) {
        feature_order.requested = rows.into_iter().map(|row| row.name).collect();
    }

    #[rstest_bdd_macros::when("the controller orders feature validation as Kafka's HashMap does")]
    fn kafka_feature_validation_order(feature_order: &mut FeatureValidationOrder) {
        feature_order.validated = Some(
            hash_map_order(feature_order.requested.iter().map(String::as_str))
                .into_iter()
                .map(str::to_owned)
                .collect(),
        );
    }

    #[rstest_bdd_macros::then("the features are validated exactly in this order:")]
    fn expected_feature_order(
        feature_order: &FeatureValidationOrder,
        #[datatable] rows: Rows<FeatureName>,
    ) {
        let expected: Vec<_> = rows.into_iter().map(|row| row.name).collect();
        assert!(feature_order.validated.as_ref() == Some(&expected));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/handlers/update_features/java_order.feature", fixtures = [feature_order: FeatureValidationOrder]);

    #[test]
    fn the_table_grows_past_three_quarters_full() {
        // Thirteen keys resize the table to 32 buckets, which moves "k12"
        // (bucket 17 of 32, bucket 1 of 16) behind every key in buckets 2-16.
        let keys: Vec<String> = (0..13).map(|index| format!("k{index}")).collect();
        let order = hash_map_order(keys.iter().map(String::as_str));
        let buckets: Vec<u32> = order.iter().map(|key| spread(key) & 31).collect();
        let mut sorted = buckets.clone();
        sorted.sort_unstable();
        assert!(order.len() == 13);
        assert!(buckets == sorted);
    }
}
