use super::{
    FeatureKind as K, FeatureLevels, FeatureUpdateDecision as D, FeatureUpdateFacts,
    FeatureUpdateType as T, feature_update_decision,
};

const fn row(update_type: T, requested: i16, current: i16, kind: K) -> FeatureUpdateFacts {
    FeatureUpdateFacts {
        update_type: Some(update_type),
        levels: FeatureLevels { requested, current },
        all_nodes_support: true,
        kind,
    }
}

const MET: K = K::Other {
    dependencies_met: true,
};

const UNMET: K = K::Other {
    dependencies_met: false,
};

const LOSSY: K = K::MetadataVersion {
    metadata_changed: true,
};

const LOSSLESS: K = K::MetadataVersion {
    metadata_changed: false,
};

mod shared_checks_follow_update_feature_order;
