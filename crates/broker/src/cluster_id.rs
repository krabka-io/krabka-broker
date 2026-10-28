//! The cluster id string forms this broker reports and accepts.
//!
//! Kafka's `Uuid.toString()` is URL-safe unpadded base64 of the 16 raw bytes,
//! e.g. `AQIDBAUGBwgJCgsMDQ4PEA`, not `java.util.UUID`'s hyphenated form, e.g.
//! `01020304-0506-0708-090a-0b0c0d0e0f10`. `Metadata` and `DescribeCluster`
//! report the cluster id in that base64 form, so a KIP-1242 `ApiVersions`
//! request that echoes it back, a `krabka-guard` freeze signature that covers
//! it, and every other place this broker reports or verifies its own cluster
//! id, encode with [`encode`] to match. [`matches`] additionally accepts the
//! hyphenated form, for a caller-supplied id such as `AddRaftVoter.ClusterId`
//! that a JVM tool may still send in that form.

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

/// Kafka's `Uuid.toString()`: URL-safe unpadded base64 of the 16 raw bytes.
#[must_use]
pub fn encode(cluster_id: uuid::Uuid) -> String { URL_SAFE_NO_PAD.encode(cluster_id.as_bytes()) }

/// Whether `request` names `cluster_id`, in either Kafka's base64 `Uuid` form
/// or `java.util.UUID`'s hyphenated form.
#[must_use]
pub fn matches(request: &str, cluster_id: uuid::Uuid) -> bool {
    request == encode(cluster_id) || request == cluster_id.to_string()
}

#[cfg(test)]
mod tests {
    use {
        super::{encode, matches},
        assert2::assert,
        rstest::fixture,
        rstest_bdd_macros::{given, then, when},
        uuid::Uuid,
    };

    #[derive(Default)]
    struct ClusterIdentity {
        id: Uuid,
        encoded: Option<String>,
        requested: Option<String>,
        accepted: Option<bool>,
    }

    #[fixture]
    fn identity() -> ClusterIdentity {
        // Keep identity inputs and observations fresh for each scenario.
        ClusterIdentity::default()
    }

    #[given("the cluster UUID is {id}")]
    fn cluster_uuid(identity: &mut ClusterIdentity, id: Uuid) { identity.id = id; }

    #[given("a request identifies the cluster as {requested}")]
    fn requested_identity(identity: &mut ClusterIdentity, requested: String) {
        identity.requested = Some(requested);
    }

    #[when("the broker encodes the cluster UUID for Kafka metadata")]
    fn encode_identity(identity: &mut ClusterIdentity) {
        identity.encoded = Some(encode(identity.id));
    }

    #[when("the broker checks the request's cluster identity")]
    fn check_identity(identity: &mut ClusterIdentity) {
        let requested = identity
            .requested
            .as_deref()
            .expect("a request identity was given");
        identity.accepted = Some(matches(requested, identity.id));
    }

    #[then("the metadata cluster ID is {expected}")]
    fn encoded_identity(identity: &ClusterIdentity, expected: String) {
        assert!(identity.encoded.as_ref() == Some(&expected));
    }

    #[then("the request identity is accepted {accepted}")]
    fn accepted_identity(identity: &ClusterIdentity, accepted: bool) {
        assert!(identity.accepted == Some(accepted));
    }

    rstest_bdd_macros::scenarios!("tests/features/migrated/src/cluster_id.feature", fixtures = [identity: ClusterIdentity]);
}
