//! The controller listener's advertised API table.
//!
//! Every entry is derived from the generated per-message constants in
//! `krabka-protocol`, so bumping the sibling revision moves the advertised
//! range and the codec that serves it together. Two shapes appear here:
//!
//! - *Version-dispatched* APIs are decoded at the version the request header
//!   carries, so they advertise the generated `MIN_VERSION..=MAX_VERSION`
//!   whole.
//! - *Pinned* APIs are the KIP-595 peer RPCs. The engine's codec
//!   ([`crate::kraft::transport::wire`]) encodes and decodes each of them at
//!   exactly one captured wire version, so the advertised range is that single
//!   version on both ends. Advertising the generated range instead would tell a
//!   JVM peer it may send, say, `Vote v0`, which the dispatch path would then
//!   decode as `Vote v2`.
//!
//! [`pinned`] checks the narrowing during const evaluation, so a sibling bump
//! that moves a generated range off the version the codec speaks fails the
//! build rather than a mixed-version rollout.

use krabka_protocol::owned::{
    add_raft_voter_request, api_versions_request, begin_quorum_epoch_request,
    broker_heartbeat_request, broker_registration_request, controller_registration_request,
    describe_cluster_request, describe_quorum_request, end_quorum_epoch_request, fetch_request,
    fetch_snapshot_request, remove_raft_voter_request, update_raft_voter_request, vote_request,
};

use crate::{
    config::ControllerApiVersion,
    kraft::transport::wire::{
        FETCH_SNAPSHOT_VERSION, FETCH_VERSION, QUORUM_EPOCH_VERSION, VOTE_VERSION,
    },
};

/// One advertised range, taken whole from a generated request message. Same
/// shape as the broker's KIP-919 Admin table in `crates/broker/src/controller_admin.rs`.
macro_rules! api_version {
    ($request:ident) => {
        ControllerApiVersion {
            api_key: $request::API_KEY,
            min_version: $request::MIN_VERSION,
            max_version: $request::MAX_VERSION,
            flexible_min: $request::FLEXIBLE_MIN,
        }
    };
    ($request:ident, pinned = $version:expr) => {
        pinned(
            $request::API_KEY,
            $version,
            $request::MIN_VERSION,
            $request::MAX_VERSION,
            $request::FLEXIBLE_MIN,
        )
    };
}

/// Narrows an advertised range to the single version the engine's codec speaks.
///
/// The bounds check is const-evaluated: the table is a `const`, so a pinned
/// version outside the generated `min..=max` aborts const evaluation and the
/// compiler rejects the table, quoting this message and pointing at the entry
/// that broke. That turns a `krabka-protocol` bump which drops or renumbers a
/// KIP-595 version into a build failure instead of an inter-controller RPC that
/// decodes at a version nobody sent.
const fn pinned(
    api_key: i16,
    version: i16,
    min: i16,
    max: i16,
    flexible_min: i16,
) -> ControllerApiVersion {
    // Written as an `if`/`else` expression rather than a bare guard clause:
    // `clippy::manual_assert` rewrites the guard form to `assert!`, which
    // `clippy.toml` disallows in favour of `assert2`, and `assert2` is not a
    // const macro.
    let version = if version >= min && version <= max {
        version
    } else {
        panic!(
            "the wire codec pins this API to a version the generated schema no longer covers; \
             re-capture the version in `kraft::transport::wire::codec` against a real broker"
        )
    };
    ControllerApiVersion {
        api_key,
        min_version: version,
        max_version: version,
        flexible_min,
    }
}

/// Every API the controller listener serves itself, ordered by API key.
///
/// The KIP-919 Admin surface the broker attaches through
/// [`ControllerAdminRouter`](crate::ControllerAdminRouter) is advertised
/// alongside this table but declared by the broker, not here.
///
/// Every pinned version still intersects what a real peer offers. A
/// `mirror.gcr.io/apache/kafka:4.0.0` controller listener, asked for
/// `ApiVersions` on its `CONTROLLER` listener, answers Fetch `4-17`, Vote
/// `0-2`, `BeginQuorumEpoch` `0-1`, `EndQuorumEpoch` `0-1` and `FetchSnapshot`
/// `0-1`, so a JVM peer negotiating against the pins below lands on exactly the
/// versions the codec speaks: Fetch v17, Vote v2, and v1 for the other three.
pub(super) const CONTROLLER_LISTENER_APIS: &[ControllerApiVersion] = &[
    api_version!(fetch_request, pinned = FETCH_VERSION),
    api_version!(api_versions_request),
    api_version!(vote_request, pinned = VOTE_VERSION),
    api_version!(begin_quorum_epoch_request, pinned = QUORUM_EPOCH_VERSION),
    api_version!(end_quorum_epoch_request, pinned = QUORUM_EPOCH_VERSION),
    api_version!(describe_quorum_request),
    api_version!(fetch_snapshot_request, pinned = FETCH_SNAPSHOT_VERSION),
    api_version!(describe_cluster_request),
    api_version!(broker_registration_request),
    api_version!(broker_heartbeat_request),
    api_version!(controller_registration_request),
    api_version!(add_raft_voter_request),
    api_version!(remove_raft_voter_request),
    api_version!(update_raft_voter_request),
];

/// The version from which requests for `api_key` carry tagged fields, if this
/// listener serves that API at all.
pub(in crate::server) fn flexible_min(api_key: i16) -> Option<i16> {
    CONTROLLER_LISTENER_APIS
        .iter()
        .find(|api| api.api_key == api_key)
        .map(|api| api.flexible_min)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use bytes::{Bytes, BytesMut};
    use krabka_ids::ApiKey;
    use krabka_protocol::{
        Decode, Encode,
        owned::{
            api_versions_response::{ApiVersion as ApiVersionEntry, ApiVersionsResponse},
            begin_quorum_epoch_request::BeginQuorumEpochRequest,
            end_quorum_epoch_request::EndQuorumEpochRequest,
            fetch_request::FetchRequest,
            fetch_snapshot_request::FetchSnapshotRequest,
            vote_request::VoteRequest,
        },
    };

    use super::*;
    use crate::{
        kraft::{
            transport::{
                api_key,
                wire::{PeerRequest, decode_vote},
            },
            types::NodeId,
        },
        network::addressing::api_version_for,
    };

    /// Whether `buf` is exactly one `T` at `version`, with nothing left over.
    fn decodes_whole<T>(buf: &[u8], version: i16) -> bool
    where
        T: for<'de> Decode<'de>,
    {
        let mut cur = buf;
        T::decode(&mut cur, version).is_ok() && cur.is_empty()
    }

    /// The ranges the listener should advertise, restated from the constants
    /// the dispatch path decodes with rather than from the table under test.
    ///
    /// The five KIP-595 peer APIs name the `wire` codec constants that
    /// `decode_vote`, `decode_begin`, `decode_end`, `decode_fetch`, and
    /// `decode_fetch_snapshot` pass to the generated decoders. The rest are
    /// decoded at the version their request header carries, so they name the
    /// generated range the codec covers.
    fn expected_entries() -> Vec<ApiVersionEntry> {
        let entry = |api_key, min_version, max_version| ApiVersionEntry {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        };
        let mut expected = vec![
            // Pinned to the one version the `wire` codec speaks.
            entry(fetch_request::API_KEY, FETCH_VERSION, FETCH_VERSION),
            entry(vote_request::API_KEY, VOTE_VERSION, VOTE_VERSION),
            entry(
                begin_quorum_epoch_request::API_KEY,
                QUORUM_EPOCH_VERSION,
                QUORUM_EPOCH_VERSION,
            ),
            entry(
                end_quorum_epoch_request::API_KEY,
                QUORUM_EPOCH_VERSION,
                QUORUM_EPOCH_VERSION,
            ),
            entry(
                fetch_snapshot_request::API_KEY,
                FETCH_SNAPSHOT_VERSION,
                FETCH_SNAPSHOT_VERSION,
            ),
            // Decoded at the version the request header carries, across the
            // whole generated range.
            entry(
                api_versions_request::API_KEY,
                api_versions_request::MIN_VERSION,
                api_versions_request::MAX_VERSION,
            ),
            entry(
                describe_quorum_request::API_KEY,
                describe_quorum_request::MIN_VERSION,
                describe_quorum_request::MAX_VERSION,
            ),
            entry(
                describe_cluster_request::API_KEY,
                describe_cluster_request::MIN_VERSION,
                describe_cluster_request::MAX_VERSION,
            ),
            entry(
                broker_registration_request::API_KEY,
                broker_registration_request::MIN_VERSION,
                broker_registration_request::MAX_VERSION,
            ),
            entry(
                broker_heartbeat_request::API_KEY,
                broker_heartbeat_request::MIN_VERSION,
                broker_heartbeat_request::MAX_VERSION,
            ),
            entry(
                controller_registration_request::API_KEY,
                controller_registration_request::MIN_VERSION,
                controller_registration_request::MAX_VERSION,
            ),
            entry(
                add_raft_voter_request::API_KEY,
                add_raft_voter_request::MIN_VERSION,
                add_raft_voter_request::MAX_VERSION,
            ),
            entry(
                remove_raft_voter_request::API_KEY,
                remove_raft_voter_request::MIN_VERSION,
                remove_raft_voter_request::MAX_VERSION,
            ),
            entry(
                update_raft_voter_request::API_KEY,
                update_raft_voter_request::MIN_VERSION,
                update_raft_voter_request::MAX_VERSION,
            ),
        ];
        expected.sort_unstable_by_key(|version| version.api_key);
        expected
    }

    /// A JVM controller peer negotiates off the `api_keys` table this listener
    /// puts on the wire, so every advertised range has to be a range the
    /// dispatch path can really decode. Comparing the decoded table whole means
    /// a `krabka-protocol` bump that widens a generated range without widening
    /// the handler shows up here.
    #[test]
    fn advertised_versions_are_the_versions_the_listener_decodes_with() {
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let body = super::super::api_versions_response_body(4, &image, None, 0);
        let response = ApiVersionsResponse::decode(&mut &body[..], 4).expect("decode response");
        assert!(response.api_keys == expected_entries());
    }

    /// The engine encodes each KIP-595 peer body at one captured version and
    /// labels the request header with the version [`api_version_for`] returns.
    /// Both have to be the version the table advertises, or a peer decodes the
    /// body at a version nobody wrote it at.
    #[test]
    fn peer_sends_carry_the_advertised_version_in_header_and_body() {
        type DecodesWhole = fn(&[u8], i16) -> bool;
        let cases: [(&str, i16, PeerRequest, DecodesWhole); 5] = [
            (
                "vote",
                api_key::VOTE,
                PeerRequest::Vote {
                    voter_id: NodeId(1),
                    candidate_epoch: 3,
                    candidate: NodeId(2),
                    last_epoch: 2,
                    last_offset: 9,
                    pre_vote: true,
                },
                decodes_whole::<VoteRequest>,
            ),
            (
                "begin quorum epoch",
                api_key::BEGIN_QUORUM_EPOCH,
                PeerRequest::BeginQuorumEpoch {
                    leader_id: NodeId(1),
                    leader_epoch: 4,
                },
                decodes_whole::<BeginQuorumEpochRequest>,
            ),
            (
                "end quorum epoch",
                api_key::END_QUORUM_EPOCH,
                PeerRequest::EndQuorumEpoch {
                    leader_id: NodeId(1),
                    leader_epoch: 4,
                },
                decodes_whole::<EndQuorumEpochRequest>,
            ),
            (
                "fetch",
                api_key::FETCH,
                PeerRequest::Fetch {
                    from: NodeId(2),
                    fetch_epoch: 1,
                    fetch_offset: 5,
                },
                decodes_whole::<FetchRequest>,
            ),
            (
                "fetch snapshot",
                api_key::FETCH_SNAPSHOT,
                PeerRequest::FetchSnapshot {
                    from: NodeId(2),
                    snapshot_id: (10, 1),
                    position: 0,
                    max_bytes: 32,
                },
                decodes_whole::<FetchSnapshotRequest>,
            ),
        ];

        for (case, key, request, decodes) in cases {
            let advertised = CONTROLLER_LISTENER_APIS
                .iter()
                .find(|api| api.api_key == key)
                .unwrap_or_else(|| panic!("{case} is advertised"));
            let sent = api_version_for(ApiKey(key)).get();
            check!(
                (sent, sent) == (advertised.min_version, advertised.max_version),
                "{case}: header version against the advertised range"
            );
            let body: Bytes = request.encode();
            check!(
                decodes(&body, sent),
                "{case}: body decodes whole at the advertised version"
            );
        }
    }

    /// An API the listener does not serve has no flexibility rule of its own;
    /// the caller falls back to the Admin router's table for those.
    #[test]
    fn flexible_min_is_reported_only_for_served_apis() {
        check!(flexible_min(vote_request::API_KEY) == Some(vote_request::FLEXIBLE_MIN));
        check!(flexible_min(fetch_request::API_KEY) == Some(fetch_request::FLEXIBLE_MIN));
        check!(
            flexible_min(krabka_protocol::owned::create_topics_request::API_KEY) == None,
            "CreateTopics is an Admin-router API, not a listener-owned one"
        );
    }

    /// Every KIP-595 peer API has a genuinely wider generated range than the
    /// version the engine's codec speaks, and [`pinned`] is what closes that
    /// gap: it collapses the range to the one version on both ends. Forwarding
    /// the generated range whole would advertise versions the codec refuses.
    #[test]
    fn pinned_narrows_the_generated_range_to_the_codec_version() {
        let cases = [
            (
                "fetch",
                fetch_request::API_KEY,
                FETCH_VERSION,
                fetch_request::MIN_VERSION,
                fetch_request::MAX_VERSION,
                fetch_request::FLEXIBLE_MIN,
            ),
            (
                "vote",
                vote_request::API_KEY,
                VOTE_VERSION,
                vote_request::MIN_VERSION,
                vote_request::MAX_VERSION,
                vote_request::FLEXIBLE_MIN,
            ),
            (
                "begin quorum epoch",
                begin_quorum_epoch_request::API_KEY,
                QUORUM_EPOCH_VERSION,
                begin_quorum_epoch_request::MIN_VERSION,
                begin_quorum_epoch_request::MAX_VERSION,
                begin_quorum_epoch_request::FLEXIBLE_MIN,
            ),
            (
                "end quorum epoch",
                end_quorum_epoch_request::API_KEY,
                QUORUM_EPOCH_VERSION,
                end_quorum_epoch_request::MIN_VERSION,
                end_quorum_epoch_request::MAX_VERSION,
                end_quorum_epoch_request::FLEXIBLE_MIN,
            ),
            (
                "fetch snapshot",
                fetch_snapshot_request::API_KEY,
                FETCH_SNAPSHOT_VERSION,
                fetch_snapshot_request::MIN_VERSION,
                fetch_snapshot_request::MAX_VERSION,
                fetch_snapshot_request::FLEXIBLE_MIN,
            ),
        ];

        for (case, api_key, version, min, max, flexible_min) in cases {
            check!(
                (min, max) != (version, version),
                "{case}: the generated range is wider than the pin, so there is a narrowing to make"
            );
            check!(
                pinned(api_key, version, min, max, flexible_min)
                    == ControllerApiVersion {
                        api_key,
                        min_version: version,
                        max_version: version,
                        flexible_min,
                    },
                "{case}"
            );
        }
    }

    /// A sibling `krabka-protocol` bump that renumbers or drops a pinned
    /// version has to stop the build rather than advertise a version the codec
    /// cannot read. [`CONTROLLER_LISTENER_APIS`] is a `const`, so the guard
    /// runs during const evaluation and the compiler rejects the entry; reached
    /// at run time, the same guard panics with the message that names the fix.
    #[test]
    #[should_panic(expected = "the wire codec pins this API to a version the generated schema")]
    fn pinned_rejects_a_version_the_generated_schema_no_longer_covers() {
        let dropped = vote_request::MAX_VERSION + 1;
        let _ = pinned(
            vote_request::API_KEY,
            dropped,
            vote_request::MIN_VERSION,
            vote_request::MAX_VERSION,
            vote_request::FLEXIBLE_MIN,
        );
    }

    /// What the narrowing buys. `Vote` is generated as `0-2`, but the engine's
    /// codec reads at the pinned version and nothing else, so an advertised
    /// range wider than the pin would invite a JVM peer to send `Vote v0` or
    /// `v1` and then refuse the body it asked for. Re-encoding the engine's own
    /// `Vote` at every generated version walks the whole range: the decoder
    /// accepts exactly the versions the table advertises.
    #[test]
    fn only_the_advertised_vote_versions_survive_the_decoder() {
        let advertised = CONTROLLER_LISTENER_APIS
            .iter()
            .find(|api| api.api_key == vote_request::API_KEY)
            .expect("Vote is advertised");
        let pinned_body: Bytes = PeerRequest::Vote {
            voter_id: NodeId(1),
            candidate_epoch: 3,
            candidate: NodeId(2),
            last_epoch: 2,
            last_offset: 9,
            pre_vote: true,
        }
        .encode();
        let request =
            VoteRequest::decode(&mut &pinned_body[..], VOTE_VERSION).expect("decode at the pin");

        for version in vote_request::MIN_VERSION..=vote_request::MAX_VERSION {
            let mut body = BytesMut::new();
            request
                .encode(&mut body, version)
                .expect("re-encode the same Vote at every generated version");
            let is_advertised =
                version >= advertised.min_version && version <= advertised.max_version;
            check!(
                decode_vote(&body.freeze()).is_some() == is_advertised,
                "Vote v{version}: advertised is {is_advertised}, the decoder disagrees"
            );
        }
    }
}
