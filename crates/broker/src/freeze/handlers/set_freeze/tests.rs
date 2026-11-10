//! The fixtures that the `SetTopicFreeze` unit tests share.
//!
//! One operator, Alice, holds a key in the trust set and authenticates on the
//! connection, so a test can present a record she signed as well as one the
//! trust set refuses.
use {
    super::{commit::submit_refusal, outcome::Refusal},
    crate::{
        cluster_id::encode,
        codes,
        config::BrokerConfig,
        freeze::signing::freeze_signing_bytes,
        handlers::RequestContext,
        operator_keys::{OperatorKeyEntry, OperatorKeys},
        test_support::{ed25519_public_key_file, frozen_topics_image, principal},
    },
    krabka_metadata::{MetadataImage, PatternType, TopicFreezeRecord},
    krabka_protocol::{
        krabka::freeze::{PATTERN_TYPE_LITERAL, SetTopicFreezeRequest},
        primitives::uuid::Uuid as WireUuid,
    },
    krabka_raft::{FreezeRefusal, RaftError},
    krabka_security::Principal,
    ring::signature::Ed25519KeyPair,
    std::net::SocketAddr,
    tempfile::TempDir,
    uuid::Uuid,
};

/// Alice's request inputs; identity and image remain owned while a context borrows them.
struct FreezeFixture {
    // Reverse declaration order retains the original local-variable teardown order.
    peer: SocketAddr,
    principal: Principal,
    image: MetadataImage,
}
fn freeze_fixture(image: MetadataImage) -> FreezeFixture {
    let principal = principal(ALICE_NAME);
    let peer = peer();
    FreezeFixture {
        peer,
        principal,
        image,
    }
}
impl FreezeFixture {
    fn context(&self) -> RequestContext<'_> { context(&self.principal, &self.peer) }

    fn environment<'a>(
        &'a self,
        config: &'a BrokerConfig,
        ctx: &'a RequestContext<'a>,
    ) -> super::checks::FreezeEnv<'a> {
        super::checks::FreezeEnv {
            config,
            image: &self.image,
            ctx,
        }
    }
}

mod approval;
mod audit;
mod scope;
mod signature;

const CLUSTER: Uuid = Uuid::from_u128(0x5150);
const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
/// Alice as an `[[operator_keys]]` entry and a record's `set_by` name her:
/// the Kafka form, which is also how `break_glass.approvers` spells her.
const ALICE: &str = "User:alice";
/// Alice as a listener authenticates her, which is the bare session name.
/// A `Principal` carries this, and the handler is what adds the `User:`.
/// Putting [`ALICE`] here instead would hide the bug this pair exists to
/// catch: the test would pass while a real connection produced `alice`.
const ALICE_NAME: &str = "alice";
const ALICE_KEY: &str = "alice-yubi";

fn image(entries: &[(&str, PatternType)]) -> MetadataImage {
    frozen_topics_image()
        .scopes(entries)
        .cluster(CLUSTER)
        .call()
}

fn peer() -> SocketAddr { "10.0.0.1:51120".parse().expect("peer address") }

fn context<'a>(principal: &'a Principal, peer: &'a SocketAddr) -> RequestContext<'a> {
    RequestContext::new(
        principal,
        peer,
        "krabka-guard",
        "conn-1",
        false,
        "PLAINTEXT",
    )
}

/// Borrow Alice's caller-owned context while inspecting a supplied image.
fn with_freeze_env(
    config: &BrokerConfig,
    image: &MetadataImage,
    check: impl FnOnce(&super::checks::FreezeEnv<'_>),
) {
    let principal = principal(ALICE_NAME);
    let peer = peer();
    let ctx = context(&principal, &peer);
    let env = super::checks::FreezeEnv {
        config,
        image,
        ctx: &ctx,
    };
    check(&env);
}

/// Retain Alice's loaded trust set and identity while checking an independent image.
fn with_alice_freeze_env(
    make_image: impl FnOnce() -> MetadataImage,
    check: impl FnOnce(&FreezeFixture, &super::checks::FreezeEnv<'_>),
) {
    let AliceKeyConfiguration {
        dir: _dir, config, ..
    } = alice_key_configuration();
    let fixture = freeze_fixture(make_image());
    let ctx = fixture.context();
    let env = fixture.environment(&config, &ctx);
    check(&fixture, &env);
}

// A broker configuration with alice's operator key loaded, and her key pair.
struct AliceKeys {
    config: BrokerConfig,
    pair: Ed25519KeyPair,
}

fn config_with_alice(dir: &TempDir) -> AliceKeys {
    let (pair, path) = ed25519_public_key_file().dir(dir.path()).call();
    let keys = OperatorKeys::load(&[OperatorKeyEntry {
        key_id: ALICE_KEY.to_owned(),
        principal: ALICE.to_owned(),
        public_key_path: path,
    }])
    .expect("load trust set");
    let config = BrokerConfig {
        operator_keys: keys,
        ..BrokerConfig::default()
    };
    AliceKeys { config, pair }
}

// `config_with_alice` together with the directory that holds her public key.
struct AliceKeyConfiguration {
    dir: TempDir,
    config: BrokerConfig,
    pair: Ed25519KeyPair,
}

fn alice_key_configuration() -> AliceKeyConfiguration {
    let dir = TempDir::new().expect("tempdir");
    let AliceKeys { config, pair } = config_with_alice(&dir);
    AliceKeyConfiguration { dir, config, pair }
}

fn freeze_request(scope: &str, pattern_type: i8) -> SetTopicFreezeRequest {
    SetTopicFreezeRequest {
        scope: scope.to_owned(),
        pattern_type,
        frozen: true,
        reason: "DR cutover".to_owned(),
        ..SetTopicFreezeRequest::default()
    }
}

// `record` signed by `pair` for the test cluster, in the same base64 form
// `krabka-guard` reads from `DescribeCluster` and `check_signature` verifies
// against (#1082).
fn sign(pair: &Ed25519KeyPair, record: &TopicFreezeRecord) -> Vec<u8> {
    let bytes = freeze_signing_bytes(&encode(CLUSTER), record);
    pair.sign(&bytes).as_ref().to_vec()
}

/// A thaw of a scope with no live entry is the operator's mistake, not a
/// broker fault, so it answers `INVALID_REQUEST` before any record is built.
/// Returning before the approval check leaves the named proposal unspent.
#[test]
fn a_thaw_of_a_scope_with_no_live_freeze_is_an_invalid_request() {
    let config = BrokerConfig::default();
    let thaw = SetTopicFreezeRequest {
        frozen: false,
        proposal_id: WireUuid(PROPOSAL.into_bytes()),
        ..freeze_request("orders", PATTERN_TYPE_LITERAL)
    };

    with_freeze_env(
        &config,
        &image(&[("payments", PatternType::Literal)]),
        |env| {
            let refusal = super::checks::prepare(env, &thaw).err();

            assert2::check!(
                refusal
                    == Some(Refusal::new(
                        codes::INVALID_REQUEST,
                        "literal:orders has no live freeze entry, so there is nothing to thaw",
                    ))
            );
        },
    );
}

/// A thaw that loses a race to another writer reaches the controller after
/// the entry has gone, or behind a newer record. The controller's refusal
/// answers the same `INVALID_REQUEST` the broker's own check does.
#[rstest::rstest]
#[case::missing(FreezeRefusal::Missing)]
#[case::stale(FreezeRefusal::Stale)]
fn a_thaw_the_controller_refuses_is_an_invalid_request(#[case] refusal: FreezeRefusal) {
    let error = RaftError::FreezeRefused {
        key: "Literal:orders".to_owned(),
        refusal,
    };

    let answer = submit_refusal(&error, &thaw_record(), true);

    assert2::check!(
        answer
            == Refusal {
                code: codes::INVALID_REQUEST,
                message: "literal:orders has no live freeze entry this thaw can replace; \
                          describe the freezes and retry"
                    .to_owned(),
                signature_verified: true,
            }
    );
}

#[test]
fn any_other_submit_failure_stays_a_coordinator_failure() {
    let error = RaftError::ChangeRejected("disk full".to_owned());

    let answer = submit_refusal(&error, &thaw_record(), false);

    assert2::check!(answer.code == codes::COORDINATOR_NOT_AVAILABLE);
}

fn thaw_record() -> TopicFreezeRecord {
    TopicFreezeRecord {
        frozen: false,
        ..super::checks::record_of(
            &freeze_request("orders", PATTERN_TYPE_LITERAL),
            PatternType::Literal,
            ALICE,
        )
    }
}
