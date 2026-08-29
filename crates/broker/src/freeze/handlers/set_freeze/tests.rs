//! The fixtures that the `SetTopicFreeze` unit tests share.
//!
//! One operator, Alice, holds a key in the trust set and authenticates on the
//! connection, so a test can present a record she signed as well as one the
//! trust set refuses.

use std::{net::SocketAddr, path::PathBuf};

use krabka_metadata::{MetadataImage, MetadataRecord, PatternType, TopicFreezeRecord};
use krabka_protocol::krabka::freeze::SetTopicFreezeRequest;
use krabka_security::{AuthMethod, Principal};
use ring::signature::{Ed25519KeyPair, KeyPair as _};
use tempfile::TempDir;
use uuid::Uuid;

use crate::{
    config::BrokerConfig,
    freeze::signing::freeze_signing_bytes,
    handlers::RequestContext,
    operator_keys::{OperatorKeyEntry, OperatorKeys},
};

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
    let mut image = MetadataImage::new(CLUSTER);
    for (scope, pattern_type) in entries {
        image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
            scope: (*scope).to_owned(),
            pattern_type: *pattern_type,
            frozen: true,
            reason: "DR cutover".to_owned(),
            set_by: ALICE.to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: Uuid::nil(),
            key_id: String::new(),
            signature: Vec::new(),
        }));
    }
    image
}

fn principal() -> Principal {
    Principal {
        name: ALICE_NAME.to_owned(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

fn peer() -> SocketAddr {
    "10.0.0.1:51120".parse().expect("peer address")
}

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

// A broker configuration with alice's operator key loaded.
fn config_with_alice(dir: &TempDir) -> (BrokerConfig, Ed25519KeyPair) {
    let rng = ring::rand::SystemRandom::new();
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
    let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse pkcs8");
    let path: PathBuf = dir.path().join("alice.pub");
    std::fs::write(&path, pair.public_key().as_ref()).expect("write public key");
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
    (config, pair)
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

// `record` signed by `pair` for the test cluster.
fn sign(pair: &Ed25519KeyPair, record: &TopicFreezeRecord) -> Vec<u8> {
    let bytes = freeze_signing_bytes(&CLUSTER.to_string(), record);
    pair.sign(&bytes).as_ref().to_vec()
}
