//! SCRAM handler tests: Kafka's client-first grammar, the `tokenauth`
//! credential-store selection for both mechanisms, `=2C`/`=3D` name decoding
//! and the GS2 authorization-id check.
//!
//! The exchanges run against a [`FakeMetadataSource`] seeded with SCRAM users
//! and delegation tokens, driven by [`KafkaScramClient`], which builds the
//! client messages the way Kafka's `ScramSaslClient` does, extensions and
//! authorization id included.

use assert2::{assert, check};
use base64::{Engine, engine::general_purpose::STANDARD as B64};
use krabka_metadata::{DelegationTokenRecord, MetadataRecord, ScramCredentialRecord};
use krabka_protocol::owned::{
    sasl_authenticate_request::SaslAuthenticateRequest,
    sasl_authenticate_response::SaslAuthenticateResponse,
};
use krabka_security::{
    AuthMethod, KafkaPrincipal, Principal, SaslMechanism, scram::hash_scram_password_with_salt,
};
use pbkdf2::{
    hmac::{Hmac, KeyInit, Mac},
    sha2::{Digest, Sha256, Sha512},
};

use super::{ClientFirst, decode_sasl_name};
use crate::{
    network::auth::{
        ConnectionAuth, SaslExchange, handle_authenticate_scram,
        test_support::assert_failed_authenticate_response,
    },
    test_support::FakeMetadataSource,
};

const USER_PASSWORD: &str = "user-password";
const TOKEN_HMAC: [u8; 32] = [0x42; 32];
const TOKEN_OWNER: &str = "owner";

/// A SCRAM client that writes the messages Kafka's `ScramSaslClient` writes.
struct KafkaScramClient {
    mechanism: SaslMechanism,
    password: Vec<u8>,
    gs2_header: String,
    client_first_bare: String,
}

impl KafkaScramClient {
    /// Returns the client and its client-first message.
    fn first(
        mechanism: SaslMechanism,
        authorization_id: Option<&str>,
        sasl_name: &str,
        extensions: &str,
        password: &[u8],
    ) -> (Self, Vec<u8>) {
        let gs2_header = format!(
            "n,{},",
            authorization_id.map_or(String::new(), |a| format!("a={a}"))
        );
        let client_first_bare = format!("n={sasl_name},r=clientnonce{extensions}");
        let message = format!("{gs2_header}{client_first_bare}").into_bytes();
        let client = Self {
            mechanism,
            password: password.to_vec(),
            gs2_header,
            client_first_bare,
        };
        (client, message)
    }

    /// The client-final message for `server_first`.
    fn last(&self, server_first: &[u8]) -> Vec<u8> {
        let server_first = std::str::from_utf8(server_first).expect("server-first is UTF-8");
        let attribute = |name: &str| {
            server_first
                .split(',')
                .find_map(|a| a.strip_prefix(name))
                .expect("server-first attribute")
        };
        let nonce = attribute("r=");
        let salt = B64.decode(attribute("s=")).expect("salt is base64");
        let iterations: u32 = attribute("i=").parse().expect("iterations");
        let salted = krabka_security::scram::pbkdf2_salted(
            &self.password,
            self.mechanism,
            iterations,
            &salt,
        );
        let without_proof = format!("c={},r={nonce}", B64.encode(&self.gs2_header));
        let auth_message = format!("{},{server_first},{without_proof}", self.client_first_bare);
        let client_key = self.hmac(&salted, b"Client Key");
        let stored_key = self.hash(&client_key);
        let signature = self.hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(&signature)
            .map(|(k, s)| k ^ s)
            .collect();
        format!("{without_proof},p={}", B64.encode(proof)).into_bytes()
    }

    fn hmac(&self, key: &[u8], data: &[u8]) -> Vec<u8> {
        if self.mechanism == SaslMechanism::ScramSha256 {
            let mut mac = <Hmac<Sha256>>::new_from_slice(key).expect("any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        } else {
            let mut mac = <Hmac<Sha512>>::new_from_slice(key).expect("any key length");
            mac.update(data);
            mac.finalize().into_bytes().to_vec()
        }
    }

    fn hash(&self, data: &[u8]) -> Vec<u8> {
        if self.mechanism == SaslMechanism::ScramSha256 {
            Sha256::digest(data).to_vec()
        } else {
            Sha512::digest(data).to_vec()
        }
    }
}

fn scram_user(user: &str, mechanism: SaslMechanism) -> MetadataRecord {
    let credential =
        hash_scram_password_with_salt(USER_PASSWORD.as_bytes(), mechanism, 4096, b"salt".to_vec());
    MetadataRecord::V1ScramCredential(ScramCredentialRecord {
        user: user.into(),
        mechanism,
        salt: credential.salt,
        stored_key: credential.stored_key,
        server_key: credential.server_key,
        iterations: credential.iterations,
    })
}

fn token(token_id: &str, expiry_timestamp_ms: i64) -> MetadataRecord {
    MetadataRecord::V1DelegationToken(DelegationTokenRecord {
        token_id: token_id.into(),
        owner: KafkaPrincipal {
            principal_type: "User".into(),
            name: TOKEN_OWNER.into(),
        },
        hmac: TOKEN_HMAC.to_vec(),
        issue_timestamp_ms: 0,
        expiry_timestamp_ms,
        max_timestamp_ms: expiry_timestamp_ms,
        renewers: vec![],
    })
}

fn token_password() -> Vec<u8> {
    B64.encode(TOKEN_HMAC).into_bytes()
}

fn authenticate(
    source: &FakeMetadataSource,
    auth: &mut ConnectionAuth,
    auth_bytes: Vec<u8>,
) -> SaslAuthenticateResponse {
    handle_authenticate_scram(
        &SaslAuthenticateRequest {
            auth_bytes: bytes::Bytes::from(auth_bytes),
            ..Default::default()
        },
        auth,
        source,
        None,
    )
}

/// What one full exchange came to.
#[derive(Debug, PartialEq, Eq)]
enum Outcome {
    /// Round 2 authenticated `principal`.
    Authenticated {
        principal: Principal,
        via_token: bool,
    },
    /// Round 1 failed with this client-facing message (`None` is Kafka's
    /// generic text, filled in at dispatch).
    FirstRoundFailed(Option<String>),
    /// Round 2 refused the proof.
    ProofRefused,
}

/// Drives both rounds, stopping at the first failure.
fn exchange(
    source: &FakeMetadataSource,
    mechanism: SaslMechanism,
    authorization_id: Option<&str>,
    sasl_name: &str,
    extensions: &str,
    password: &[u8],
) -> Outcome {
    let mut auth = ConnectionAuth::Negotiating {
        mechanism,
        exchange: SaslExchange::ScramPending,
        pending_token_expiry_ms: None,
    };
    let (client, first) =
        KafkaScramClient::first(mechanism, authorization_id, sasl_name, extensions, password);
    let round1 = authenticate(source, &mut auth, first);
    if round1.error_code != 0 {
        assert_failed_authenticate_response(&round1, round1.error_message.as_deref());
        return Outcome::FirstRoundFailed(round1.error_message);
    }
    let round2 = authenticate(source, &mut auth, client.last(&round1.auth_bytes));
    if round2.error_code != 0 {
        assert_failed_authenticate_response(&round2, None);
        return Outcome::ProofRefused;
    }
    match auth {
        ConnectionAuth::Authenticated {
            principal,
            mechanism: authenticated_mechanism,
            authenticated_via_token,
            ..
        } => {
            assert!(authenticated_mechanism == mechanism);
            Outcome::Authenticated {
                principal,
                via_token: authenticated_via_token,
            }
        }
        other => panic!("round 2 succeeded without authenticating: {other:?}"),
    }
}

fn principal(name: &str, mechanism: SaslMechanism) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::from_sasl(mechanism),
        groups: vec![],
    }
}

/// One exchange row: case name, GS2 authorization id, SASL name, extensions,
/// password and the expected outcome.
type ExchangeCase<'a> = (
    &'a str,
    Option<&'a str>,
    &'a str,
    &'a str,
    &'a [u8],
    Outcome,
);

/// Kafka's `ScramSaslServer` reads the token store if and only if the
/// client-first message carries `tokenauth=true`, for both mechanisms.
#[test]
fn tokenauth_extension_selects_the_credential_store() {
    const TOKEN_AUTH: &str = ",tokenauth=true";
    let expiry = crate::time_util::now_ms() + 60_000;
    for mechanism in [SaslMechanism::ScramSha256, SaslMechanism::ScramSha512] {
        let source = FakeMetadataSource::builder()
            .records(&[
                scram_user("alice", mechanism),
                // A token whose id is also a SCRAM user name.
                scram_user("shared", mechanism),
                token("shared", expiry),
                token("tok", expiry),
                token("expired", crate::time_util::now_ms() - 1),
                // `a,b=c` on the wire is `a=2Cb=3Dc`.
                scram_user("a,b=c", mechanism),
            ])
            .build();
        let user = USER_PASSWORD.as_bytes();
        let token_pw = token_password();
        let owner = Outcome::Authenticated {
            principal: principal(TOKEN_OWNER, mechanism),
            via_token: true,
        };
        let generic_failure = Outcome::FirstRoundFailed(None);
        // (case, authzid, sasl name, extensions, password, expected)
        let cases: [ExchangeCase<'_>; 12] = [
            (
                "SCRAM user without extension",
                None,
                "alice",
                "",
                user,
                Outcome::Authenticated {
                    principal: principal("alice", mechanism),
                    via_token: false,
                },
            ),
            (
                "token with tokenauth",
                None,
                "tok",
                TOKEN_AUTH,
                &token_pw,
                owner,
            ),
            (
                "tokenauth parsed as Java parseBoolean",
                None,
                "tok",
                ",tokenauth=TRUE",
                &token_pw,
                Outcome::Authenticated {
                    principal: principal(TOKEN_OWNER, mechanism),
                    via_token: true,
                },
            ),
            (
                "token id without tokenauth",
                None,
                "tok",
                "",
                &token_pw,
                generic_failure,
            ),
            (
                "tokenauth=false reads the user store",
                None,
                "tok",
                ",tokenauth=false",
                &token_pw,
                Outcome::FirstRoundFailed(None),
            ),
            (
                "SCRAM user with tokenauth",
                None,
                "alice",
                TOKEN_AUTH,
                user,
                Outcome::FirstRoundFailed(None),
            ),
            (
                "shared name with tokenauth uses the token",
                None,
                "shared",
                TOKEN_AUTH,
                &token_pw,
                Outcome::Authenticated {
                    principal: principal(TOKEN_OWNER, mechanism),
                    via_token: true,
                },
            ),
            (
                "shared name with tokenauth refuses the user password",
                None,
                "shared",
                TOKEN_AUTH,
                user,
                Outcome::ProofRefused,
            ),
            (
                "shared name without tokenauth uses the user",
                None,
                "shared",
                "",
                user,
                Outcome::Authenticated {
                    principal: principal("shared", mechanism),
                    via_token: false,
                },
            ),
            (
                "expired token",
                None,
                "expired",
                TOKEN_AUTH,
                &token_pw,
                Outcome::FirstRoundFailed(None),
            ),
            (
                "escaped SASL name",
                None,
                "a=2Cb=3Dc",
                "",
                user,
                Outcome::Authenticated {
                    principal: principal("a,b=c", mechanism),
                    via_token: false,
                },
            ),
            (
                "authorization id other than the user",
                Some("mallory"),
                "alice",
                "",
                user,
                Outcome::FirstRoundFailed(Some(
                    "Authentication failed: Client requested an authorization id that is \
                     different from username"
                        .into(),
                )),
            ),
        ];
        for (case, authorization_id, sasl_name, extensions, password, expected) in cases {
            check!(
                exchange(
                    &source,
                    mechanism,
                    authorization_id,
                    sasl_name,
                    extensions,
                    password
                ) == expected,
                "{mechanism:?}: {case}"
            );
        }
    }
}

/// A token session carries the token expiry from round 1 to round 2.
#[test]
fn token_round_one_threads_the_token_expiry() {
    let expiry = crate::time_util::now_ms() + 60_000;
    let source = FakeMetadataSource::builder()
        .records(&[token("tok", expiry)])
        .build();
    let mut auth = ConnectionAuth::Negotiating {
        mechanism: SaslMechanism::ScramSha512,
        exchange: SaslExchange::ScramPending,
        pending_token_expiry_ms: None,
    };
    let (_, first) = KafkaScramClient::first(
        SaslMechanism::ScramSha512,
        None,
        "tok",
        ",tokenauth=true",
        &token_password(),
    );
    let round1 = authenticate(&source, &mut auth, first);
    assert!(round1.error_code == 0);
    let ConnectionAuth::Negotiating {
        pending_token_expiry_ms,
        ..
    } = auth
    else {
        panic!("round 1 must keep negotiating, got {auth:?}");
    };
    assert!(pending_token_expiry_ms == Some(expiry));
}

/// `ClientFirst::parse` follows Kafka's `ClientFirstMessage` pattern.
#[test]
fn client_first_parses_as_kafka_does() {
    let parsed = |authorization_id, sasl_name, extensions| {
        Some(ClientFirst {
            authorization_id,
            sasl_name,
            extensions,
        })
    };
    let cases: [(&str, Option<ClientFirst<'_>>); 11] = [
        ("n,,n=alice,r=abc", parsed(None, "alice", vec![])),
        (
            "n,a=alice,n=alice,r=abc",
            parsed(Some("alice"), "alice", vec![]),
        ),
        ("n,,m=ext,n=alice,r=abc", parsed(None, "alice", vec![])),
        (
            "n,,n=tok,r=abc,tokenauth=true,other=x",
            parsed(None, "tok", vec![("tokenauth", "true"), ("other", "x")]),
        ),
        ("n,,n=a=2Cb=3Dc,r=abc", parsed(None, "a=2Cb=3Dc", vec![])),
        // Only the `n` GS2 flag, a non-empty name and nonce, and alphabetic
        // extension keys match.
        ("y,,n=alice,r=abc", None),
        ("n,,n=,r=abc", None),
        ("n,,n=alice,r=", None),
        ("n,,n=a=41,r=abc", None),
        ("n,,n=alice,r=abc,token_auth=true", None),
        ("n,,r=abc,n=alice", None),
    ];
    for (message, expected) in cases {
        check!(
            ClientFirst::parse(message.as_bytes()) == expected,
            "{message}"
        );
    }
}

/// `ScramFormatter.username`.
#[test]
fn sasl_name_decoding_matches_scram_formatter() {
    for (sasl_name, expected) in [
        ("alice", Some("alice")),
        ("a=2Cb", Some("a,b")),
        ("a=3Db", Some("a=b")),
        ("=3D2C", Some("=2C")),
        ("a=41", None),
    ] {
        check!(
            decode_sasl_name(sasl_name).as_deref() == expected,
            "{sasl_name}"
        );
    }
}
