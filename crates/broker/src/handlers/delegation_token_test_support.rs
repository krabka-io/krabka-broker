//! Controller and identity fixtures shared by the KIP-48 token handlers.

use {
    crate::{
        codes::{NONE, NOT_CONTROLLER, UNKNOWN_SERVER_ERROR},
        features::METADATA_VERSION,
        network::auth::ConnectionAuth,
        test_support::{FakeMetadataSource, KafkaErrorCode, UnixMillis, principal},
        time_util::now_ms,
    },
    assert2::assert,
    krabka_metadata::{
        DelegationTokenRecord, FeatureLevelRecord, MetadataRecord, metadata_version,
    },
    krabka_raft::{Controller, ControllerConfig, ControllerHandle, NodeId, RaftError},
    krabka_security::{AuthMethod, KafkaPrincipal, Principal, SaslMechanism},
    std::{
        future::Future,
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time::timeout,
};

/// The controller, directory and HMAC secret used by token wire fixtures.
pub(super) struct TokenFixture {
    pub(super) directory: tempfile::TempDir,
    pub(super) controller: Arc<ControllerHandle>,
    pub(super) secret: krabka_security::SecretBytes,
}

impl TokenFixture {
    pub(super) async fn new() -> Self {
        let ControllerResources {
            directory,
            controller,
        } = controller_resources().await;
        Self::with_controller(directory, controller)
    }

    fn with_controller(directory: tempfile::TempDir, controller: Arc<ControllerHandle>) -> Self {
        let secret = krabka_security::SecretBytes::new(b"k".to_vec());
        Self {
            directory,
            controller,
            secret,
        }
    }
}

struct ControllerResources {
    directory: tempfile::TempDir,
    controller: Arc<ControllerHandle>,
}
async fn controller_resources() -> ControllerResources {
    let directory = tempfile::TempDir::new().unwrap();
    let controller = test_controller(directory.path().into()).await;
    ControllerResources {
        directory,
        controller,
    }
}

#[derive(Clone, Copy, Default, derive_more::Display, derive_more::From, derive_more::Into)]
pub(super) struct TokenMutationIndex(pub usize);

/// Kafka's default `delegation.token.expiry.time.ms`.
pub(super) const DAY_MS: i64 = 24 * 60 * 60 * 1_000;

pub(super) struct RefusalToken {
    pub(super) hmac: Vec<u8>,
    pub(super) expires_at: i64,
}
pub(super) struct RefusalTokens {
    pub(super) live: RefusalToken,
    pub(super) expired: RefusalToken,
}

pub(super) struct RefusalFixture {
    pub(super) resources: TokenFixture,
    pub(super) live: RefusalToken,
    pub(super) expired: RefusalToken,
}

impl RefusalFixture {
    pub(super) async fn new() -> Self {
        let ControllerResources {
            directory,
            controller,
        } = controller_resources().await;
        let RefusalTokens { live, expired } = refusal_tokens(&controller).await;
        Self {
            resources: TokenFixture::with_controller(directory, controller),
            live,
            expired,
        }
    }
}

pub(super) struct SeededToken {
    pub(super) token_id: String,
    pub(super) hmac: Vec<u8>,
    pub(super) maximum: i64,
}

/// Seed one mutation case against one wall-clock reading for both limits.
#[bon::builder]
pub(super) async fn seed_mutation_token(
    controller: &ControllerHandle,
    #[builder(into)] index: TokenMutationIndex,
    #[builder(into, default = TokenPeriodMillis(60_000))] expiry_delta: TokenPeriodMillis,
    #[builder(into, default = TokenPeriodMillis(DAY_MS))] maximum_delta: TokenPeriodMillis,
) -> SeededToken {
    let token_id = format!("tok-{}", index.0);
    let hmac = hmac_for(&token_id);
    let seeded_at = now_ms();
    let maximum = seeded_at + maximum_delta.0;
    let defaults = TokenSetup::for_mutation();
    seed_token()
        .controller(controller)
        .token_id(&token_id)
        .owner(defaults.owner)
        .maybe_requester(defaults.requester)
        .issued_at(defaults.issued_at)
        .expires_at(seeded_at + expiry_delta.0)
        .max_expires_at(maximum)
        .renewers(defaults.renewers)
        .call()
        .await;
    SeededToken {
        token_id,
        hmac,
        maximum,
    }
}

pub(super) async fn mutation_token_for_case(
    controller: &ControllerHandle,
    index: usize,
    expiry_delta: TokenPeriodMillis,
    maximum_delta: TokenPeriodMillis,
) -> SeededToken {
    seed_mutation_token()
        .controller(controller)
        .index(TokenMutationIndex(index))
        .expiry_delta(expiry_delta)
        .maximum_delta(maximum_delta)
        .call()
        .await
}

/// Prepared controller state for a token mutation, retained through its storage assertions.
#[derive(Default)]
pub(super) struct TokenMutationContext<Response> {
    pub refusals: Option<RefusalFixture>,
    pub fixture: Option<TokenFixture>,
    pub seeded: Option<SeededToken>,
    pub authentication: Option<ConnectionAuth>,
    pub secret: SecretProvision,
    pub hmac: Vec<u8>,
    pub period: i64,
    pub execution: Option<MutationExecution<Response>>,
}

impl<Response: Default> TokenMutationContext<Response> {
    pub async fn refusal_request(
        token: &str,
        caller: &str,
        credential: &str,
        secret: &str,
        period: i64,
    ) -> Self {
        let refusals = RefusalFixture::new().await;
        let hmac = match token {
            "live" => refusals.live.hmac.clone(),
            "expired" => refusals.expired.hmac.clone(),
            "missing" => vec![0xFF; 32],
            "wrong-secret" => krabka_security::compute_token_hmac(b"other", "live"),
            _ => panic!("unknown token selection: {token}"),
        };
        let authentication = match credential {
            "password" => authed(caller),
            "delegation-token" => authed_with_token(caller, CredentialSource::DelegationToken),
            _ => panic!("unknown credential source: {credential}"),
        };
        let secret = match secret {
            "configured" => SecretProvision::Configured,
            "absent" => SecretProvision::Absent,
            _ => panic!("unknown secret provision: {secret}"),
        };
        Self {
            refusals: Some(refusals),
            authentication: Some(authentication),
            hmac,
            period,
            secret,
            ..Self::default()
        }
    }

    pub async fn allowed_request(caller: &str, expiry: i64, maximum: i64, period: i64) -> Self {
        let fixture = TokenFixture::new().await;
        let seeded = mutation_token_for_case(
            &fixture.controller,
            0,
            TokenPeriodMillis(expiry),
            TokenPeriodMillis(maximum),
        )
        .await;
        Self {
            hmac: seeded.hmac.clone(),
            authentication: Some(authed(caller)),
            period,
            fixture: Some(fixture),
            seeded: Some(seeded),
            ..Self::default()
        }
    }

    pub fn resources(&self) -> &TokenFixture {
        self.refusals.as_ref().map_or_else(
            || self.fixture.as_ref().expect("token mutation fixture"),
            |refusals| &refusals.resources,
        )
    }

    pub fn expiry_setup(&self, deadline: &str) -> TokenExpirySetup {
        let period = if deadline == "maximum" {
            None
        } else {
            Some(TokenPeriodMillis(
                deadline.parse().expect("expiry period milliseconds"),
            ))
        };
        TokenExpirySetup {
            window: self.execution.as_ref().expect("mutation executed").window,
            expectation: ExpiryExpectation::for_period(
                period,
                UnixMillis(self.seeded.as_ref().expect("token seeded").maximum),
            ),
        }
    }

    pub async fn check_refusal_deadlines(&self) {
        let refusals = self.refusals.as_ref().expect("refusal tokens given");
        let controller = &refusals.resources.controller;
        assert!(stored_expiry(controller, "live") == Some(refusals.live.expires_at));
        assert!(stored_expiry(controller, "expired") == Some(refusals.expired.expires_at));
        controller.cancel().await;
    }
}

pub(super) struct MutationResponse<Response> {
    pub(super) actual: Response,
    pub(super) expected: Response,
    pub(super) case: &'static str,
}
#[derive(Clone, Copy, Default)]
pub(super) enum SecretProvision {
    #[default]
    Configured,
    Absent,
}

impl SecretProvision {
    pub(super) fn get(
        self,
        secret: &krabka_security::SecretBytes,
    ) -> Option<&krabka_security::SecretBytes> {
        match self {
            Self::Configured => Some(secret),
            Self::Absent => None,
        }
    }
}

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub(super) struct TokenPeriodMillis(pub i64);

/// Starts a single-voter controller and waits for its leader.
pub(super) async fn test_controller(log_dir: PathBuf) -> Arc<ControllerHandle> {
    let config = ControllerConfig {
        election_timeout: krabka_units::millis(200),
        heartbeat_interval: Some(krabka_units::millis(50)),
        client_id: "test".into(),
        ..ControllerConfig::for_tests(NodeId(1), log_dir)
    };
    let controller = Arc::new(Controller::start(config).await.unwrap());
    let mut leader = controller.watch_leader();
    let deadline = Instant::now() + Duration::from_secs(5);
    while leader.borrow().is_none() {
        assert!(Instant::now() < deadline, "no leader in 5s");
        let _ = timeout(Duration::from_millis(100), leader.changed()).await;
    }
    controller
}

#[derive(Clone, Copy, Default)]
pub(super) enum CredentialSource {
    #[default]
    Password,
    DelegationToken,
}

/// An authenticated SCRAM connection, optionally using a delegation token.
pub(super) fn authed_with_token(name: &str, source: CredentialSource) -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: Principal {
            name: name.into(),
            auth_method: AuthMethod::SaslScramSha256,
            groups: vec![],
        },
        mechanism: SaslMechanism::ScramSha256,
        expires_at_ms: None,
        authenticated_via_token: matches!(source, CredentialSource::DelegationToken),
    }
}

/// An authenticated SCRAM connection established with a delegation-token credential.
pub(super) fn token_authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, CredentialSource::DelegationToken)
}

pub(super) fn authed(name: &str) -> ConnectionAuth {
    authed_with_token(name, CredentialSource::Password)
}

/// The synthetic authenticated state of PLAINTEXT and SSL without mTLS.
/// Token APIs must refuse it.
pub(super) fn anonymous() -> ConnectionAuth {
    ConnectionAuth::Authenticated {
        principal: principal("ANONYMOUS"),
        mechanism: SaslMechanism::Plain,
        expires_at_ms: None,
        authenticated_via_token: false,
    }
}

pub(super) fn kp(name: &str) -> KafkaPrincipal {
    KafkaPrincipal {
        principal_type: "User".into(),
        name: name.into(),
    }
}

/// The HMAC the fixture secret key `k` gives `token_id`.
pub(super) fn hmac_for(token_id: &str) -> Vec<u8> {
    krabka_security::compute_token_hmac(b"k", token_id)
}

/// Independent token metadata shared by description and mutation fixtures.
pub(super) struct TokenSetup<'a> {
    pub token_id: &'a str,
    pub owner: KafkaPrincipal,
    pub requester: Option<KafkaPrincipal>,
    pub issued_at: UnixMillis,
    pub expires_at: UnixMillis,
    pub max_expires_at: UnixMillis,
    pub renewers: Vec<KafkaPrincipal>,
}

impl Default for TokenSetup<'_> {
    fn default() -> Self {
        Self {
            token_id: "t-a",
            owner: kp("alice"),
            requester: None,
            issued_at: UnixMillis(1_000),
            expires_at: UnixMillis(2_000),
            max_expires_at: UnixMillis(3_000),
            renewers: vec![],
        }
    }
}

impl TokenSetup<'_> {
    /// The mutation tests use the minter and Bob as requester and renewer.
    pub(super) fn for_mutation() -> Self {
        Self {
            requester: Some(kp("minter")),
            issued_at: UnixMillis(0),
            renewers: vec![kp("bob")],
            ..Self::default()
        }
    }
}

#[bon::builder]
pub(super) async fn seed_token(
    controller: &ControllerHandle,
    #[builder(default = TokenSetup::default().token_id)] token_id: &'_ str,
    #[builder(default = TokenSetup::default().owner)] owner: KafkaPrincipal,
    requester: Option<KafkaPrincipal>,
    #[builder(into, default = TokenSetup::default().issued_at)] issued_at: UnixMillis,
    #[builder(into, default = TokenSetup::default().expires_at)] expires_at: UnixMillis,
    #[builder(into, default = TokenSetup::default().max_expires_at)] max_expires_at: UnixMillis,
    #[builder(default = TokenSetup::default().renewers)] renewers: Vec<KafkaPrincipal>,
) {
    let setup = TokenSetup {
        token_id,
        owner,
        requester,
        issued_at,
        expires_at,
        max_expires_at,
        renewers,
    };
    let TokenSetup {
        token_id,
        owner,
        requester,
        issued_at,
        expires_at,
        max_expires_at,
        renewers,
    } = setup;
    let token = DelegationTokenRecord {
        token_id: token_id.into(),
        requester: requester.unwrap_or_else(|| owner.clone()),
        owner,
        issue_timestamp_ms: issued_at.0,
        expiry_timestamp_ms: expires_at.0,
        max_timestamp_ms: max_expires_at.0,
        renewers,
    };
    controller
        .submit_change(vec![MetadataRecord::V1DelegationToken(token)])
        .await
        .expect("seed token");
}

/// A live token and one just expired, both with the same future maximum.
pub(super) async fn refusal_tokens(controller: &ControllerHandle) -> RefusalTokens {
    struct TokenToSeed<'a> {
        id: &'a str,
        token: &'a RefusalToken,
    }
    let now = now_ms();
    let live = RefusalToken {
        hmac: hmac_for("live"),
        expires_at: now + 60_000,
    };
    let expired = RefusalToken {
        hmac: hmac_for("expired"),
        expires_at: now - 1,
    };

    for seed in [
        TokenToSeed {
            id: "live",
            token: &live,
        },
        TokenToSeed {
            id: "expired",
            token: &expired,
        },
    ] {
        seed_token()
            .controller(controller)
            .token_id(seed.id)
            .owner(TokenSetup::for_mutation().owner)
            .maybe_requester(TokenSetup::for_mutation().requester)
            .issued_at(TokenSetup::for_mutation().issued_at)
            .expires_at(seed.token.expires_at)
            .max_expires_at(now + DAY_MS)
            .renewers(TokenSetup::for_mutation().renewers)
            .call()
            .await;
    }
    RefusalTokens { live, expired }
}

#[derive(Clone, Copy, Default)]
pub(super) struct TokenMutationWindow {
    before: UnixMillis,
    after: UnixMillis,
}

pub(super) struct MutationExecution<Response> {
    pub(super) window: TokenMutationWindow,
    pub(super) response: Response,
}
/// Pin independent wall-clock bounds around execution of a token mutation.
pub(super) async fn in_mutation_window<F: Future>(mutation: F) -> MutationExecution<F::Output> {
    let before = UnixMillis(now_ms());
    let response = mutation.await;
    let after = UnixMillis(now_ms());
    MutationExecution {
        window: TokenMutationWindow { before, after },
        response,
    }
}

#[derive(Clone, Copy)]
pub(super) enum ExpiryExpectation {
    RelativeToMutation(TokenPeriodMillis),
    LifetimeCap(UnixMillis),
}

impl ExpiryExpectation {
    pub fn for_period(period: Option<TokenPeriodMillis>, maximum: UnixMillis) -> Self {
        period.map_or(Self::LifetimeCap(maximum), Self::RelativeToMutation)
    }
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
pub(super) struct TokenExpirySetup {
    pub window: TokenMutationWindow,
    #[default(ExpiryExpectation::LifetimeCap(UnixMillis(0)))]
    pub expectation: ExpiryExpectation,
}

#[derive(Clone, Copy)]
pub(super) struct TokenMutationOutcome {
    pub error: KafkaErrorCode,
    pub expiry: UnixMillis,
}

impl TokenMutationOutcome {
    pub(super) fn from_wire(error_code: i16, expiry_timestamp_ms: i64) -> Self {
        Self {
            error: KafkaErrorCode(error_code),
            expiry: UnixMillis(expiry_timestamp_ms),
        }
    }
}

impl TokenExpirySetup {
    pub(super) fn check(self, case: &str, outcome: TokenMutationOutcome) {
        check_expiry_window()
            .case(case)
            .outcome(outcome)
            .window(self.window)
            .expectation(self.expectation)
            .call();
    }
}
/// The expiry lies in the independently pinned clock window, or equals the lifetime cap.
#[bon::builder]
pub(super) fn check_expiry_window(
    case: &str,
    outcome: TokenMutationOutcome,
    #[builder(default)] window: TokenMutationWindow,
    #[builder(default = ExpiryExpectation :: LifetimeCap (UnixMillis (0)))]
    expectation: ExpiryExpectation,
) {
    let setup = TokenExpirySetup {
        window,
        expectation,
    };
    assert!(outcome.error.0 == NONE, "{case}");
    match setup.expectation {
        ExpiryExpectation::RelativeToMutation(delta) => {
            let before = setup.window.before.0;
            let after = setup.window.after.0;
            assert!(
                (before + delta.0..=after + delta.0).contains(&outcome.expiry.0),
                "{case}: {} not in [{}, {}]",
                outcome.expiry.0,
                before + delta.0,
                after + delta.0
            );
        }
        ExpiryExpectation::LifetimeCap(maximum) => assert!(outcome.expiry == maximum, "{case}"),
    }
}

pub(super) fn stored_expiry(controller: &ControllerHandle, token_id: &str) -> Option<i64> {
    controller
        .current_image()
        .delegation_token_by_id(token_id)
        .map(|token| token.expiry_timestamp_ms)
}

/// The id of the token that [`failing_writes_source`] serves.
pub(super) const FAILING_WRITE_TOKEN_ID: &str = "tok-failing-write";

/// A metadata source that admits every token request and fails every write.
///
/// It serves an image at the delegation-token `metadata.version` holding one
/// live token, [`FAILING_WRITE_TOKEN_ID`], that `alice` owns and may renew.
/// Both `submit_change` and `submit_delegation_token_mutations` fail with the
/// error `failure` builds, as a controller write fails during a failover.
pub(super) fn failing_writes_source(failure: fn() -> RaftError) -> FakeMetadataSource {
    let now = now_ms();
    FakeMetadataSource::builder()
        .records(&[
            MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: METADATA_VERSION.to_owned(),
                level: metadata_version::DELEGATION_TOKEN_MIN_LEVEL,
            }),
            MetadataRecord::V1DelegationToken(DelegationTokenRecord {
                token_id: FAILING_WRITE_TOKEN_ID.to_owned(),
                owner: kp("alice"),
                requester: kp("alice"),
                issue_timestamp_ms: now,
                expiry_timestamp_ms: now + 60_000,
                max_timestamp_ms: now + DAY_MS,
                renewers: vec![],
            }),
        ])
        .on_submit(move |_| Err(failure()))
        .on_token_mutations(move |_| Err(failure()))
        .build()
}

/// The error code a token handler answers when its controller write fails,
/// for each kind of failure: Kafka's `ApiError.fromThrowable` of the
/// controller's exception. A write that lost or never found the active
/// controller is `NOT_CONTROLLER`, which the admin client retries. Anything
/// else is `UNKNOWN_SERVER_ERROR`.
pub(super) fn failed_write_cases() -> [(&'static str, fn() -> RaftError, i16); 5] {
    [
        (
            "not leader",
            || RaftError::NotLeader {
                current_leader: Some(NodeId(2)),
            },
            NOT_CONTROLLER,
        ),
        (
            "leader unknown",
            || RaftError::LeaderUnknown,
            NOT_CONTROLLER,
        ),
        (
            "uncommitted tail",
            || RaftError::UncommittedTail,
            NOT_CONTROLLER,
        ),
        ("shutdown", || RaftError::Shutdown, UNKNOWN_SERVER_ERROR),
        (
            "rejected",
            || RaftError::ChangeRejected("stale".to_owned()),
            UNKNOWN_SERVER_ERROR,
        ),
    ]
}
