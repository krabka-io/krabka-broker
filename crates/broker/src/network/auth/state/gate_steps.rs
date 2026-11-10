//! Step definitions for the SASL request gate and the delegation-token
//! admission that a connection's [`ConnectionAuth`] state decides.

use {
    super::{ConnectionAuth, SaslExchange, TokenApi, TokenApiAdmission},
    crate::{
        handlers::ApiKeyCode,
        network::auth::test_support::{authenticated, oauth_reauthenticating},
    },
    assert2::assert,
    krabka_security::{AuthMethod, SaslMechanism},
    std::str::FromStr,
};

/// The state a scenario puts the connection in.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum SaslState {
    Anonymous,
    Negotiating,
    Reauthenticating,
    ReauthBadMechanism,
    ScramAuthenticated,
    AuthenticatedAnonymous,
    TokenAuthenticated,
    MtlsAuthenticated,
}

impl SaslState {
    fn connection(self) -> ConnectionAuth {
        match self {
            Self::Anonymous => ConnectionAuth::Anonymous,
            Self::Negotiating => ConnectionAuth::Negotiating {
                mechanism: SaslMechanism::Plain,
                exchange: SaslExchange::Plain,
                pending_token_expiry_ms: None,
            },
            Self::Reauthenticating => oauth_reauthenticating("alice", 2_000_000),
            Self::ReauthBadMechanism => ConnectionAuth::ReauthBadMechanism,
            Self::ScramAuthenticated => session(AuthMethod::SaslScramSha512, false),
            Self::AuthenticatedAnonymous => session(AuthMethod::Anonymous, false),
            Self::TokenAuthenticated => session(AuthMethod::SaslScramSha256, true),
            Self::MtlsAuthenticated => session(AuthMethod::MTls, false),
        }
    }
}

/// An authenticated session of `alice` under `auth_method`.
fn session(auth_method: AuthMethod, via_token: bool) -> ConnectionAuth {
    authenticated(
        "alice",
        auth_method,
        SaslMechanism::ScramSha512,
        None,
        via_token,
    )
}

/// A request the gate scenarios send, in the order the `When` step names
/// them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, krabka_bdd::StepParam)]
enum Probe {
    #[step_param("SaslHandshake")]
    SaslHandshake,
    #[step_param("ApiVersions")]
    ApiVersions,
    #[step_param("SaslAuthenticate")]
    SaslAuthenticate,
    #[step_param("Produce")]
    Produce,
    #[step_param("Metadata")]
    Metadata,
}

impl Probe {
    const ALL: [Self; 5] = [
        Self::SaslHandshake,
        Self::ApiVersions,
        Self::SaslAuthenticate,
        Self::Produce,
        Self::Metadata,
    ];

    const fn api_key(self) -> ApiKeyCode {
        match self {
            Self::SaslHandshake => 17,
            Self::ApiVersions => 18,
            Self::SaslAuthenticate => 36,
            Self::Produce => 0,
            Self::Metadata => 3,
        }
    }
}

/// The admitted requests, `,`-separated in probe order.
struct Admitted(Vec<Probe>);

impl FromStr for Admitted {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        text.split(',')
            .map(|probe| probe.trim().parse().map_err(|e| format!("{e}")))
            .collect::<Result<_, _>>()
            .map(Self)
    }
}

/// What a scenario says the broker answers a delegation-token API.
#[derive(Clone, Copy, Debug, krabka_bdd::StepParam)]
enum Admission {
    Allowed,
    Rejected,
}

impl From<Admission> for TokenApiAdmission {
    fn from(admission: Admission) -> Self {
        match admission {
            Admission::Allowed => Self::Allow,
            Admission::Rejected => Self::Reject,
        }
    }
}

const GIVEN_STATE: &str = "Given a connection whose SASL state is <state>";

struct SaslGate {
    connection: krabka_bdd::Given<ConnectionAuth>,
    admitted: krabka_bdd::Given<Vec<Probe>>,
    token_admissions: krabka_bdd::Given<Vec<TokenApiAdmission>>,
}

#[rstest::fixture]
fn sasl_gate() -> SaslGate {
    SaslGate {
        connection: krabka_bdd::Given::new("connection", GIVEN_STATE),
        admitted: krabka_bdd::Given::new(
            "admitted",
            "When the connection sends each of SaslHandshake, ApiVersions, SaslAuthenticate, Produce, Metadata",
        ),
        token_admissions: krabka_bdd::Given::new(
            "token_admissions",
            "When the connection sends each of CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken",
        ),
    }
}

#[krabka_bdd::given("a connection whose SASL state is {state}")]
fn connection_state(sasl_gate: &mut SaslGate, state: SaslState) {
    sasl_gate.connection.set(state.connection());
}

#[krabka_bdd::when(
    "the connection sends each of SaslHandshake, ApiVersions, SaslAuthenticate, Produce, Metadata"
)]
fn sends_probes(sasl_gate: &mut SaslGate) {
    let connection = sasl_gate.connection.get();
    let admitted = Probe::ALL
        .into_iter()
        .filter(|probe| connection.allows_request(probe.api_key()))
        .collect();
    sasl_gate.admitted.set(admitted);
}

#[krabka_bdd::then("the broker admits {admitted}")]
fn broker_admits(sasl_gate: &SaslGate, admitted: Admitted) {
    assert!(*sasl_gate.admitted.get() == admitted.0);
}

#[krabka_bdd::then("the broker refuses every request")]
fn broker_refuses_everything(sasl_gate: &SaslGate) {
    assert!(*sasl_gate.admitted.get() == Vec::new());
}

#[krabka_bdd::when(
    "the connection sends each of CreateDelegationToken, RenewDelegationToken, ExpireDelegationToken, DescribeDelegationToken"
)]
fn sends_token_requests(sasl_gate: &mut SaslGate) {
    let connection = sasl_gate.connection.get();
    let admissions = [
        TokenApi::Create,
        TokenApi::Renew,
        TokenApi::Expire,
        TokenApi::Describe,
    ]
    .map(|api| connection.token_api_admission(api));
    sasl_gate.token_admissions.set(admissions.to_vec());
}

#[krabka_bdd::then("each delegation-token API is {admission}")]
fn token_admission(sasl_gate: &SaslGate, admission: Admission) {
    assert!(*sasl_gate.token_admissions.get() == vec![TokenApiAdmission::from(admission); 4]);
}

krabka_bdd::scenarios!(
    "tests/features/migrated/src/network/auth/state",
    fixtures = [sasl_gate: SaslGate],
);
