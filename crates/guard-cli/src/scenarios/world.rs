//! The state one scenario carries from its Given steps to its Then steps.

use {
    super::{
        driver::{self, Cluster, PlaintextBroker},
        params::{BrokerAnswer, Outcome},
        transcript::{Run, Transcript},
    },
    krabka_bdd::Given,
    krabka_units::Time,
    std::{collections::BTreeMap, path::PathBuf},
};

/// One scenario's state.
///
/// The cluster boots on the first step that needs it, so a Given that
/// configures the cluster has to come before any step that acts on it.
pub struct GuardScenario {
    /// `break_glass.proposal_ttl`, when a Given caps it.
    proposal_ttl: Option<Time>,
    /// The running cluster, once a step needed one.
    cluster: Option<Cluster>,
    /// Whether the bootstrap address is one where no broker listens.
    pub unreachable: bool,
    /// The proposal the steps call "the proposal" or "that proposal".
    pub proposal: Given<uuid::Uuid>,
    /// What the tool reported for the When.
    pub outcome: Given<Outcome>,
    /// The lines the tool wrote for the When.
    pub transcript: Given<Transcript>,
    /// The trust file on the verifying operator's machine.
    pub trust_file: Given<PathBuf>,
    /// The broker answer a When hands the tool.
    pub answer: Given<BrokerAnswer>,
    /// The plaintext broker, once a step needed one.
    plaintext: Option<PlaintextBroker>,
    /// The variables the operator's shell exports.
    pub environment: BTreeMap<String, String>,
}

#[rstest::fixture]
pub fn guard() -> GuardScenario {
    let proposal = "Given alice has proposed deleting the doomed topic";
    GuardScenario {
        proposal_ttl: None,
        cluster: None,
        unreachable: false,
        proposal: Given::new("proposal", proposal),
        outcome: Given::new("outcome", "When <operator> tries to <action>"),
        transcript: Given::new("transcript", "When <operator> lists <what>"),
        trust_file: Given::new("trust_file", "Given the operator's machine holds <keys>"),
        answer: Given::new("answer", "When the broker answers a request with <answer>"),
        plaintext: None,
        environment: BTreeMap::new(),
    }
}

impl GuardScenario {
    /// Cap how long a proposal stays open on the cluster this scenario boots.
    ///
    /// # Panics
    ///
    /// Panics when the cluster is already running, because the broker reads
    /// the cap when it starts.
    pub fn cap_proposal_lifetime(&mut self, cap: Time) {
        assert2::assert!(
            self.cluster.is_none(),
            "the lifetime cap has to be set before any step boots the cluster"
        );
        self.proposal_ttl = Some(cap);
    }

    /// Remember what the When's run reported and the lines it wrote.
    pub fn record(&mut self, run: Run) {
        self.outcome.set(run.outcome);
        self.transcript.set(run.transcript);
    }

    /// The cluster, booted on first use.
    pub async fn cluster(&mut self) -> &Cluster {
        let proposal_ttl = self.proposal_ttl;
        let cluster = match self.cluster.take() {
            Some(cluster) => cluster,
            None => driver::boot().maybe_proposal_ttl(proposal_ttl).call().await,
        };
        self.cluster.insert(cluster)
    }

    /// The plaintext broker the shipped tool can reach, booted on first use.
    pub async fn plaintext_broker(&mut self) -> &PlaintextBroker {
        let broker = match self.plaintext.take() {
            Some(broker) => broker,
            None => PlaintextBroker::boot().await,
        };
        self.plaintext.insert(broker)
    }
}
