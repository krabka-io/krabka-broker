//! The step words, parsed once into the values the driver takes.

use {
    crate::{EXIT_BAD_SIGNATURE, EXIT_MISMATCH, EXIT_NO_APPROVAL, EXIT_REFUSED, EXIT_UNREACHABLE},
    krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
    std::{fmt, str::FromStr},
};

/// One of the three approvers the scenario cluster knows.
///
/// Each one authenticates as `User:<name>` and holds an operator key named
/// `<name>-yubi`, which is the spelling the README uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub enum Operator {
    Alice,
    Bob,
    Carol,
}

impl Operator {
    /// Every operator the cluster authenticates.
    pub const ALL: [Self; 3] = [Self::Alice, Self::Bob, Self::Carol];

    /// The SASL user name, which is the step word.
    pub fn user(self) -> String { self.to_string() }

    /// The SASL password the cluster holds for this operator.
    pub fn password(self) -> String { format!("{self}-secret") }

    /// The principal the broker authenticates this operator as.
    pub fn principal(self) -> String { format!("User:{self}") }

    /// The operator key this operator signs with.
    pub fn key_id(self) -> String { format!("{self}-yubi") }
}

/// A freeze scope, in the words an operator uses for it.
///
/// `the payments topic` is a literal scope. `every topic starting with
/// tenant-a.` is a prefixed one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    /// One topic, by name.
    Topic(String),
    /// Every topic whose name starts with the prefix.
    Prefix(String),
}

impl Scope {
    /// The topic name or the prefix.
    pub fn name(&self) -> &str {
        match self {
            Self::Topic(name) | Self::Prefix(name) => name,
        }
    }

    /// The wire pattern type the registry reports for this scope.
    pub fn pattern_type(&self) -> i8 {
        match self {
            Self::Topic(_) => PATTERN_TYPE_LITERAL,
            Self::Prefix(_) => PATTERN_TYPE_PREFIXED,
        }
    }

    /// The command-line flag that names this scope.
    pub fn flag(&self) -> &'static str {
        match self {
            Self::Topic(_) => "--topic",
            Self::Prefix(_) => "--prefix",
        }
    }
}

impl FromStr for Scope {
    type Err = String;

    fn from_str(words: &str) -> Result<Self, Self::Err> {
        if let Some(prefix) = words.strip_prefix("every topic starting with ") {
            return Ok(Self::Prefix(prefix.to_owned()));
        }
        words
            .strip_prefix("the ")
            .and_then(|rest| rest.strip_suffix(" topic"))
            .map(|name| Self::Topic(name.to_owned()))
            .ok_or_else(|| {
                format!(
                    "{words:?} is not a scope: write `the <name> topic` or `every topic starting \
                     with <prefix>`"
                )
            })
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Topic(name) => write!(f, "the {name} topic"),
            Self::Prefix(prefix) => write!(f, "every topic starting with {prefix}"),
        }
    }
}

/// What an operator hands `--sign-with`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeySource {
    /// The PKCS#8 key the operator owns, under that operator's key id.
    OperatorKey(Operator),
    /// A file that holds no PKCS#8 key at all.
    NotAKey,
}

impl FromStr for KeySource {
    type Err = String;

    fn from_str(words: &str) -> Result<Self, Self::Err> {
        if words == "a file that holds no key" {
            return Ok(Self::NotAKey);
        }
        words
            .strip_suffix("'s key")
            .ok_or_else(|| {
                format!(
                    "{words:?} is not a key: write `<operator>'s key` or `a file that holds no key`"
                )
            })?
            .parse::<Operator>()
            .map(Self::OperatorKey)
            .map_err(|error| error.to_string())
    }
}

/// What the tool reported, read off its exit code.
///
/// The step words are the meanings the README's exit-code table gives each
/// number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub enum Outcome {
    /// `0`: the broker accepted the request.
    #[step_param("success")]
    Success,
    /// `1`: the broker, or the tool, refused the request.
    #[step_param("a refusal")]
    Refusal,
    /// `2`: nothing is known about the outcome.
    #[step_param("an unreachable broker")]
    UnreachableBroker,
    /// `3`: the registry names a key this machine does not hold.
    #[step_param("an unverifiable registry")]
    UnverifiableRegistry,
    /// `4`: the action needs an approval that does not exist.
    #[step_param("a missing approval")]
    MissingApproval,
    /// `5`: a signature did not verify, or was needed and missing.
    #[step_param("a signature failure")]
    SignatureFailure,
    /// The parser refused the command line, so nothing reached a broker.
    #[step_param("a refused command line")]
    CommandLineRefused,
}

impl Outcome {
    /// The outcome an exit code reports.
    ///
    /// # Panics
    ///
    /// Panics on a number the README does not define, which is a defect in
    /// the tool rather than an outcome a scenario can name.
    pub fn from_exit_code(code: i32) -> Self {
        match code {
            0 => Self::Success,
            EXIT_REFUSED => Self::Refusal,
            EXIT_UNREACHABLE => Self::UnreachableBroker,
            EXIT_MISMATCH => Self::UnverifiableRegistry,
            EXIT_NO_APPROVAL => Self::MissingApproval,
            EXIT_BAD_SIGNATURE => Self::SignatureFailure,
            other => panic!("exit code {other} is not one the README defines"),
        }
    }
}

/// A broker answer, by the error name Kafka or KFC-9 gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub enum BrokerAnswer {
    #[step_param("no error")]
    NoError,
    #[step_param("CLUSTER_AUTHORIZATION_FAILED")]
    ClusterAuthorizationFailed,
    #[step_param("BREAK_GLASS_APPROVAL_REQUIRED")]
    BreakGlassApprovalRequired,
    #[step_param("BREAK_GLASS_DUPLICATE_APPROVER")]
    BreakGlassDuplicateApprover,
    #[step_param("OPERATOR_SIGNATURE_INVALID")]
    OperatorSignatureInvalid,
    #[step_param("OPERATOR_SIGNATURE_REQUIRED")]
    OperatorSignatureRequired,
    #[step_param("FREEZE_LIMIT_EXCEEDED")]
    FreezeLimitExceeded,
}

impl BrokerAnswer {
    /// The error code the response carries.
    pub fn code(self) -> i16 {
        use crate::broker_codes as codes;

        match self {
            Self::NoError => 0,
            // Kafka's own code, which every private key answers a denied
            // request with.
            Self::ClusterAuthorizationFailed => 31,
            Self::BreakGlassApprovalRequired => codes::BREAK_GLASS_APPROVAL_REQUIRED,
            Self::BreakGlassDuplicateApprover => codes::BREAK_GLASS_DUPLICATE_APPROVER,
            Self::OperatorSignatureInvalid => codes::OPERATOR_SIGNATURE_INVALID,
            Self::OperatorSignatureRequired => codes::OPERATOR_SIGNATURE_REQUIRED,
            Self::FreezeLimitExceeded => codes::FREEZE_LIMIT_EXCEEDED,
        }
    }
}

/// How long an operator asks a proposal to stay open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lifetime {
    /// The operator names no lifetime, so the cluster picks one.
    Default,
    /// The text the operator passes, exactly as typed.
    Typed(String),
}

impl FromStr for Lifetime {
    type Err = String;

    fn from_str(words: &str) -> Result<Self, Self::Err> {
        Ok(if words == "the default" {
            Self::Default
        } else {
            Self::Typed(words.to_owned())
        })
    }
}

/// What the registry holds before an operator verifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub enum RegistryContents {
    #[step_param("a freeze alice signed")]
    AliceSigned,
    #[step_param("a freeze alice signed and an unsigned freeze")]
    AliceSignedAndUnsigned,
    #[step_param("an unsigned freeze")]
    Unsigned,
    #[step_param("a freeze alice signed and a freeze carol signed")]
    AliceAndCarolSigned,
}

/// The operator public keys on the machine that verifies the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub enum LocalKeys {
    #[step_param("alice's public key")]
    AlicesPublicKey,
    #[step_param("only bob's public key")]
    OnlyBobsPublicKey,
    #[step_param("another public key under alice's key id")]
    AnotherKeyUnderAlicesId,
    #[step_param("a trust file with no operator keys")]
    NoOperatorKeys,
}
