//! Administers krabka topic write freezes and break-glass proposals.
//!
//! A write freeze is a broker-owned state where the cluster is up, every read
//! works, and the broker refuses every new client write to a topic. A
//! break-glass proposal is a standing authorization that two different people
//! agreed to, which a privileged transition spends. This is the operator's side
//! of both: freeze a scope, read the registry back and prove it, lift a freeze
//! with an approval, and open, approve, withdraw and read the proposals.
//!
//! It is one command for one incident. It is a library as well as a binary so
//! tests call [`run_from_args`] in process: a test that spawns the binary needs
//! a Cargo working tree to build it from, and a Bazel test sandbox has none.
//! That is the same reason `krabka-barrier` and `krabka-format` are libraries.
//!
//! # The two properties that matter
//!
//! `--sign-with` never leaves the machine. It takes a PKCS#8 Ed25519 key file,
//! builds the canonical signing bytes here, signs them here, and puts only the
//! `key_id` and the detached signature on the wire. The private key never
//! reaches a broker, so a broker cannot make a signature in an operator's name.
//!
//! `freeze list --verify-signatures` checks the registry here as well, against
//! operator public keys on this machine. That makes the operator's own machine,
//! and not the broker that served the rows, the thing that says the registry is
//! authentic.
//!
//! # Exit codes
//!
//! A runbook branches on `$?`, so every number means one thing across this tool
//! and `krabka-barrier`. See [`EXIT_REFUSED`], [`EXIT_UNREACHABLE`],
//! [`EXIT_MISMATCH`], [`EXIT_NO_APPROVAL`] and [`EXIT_BAD_SIGNATURE`].
//!
//! # The api keys
//!
//! Every subcommand speaks one krabka-private api key, in the 1015 to 1019
//! range. A JVM `AdminClient` cannot send those. The freeze is visible to the
//! JVM tools another way: `DescribeConfigs` reports a read-only `write.freeze`
//! key for every topic, so `kafka-configs --describe` shows it.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use krabka_protocol::{
    krabka::{
        break_glass as bg,
        freeze::{self as api, PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
    },
    owned::describe_cluster_request::DescribeClusterRequest,
    primitives::uuid::Uuid as WireUuid,
};
use krabka_units::{Time, convert::TimeExt as _};

pub mod signing;
pub mod verify;

pub use verify::{CheckedEntry, Unproved, VerifyOutcome};

/// The exit code for a request the broker refused.
pub const EXIT_REFUSED: i32 = 1;
/// The exit code for a transport failure, where nothing is known about the
/// request's outcome.
pub const EXIT_UNREACHABLE: i32 = 2;
/// The exit code for a registry the local trust set does not match.
///
/// It carries `krabka-barrier`'s meaning: the tool asked the cluster for
/// something and what came back does not agree with what the operator holds. A
/// registry entry that names an operator key this machine does not have is that
/// disagreement, and it is not the same answer as a signature that failed. This
/// one says the tool could not check.
pub const EXIT_MISMATCH: i32 = 3;
/// The exit code for an action that needs a break-glass approval which does not
/// exist.
///
/// This is the code a runbook branches on. It separates "go and get a second
/// person" from every other refusal.
pub const EXIT_NO_APPROVAL: i32 = 4;
/// The exit code for a signature that did not verify, or that the broker needed
/// and did not get.
///
/// It keeps "the tool could not check" apart from "the tool checked and the
/// answer is wrong". KFC-5's verifier draws the same distinction.
pub const EXIT_BAD_SIGNATURE: i32 = 5;

/// The tool's command line.
///
/// Shared by the binary and by [`run_from_args`], so both accept exactly the
/// same flags.
#[derive(Parser)]
#[command(
    name = "krabka-guard",
    version,
    about = "Freeze topic writes, lift a freeze, and run the break-glass two-person rule"
)]
pub struct Cli {
    /// One or more `host:port` pairs to bootstrap against.
    #[arg(long, short = 'b', env = "KRABKA_BOOTSTRAP_SERVER", required = true)]
    pub bootstrap_server: String,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The two halves of the tool.
#[derive(Subcommand)]
pub enum Command {
    /// Administer the topic write-freeze registry.
    Freeze {
        /// What to do to the registry.
        #[command(subcommand)]
        command: FreezeCommand,
    },
    /// Administer the break-glass two-person rule.
    #[command(name = "break-glass")]
    BreakGlass {
        /// What to do to the proposals.
        #[command(subcommand)]
        command: BreakGlassCommand,
    },
}

/// The freeze registry subcommands.
#[derive(Subcommand)]
pub enum FreezeCommand {
    /// Freeze a topic or a topic-name prefix.
    ///
    /// A freeze is the safe direction, so one command sets it. The broker takes
    /// an unsigned freeze unless `freeze.require_signature` is on, and an
    /// unsigned entry is an attestation rather than a proof.
    Set {
        /// The scope to freeze.
        #[command(flatten)]
        scope: ScopeArgs,
        /// Free text that says why. The broker keeps it in the metadata log and
        /// in the audit event.
        #[arg(long)]
        reason: String,
        /// The detached operator signature, if the operator makes one.
        #[command(flatten)]
        signing: FreezeSigningArgs,
    },
    /// Print the live registry entries.
    List {
        /// Only print the entries whose scope is exactly this.
        #[arg(long)]
        scope: Option<String>,
        /// Check every returned entry against operator public keys on this
        /// machine, rather than taking the broker's word for the registry.
        #[arg(long, requires = "operator_keys")]
        verify_signatures: bool,
        /// A TOML file carrying the `[[operator_keys]]` block, which may be the
        /// `broker.toml` the cluster runs on.
        #[arg(long)]
        operator_keys: Option<PathBuf>,
    },
    /// Lift a freeze.
    ///
    /// A thaw is the dangerous direction. The broker refuses one that names no
    /// approved break-glass proposal, and it refuses one that carries no
    /// signature whatever its configuration says.
    Clear {
        /// The scope to thaw. It must name the entry exactly: a freeze on
        /// `--prefix tenant-a.` is not lifted by naming one topic under it.
        #[command(flatten)]
        scope: ScopeArgs,
        /// The approved break-glass proposal that authorizes the thaw.
        #[arg(long, value_parser = parse_uuid)]
        proposal: uuid::Uuid,
        /// Free text that says why the freeze is lifted. The metadata log and
        /// the audit event keep it, so the record names the reason as well as
        /// the person.
        #[arg(long, default_value = "")]
        reason: String,
        /// The detached operator signature. A thaw always needs one.
        #[command(flatten)]
        signing: FreezeSigningArgs,
    },
}

/// The break-glass subcommands.
#[derive(Subcommand)]
pub enum BreakGlassCommand {
    /// Open a proposal, which a second person then approves.
    Propose {
        /// The transition the proposal authorizes.
        #[arg(long, value_enum)]
        action: Action,
        /// What the transition applies to: a topic, a broker id, or a
        /// partition, depending on the action.
        #[arg(long)]
        target: String,
        /// Free text that says why the transition is needed.
        #[arg(long)]
        reason: String,
        /// How long the proposal stays open. Takes any time unit, so `30m`,
        /// `1h` and `90s` all work, and a number with no unit is refused. The
        /// broker caps it at `break_glass.proposal_ttl`. Omit to take that
        /// value.
        #[arg(long, value_parser = parse_time)]
        ttl: Option<Time>,
    },
    /// Add one approval to a proposal.
    ///
    /// The broker refuses the proposer, a principal outside
    /// `break_glass.approvers`, and a principal that already approved. Those
    /// three checks are what make this a two-person rule rather than a
    /// two-click rule.
    Approve {
        /// The proposal to approve.
        #[arg(long, value_parser = parse_uuid)]
        proposal: uuid::Uuid,
        /// The PKCS#8 Ed25519 key that signs the approval. The broker demands
        /// one for every action in `break_glass.signed_actions`.
        #[arg(long, requires = "key_id")]
        sign_with: Option<PathBuf>,
        /// The operator key that `--sign-with` holds, as `[[operator_keys]]`
        /// names it.
        #[arg(long, requires = "sign_with")]
        key_id: Option<String>,
    },
    /// Withdraw a proposal, so nothing can spend it.
    ///
    /// A withdraw rides the approve api key with the request's `withdraw` flag
    /// set. Approve and withdraw both name a proposal that exists and both act
    /// on it, so they share one request. A propose names no proposal, because a
    /// propose is what creates one.
    Withdraw {
        /// The proposal to withdraw.
        #[arg(long, value_parser = parse_uuid)]
        proposal: uuid::Uuid,
    },
    /// Print the proposals and their approvals.
    List {
        /// Drop the proposals that a transition consumed, that an operator
        /// withdrew, and that expired.
        #[arg(long)]
        pending: bool,
        /// Print this proposal alone.
        #[arg(long, value_parser = parse_uuid)]
        proposal: Option<uuid::Uuid>,
    },
}

/// The scope a freeze command acts on.
///
/// Exactly one of the two is given. A literal scope names one topic, and a
/// prefixed scope names every topic whose name starts with it, including one
/// the cluster creates later. The two words are Kafka's ACL pattern types, so a
/// namespace freeze uses the vocabulary an operator knows from `kafka-acls`.
#[derive(Args, Debug, Clone)]
#[group(required = true, multiple = false)]
pub struct ScopeArgs {
    /// One topic, by name.
    #[arg(long)]
    pub topic: Option<String>,
    /// Every topic whose name starts with this prefix.
    #[arg(long)]
    pub prefix: Option<String>,
}

/// The detached operator signature on a freeze or a thaw.
///
/// The three flags travel together. The broker binds the signature to the
/// principal on the connection and to the principal that `[[operator_keys]]`
/// binds the key to, and both of those have to be the name the signed bytes
/// carry. Only the operator knows what their listener authenticates them as, so
/// the tool asks rather than guesses.
#[derive(Args, Debug, Clone)]
pub struct FreezeSigningArgs {
    /// The PKCS#8 Ed25519 key file. It is read here, used here, and never
    /// sent.
    #[arg(long, requires_all = ["key_id", "principal"])]
    pub sign_with: Option<PathBuf>,
    /// The operator key that `--sign-with` holds, as `[[operator_keys]]` names
    /// it.
    #[arg(long, requires = "sign_with")]
    pub key_id: Option<String>,
    /// The principal the broker authenticates on this connection, which is the
    /// author the record carries.
    #[arg(long, requires = "sign_with")]
    pub principal: Option<String>,
}

/// A gated transition that a break-glass proposal authorizes.
///
/// The wire values are the values of the broker's own action type. The names
/// here are the same names `break_glass.signed_actions` and the audit event
/// use, spelled with dashes as a command line spells a word.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Lift a topic write freeze.
    ThawTopicFreeze,
    /// Elect a leader that is not in the in-sync replica set.
    UncleanElectLeaders,
    /// Recover a partition from a replica that may be behind.
    UncleanRecovery,
    /// Remove a broker from the cluster.
    UnregisterBroker,
    /// Cancel a partition reassignment that is in flight.
    CancelReassignment,
    /// Delete a topic.
    DeleteTopic,
    /// Delete records from the head of a partition.
    DeleteRecords,
}

impl Action {
    /// The wire value the private api keys carry for this action.
    #[must_use]
    pub fn wire(self) -> i8 {
        match self {
            Action::ThawTopicFreeze => 1,
            Action::UncleanElectLeaders => 2,
            Action::UncleanRecovery => 3,
            Action::UnregisterBroker => 4,
            Action::CancelReassignment => 5,
            Action::DeleteTopic => 6,
            Action::DeleteRecords => 7,
        }
    }
}

/// The name of an action wire value, in the spelling every krabka surface uses.
#[must_use]
pub fn action_name(wire: i8) -> &'static str {
    match wire {
        1 => "thaw_topic_freeze",
        2 => "unclean_elect_leaders",
        3 => "unclean_recovery",
        4 => "unregister_broker",
        5 => "cancel_reassignment",
        6 => "delete_topic",
        7 => "delete_records",
        _ => "unknown",
    }
}

/// The word for a pattern-type wire value.
#[must_use]
pub fn pattern_name(pattern_type: i8) -> &'static str {
    match pattern_type {
        PATTERN_TYPE_LITERAL => "literal",
        PATTERN_TYPE_PREFIXED => "prefixed",
        _ => "unknown",
    }
}

/// Parse a time argument.
///
/// Delegates to `krabka_units`, so every unit the broker's own configuration
/// accepts works here too: `ns`, `us`, `ms`, `s`, `m`, `h`, `d` and their long
/// forms.
///
/// A number with no unit is refused, and only `0` is exempt. That is the units
/// crate's rule and it is the right one here: `--ttl 30` from someone who meant
/// minutes would otherwise open a proposal for thirty milliseconds.
fn parse_time(raw: &str) -> Result<Time, String> {
    krabka_units::parse::time(raw).map_err(|e| e.to_string())
}

/// Parse a proposal id.
fn parse_uuid(raw: &str) -> Result<uuid::Uuid, String> {
    uuid::Uuid::parse_str(raw).map_err(|e| format!("{raw:?} is not a proposal id: {e}"))
}

/// Run the tool from an argv-style iterator, returning its exit code.
///
/// `0` means the broker accepted the request. `1` means it refused one, and the
/// reason is on stderr. `2` means the broker could not be reached, so nothing
/// is known about the outcome. `3` means the registry names an operator key
/// this machine does not hold. `4` means the action needs a break-glass
/// approval that does not exist. `5` means a signature did not verify.
///
/// # Panics
///
/// Panics if `argv` does not parse, which for a caller passing a literal
/// argument list is a bug in that list rather than a runtime condition.
pub async fn run_from_args<I, T>(argv: I) -> i32
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    run(Cli::parse_from(argv)).await
}

/// Run one parsed command.
pub async fn run(cli: Cli) -> i32 {
    let client = match krabka_client_core::Client::builder()
        .bootstrap(&cli.bootstrap_server)
        .client_id("krabka-guard")
        .build()
        .await
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("cannot reach {}: {error}", cli.bootstrap_server);
            return EXIT_UNREACHABLE;
        }
    };
    match dispatch(&client, cli.command).await {
        Ok(code) => code,
        Err(failure) => {
            eprintln!("{}", failure.message());
            failure.exit_code()
        }
    }
}

/// Send one command's request and print its response.
///
/// # Errors
///
/// Returns the [`Failure`] of a step that stopped before a broker answered: a
/// key file that cannot be read, a proposal that cannot be looked up, or a
/// request that did not complete.
async fn dispatch(client: &krabka_client_core::Client, command: Command) -> Result<i32, Failure> {
    match command {
        Command::Freeze { command } => match command {
            FreezeCommand::Set {
                scope,
                reason,
                signing,
            } => set_freeze(client, &scope, reason, &signing, true, uuid::Uuid::nil()).await,
            FreezeCommand::Clear {
                scope,
                proposal,
                reason,
                signing,
            } => set_freeze(client, &scope, reason, &signing, false, proposal).await,
            FreezeCommand::List {
                scope,
                verify_signatures,
                operator_keys,
            } => list_freezes(client, scope, verify_signatures, operator_keys.as_deref()).await,
        },
        Command::BreakGlass { command } => match command {
            BreakGlassCommand::Propose {
                action,
                target,
                reason,
                ttl,
            } => propose(client, action, target, reason, ttl).await,
            BreakGlassCommand::Approve {
                proposal,
                sign_with,
                key_id,
            } => approve(client, proposal, sign_with.as_deref(), key_id.as_deref()).await,
            BreakGlassCommand::Withdraw { proposal } => withdraw(client, proposal).await,
            BreakGlassCommand::List { pending, proposal } => {
                list_proposals(client, pending, proposal).await
            }
        },
    }
}

/// Set or lift one freeze.
///
/// The two directions share this because they share a request. `frozen` and the
/// proposal are what separate them, and both are inside the signed bytes, so a
/// signature captured from a freeze cannot be replayed as the thaw.
async fn set_freeze(
    client: &krabka_client_core::Client,
    scope: &ScopeArgs,
    reason: String,
    signing: &FreezeSigningArgs,
    frozen: bool,
    proposal: uuid::Uuid,
) -> Result<i32, Failure> {
    let scope = scope.resolve()?;
    let set_at_ms = now_ms();
    let proposal_id = WireUuid(*proposal.as_bytes());

    let (key_id, signature) = match signing.resolve()? {
        None => (String::new(), Vec::new()),
        Some(signing) => {
            // The cluster id is inside the signed bytes, so a signature made
            // here cannot be replayed into a second cluster. It is read before
            // the signature is made and never after.
            let cluster_id = cluster_id(client).await?;
            let signer = signing::load_signer(signing.key_path, signing.key_id)
                .map_err(Failure::Signature)?;
            let bytes = signing::freeze_signing_bytes(&signing::FreezeSigningInput {
                cluster_id: &cluster_id,
                pattern_type: scope.pattern_type,
                scope: &scope.name,
                frozen,
                reason: &reason,
                set_by: signing.principal,
                set_at_ms,
                proposal_id: proposal_id.0,
            });
            (signing.key_id.to_owned(), signer.sign(&bytes))
        }
    };

    let request = api::SetTopicFreezeRequest {
        scope: scope.name,
        pattern_type: scope.pattern_type,
        frozen,
        reason,
        proposal_id,
        set_at_ms,
        key_id,
        signature,
        ..api::SetTopicFreezeRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    Ok(report_set_freeze(&response, frozen))
}

/// Read the registry, and prove it when the operator asks.
async fn list_freezes(
    client: &krabka_client_core::Client,
    scope: Option<String>,
    verify_signatures: bool,
    operator_keys: Option<&std::path::Path>,
) -> Result<i32, Failure> {
    let request = api::DescribeTopicFreezesRequest {
        scope_filter: scope,
        pattern_type_filter: PATTERN_TYPE_ANY,
        ..api::DescribeTopicFreezesRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Ok(report_error(
            response.error_code,
            response.error_message.as_deref(),
        ));
    }
    let Some(operator_keys) = operator_keys.filter(|_| verify_signatures) else {
        for freeze in &response.freezes {
            print_freeze(freeze, None);
        }
        return Ok(0);
    };
    let keys = verify::load_trust_set(operator_keys).map_err(Failure::Signature)?;
    let cluster_id = cluster_id(client).await?;
    let outcome = verify::verify_registry(&cluster_id, &keys, &response.freezes);
    Ok(report_verify(&response.freezes, &outcome))
}

/// Open a proposal.
async fn propose(
    client: &krabka_client_core::Client,
    action: Action,
    target: String,
    reason: String,
    ttl: Option<Time>,
) -> Result<i32, Failure> {
    let request = bg::ProposeBreakGlassRequest {
        action: action.wire(),
        target,
        reason,
        // Zero asks the broker for `break_glass.proposal_ttl`.
        ttl_ms: ttl.map_or(0, Time::millis_i64),
        ..bg::ProposeBreakGlassRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Ok(report_error(
            response.error_code,
            response.error_message.as_deref(),
        ));
    }
    println!(
        "proposal {}",
        uuid::Uuid::from_bytes(response.proposal_id.0)
    );
    println!("expires  {}", response.expires_at_ms);
    Ok(0)
}

/// Add one approval to a proposal.
///
/// A signature covers the proposal the broker holds, and not one the caller
/// supplies, so a signed approval reads the proposal back first and signs what
/// is stored.
async fn approve(
    client: &krabka_client_core::Client,
    proposal: uuid::Uuid,
    sign_with: Option<&std::path::Path>,
    key_id: Option<&str>,
) -> Result<i32, Failure> {
    let proposal_id = WireUuid(*proposal.as_bytes());
    let (key_id, signature) = match (sign_with, key_id) {
        (Some(path), Some(key_id)) => {
            let stored = read_proposal(client, proposal_id).await?;
            let signer = signing::load_signer(path, key_id).map_err(Failure::Signature)?;
            let bytes = signing::approval_signing_bytes(&signing::ApprovalSigningInput {
                proposal_id: stored.proposal_id.0,
                action: stored.action,
                target: &stored.target,
                proposer: &stored.proposer,
                created_at_ms: stored.created_at_ms,
                expires_at_ms: stored.expires_at_ms,
            });
            (key_id.to_owned(), signer.sign(&bytes))
        }
        _ => (String::new(), Vec::new()),
    };
    let request = bg::ApproveBreakGlassRequest {
        proposal_id,
        key_id,
        signature,
        withdraw: false,
        ..bg::ApproveBreakGlassRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Ok(report_error(
            response.error_code,
            response.error_message.as_deref(),
        ));
    }
    println!(
        "approvals {} of {}",
        response.approvals_held, response.approvals_required
    );
    Ok(0)
}

/// Withdraw a proposal.
///
/// This rides api key 1018 with the `withdraw` flag set, and never key 1017.
async fn withdraw(
    client: &krabka_client_core::Client,
    proposal: uuid::Uuid,
) -> Result<i32, Failure> {
    let request = bg::ApproveBreakGlassRequest {
        proposal_id: WireUuid(*proposal.as_bytes()),
        withdraw: true,
        ..bg::ApproveBreakGlassRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Ok(report_error(
            response.error_code,
            response.error_message.as_deref(),
        ));
    }
    println!("withdrawn {proposal}");
    Ok(0)
}

/// Print the proposals and their approvals.
async fn list_proposals(
    client: &krabka_client_core::Client,
    pending: bool,
    proposal: Option<uuid::Uuid>,
) -> Result<i32, Failure> {
    let request = bg::DescribeBreakGlassRequest {
        pending_only: pending,
        proposal_id: proposal.map_or(WireUuid::ZERO, |id| WireUuid(*id.as_bytes())),
        ..bg::DescribeBreakGlassRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Ok(report_error(
            response.error_code,
            response.error_message.as_deref(),
        ));
    }
    for stored in &response.proposals {
        print_proposal(stored);
    }
    Ok(0)
}

/// Read one stored proposal, so a signature covers what the broker holds.
async fn read_proposal(
    client: &krabka_client_core::Client,
    proposal_id: WireUuid,
) -> Result<bg::DescribedBreakGlassProposal, Failure> {
    let request = bg::DescribeBreakGlassRequest {
        proposal_id,
        ..bg::DescribeBreakGlassRequest::default()
    };
    let response = client.send(request).await.map_err(Failure::from)?;
    if response.error_code != 0 {
        return Err(Failure::Refused(format!(
            "cannot read the proposal to sign it: {}",
            described_error(response.error_code, response.error_message.as_deref())
        )));
    }
    let id = uuid::Uuid::from_bytes(proposal_id.0);
    response
        .proposals
        .into_iter()
        .find(|stored| stored.proposal_id == proposal_id)
        .ok_or_else(|| Failure::Refused(format!("the cluster holds no proposal {id}")))
}

/// Read the cluster id that a freeze signature covers.
async fn cluster_id(client: &krabka_client_core::Client) -> Result<String, Failure> {
    let response = client
        .send(DescribeClusterRequest::default())
        .await
        .map_err(Failure::from)?;
    if response.error_code != 0 {
        return Err(Failure::Refused(format!(
            "cannot read the cluster id: {}",
            described_error(response.error_code, response.error_message.as_deref())
        )));
    }
    Ok(response.cluster_id)
}

/// A step that stopped before a broker's answer decided the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Failure {
    /// The request did not complete, so nothing is known about its outcome.
    Transport(String),
    /// The tool or the broker refused to go on.
    Refused(String),
    /// A key could not be read, or a signature could not be checked.
    Signature(String),
}

impl From<krabka_client_core::ClientError> for Failure {
    /// A client error is always a transport failure here.
    ///
    /// Nothing is known about the request's outcome, which is what separates
    /// this from a refusal the broker answered with.
    fn from(error: krabka_client_core::ClientError) -> Self {
        Failure::Transport(format!(
            "the request did not complete, so its outcome is unknown: {error}"
        ))
    }
}

impl Failure {
    /// The exit code this failure reports.
    fn exit_code(&self) -> i32 {
        match self {
            Failure::Transport(_) => EXIT_UNREACHABLE,
            Failure::Refused(_) => EXIT_REFUSED,
            Failure::Signature(_) => EXIT_BAD_SIGNATURE,
        }
    }

    /// The line this failure prints on stderr.
    fn message(&self) -> &str {
        match self {
            Failure::Transport(message)
            | Failure::Refused(message)
            | Failure::Signature(message) => message,
        }
    }
}

impl ScopeArgs {
    /// The scope these arguments name.
    ///
    /// # Errors
    ///
    /// Returns a refusal when neither or both of `--topic` and `--prefix` are
    /// given. The parser already rules that out, and this keeps the function
    /// total rather than defaulting to an empty prefix that would name every
    /// topic in the cluster.
    fn resolve(&self) -> Result<Scope, Failure> {
        match (self.topic.as_deref(), self.prefix.as_deref()) {
            (Some(topic), None) => Ok(Scope {
                name: topic.to_owned(),
                pattern_type: PATTERN_TYPE_LITERAL,
            }),
            (None, Some(prefix)) => Ok(Scope {
                name: prefix.to_owned(),
                pattern_type: PATTERN_TYPE_PREFIXED,
            }),
            _ => Err(Failure::Refused(
                "name exactly one of --topic and --prefix".to_owned(),
            )),
        }
    }
}

/// One resolved freeze scope.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Scope {
    /// A topic name, or a topic-name prefix.
    name: String,
    /// `3` literal, `4` prefixed.
    pattern_type: i8,
}

impl FreezeSigningArgs {
    /// The signing material, when the operator asked for a signature.
    ///
    /// # Errors
    ///
    /// Returns a refusal when `--sign-with` is given without both of the other
    /// two. The parser already rules that out, and this keeps the function
    /// total.
    fn resolve(&self) -> Result<Option<FreezeSigning<'_>>, Failure> {
        let Some(key_path) = self.sign_with.as_deref() else {
            return Ok(None);
        };
        match (self.key_id.as_deref(), self.principal.as_deref()) {
            (Some(key_id), Some(principal)) => Ok(Some(FreezeSigning {
                key_path,
                key_id,
                principal,
            })),
            _ => Err(Failure::Refused(
                "--sign-with also needs --key-id and --principal".to_owned(),
            )),
        }
    }
}

/// The resolved signing material for one freeze or thaw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FreezeSigning<'a> {
    /// The PKCS#8 key file, which never leaves this machine.
    key_path: &'a std::path::Path,
    /// The key id that the request carries.
    key_id: &'a str,
    /// The author name the signed bytes carry.
    principal: &'a str,
}

/// The current wall clock, in milliseconds since the Unix epoch.
///
/// A signed record carries it, and the broker refuses one outside
/// `freeze.signature_max_skew` of its own clock. A clock before the epoch
/// saturates at zero, which the broker then refuses as far outside the window.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| i64::try_from(since.as_millis()).unwrap_or(0))
}

/// The exit code that one broker error code becomes.
///
/// Three codes get their own number because a runbook acts on them
/// differently. An action that needs an approval sends the operator for a
/// second person. A signature failure sends them to their key material. Every
/// other refusal is a refusal.
#[must_use]
pub fn exit_for_code(code: i16) -> i32 {
    use krabka_broker::codes;

    match code {
        codes::NONE => 0,
        codes::BREAK_GLASS_APPROVAL_REQUIRED => EXIT_NO_APPROVAL,
        codes::OPERATOR_SIGNATURE_INVALID | codes::OPERATOR_SIGNATURE_REQUIRED => {
            EXIT_BAD_SIGNATURE
        }
        _ => EXIT_REFUSED,
    }
}

/// Describe an error code.
///
/// `krabka-broker` has no code-to-name table, so this prints the number and
/// names the krabka-private codes that no Kafka reference lists.
#[must_use]
pub fn code_name(code: i16) -> String {
    use krabka_broker::codes;

    let note = match code {
        codes::BREAK_GLASS_APPROVAL_REQUIRED => {
            Some("no approved break-glass proposal covers this")
        }
        codes::BREAK_GLASS_DUPLICATE_APPROVER => Some("this principal already approved"),
        codes::BREAK_GLASS_NOT_AN_APPROVER => Some("this principal is not a configured approver"),
        codes::OPERATOR_SIGNATURE_INVALID => Some("the operator signature did not verify"),
        codes::OPERATOR_SIGNATURE_REQUIRED => Some("this action needs an operator signature"),
        codes::FREEZE_SCOPE_INVALID => Some("the scope is empty or reaches an internal topic"),
        codes::FREEZE_LIMIT_EXCEEDED => Some("the registry is at freeze.max_entries"),
        _ => None,
    };
    note.map_or_else(
        || format!("error {code}"),
        |note| format!("error {code} ({note})"),
    )
}

/// One error code and its message, as a line.
fn described_error(code: i16, message: Option<&str>) -> String {
    format!(
        "{}{}",
        code_name(code),
        message.map_or_else(String::new, |m| format!(": {m}"))
    )
}

/// Print a refusal and turn it into an exit code.
fn report_error(code: i16, message: Option<&str>) -> i32 {
    eprintln!("{}", described_error(code, message));
    exit_for_code(code)
}

/// The outcome of one freeze or thaw.
fn report_set_freeze(response: &api::SetTopicFreezeResponse, frozen: bool) -> i32 {
    if response.error_code != 0 {
        return report_error(response.error_code, response.error_message.as_deref());
    }
    println!("{}\tok", if frozen { "frozen" } else { "thawed" });
    0
}

/// One line per registry entry.
fn print_freeze(freeze: &api::DescribedTopicFreeze, checked: Option<&CheckedEntry>) {
    let proof = match checked.map(|entry| entry.unproved) {
        None => String::new(),
        Some(None) => format!("\tverified by {}", freeze.key_id),
        Some(Some(unproved)) => format!("\t{}", unproved.reason()),
    };
    println!(
        "{}:{}\tset by {} at {}\t{}{proof}",
        pattern_name(freeze.pattern_type),
        freeze.scope,
        freeze.set_by,
        freeze.set_at_ms,
        freeze.reason,
    );
}

/// One block per proposal.
fn print_proposal(proposal: &bg::DescribedBreakGlassProposal) {
    let state = if proposal.withdrawn {
        "withdrawn"
    } else if proposal.consumed_at_ms > 0 {
        "consumed"
    } else {
        "open"
    };
    println!(
        "proposal {} {state}",
        uuid::Uuid::from_bytes(proposal.proposal_id.0)
    );
    println!("  action     {}", action_name(proposal.action));
    println!("  target     {}", proposal.target);
    println!("  proposer   {}", proposal.proposer);
    println!("  reason     {}", proposal.reason);
    println!("  created    {}", proposal.created_at_ms);
    println!("  expires    {}", proposal.expires_at_ms);
    for approval in &proposal.approvals {
        println!(
            "  approved   {} at {} {}",
            approval.principal,
            approval.approved_at_ms,
            approval_evidence(approval)
        );
    }
}

/// What one approval offers as evidence of who made it.
///
/// The broker never stores a signature it did not check, so a `key_id` on a
/// stored approval is already proof that the signature verified there. The
/// first bytes of the signature are printed so an auditor can line one approval
/// up against the audit event that carries the whole of it.
fn approval_evidence(approval: &bg::BreakGlassApproval) -> String {
    /// How many leading signature bytes identify one approval in a report.
    const PREFIX: usize = 8;

    if approval.key_id.is_empty() {
        return "unsigned".to_owned();
    }
    format!(
        "signed by {} ({}...)",
        approval.key_id,
        hex::encode(&approval.signature[..approval.signature.len().min(PREFIX)])
    )
}

/// What a local verification of the registry found.
///
/// A signature that failed outranks a key this machine does not hold, because
/// the first says the tool checked and the answer is wrong, and the second says
/// the tool could not check.
fn report_verify(freezes: &[api::DescribedTopicFreeze], outcome: &VerifyOutcome) -> i32 {
    for (freeze, checked) in freezes.iter().zip(&outcome.entries) {
        print_freeze(freeze, Some(checked));
    }
    if outcome.any_signature_failed() {
        eprintln!("the registry does not verify against the operator keys on this machine");
        return EXIT_BAD_SIGNATURE;
    }
    if outcome.any_key_is_unknown() {
        eprintln!("the registry names an operator key that this machine does not hold");
        return EXIT_MISMATCH;
    }
    println!(
        "{} of {} entries are proved by an operator signature",
        outcome.proved(),
        outcome.entries.len()
    );
    0
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_broker::codes;

    use super::*;

    /// A runbook branches on `$?`, so each number has to mean one thing. The
    /// three codes that get their own number are the three an operator acts on
    /// differently; every other refusal is a refusal.
    #[test]
    fn every_broker_code_maps_to_the_exit_code_a_runbook_expects() {
        let cases: [(&'static str, i16, i32); 12] = [
            ("no error", codes::NONE, 0),
            (
                "an action that needs an approval",
                codes::BREAK_GLASS_APPROVAL_REQUIRED,
                EXIT_NO_APPROVAL,
            ),
            (
                "a signature that did not verify",
                codes::OPERATOR_SIGNATURE_INVALID,
                EXIT_BAD_SIGNATURE,
            ),
            (
                "a signature the broker needed and did not get",
                codes::OPERATOR_SIGNATURE_REQUIRED,
                EXIT_BAD_SIGNATURE,
            ),
            (
                "a principal that already approved",
                codes::BREAK_GLASS_DUPLICATE_APPROVER,
                EXIT_REFUSED,
            ),
            (
                "a principal outside the approver set",
                codes::BREAK_GLASS_NOT_AN_APPROVER,
                EXIT_REFUSED,
            ),
            (
                "a scope that reaches an internal topic",
                codes::FREEZE_SCOPE_INVALID,
                EXIT_REFUSED,
            ),
            (
                "a registry at its ceiling",
                codes::FREEZE_LIMIT_EXCEEDED,
                EXIT_REFUSED,
            ),
            (
                "a caller with no cluster right",
                codes::CLUSTER_AUTHORIZATION_FAILED,
                EXIT_REFUSED,
            ),
            ("a malformed request", codes::INVALID_REQUEST, EXIT_REFUSED),
            (
                "a broker that is not the controller",
                codes::NOT_CONTROLLER,
                EXIT_REFUSED,
            ),
            (
                "a code this build does not know",
                codes::UNKNOWN_SERVER_ERROR,
                EXIT_REFUSED,
            ),
        ];
        for (case, code, expected) in cases {
            check!(exit_for_code(code) == expected, "{case}");
        }
    }

    /// Every exit code is a distinct number, because a runbook that reads two
    /// meanings out of one number is a runbook that does the wrong thing.
    #[test]
    fn the_exit_codes_are_distinct() {
        let codes = [
            ("refused", EXIT_REFUSED),
            ("unreachable", EXIT_UNREACHABLE),
            ("mismatch", EXIT_MISMATCH),
            ("no approval", EXIT_NO_APPROVAL),
            ("bad signature", EXIT_BAD_SIGNATURE),
        ];
        for (index, (left_name, left)) in codes.iter().enumerate() {
            for (right_name, right) in &codes[index + 1..] {
                check!(left != right, "{left_name} and {right_name}");
            }
        }
    }

    /// The three codes `krabka-barrier` also ships keep the numbers it gives
    /// them, so one runbook can branch on both tools.
    #[test]
    fn the_shared_exit_codes_keep_the_barrier_meanings() {
        check!(EXIT_REFUSED == 1);
        check!(EXIT_UNREACHABLE == 2);
        check!(EXIT_MISMATCH == 3);
    }

    /// A time argument takes any unit the broker's own configuration takes, so
    /// an operator never has to convert to milliseconds by hand, and a number
    /// with no unit is refused rather than guessed at.
    #[test]
    fn a_time_argument_takes_any_unit() {
        let cases = [
            ("500ms", Some(500)),
            ("30s", Some(30_000)),
            ("30m", Some(1_800_000)),
            ("1h", Some(3_600_000)),
            ("1 hour", Some(3_600_000)),
            // A unit is required, so a number alone cannot be read as the
            // wrong scale. Zero is the one exemption, having no scale.
            ("30", None),
            ("0", Some(0)),
            ("banana", None),
            ("", None),
        ];
        for (raw, expected) in cases {
            check!(
                parse_time(raw).ok().map(Time::millis_i64) == expected,
                "{raw}"
            );
        }
    }

    /// `--bootstrap-server` is the one flag every subcommand needs, so the
    /// parser refuses a command line without it rather than defaulting to a
    /// guess about where the cluster is.
    #[test]
    fn a_command_line_without_a_bootstrap_server_is_refused() {
        assert!(Cli::try_parse_from(["krabka-guard", "freeze", "list"]).is_err());
        assert!(
            Cli::try_parse_from(["krabka-guard", "-b", "localhost:9092", "freeze", "list"]).is_ok()
        );
    }

    /// `--topic` and `--prefix` name the two pattern types, and exactly one of
    /// them is given. A freeze with neither would have no scope, and a freeze
    /// with both would have two.
    #[test]
    fn a_freeze_names_exactly_one_scope() {
        let base = ["krabka-guard", "-b", "localhost:9092", "freeze", "set"];
        let reason = ["--reason", "DR cutover"];

        let literal = Cli::try_parse_from(
            base.iter()
                .chain(["--topic", "orders"].iter())
                .chain(reason.iter()),
        )
        .expect("a literal scope parses");
        check!(freeze_scope(literal) == Ok(scope("orders", PATTERN_TYPE_LITERAL)));

        let prefixed = Cli::try_parse_from(
            base.iter()
                .chain(["--prefix", "tenant-a."].iter())
                .chain(reason.iter()),
        )
        .expect("a prefixed scope parses");
        check!(freeze_scope(prefixed) == Ok(scope("tenant-a.", PATTERN_TYPE_PREFIXED)));

        assert!(
            Cli::try_parse_from(base.iter().chain(reason.iter())).is_err(),
            "a freeze with no scope is refused"
        );
        assert!(
            Cli::try_parse_from(
                base.iter()
                    .chain(["--topic", "orders", "--prefix", "tenant-a."].iter())
                    .chain(reason.iter())
            )
            .is_err(),
            "a freeze with two scopes is refused"
        );
    }

    /// The three signing flags travel together. A key file with no key id
    /// would produce a signature the broker cannot look up, and a key id with
    /// no key file would name a signature that was never made.
    #[test]
    fn the_signing_flags_travel_together() {
        let base = [
            "krabka-guard",
            "-b",
            "localhost:9092",
            "freeze",
            "set",
            "--topic",
            "orders",
            "--reason",
            "DR cutover",
        ];
        let cases: [(&'static str, Vec<&'static str>, bool); 5] = [
            ("no signature at all", vec![], true),
            (
                "all three flags",
                vec![
                    "--sign-with",
                    "key.pk8",
                    "--key-id",
                    "alice-yubi",
                    "--principal",
                    "User:alice",
                ],
                true,
            ),
            (
                "a key file with no key id",
                vec!["--sign-with", "key.pk8", "--principal", "User:alice"],
                false,
            ),
            (
                "a key file with no principal",
                vec!["--sign-with", "key.pk8", "--key-id", "alice-yubi"],
                false,
            ),
            (
                "a key id with no key file",
                vec!["--key-id", "alice-yubi"],
                false,
            ),
        ];
        for (case, extra, parses) in cases {
            let line = base.iter().copied().chain(extra);
            check!(Cli::try_parse_from(line).is_ok() == parses, "{case}");
        }
    }

    /// `--verify-signatures` with no key file would check the registry against
    /// an empty trust set, which is a check that silently does nothing.
    #[test]
    fn verifying_signatures_needs_a_local_key_file() {
        let base = ["krabka-guard", "-b", "localhost:9092", "freeze", "list"];
        assert!(
            Cli::try_parse_from(base.iter().chain(["--verify-signatures"].iter())).is_err(),
            "verifying with no key file is refused"
        );
        assert!(
            Cli::try_parse_from(
                base.iter()
                    .chain(["--verify-signatures", "--operator-keys", "keys.toml"].iter())
            )
            .is_ok(),
            "verifying with a key file parses"
        );
    }

    /// The break-glass action names reach the wire as the values the broker's
    /// own action type carries, and no action takes the zero that a default
    /// request holds.
    #[test]
    fn every_action_carries_its_own_wire_value_and_name() {
        let cases: [(&'static str, Action, i8, &'static str); 7] = [
            ("thaw", Action::ThawTopicFreeze, 1, "thaw_topic_freeze"),
            (
                "unclean election",
                Action::UncleanElectLeaders,
                2,
                "unclean_elect_leaders",
            ),
            (
                "unclean recovery",
                Action::UncleanRecovery,
                3,
                "unclean_recovery",
            ),
            (
                "unregister broker",
                Action::UnregisterBroker,
                4,
                "unregister_broker",
            ),
            (
                "cancel reassignment",
                Action::CancelReassignment,
                5,
                "cancel_reassignment",
            ),
            ("delete topic", Action::DeleteTopic, 6, "delete_topic"),
            ("delete records", Action::DeleteRecords, 7, "delete_records"),
        ];
        for (case, action, wire, name) in cases {
            check!(action.wire() == wire, "{case}");
            check!(action_name(wire) == name, "{case}");
        }
        check!(action_name(0) == "unknown");
        check!(action_name(8) == "unknown");
    }

    /// The command line spells an action with dashes, which is what the
    /// documented runbook types.
    #[test]
    fn an_action_is_spelled_with_dashes_on_the_command_line() {
        let cli = Cli::try_parse_from([
            "krabka-guard",
            "-b",
            "localhost:9092",
            "break-glass",
            "propose",
            "--action",
            "delete-topic",
            "--target",
            "doomed",
            "--reason",
            "test data only",
            "--ttl",
            "30m",
        ])
        .expect("parses");
        let Command::BreakGlass {
            command: BreakGlassCommand::Propose { action, ttl, .. },
        } = cli.command
        else {
            panic!("expected a propose");
        };
        check!(action == Action::DeleteTopic);
        check!(ttl.map(Time::millis_i64) == Some(1_800_000));
    }

    /// An approval says who made it, and a signed one says which key. The
    /// broker never stores a signature it did not check, so a `key_id` here is
    /// already proof that the signature verified on the broker.
    #[test]
    fn an_approval_reports_the_evidence_it_carries() {
        let unsigned = bg::BreakGlassApproval {
            principal: "User:bob".to_owned(),
            approved_at_ms: 1_770_000_000_000,
            ..bg::BreakGlassApproval::default()
        };
        check!(approval_evidence(&unsigned) == "unsigned");

        let signed = bg::BreakGlassApproval {
            key_id: "bob-yubi".to_owned(),
            signature: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06],
            ..unsigned
        };
        check!(approval_evidence(&signed) == "signed by bob-yubi (deadbeef01020304...)");
    }

    #[test]
    fn a_pattern_type_reads_as_a_word() {
        check!(pattern_name(PATTERN_TYPE_LITERAL) == "literal");
        check!(pattern_name(PATTERN_TYPE_PREFIXED) == "prefixed");
        check!(pattern_name(PATTERN_TYPE_ANY) == "unknown");
    }

    /// A scope with neither half is unreachable through the parser, and the
    /// resolver still refuses it rather than falling back to an empty prefix,
    /// which would name every topic in the cluster.
    #[test]
    fn a_scope_with_neither_half_is_refused_rather_than_defaulted() {
        let empty = ScopeArgs {
            topic: None,
            prefix: None,
        };
        check!(
            empty.resolve()
                == Err(Failure::Refused(
                    "name exactly one of --topic and --prefix".to_owned()
                ))
        );
    }

    /// A key file with no key id is unreachable through the parser, and the
    /// resolver still refuses it rather than signing under an empty key id.
    #[test]
    fn signing_material_with_a_missing_half_is_refused() {
        let half = FreezeSigningArgs {
            sign_with: Some(PathBuf::from("key.pk8")),
            key_id: None,
            principal: Some("User:alice".to_owned()),
        };
        check!(half.resolve().is_err());

        let none = FreezeSigningArgs {
            sign_with: None,
            key_id: None,
            principal: None,
        };
        check!(none.resolve() == Ok(None));
    }

    /// A transport failure says that nothing is known about the outcome, which
    /// is the difference between it and a refusal.
    #[test]
    fn a_failure_reports_its_own_exit_code() {
        let cases: [(&'static str, Failure, i32); 3] = [
            (
                "a request that did not complete",
                Failure::Transport("gone".to_owned()),
                EXIT_UNREACHABLE,
            ),
            ("a refusal", Failure::Refused("no".to_owned()), EXIT_REFUSED),
            (
                "a key that cannot be read",
                Failure::Signature("bad key".to_owned()),
                EXIT_BAD_SIGNATURE,
            ),
        ];
        for (case, failure, expected) in cases {
            check!(failure.exit_code() == expected, "{case}");
            check!(!failure.message().is_empty(), "{case}");
        }
    }

    /// The private codes are the ones an operator of this tool meets that no
    /// Kafka reference lists, so each one gets a word beside its number.
    #[test]
    fn a_private_code_reads_as_more_than_a_number() {
        check!(code_name(codes::BREAK_GLASS_APPROVAL_REQUIRED).contains("break-glass"));
        check!(code_name(codes::OPERATOR_SIGNATURE_INVALID).contains("signature"));
        check!(code_name(codes::FREEZE_LIMIT_EXCEEDED).contains("freeze.max_entries"));
        check!(code_name(codes::NOT_CONTROLLER) == "error 41");
    }

    /// The scope a parsed `freeze set` names.
    fn freeze_scope(cli: Cli) -> Result<Scope, Failure> {
        let Command::Freeze {
            command: FreezeCommand::Set { scope, .. },
        } = cli.command
        else {
            panic!("expected a freeze set");
        };
        scope.resolve()
    }

    fn scope(name: &str, pattern_type: i8) -> Scope {
        Scope {
            name: name.to_owned(),
            pattern_type,
        }
    }
}
