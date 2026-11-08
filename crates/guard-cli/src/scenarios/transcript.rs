//! What one run of the tool wrote, and the list lines read back out of it.
//!
//! The driver hands the tool two buffers in place of stdout and stderr, so a
//! scenario reads exactly the lines an operator would. The parsers here are
//! the one place that knows the shape of those lines.

use super::params::Outcome;

/// What one run of the tool reported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    /// The outcome its exit code reports.
    pub outcome: Outcome,
    /// The lines it wrote.
    pub transcript: Transcript,
}

impl Run {
    /// A run that ended with `code` after writing `stdout` and `stderr`.
    pub fn new(code: i32, stdout: &[u8], stderr: &[u8]) -> Self {
        Self {
            outcome: Outcome::from_exit_code(code),
            transcript: Transcript {
                stdout: String::from_utf8_lossy(stdout).into_owned(),
                stderr: String::from_utf8_lossy(stderr).into_owned(),
            },
        }
    }

    /// A command line the parser refused, which writes nothing a scenario
    /// reads.
    pub fn command_line_refused() -> Self {
        Self {
            outcome: Outcome::CommandLineRefused,
            transcript: Transcript::default(),
        }
    }
}

/// The two streams one run wrote.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Transcript {
    /// What the tool wrote to stdout: the broker's answer.
    pub stdout: String,
    /// What the tool wrote to stderr: why it refused, or why it stopped.
    pub stderr: String,
}

/// One registry entry as a freeze listing writes it.
///
/// The time the entry was set is left out, because a scenario cannot know it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedFreeze {
    /// The pattern type and the scope, as `literal:payments`.
    pub scope: String,
    /// The principal that set the entry.
    pub set_by: String,
    /// The reason the entry gives.
    pub reason: String,
    /// What a verified listing says proves the entry. `None` when the listing
    /// was not verified.
    pub proof: Option<String>,
}

/// One proposal as a proposal listing writes it, reduced to what the
/// scenarios check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedProposal {
    /// The proposal's id.
    pub id: uuid::Uuid,
    /// `open`, `withdrawn` or `consumed`.
    pub state: String,
    /// The approvals it holds, in the order they were written.
    pub approvals: Vec<ListedApproval>,
}

/// One approval line of a listed proposal, without the time it was made.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedApproval {
    /// The principal that approved.
    pub principal: String,
    /// What the approval offers as evidence of who made it.
    pub evidence: String,
}

impl Transcript {
    /// The stdout lines, in order.
    pub fn stdout_lines(&self) -> Vec<&str> { self.stdout.lines().collect() }

    /// Every registry entry stdout lists, in order.
    ///
    /// # Panics
    ///
    /// Panics on an entry line that does not have the shape the tool writes,
    /// which is a defect in the report rather than an outcome.
    pub fn listed_freezes(&self) -> Vec<ListedFreeze> {
        self.stdout
            .lines()
            .filter(|line| line.contains("\tset by "))
            .map(|line| {
                let mut fields = line.split('\t');
                let scope = fields.next().unwrap_or_default().to_owned();
                let (set_by, _set_at) = fields
                    .next()
                    .and_then(|set| set.strip_prefix("set by "))
                    .and_then(|set| set.rsplit_once(" at "))
                    .unwrap_or_else(|| panic!("{line:?} names no author"));
                let set_by = set_by.to_owned();
                let reason = fields.next().unwrap_or_default().to_owned();
                let proof = fields.next().map(str::to_owned);
                ListedFreeze {
                    scope,
                    set_by,
                    reason,
                    proof,
                }
            })
            .collect()
    }

    /// Every proposal stdout lists, in order.
    ///
    /// # Panics
    ///
    /// Panics on a proposal or approval line that does not have the shape the
    /// tool writes.
    pub fn listed_proposals(&self) -> Vec<ListedProposal> {
        let mut proposals: Vec<ListedProposal> = Vec::new();
        for line in self.stdout.lines() {
            if let Some(header) = line.strip_prefix("proposal ") {
                let (id, state) = header
                    .split_once(' ')
                    .unwrap_or_else(|| panic!("{line:?} names no state"));
                proposals.push(ListedProposal {
                    id: id.parse().expect("a proposal line starts with its id"),
                    state: state.to_owned(),
                    approvals: Vec::new(),
                });
            } else if let Some(approval) = line.strip_prefix("  approved   ") {
                let (principal, rest) = approval
                    .split_once(" at ")
                    .unwrap_or_else(|| panic!("{line:?} names no approval time"));
                let (_approved_at, evidence) = rest
                    .split_once(' ')
                    .unwrap_or_else(|| panic!("{line:?} names no evidence"));
                proposals
                    .last_mut()
                    .expect("an approval line follows its proposal")
                    .approvals
                    .push(ListedApproval {
                        principal: principal.to_owned(),
                        evidence: evidence.to_owned(),
                    });
            }
        }
        proposals
    }
}
