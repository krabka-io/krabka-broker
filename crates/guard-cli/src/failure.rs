//! A step that stopped before a broker's answer decided the outcome.
//!
//! The three kinds are the three things an operator does next: a request that
//! never completed says nothing about what the cluster did, a refusal says the
//! cluster declined, and a signature failure sends the operator to their key
//! material. Each one carries its own exit code so a runbook can tell them
//! apart.

use super::{EXIT_BAD_SIGNATURE, EXIT_REFUSED, EXIT_UNREACHABLE};

/// A step that stopped before a broker's answer decided the outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Failure {
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
    pub(super) fn exit_code(&self) -> i32 {
        match self {
            Failure::Transport(_) => EXIT_UNREACHABLE,
            Failure::Refused(_) => EXIT_REFUSED,
            Failure::Signature(_) => EXIT_BAD_SIGNATURE,
        }
    }

    /// The line this failure prints on stderr.
    pub(super) fn message(&self) -> &str {
        match self {
            Failure::Transport(message)
            | Failure::Refused(message)
            | Failure::Signature(message) => message,
        }
    }
}
