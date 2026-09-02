//! The command line `krabka format` accepts, and the scalar value parsers that
//! belong to no larger concern.
//!
//! [`FormatArgs`] is the one description of that command line: the binary and
//! [`crate::run_from_args`] both parse into it, so a caller of either sees the
//! same flags. The structured values behind `--feature`, `--add-scram`, and
//! `--add-acl` parse in the module that owns each of those concerns, and this
//! module names their parsers in the `value_parser` attributes.

use std::path::PathBuf;

use clap::Args;
use krabka_metadata::AclEntry;
use krabka_security::SaslMechanism;
use uuid::Uuid;

use super::{acl::parse_acl_spec, features::parse_feature_spec, scram::parse_scram_spec};
use crate::ids::DirectoryId;

#[derive(Args, Debug)]
pub struct FormatArgs {
    /// Directory to format. Must be empty or non-existent.
    #[arg(long)]
    pub(super) log_dir: PathBuf,
    /// Cluster id. Generated if not provided.
    #[arg(long)]
    pub(super) cluster_id: Option<Uuid>,
    /// Bootstrap `metadata.version` (KIP-778), e.g. `4.0` or `4.0-IV3`.
    /// Defaults to the broker's maximum supported level when omitted.
    #[arg(long)]
    pub(super) release_version: Option<String>,
    /// Set an individual feature's finalized level at format time (KIP-1022),
    /// e.g. `--feature transaction.version=2`. May be repeated. Combines with
    /// `--release-version` (which sets the base release) for every feature
    /// except `metadata.version`, where the two conflict.
    #[arg(long = "feature", value_parser = parse_feature_spec)]
    pub(super) feature: Vec<(String, i16)>,
    /// Seed a SCRAM credential. May be repeated.
    /// Format: `SCRAM-SHA-256=[name=<u>,password=<p>,iterations=<n>]`
    /// or `SCRAM-SHA-512=[name=<u>,password=<p>,iterations=<n>]`
    /// (iterations defaults to 4096 when omitted)
    #[arg(long, value_parser = parse_scram_spec)]
    pub(super) add_scram: Vec<ScramSpec>,
    /// Seed an ACL entry. May be repeated.
    /// Format: `principal=User:<name>,host=<ip|*>,operation=<Op>,permission=<Allow|Deny>,resource=<Type>:<Name>[:<Pattern>]`
    /// Pattern defaults to `Literal`.
    #[arg(long, value_parser = parse_acl_spec)]
    pub(super) add_acl: Vec<AclEntry>,
    /// This node's raft id. Required with `--standalone` and
    /// `--initial-controllers` so the local directory id can be persisted.
    #[arg(long, value_parser = parse_node_id)]
    pub(super) node_id: Option<krabka_metadata::NodeId>,
    /// Stable directory identity. Intended for orchestrators that must verify
    /// the exact node incarnation before declaring it ready.
    #[arg(long, value_parser = parse_directory_id)]
    pub(super) directory_id: Option<DirectoryId>,
    /// Format this node as the sole initial controller voter.
    #[arg(
        long,
        conflicts_with_all = ["initial_controllers", "no_initial_controllers"]
    )]
    pub(super) standalone: bool,
    /// Explicit initial controllers: `id@host:port:directory-id`, comma-separated.
    #[arg(
        long,
        value_delimiter = ',',
        conflicts_with_all = ["standalone", "no_initial_controllers"]
    )]
    pub(super) initial_controllers: Vec<String>,
    /// Format a dynamic controller that will join an existing quorum.
    #[arg(
        long,
        conflicts_with_all = ["standalone", "initial_controllers"]
    )]
    pub(super) no_initial_controllers: bool,
    /// This node's controller listener (`host:port`) — written into the
    /// `VotersRecord` when `--standalone`.
    #[arg(long)]
    pub(super) controller_listener: Option<String>,
    /// Exit 0 without touching an already-formatted directory, instead of
    /// refusing it. Matches Kafka's `kafka-storage.sh format
    /// --ignore-formatted`.
    ///
    /// This is what makes the formatter safe to run unconditionally, which a
    /// Kubernetes init container has to: the image carries no shell, so there
    /// is nothing to test the directory with before the call, and a pod that
    /// restarts against its existing volume would otherwise fail every
    /// restart after the first.
    #[arg(long)]
    pub(super) ignore_formatted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramSpec {
    pub(super) mechanism: SaslMechanism,
    pub(super) name: String,
    pub(super) password: String,
    pub(super) iterations: u32,
}

/// Parse a node id: a bare `u64` wrapped in the `NodeId` newtype.
fn parse_node_id(s: &str) -> Result<krabka_metadata::NodeId, String> {
    let id: u64 = s.trim().parse().map_err(|e| format!("node id: {e}"))?;
    Ok(krabka_metadata::NodeId(id))
}

fn parse_directory_id(s: &str) -> Result<DirectoryId, String> {
    Uuid::parse_str(s)
        .map(DirectoryId)
        .map_err(|error| format!("directory id: {error}"))
}

#[cfg(test)]
mod tests {

    use assert2::check;

    use super::*;

    /// A node id is a bare `u64`, and everything else is an error rather than
    /// a silent zero.
    #[test]
    fn parse_node_id_takes_an_integer_and_nothing_else() {
        check!(parse_node_id("7").map(|n| n.0) == Ok(7));
        check!(
            parse_node_id("  7  ").map(|n| n.0) == Ok(7),
            "surrounding space is trimmed"
        );
        for bad in ["", "-1", "1.0", "one", "0x7"] {
            check!(parse_node_id(bad).is_err(), "{bad:?} should not parse");
        }
    }
}
