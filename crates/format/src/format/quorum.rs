//! The KIP-853 quorum decision: whether a format is dynamic, and which voters
//! its initial voter set holds.
//!
//! `--standalone`, `--initial-controllers`, and `--no-initial-controllers` are
//! one choice expressed three ways, and each answers both questions together
//! with the `kraft.version` feature. The rules that reconcile them, and the
//! `id@host:port:directory-id` parse the explicit form needs, live here rather
//! than in the run that writes their result to the checkpoint.

use std::collections::BTreeSet;

use krabka_metadata::{
    KRaftVersionRange, Voter, VoterEndpoint, VoterSet, metadata_version::KRAFT_VERSION_FEATURE,
};
use uuid::Uuid;

use super::args::FormatArgs;
use crate::ids::DirectoryId;

/// Resolve the KIP-853 format mode and validate its kraft.version selection.
///
/// The three explicit quorum flags select dynamic membership and therefore
/// imply level 1. Omitting all three retains the static level-0 path.
pub(super) fn is_dynamic_format(args: &FormatArgs) -> Result<bool, String> {
    let dynamic =
        args.standalone || !args.initial_controllers.is_empty() || args.no_initial_controllers;
    let mut requested = None;
    for (name, level) in &args.feature {
        if name != KRAFT_VERSION_FEATURE {
            continue;
        }
        if requested.replace(*level).is_some() {
            return Err("feature kraft.version specified more than once".into());
        }
        if !(0..=1).contains(level) {
            return Err(format!(
                "feature kraft.version={level} is outside the supported range 0..=1"
            ));
        }
    }

    match (dynamic, requested) {
        (true, None | Some(1)) => Ok(true),
        (true, Some(0)) => Err(
            "--standalone, --initial-controllers, and --no-initial-controllers require kraft.version=1"
                .into(),
        ),
        (false, None | Some(0)) => Ok(false),
        (false, Some(1)) => Err(
            "kraft.version=1 requires --standalone, --initial-controllers, or --no-initial-controllers"
                .into(),
        ),
        _ => unreachable!("kraft.version range was validated above"),
    }
}

/// Parse one `--initial-controllers` entry: `id@host:port:directory-id`.
///
/// The directory uuid is the trailing colon-delimited field, so we split
/// it off the right first, then peel `host:port` off the remainder.
fn parse_initial_controller(spec: &str) -> Result<Voter, String> {
    let (id_part, rest) = spec.split_once('@').ok_or("missing '@'")?;
    let id = krabka_metadata::NodeId(id_part.parse::<u64>().map_err(|_| "bad id")?);
    let (host_port, dir_part) = rest.rsplit_once(':').ok_or("missing directory uuid")?;
    let dir: Uuid = dir_part.parse().map_err(|_| "bad directory uuid")?;
    if dir.is_nil() {
        return Err("directory uuid must not be nil".into());
    }
    let (host, port) = host_port.rsplit_once(':').ok_or("missing host:port")?;
    if host.is_empty() {
        return Err("host must not be empty".into());
    }
    let port: u16 = port.parse().map_err(|_| "bad port")?;
    if port == 0 {
        return Err("port must not be zero".into());
    }
    Ok(Voter {
        id,
        directory_id: dir,
        endpoints: vec![VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.to_string(),
            port,
        }],
        kraft_version: KRaftVersionRange::default(),
    })
}

/// Derive the initial controller voter set from the format args.
///
/// - `--standalone`: a singleton set holding just this node (requires
///   `--node-id` + `--controller-listener`).
/// - `--initial-controllers`: the explicitly-listed voters.
/// - `--no-initial-controllers` or static mode: an empty set.
pub(super) fn build_initial_voters(
    args: &FormatArgs,
    directory_id: DirectoryId,
) -> Result<VoterSet, String> {
    if args.standalone {
        let id = args.node_id.ok_or("--standalone requires --node-id")?;
        let listener = args
            .controller_listener
            .as_deref()
            .ok_or("--standalone requires --controller-listener")?;
        let (host, port) = listener
            .rsplit_once(':')
            .ok_or("--controller-listener must be host:port")?;
        if host.is_empty() {
            return Err("--controller-listener host must not be empty".into());
        }
        let port: u16 = port.parse().map_err(|_| "bad --controller-listener port")?;
        if port == 0 {
            return Err("--controller-listener port must not be zero".into());
        }
        Ok(VoterSet::from_voters([Voter {
            id,
            // `Voter.directory_id` is a raw `Uuid` (owned by `krabka_voters`);
            // unwrap the newtype at this crate boundary.
            directory_id: directory_id.into(),
            endpoints: vec![VoterEndpoint {
                name: "CONTROLLER".into(),
                host: host.to_string(),
                port,
            }],
            kraft_version: KRaftVersionRange::default(),
        }]))
    } else if !args.initial_controllers.is_empty() {
        let voters: Vec<_> = args
            .initial_controllers
            .iter()
            .map(|s| parse_initial_controller(s))
            .collect::<Result<_, _>>()?;
        let mut node_ids = BTreeSet::new();
        let mut directory_ids = BTreeSet::new();
        for voter in &voters {
            if !node_ids.insert(voter.id) {
                return Err(format!("duplicate initial controller id {}", voter.id));
            }
            if !directory_ids.insert(voter.directory_id) {
                return Err(format!(
                    "duplicate initial controller directory id {}",
                    voter.directory_id
                ));
            }
        }
        let voters = VoterSet::from_voters(voters);
        let node_id = args
            .node_id
            .ok_or("--initial-controllers requires --node-id")?;
        if !voters.contains(node_id) {
            return Err(format!(
                "--initial-controllers does not contain local --node-id {node_id}"
            ));
        }
        Ok(voters)
    } else {
        Ok(VoterSet::default())
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn parses_initial_controller_spec() {
        let v =
            parse_initial_controller("3@host:9093:00000000-0000-0000-0000-000000000003").unwrap();
        assert2::assert!(
            v == Voter {
                id: krabka_metadata::NodeId(3),
                directory_id: Uuid::from_u128(3),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_string(),
                    host: "host".to_string(),
                    port: 9093,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }
        );
    }

    #[test]
    fn rejects_initial_controller_without_at() {
        assert2::assert!(parse_initial_controller("3:host:9093:uuid").is_err());
    }

    #[test]
    fn rejects_initial_controller_bad_uuid() {
        assert2::assert!(parse_initial_controller("3@host:9093:not-a-uuid").is_err());
    }

    #[test]
    fn parse_initial_controller_error_branches() {
        for bad in [
            "notanum@host:9093:00000000-0000-0000-0000-000000000003", // bad id
            "3@host9093",                                             // missing directory uuid
            "3@host:notaport:00000000-0000-0000-0000-000000000003",   // bad port
            "3@hostonly:00000000-0000-0000-0000-000000000003",        // missing host:port
        ] {
            assert2::assert!(parse_initial_controller(bad).is_err());
        }
    }
}
