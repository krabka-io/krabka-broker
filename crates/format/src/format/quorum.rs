//! The KIP-853 quorum decision: whether a format is dynamic, and which voters
//! its initial voter set holds.
//!
//! `--standalone`, `--initial-controllers`, and `--no-initial-controllers` are
//! one choice expressed three ways, and each answers both questions together
//! with the `kraft.version` feature. The rules that reconcile them, and the
//! `id@host:port:directory-id` parse the explicit form needs, live here rather
//! than in the run that writes their result to the checkpoint.

use {
    super::args::FormatArgs,
    crate::ids::DirectoryId,
    krabka_metadata::{
        FeatureLevelRecord, KRaftVersionRange, Voter, VoterEndpoint, VoterSet,
        metadata_version::KRAFT_VERSION_FEATURE,
    },
    std::collections::BTreeSet,
    uuid::Uuid,
};

/// Resolve the KIP-853 format mode and validate its kraft.version selection.
///
/// The three explicit quorum flags select dynamic membership and therefore
/// imply level 1. Omitting all three retains the static level-0 path.
pub(super) fn is_dynamic_format(args: &FormatArgs) -> Result<bool, String> {
    let dynamic =
        args.standalone || !args.initial_controllers.is_empty() || args.no_initial_controllers;
    let mut requested = None;
    for FeatureLevelRecord { name, level } in &args.feature {
        if name != KRAFT_VERSION_FEATURE {
            continue;
        }
        if requested.replace(*level).is_some() {
            return Err("feature kraft.version specified more than once".into());
        }
    }

    // As in `Formatter.effectiveKRaftFeatureLevel`, the level is reconciled
    // with the quorum flags before `Feature.fromFeatureLevel` checks that the
    // feature defines it, so a level above 1 without a quorum flag is a mode
    // conflict and not a `No feature` error.
    match (dynamic, requested) {
        (true, Some(0)) => Err(
            "--standalone, --initial-controllers, and --no-initial-controllers require kraft.version=1"
                .into(),
        ),
        (false, Some(level)) if level != 0 => Err(format!(
            "kraft.version={level} requires --standalone, --initial-controllers, or --no-initial-controllers"
        )),
        (_, Some(level)) if !(0..=1).contains(&level) => Err(format!(
            "No feature:kraft.version with feature level {level}"
        )),
        (dynamic, _) => Ok(dynamic),
    }
}

/// Parse one `--initial-controllers` entry: `id@host:port:directory-id`,
/// whose endpoint is named `listener_name`.
///
/// The directory uuid is the trailing colon-delimited field, so we split
/// it off the right first, then peel `host:port` off the remainder. It is in
/// Kafka's base64 form, as `kafka-storage format` takes it, or the hyphenated
/// form.
fn parse_initial_controller(spec: &str, listener_name: &str) -> Result<Voter, String> {
    let (id_part, rest) = spec.split_once('@').ok_or("missing '@'")?;
    let id = krabka_metadata::NodeId(id_part.parse::<u64>().map_err(|_| "bad id")?);
    let (host_port, dir_part) = rest.rsplit_once(':').ok_or("missing directory uuid")?;
    let dir: Uuid = DirectoryId::parse_cli(dir_part)
        .map_err(|_| "bad directory uuid")?
        .into();
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
            name: listener_name.to_owned(),
            host: host.to_owned(),
            port,
        }],
        kraft_version: KRaftVersionRange::default(),
    })
}

/// Derive the initial controller voter set from the format args.
///
/// - `--standalone`: a singleton set holding just this node (requires
///   `--controller-listener`).
/// - `--initial-controllers`: the explicitly-listed voters, which must
///   include this node.
/// - `--no-initial-controllers` or static mode: an empty set.
pub(super) fn build_initial_voters(
    args: &FormatArgs,
    directory_id: DirectoryId,
) -> Result<VoterSet, String> {
    if args.standalone {
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
            id: args.node_id,
            // `Voter.directory_id` is a raw `Uuid` (owned by `krabka_voters`);
            // unwrap the newtype at this crate boundary.
            directory_id: directory_id.into(),
            endpoints: vec![VoterEndpoint {
                name: args.controller_listener_name.clone(),
                host: host.to_owned(),
                port,
            }],
            kraft_version: KRaftVersionRange::default(),
        }]))
    } else if !args.initial_controllers.is_empty() {
        let voters: Vec<_> = args
            .initial_controllers
            .iter()
            .map(|s| parse_initial_controller(s, &args.controller_listener_name))
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
        if !voters.contains(args.node_id) {
            return Err(format!(
                "--initial-controllers does not contain local --node-id {}",
                args.node_id
            ));
        }
        Ok(voters)
    } else {
        Ok(VoterSet::default())
    }
}

#[cfg(test)]
mod tests {

    use {super::*, crate::Cli};

    #[test]
    fn parses_initial_controller_spec() {
        let v = parse_initial_controller(
            "3@host:9093:00000000-0000-0000-0000-000000000003",
            "CONTROLLER",
        )
        .unwrap();
        assert2::assert!(
            v == Voter {
                id: krabka_metadata::NodeId(3),
                directory_id: Uuid::from_u128(3),
                endpoints: vec![VoterEndpoint {
                    name: "CONTROLLER".to_owned(),
                    host: "host".to_owned(),
                    port: 9093,
                }],
                kraft_version: KRaftVersionRange { min: 0, max: 1 },
            }
        );
    }

    struct QuorumFormatScenario {
        arguments: Vec<String>,
        controller: String,
        parsed_controller: Option<Result<Voter, String>>,
        dynamic: Option<Result<bool, String>>,
        voters: Option<Result<VoterSet, String>>,
    }
    #[rstest::fixture]
    fn quorum_format() -> QuorumFormatScenario {
        let arguments = ["krabka-format", "--log-dir", "/data", "--node-id", "3"]
            .map(str::to_owned)
            .to_vec();
        QuorumFormatScenario {
            arguments,
            controller: String::new(),
            parsed_controller: None,
            dynamic: None,
            voters: None,
        }
    }
    #[rstest_bdd_macros::given("initial controller specification {specification}")]
    fn initial_controller_spec(quorum_format: &mut QuorumFormatScenario, specification: String) {
        quorum_format.controller = specification;
    }
    #[rstest_bdd_macros::when("the initial controller specification is parsed")]
    fn parse_quorum_controller(quorum_format: &mut QuorumFormatScenario) {
        quorum_format.parsed_controller = Some(parse_initial_controller(
            &quorum_format.controller,
            "CONTROLLER",
        ));
    }
    #[rstest_bdd_macros::then("the controller directory ID is UUID 3")]
    fn controller_directory_id(quorum_format: &QuorumFormatScenario) {
        let voter = quorum_format
            .parsed_controller
            .as_ref()
            .expect("controller parsed")
            .as_ref()
            .expect("Kafka directory ID accepted");
        assert2::assert!(voter.directory_id == Uuid::from_u128(3));
    }

    #[rstest_bdd_macros::given("a dynamic formatter with requested kraft.version {level}")]
    fn quorum_feature_level(quorum_format: &mut QuorumFormatScenario, level: i16) {
        quorum_format.arguments.extend([
            "--no-initial-controllers".into(),
            "--feature".into(),
            format!("kraft.version={level}"),
        ]);
    }
    #[rstest_bdd_macros::when("the formatter resolves dynamic quorum mode")]
    fn resolve_dynamic_mode(quorum_format: &mut QuorumFormatScenario) {
        use clap::Parser as _;
        let cli = Cli::try_parse_from(&quorum_format.arguments).expect("formatter command line");
        quorum_format.dynamic = Some(is_dynamic_format(&cli.args));
    }
    #[rstest_bdd_macros::then(
        "the quorum mode error is No feature:kraft.version with feature level {level}"
    )]
    fn undefined_kraft_level(quorum_format: &QuorumFormatScenario, level: i16) {
        assert2::assert!(
            quorum_format.dynamic
                == Some(Err(format!(
                    "No feature:kraft.version with feature level {level}"
                )))
        );
    }

    #[rstest_bdd_macros::given("formatter quorum mode {mode}")]
    fn named_quorum_mode(quorum_format: &mut QuorumFormatScenario, mode: String) {
        match mode.as_str() {
            "standalone" => quorum_format.arguments.extend([
                "--standalone".into(),
                "--controller-listener".into(),
                "host:9093".into(),
            ]),
            "explicit" => quorum_format.arguments.extend([
                "--initial-controllers".into(),
                "3@host:9093:00000000-0000-0000-0000-000000000003".into(),
            ]),
            _ => panic!("unknown formatter quorum mode {mode}"),
        }
    }
    #[rstest_bdd_macros::given("formatter controller listener name {listener}")]
    fn quorum_listener(quorum_format: &mut QuorumFormatScenario, listener: String) {
        if listener != "omitted" {
            quorum_format
                .arguments
                .extend(["--controller-listener-name".into(), listener]);
        }
    }
    #[rstest_bdd_macros::when("the formatter builds the initial voters")]
    fn build_named_voters(quorum_format: &mut QuorumFormatScenario) {
        use clap::Parser as _;
        let cli = Cli::try_parse_from(&quorum_format.arguments).expect("formatter command line");
        quorum_format.voters = Some(build_initial_voters(
            &cli.args,
            DirectoryId(Uuid::from_u128(3)),
        ));
    }
    #[rstest_bdd_macros::then("the sole voter endpoint listener is {listener}")]
    fn named_voter_endpoint(quorum_format: &QuorumFormatScenario, listener: String) {
        let named: Vec<String> = quorum_format
            .voters
            .as_ref()
            .expect("voters built")
            .as_ref()
            .expect("valid quorum")
            .iter()
            .flat_map(|voter| voter.endpoints.iter().map(|endpoint| endpoint.name.clone()))
            .collect();
        assert2::assert!(named == vec![listener]);
    }
    rstest_bdd_macros::scenarios!("tests/features/quorum_format", fixtures = [quorum_format: QuorumFormatScenario]);

    #[test]
    fn rejects_initial_controller_without_at() {
        assert2::assert!(parse_initial_controller("3:host:9093:uuid", "CONTROLLER").is_err());
    }

    #[test]
    fn rejects_initial_controller_bad_uuid() {
        assert2::assert!(parse_initial_controller("3@host:9093:not-a-uuid", "CONTROLLER").is_err());
    }

    #[yare::parameterized(
        invalid_id = { "notanum@host:9093:00000000-0000-0000-0000-000000000003" },
        missing_port_and_id = { "3@host9093" },
        invalid_port = { "3@host:notaport:00000000-0000-0000-0000-000000000003" },
        missing_host_port = { "3@hostonly:00000000-0000-0000-0000-000000000003" },
    )]
    fn parse_initial_controller_error_branches(bad: &str) {
        assert2::assert!(parse_initial_controller(bad, "CONTROLLER").is_err());
    }
}
