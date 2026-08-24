//! Administers krabka barrier groups.
//!
//! A barrier group is a named set of topics. A coordinator injects an
//! epoch-stamped marker into every partition of the set and publishes the
//! resulting offsets as a cut, which is an exact and reproducible point in
//! every one of those topics at once. This is the operator's side of that:
//! define a group, trigger a cut, read the cuts back, and prove that the
//! marker a cut names is really in the log.
//!
//! This is the `crabka barrier` command from the monorepo's `crabka-cli`. That
//! crate also drives the gres layer, which is why it could not follow the
//! broker into this repository. It is a library as well as a binary so tests
//! call [`run_from_args`] in process: a test that spawns the binary needs a
//! Cargo working tree to build it from, and a Bazel test sandbox has none.
//!
//! Every subcommand speaks one krabka-private api key, in the 1010 to 1014
//! range. A JVM `AdminClient` cannot send those, which is why the cuts are also
//! published to `__barrier_state` where any consumer can read them.

use clap::{Parser, Subcommand};
use crabka_units::{Time, convert::TimeExt};

mod verify;

pub use verify::{Mismatch, VerifyOutcome};

/// The exit code for a request the broker refused.
const EXIT_REFUSED: i32 = 1;
/// The exit code for a transport failure, where nothing is known about the
/// request's outcome.
const EXIT_UNREACHABLE: i32 = 2;
/// The exit code for a cut whose log does not match what it claims.
const EXIT_MISMATCH: i32 = 3;

/// The tool's command line.
///
/// Shared by the binary and by [`run_from_args`], so both accept exactly the
/// same flags.
#[derive(Parser)]
#[command(
    name = "crabka-barrier",
    version,
    about = "Define barrier groups, trigger cuts, and verify a cut against the log"
)]
pub struct Cli {
    /// One or more `host:port` pairs to bootstrap against.
    #[arg(long, short = 'b', env = "CRABKA_BOOTSTRAP_SERVER", required = true)]
    pub bootstrap_server: String,

    /// What to do.
    #[command(subcommand)]
    pub command: Command,
}

/// The subcommands, one per barrier api.
#[derive(Subcommand)]
pub enum Command {
    /// Create a barrier group, or update one that exists.
    Define {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// A topic the group cuts across. Repeat for each one.
        #[arg(long = "topic", required = true)]
        topics: Vec<String>,
        /// How often the coordinator injects a cut on its own. Takes any
        /// time unit, so `500ms`, `30s`, `5m` and `1h` all work. A number
        /// with no unit is refused. Omit for on-demand only.
        #[arg(long, value_parser = parse_time)]
        interval: Option<Time>,
        /// How many cuts the group keeps before the oldest is tombstoned.
        #[arg(long, default_value_t = 100)]
        retained_cuts: i32,
    },
    /// Delete a barrier group and its cuts.
    Delete {
        /// The group's name.
        #[arg(long)]
        group: String,
    },
    /// Print a group's definition and its latest epoch.
    Describe {
        /// A group to describe. Repeat for each one, or omit for every group.
        #[arg(long = "group")]
        groups: Vec<String>,
    },
    /// Inject a cut now and print the offsets it took.
    Trigger {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// How long the coordinator retries a partition that has no marker
        /// yet. Takes any time unit, the same as `--interval`. The broker
        /// clamps this to its own configured ceiling. Omit to take the
        /// broker's default.
        #[arg(long, value_parser = parse_time)]
        timeout: Option<Time>,
    },
    /// Print a group's retained cuts.
    List {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// Skip every cut below this epoch.
        #[arg(long, default_value_t = 0)]
        from_epoch: i64,
        /// How many cuts to print. `-1` prints every retained cut.
        #[arg(long, default_value_t = -1)]
        max_results: i32,
    },
    /// Read the log at a cut's offsets and prove each one holds that cut's
    /// marker.
    ///
    /// A cut is only worth as much as the markers behind it. This fetches the
    /// batch at every offset the cut names and checks it is a barrier control
    /// batch carrying this group and this epoch.
    Verify {
        /// The group's name.
        #[arg(long)]
        group: String,
        /// The epoch to verify.
        #[arg(long)]
        epoch: i64,
    },
}

/// Parse a time argument.
///
/// Delegates to `crabka_units`, so every unit the broker's own configuration
/// accepts works here too: `ns`, `us`, `ms`, `s`, `m`, `h`, `d` and their long
/// forms.
///
/// A number with no unit is refused, and only `0` is exempt. That is the units
/// crate's rule and it is the right one here: `--timeout 30` from someone who
/// meant milliseconds would otherwise wait thirty seconds without complaining.
fn parse_time(raw: &str) -> Result<Time, String> {
    crabka_units::parse::time(raw).map_err(|e| e.to_string())
}

/// A time as the whole milliseconds the wire carries.
///
/// The barrier apis spell every duration as milliseconds, so this is where a
/// typed `Time` stops being one.
fn as_millis_i64(time: Time) -> i64 {
    time.millis_i64()
}

/// Run the tool from an argv-style iterator, returning its exit code.
///
/// `0` means the broker accepted the request. `1` means it refused one, and the
/// reason is on stderr. `2` means the broker could not be reached, so nothing
/// is known about the outcome. `3` means a cut does not match the log.
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
    let client = match crabka_client_core::Client::builder()
        .bootstrap(&cli.bootstrap_server)
        .client_id("crabka-barrier")
        .build()
        .await
    {
        Ok(client) => client,
        Err(error) => {
            eprintln!("cannot reach {}: {error}", cli.bootstrap_server);
            return EXIT_UNREACHABLE;
        }
    };
    dispatch(&client, cli.command).await
}

/// Send one command's request and print its response.
async fn dispatch(client: &crabka_client_core::Client, command: Command) -> i32 {
    use crabka_protocol::krabka::barrier as api;

    match command {
        Command::Define {
            group,
            topics,
            interval,
            retained_cuts,
        } => {
            let request = api::AlterBarrierGroupsRequest {
                groups: vec![api::AlterableBarrierGroup {
                    group,
                    topics,
                    // -1 is the sentinel for "no periodic injection".
                    interval_ms: interval.map_or(-1, as_millis_i64),
                    retained_cuts,
                    delete: false,
                    ..api::AlterableBarrierGroup::default()
                }],
                ..api::AlterBarrierGroupsRequest::default()
            };
            match client.send(request).await {
                Ok(response) => report_alter(&response),
                Err(error) => unreachable_broker(&error),
            }
        }
        Command::Delete { group } => {
            let request = api::AlterBarrierGroupsRequest {
                groups: vec![api::AlterableBarrierGroup {
                    group,
                    delete: true,
                    ..api::AlterableBarrierGroup::default()
                }],
                ..api::AlterBarrierGroupsRequest::default()
            };
            match client.send(request).await {
                Ok(response) => report_alter(&response),
                Err(error) => unreachable_broker(&error),
            }
        }
        Command::Describe { groups } => {
            let request = api::DescribeBarrierGroupsRequest {
                groups,
                ..api::DescribeBarrierGroupsRequest::default()
            };
            match client.send(request).await {
                Ok(response) => report_describe(&response),
                Err(error) => unreachable_broker(&error),
            }
        }
        Command::Trigger { group, timeout } => {
            let request = api::TriggerBarrierRequest {
                group,
                // A non-positive timeout asks the broker for its own default.
                timeout_ms: timeout
                    .map(as_millis_i64)
                    .and_then(|ms| i32::try_from(ms).ok())
                    .unwrap_or(0),
                ..api::TriggerBarrierRequest::default()
            };
            match client.send(request).await {
                Ok(response) => report_trigger(&response),
                Err(error) => unreachable_broker(&error),
            }
        }
        Command::List {
            group,
            from_epoch,
            max_results,
        } => {
            let request = api::ListBarrierCutsRequest {
                group,
                from_epoch,
                max_results,
                ..api::ListBarrierCutsRequest::default()
            };
            match client.send(request).await {
                Ok(response) => report_list(&response),
                Err(error) => unreachable_broker(&error),
            }
        }
        Command::Verify { group, epoch } => match verify::verify(client, &group, epoch).await {
            Ok(outcome) => report_verify(&outcome),
            Err(error) => {
                eprintln!("{error}");
                EXIT_REFUSED
            }
        },
    }
}

/// Print a transport failure, where the request's outcome is unknown.
fn unreachable_broker(error: &crabka_client_core::ClientError) -> i32 {
    eprintln!("the request did not complete, so its outcome is unknown: {error}");
    EXIT_UNREACHABLE
}

/// Describe an error code.
///
/// `crabka-broker` has no code-to-name table, so this prints the number. The
/// one code an operator of this tool meets that a Kafka reference will not
/// list is the krabka-private one, so that gets a word.
fn code_name(code: i16) -> String {
    if code == crabka_broker::codes::BARRIER_INJECTION_IN_PROGRESS {
        return format!("error {code} (an injection is already in flight, retry)");
    }
    format!("error {code}")
}

/// One line per altered group.
fn report_alter(response: &crabka_protocol::krabka::barrier::AlterBarrierGroupsResponse) -> i32 {
    let mut exit = 0;
    for result in &response.results {
        if result.error_code == 0 {
            println!("{}\tok", result.group);
        } else {
            eprintln!(
                "{}\t{}{}",
                result.group,
                code_name(result.error_code),
                result
                    .error_message
                    .as_ref()
                    .map_or_else(String::new, |m| format!(": {m}"))
            );
            exit = EXIT_REFUSED;
        }
    }
    exit
}

/// One block per described group.
fn report_describe(
    response: &crabka_protocol::krabka::barrier::DescribeBarrierGroupsResponse,
) -> i32 {
    let mut exit = 0;
    for group in &response.groups {
        if group.error_code != 0 {
            eprintln!("{}\t{}", group.group, code_name(group.error_code));
            exit = EXIT_REFUSED;
            continue;
        }
        println!("group           {}", group.group);
        println!("topics          {}", group.topics.join(", "));
        println!(
            "interval        {}",
            if group.interval_ms < 0 {
                "on demand only".to_owned()
            } else {
                format!("{}ms", group.interval_ms)
            }
        );
        println!("retained cuts   {}", group.retained_cuts);
        // The broker spells "never injected" as 0 and allocates 1 for the
        // first cut, while the wire field's own default is -1. Either way no
        // real epoch is at or below zero, so both read as none.
        println!(
            "last epoch      {}",
            if group.last_epoch <= 0 {
                "none yet".to_owned()
            } else {
                group.last_epoch.to_string()
            }
        );
        println!("coordinator     {}", group.coordinator_id);
        println!();
    }
    exit
}

/// The epoch a trigger produced, and the offsets it took.
fn report_trigger(response: &crabka_protocol::krabka::barrier::TriggerBarrierResponse) -> i32 {
    if response.error_code != 0 {
        eprintln!(
            "{}{}",
            code_name(response.error_code),
            response
                .error_message
                .as_ref()
                .map_or_else(String::new, |m| format!(": {m}"))
        );
        return EXIT_REFUSED;
    }
    println!("epoch  {}", response.epoch);
    println!("status {}", status_name(response.status));
    for topic in &response.topics {
        for partition in &topic.partitions {
            println!(
                "{}\t{}\t{}",
                topic.topic, partition.partition, partition.offset
            );
        }
    }
    // A partial cut is a published outcome, not a failure: the epoch is
    // consumed and the markers that did land cannot be withdrawn. Naming the
    // gaps is what lets a reader skip the epoch deterministically.
    for missing in &response.missing {
        eprintln!("no marker: {}\t{}", missing.topic, missing.partition);
    }
    0
}

/// One line per partition of each retained cut.
fn report_list(response: &crabka_protocol::krabka::barrier::ListBarrierCutsResponse) -> i32 {
    if response.error_code != 0 {
        eprintln!(
            "{}{}",
            code_name(response.error_code),
            response
                .error_message
                .as_ref()
                .map_or_else(String::new, |m| format!(": {m}"))
        );
        return EXIT_REFUSED;
    }
    for cut in &response.cuts {
        println!(
            "epoch {} {} triggered {} completed {}",
            cut.epoch,
            status_name(cut.status),
            cut.triggered_at,
            cut.completed_at
        );
        for topic in &cut.topics {
            for partition in &topic.partitions {
                println!(
                    "  {}\t{}\t{}",
                    topic.topic, partition.partition, partition.offset
                );
            }
        }
        for missing in &cut.missing {
            println!("  {}\t{}\tno marker", missing.topic, missing.partition);
        }
    }
    0
}

/// What a verify found.
fn report_verify(outcome: &VerifyOutcome) -> i32 {
    for checked in &outcome.checked {
        println!("{}\t{}\t{}\tok", checked.0, checked.1, checked.2);
    }
    if outcome.mismatches.is_empty() {
        println!(
            "cut {} verified: {} markers are in the log",
            outcome.epoch,
            outcome.checked.len()
        );
        return 0;
    }
    for mismatch in &outcome.mismatches {
        eprintln!(
            "{}\t{}\t{}\t{}",
            mismatch.topic, mismatch.partition, mismatch.offset, mismatch.reason
        );
    }
    eprintln!(
        "cut {} does not match the log: {} of {} offsets are wrong",
        outcome.epoch,
        outcome.mismatches.len(),
        outcome.checked.len() + outcome.mismatches.len()
    );
    EXIT_MISMATCH
}

/// The name of a cut status code.
fn status_name(status: i8) -> &'static str {
    if status == crabka_protocol::krabka::barrier::CUT_STATUS_COMPLETE {
        "complete"
    } else {
        "partial"
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// A time argument takes any unit the broker's own configuration takes,
    /// so an operator never has to convert to milliseconds by hand, and a
    /// number with no unit is refused rather than guessed at.
    #[test]
    fn a_time_argument_takes_any_unit() {
        let cases = [
            ("500ms", Some(500)),
            ("30s", Some(30_000)),
            ("5m", Some(300_000)),
            ("1h", Some(3_600_000)),
            ("2d", Some(172_800_000)),
            ("250us", Some(0)),
            ("1 minute", Some(60_000)),
            // A unit is required, so a number alone cannot be read as the
            // wrong scale. Zero is the one exemption, having no scale.
            ("90", None),
            ("0", Some(0)),
            ("banana", None),
            ("", None),
        ];
        for (raw, expected) in cases {
            check!(parse_time(raw).ok().map(as_millis_i64) == expected, "{raw}");
        }
    }

    /// `--bootstrap-server` is the one flag every subcommand needs, so the
    /// parser refuses a command line without it rather than defaulting to a
    /// guess about where the cluster is.
    #[test]
    fn a_command_line_without_a_bootstrap_server_is_refused() {
        assert!(Cli::try_parse_from(["crabka-barrier", "describe"]).is_err());
        assert!(
            Cli::try_parse_from(["crabka-barrier", "-b", "localhost:9092", "describe"]).is_ok()
        );
    }

    /// A group with no periodic injection is the default, and it reaches the
    /// wire as the -1 the broker reads as "on demand only".
    #[test]
    fn define_without_an_interval_asks_for_on_demand_only() {
        let cli = Cli::try_parse_from([
            "crabka-barrier",
            "-b",
            "localhost:9092",
            "define",
            "--group",
            "g",
            "--topic",
            "orders",
        ])
        .expect("parses");
        let Command::Define {
            interval,
            retained_cuts,
            topics,
            ..
        } = cli.command
        else {
            panic!("expected define");
        };
        check!(interval == None);
        check!(retained_cuts == 100);
        check!(topics == vec!["orders".to_owned()]);
    }

    /// `list` defaults to every retained cut, matching the request default the
    /// wire carries.
    #[test]
    fn list_defaults_to_every_retained_cut() {
        let cli = Cli::try_parse_from([
            "crabka-barrier",
            "-b",
            "localhost:9092",
            "list",
            "--group",
            "g",
        ])
        .expect("parses");
        let Command::List {
            max_results,
            from_epoch,
            ..
        } = cli.command
        else {
            panic!("expected list");
        };
        check!(max_results == -1);
        check!(from_epoch == 0);
    }

    #[test]
    fn a_status_code_reads_as_a_word() {
        check!(status_name(crabka_protocol::krabka::barrier::CUT_STATUS_COMPLETE) == "complete");
        check!(status_name(crabka_protocol::krabka::barrier::CUT_STATUS_PARTIAL) == "partial");
    }
}
