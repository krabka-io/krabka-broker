//! The tool's command line: the `clap` types and the argument conversions.
//!
//! `Cli` and `Command` spell every flag each barrier subcommand takes, and
//! `parse_time` and `Time::millis_i64` are the two halves of the one conversion
//! the wire forces: an operator writes a duration in whatever unit reads best,
//! and the barrier apis carry milliseconds. Parsing is separate from sending
//! so a test can prove a command line reaches the right shape without a
//! broker.

use {
    clap::{Parser, Subcommand},
    krabka_cli_connection::ConnectionArgs,
    krabka_units::{Time, convert::TimeExt as _, parse},
};

/// The tool's command line.
///
/// Shared by the binary and by [`crate::run_from_args`], so both accept
/// exactly the same flags.
#[derive(Parser)]
#[command(
    name = "krabka-barrier",
    version,
    about = "Define barrier groups, trigger cuts, and verify a cut against the log"
)]
pub struct Cli {
    /// Where the cluster is.
    #[command(flatten)]
    pub connection: ConnectionArgs,

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
        /// broker's default. A negative value is refused, and so is one that
        /// rounds to 0 ms (below 0.5ms) other than `0`, since the broker counts
        /// this in whole milliseconds.
        #[arg(long, value_parser = parse_timeout)]
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
/// Delegates to `krabka_units`, so every unit the broker's own configuration
/// accepts works here too: `ns`, `us`, `ms`, `s`, `m`, `h`, `d` and their long
/// forms.
///
/// A number with no unit is refused, and only `0` is exempt. That is the units
/// crate's rule and it is the right one here: `--timeout 30` from someone who
/// meant milliseconds would otherwise wait thirty seconds without complaining.
fn parse_time(raw: &str) -> Result<Time, String> { parse::time(raw).map_err(|e| e.to_string()) }

/// Parse `trigger --timeout`: a non-negative time argument that is `0` or
/// rounds to at least one millisecond.
///
/// The request carries whole milliseconds and a non-positive value asks for
/// the broker's default, so `250us` or `-5s` would otherwise reach the broker
/// as that default rather than as the timeout the operator gave.
fn parse_timeout(raw: &str) -> Result<Time, String> {
    let timeout = parse::non_negative_time(raw).map_err(|e| e.to_string())?;
    if timeout.nanos_i64() != 0 && timeout.millis_i64() == 0 {
        return Err(format!(
            "{raw} rounds to 0 ms, which asks for the broker's default; give at least 1ms"
        ));
    }
    Ok(timeout)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        std::path::PathBuf,
    };

    krabka_macros::duration_parser_fixture!(
        {
            /// A time argument takes any unit the broker's own configuration takes,
            /// so an operator never has to convert to milliseconds by hand, and a
            /// number with no unit is refused rather than guessed at.

        },
        [
            five_minutes = { "5m", Some(300_000) },
            one_hour = { "1h", Some(3_600_000) },
            two_days = { "2d", Some(172_800_000) },
            submillisecond = { "250us", Some(0) },
            one_minute_words = { "1 minute", Some(60_000) },
            // A unit is required, so a number alone cannot be read as the
            // wrong scale. Zero is the one exemption, having no scale.
            missing_unit = { "90", None },
            zero = { "0", Some(0) },
            invalid_word = { "banana", None },
            empty = { "", None },
        ]
    );

    /// `--timeout` takes what `--interval` takes, except a value that rounds
    /// to 0 ms and so would ask for the broker's default.
    #[yare::parameterized(
        one_millisecond = { "1ms", Some(1) },
        five_seconds = { "5s", Some(5_000) },
        rounds_up_to_one_millisecond = { "600us", Some(1) },
        zero = { "0", Some(0) },
        submillisecond = { "250us", None },
        one_nanosecond = { "1ns", None },
        missing_unit = { "90", None },
        negative = { "-5s", None },
        large_negative = { "-30d", None },
    )]
    fn parse_timeout_refuses_a_value_that_rounds_to_zero_milliseconds(
        raw: &str,
        expected_ms: Option<i64>,
    ) {
        check!(parse_timeout(raw).ok().map(Time::millis_i64) == expected_ms);
    }

    krabka_macros::bootstrap_parser_fixture!(["krabka-barrier", "describe"], [
        "krabka-barrier",
        "-b",
        "localhost:9092",
        "describe"
    ]);

    /// `--command-config` reaches a TLS or SASL listener the way
    /// `kafka-topics --command-config` does, and it is refused after the
    /// subcommand, where no connection flag goes.
    #[test]
    fn the_command_config_comes_before_the_subcommand() {
        let cli = Cli::try_parse_from([
            "krabka-barrier",
            "-b",
            "broker-1:9093",
            "--command-config",
            "/etc/krabka/barrier.properties",
            "describe",
        ])
        .expect("parses");
        check!(
            cli.connection.security.command_config
                == Some(PathBuf::from("/etc/krabka/barrier.properties"))
        );
        let plaintext = Cli::try_parse_from(["krabka-barrier", "-b", "broker-1:9093", "describe"])
            .expect("parses");
        check!(plaintext.connection.security.command_config == None);
        assert!(
            Cli::try_parse_from([
                "krabka-barrier",
                "-b",
                "broker-1:9093",
                "describe",
                "--command-config",
                "/etc/krabka/barrier.properties",
            ])
            .is_err()
        );
    }
}
