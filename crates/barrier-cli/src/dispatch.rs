//! Runs one command line: builds the client, sends the subcommand's request,
//! and hands the response to the reporter.
//!
//! This is the only module that talks to a broker, so every exit code the tool
//! can return is decided here or in `report`. A transport failure is kept
//! distinct from a refusal throughout, because a request that did not complete
//! says nothing at all about whether the broker acted on it.

use {
    crate::{
        EXIT_REFUSED, EXIT_UNREACHABLE,
        cli::{Cli, Command},
        console::Console,
        report::{report_alter, report_describe, report_list, report_trigger, report_verify},
        verify,
    },
    krabka_client_core::Client,
    krabka_protocol::krabka::barrier as api,
    krabka_units::{Time, convert::TimeExt as _},
    std::io,
};

krabka_macros::bootstrap_cli_entrypoint!({
    /// Run the tool from an argv-style iterator, returning its exit code.
    /// Without `--bootstrap-server`, it reads `KRABKA_BOOTSTRAP_SERVER`.
    ///
    /// `0` means the broker accepted the request. `1` means it refused one, and the
    /// reason is on stderr. `2` means no request completed, because the broker
    /// could not be reached or refused the login, or `--command-config` was
    /// unusable. `3` means a cut does not match the log.
    ///
    /// # Panics
    ///
    /// Panics if `argv` does not parse, which for a caller passing a literal
    /// argument list is a bug in that list rather than a runtime condition.
});

/// The `timeout_ms` that asks the broker for its own default. The broker reads
/// any non-positive value that way.
const BROKER_DEFAULT_TIMEOUT_MS: i32 = 0;

/// The `timeout_ms` a `trigger --timeout` sends.
///
/// No timeout asks for the broker's default. A timeout above `i32::MAX`
/// milliseconds saturates there, so the broker clamps it to its ceiling, as the
/// `--timeout` help says.
fn trigger_timeout_ms(timeout: Option<Time>) -> i32 {
    timeout.map_or(BROKER_DEFAULT_TIMEOUT_MS, |timeout| {
        i32::try_from(timeout.millis_i64()).unwrap_or(i32::MAX)
    })
}

/// Run one parsed command, writing to stdout and stderr.
pub async fn run(cli: Cli) -> i32 {
    let (mut out, mut err) = (io::stdout(), io::stderr());
    run_with(cli, &mut Console::new(&mut out, &mut err)).await
}

/// Run one parsed command, writing its lines to `console`.
///
/// The exit code means what it does for [`run_from_args`].
pub async fn run_with(cli: Cli, console: &mut Console<'_>) -> i32 {
    let client = match cli.connection.connect("krabka-barrier").await {
        Ok(client) => client,
        Err(error) => {
            console.err(error.to_string());
            return EXIT_UNREACHABLE;
        }
    };
    dispatch(&client, cli.command, console).await
}

/// Send one command's request and print its response.
async fn dispatch(client: &Client, command: Command, console: &mut Console<'_>) -> i32 {
    match command {
        Command::Define {
            group,
            topics,
            interval,
            retained_cuts,
        } => {
            send_alter(client, console, api::AlterableBarrierGroup {
                group,
                topics,
                // -1 is the sentinel for "no periodic injection".
                interval_ms: interval.map_or(-1, Time::millis_i64),
                retained_cuts,
                delete: false,
                ..api::AlterableBarrierGroup::default()
            })
            .await
        }
        Command::Delete { group } => {
            send_alter(client, console, api::AlterableBarrierGroup {
                group,
                delete: true,
                ..api::AlterableBarrierGroup::default()
            })
            .await
        }
        Command::Describe { groups } => {
            let request = api::DescribeBarrierGroupsRequest {
                groups,
                ..api::DescribeBarrierGroupsRequest::default()
            };
            send_then(client, console, request, report_describe).await
        }
        Command::Trigger { group, timeout } => {
            let request = api::TriggerBarrierRequest {
                group,
                timeout_ms: trigger_timeout_ms(timeout),
                ..api::TriggerBarrierRequest::default()
            };
            send_then(client, console, request, report_trigger).await
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
            send_then(client, console, request, report_list).await
        }
        Command::Verify { group, epoch } => match verify::verify(client, &group, epoch).await {
            Ok(outcome) => report_verify(&outcome, console),
            Err(error) => {
                console.err(error);
                EXIT_REFUSED
            }
        },
    }
}

async fn send_alter(
    client: &Client,
    console: &mut Console<'_>,
    group: api::AlterableBarrierGroup,
) -> i32 {
    send_then(
        client,
        console,
        api::AlterBarrierGroupsRequest {
            groups: vec![group],
            ..api::AlterBarrierGroupsRequest::default()
        },
        report_alter,
    )
    .await
}

/// Send one request and hand its response to `report`.
///
/// A transport failure is printed as such and ends on `EXIT_UNREACHABLE`,
/// because a request that did not complete says nothing about whether the
/// broker acted on it.
async fn send_then<R: krabka_client_core::ProtocolRequest>(
    client: &Client,
    console: &mut Console<'_>,
    request: R,
    report: impl FnOnce(&R::Response, &mut Console<'_>) -> i32,
) -> i32 {
    match client.send(request).await {
        Ok(response) => report(&response, console),
        Err(error) => {
            console.err(format!(
                "the request did not complete, so its outcome is unknown: {error}"
            ));
            EXIT_UNREACHABLE
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_units::prelude::{days, secs},
    };

    /// `trigger --timeout` reaches the wire in whole milliseconds. A timeout
    /// past `i32::MAX` saturates, so the broker clamps it to its ceiling
    /// rather than reading it as "use the default".
    #[yare::parameterized(
        omitted = { None, BROKER_DEFAULT_TIMEOUT_MS },
        five_seconds = { Some(secs(5)), 5_000 },
        largest_that_fits = { Some(Time::from_millis(i64::from(i32::MAX))), i32::MAX },
        one_past_the_field = { Some(Time::from_millis(i64::from(i32::MAX) + 1)), i32::MAX },
        thirty_days = { Some(days(30)), i32::MAX },
    )]
    fn trigger_timeout_ms_saturates_past_the_wire_field(timeout: Option<Time>, expected: i32) {
        check!(trigger_timeout_ms(timeout) == expected);
    }
}
