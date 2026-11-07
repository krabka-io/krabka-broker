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
        report::{report_alter, report_describe, report_list, report_trigger, report_verify},
        verify,
    },
    clap::Parser,
    krabka_protocol::krabka::barrier as api,
    krabka_units::{Time, convert::TimeExt as _},
};

krabka_macros::parsed_cli_entrypoint!({
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

/// Run one parsed command.
pub async fn run(cli: Cli) -> i32 {
    let client = krabka_macros::connect_cli_client!(
        &cli.bootstrap_server,
        "krabka-barrier",
        EXIT_UNREACHABLE
    );
    dispatch(&client, cli.command).await
}

/// Send one command's request and print its response.
async fn dispatch(client: &krabka_client_core::Client, command: Command) -> i32 {
    match command {
        Command::Define {
            group,
            topics,
            interval,
            retained_cuts,
        } => {
            send_alter(client, api::AlterableBarrierGroup {
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
            send_alter(client, api::AlterableBarrierGroup {
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
            send_then(client, request, report_describe).await
        }
        Command::Trigger { group, timeout } => {
            let request = api::TriggerBarrierRequest {
                group,
                timeout_ms: trigger_timeout_ms(timeout),
                ..api::TriggerBarrierRequest::default()
            };
            send_then(client, request, report_trigger).await
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
            send_then(client, request, report_list).await
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

async fn send_alter(client: &krabka_client_core::Client, group: api::AlterableBarrierGroup) -> i32 {
    send_then(
        client,
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
    client: &krabka_client_core::Client,
    request: R,
    report: impl FnOnce(&R::Response) -> i32,
) -> i32 {
    match client.send(request).await {
        Ok(response) => report(&response),
        Err(error) => {
            eprintln!("the request did not complete, so its outcome is unknown: {error}");
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
