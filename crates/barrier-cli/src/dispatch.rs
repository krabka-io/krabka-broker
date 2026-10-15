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
                // A non-positive timeout asks the broker for its own default.
                timeout_ms: timeout
                    .map(Time::millis_i64)
                    .and_then(|ms| i32::try_from(ms).ok())
                    .unwrap_or(0),
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
