//! Runs one command line: builds the client, sends the subcommand's request,
//! and hands the response to the reporter.
//!
//! This is the only module that talks to a broker, so every exit code the tool
//! can return is decided here or in `report`. A transport failure is kept
//! distinct from a refusal throughout, because a request that did not complete
//! says nothing at all about whether the broker acted on it.

use clap::Parser;
use krabka_protocol::krabka::barrier as api;

use crate::{
    EXIT_REFUSED, EXIT_UNREACHABLE,
    cli::{Cli, Command, as_millis_i64},
    report::{report_alter, report_describe, report_list, report_trigger, report_verify},
    verify,
};

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
    let client = match krabka_client_core::Client::builder()
        .bootstrap(&cli.bootstrap_server)
        .client_id("krabka-barrier")
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
async fn dispatch(client: &krabka_client_core::Client, command: Command) -> i32 {
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
fn unreachable_broker(error: &krabka_client_core::ClientError) -> i32 {
    eprintln!("the request did not complete, so its outcome is unknown: {error}");
    EXIT_UNREACHABLE
}
