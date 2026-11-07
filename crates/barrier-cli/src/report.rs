//! Renders a barrier response as the lines an operator reads.
//!
//! Each report turns one response into stdout for what succeeded, stderr for
//! what did not, and the exit code the tool ends on. Keeping the rendering
//! apart from the sending is what lets the shape of the output be reasoned
//! about on its own: a tab-separated row per partition, so `cut` and `awk`
//! read it as well as a person does.

use {
    crate::{
        EXIT_MISMATCH, EXIT_REFUSED, broker_codes::BARRIER_INJECTION_IN_PROGRESS,
        console::Console, verify::VerifyOutcome,
    },
    krabka_protocol::krabka::{
        barrier,
        barrier::{
            AlterBarrierGroupsResponse, DescribeBarrierGroupsResponse, ListBarrierCutsResponse,
            TriggerBarrierResponse,
        },
    },
};

/// Describe an error code.
///
/// `krabka-broker` has no code-to-name table, so this prints the number. The
/// one code an operator of this tool meets that a Kafka reference will not
/// list is the krabka-private one, so that gets a word.
fn code_name(code: i16) -> String {
    if code == BARRIER_INJECTION_IN_PROGRESS {
        return format!("error {code} (an injection is already in flight, retry)");
    }
    format!("error {code}")
}

/// An error code and the broker's message, if it sent one.
pub fn described_error(code: i16, message: Option<&str>) -> String {
    let name = code_name(code);
    match message {
        Some(m) => format!("{name}: {m}"),
        None => name,
    }
}

fn report_refused(console: &mut Console<'_>, code: i16, message: Option<&str>) -> i32 {
    console.err(described_error(code, message));
    EXIT_REFUSED
}

/// One line per altered group.
pub fn report_alter(response: &AlterBarrierGroupsResponse, console: &mut Console<'_>) -> i32 {
    let mut exit = 0;
    for result in &response.results {
        if result.error_code == 0 {
            console.out(format!("{}\tok", result.group));
        } else {
            console.err(format!(
                "{}\t{}",
                result.group,
                described_error(result.error_code, result.error_message.as_deref())
            ));
            exit = EXIT_REFUSED;
        }
    }
    exit
}

/// One block per described group.
pub fn report_describe(response: &DescribeBarrierGroupsResponse, console: &mut Console<'_>) -> i32 {
    let mut exit = 0;
    for group in &response.groups {
        if group.error_code != 0 {
            console.err(format!("{}\t{}", group.group, code_name(group.error_code)));
            exit = EXIT_REFUSED;
            continue;
        }
        console.out(format!("group           {}", group.group));
        console.out(format!("topics          {}", group.topics.join(", ")));
        console.out(format!(
            "interval        {}",
            if group.interval_ms < 0 {
                "on demand only".to_owned()
            } else {
                format!("{}ms", group.interval_ms)
            }
        ));
        console.out(format!("retained cuts   {}", group.retained_cuts));
        // The broker spells "never injected" as 0 and allocates 1 for the
        // first cut, while the wire field's own default is -1. Either way no
        // real epoch is at or below zero, so both read as none.
        console.out(format!(
            "last epoch      {}",
            if group.last_epoch <= 0 {
                "none yet".to_owned()
            } else {
                group.last_epoch.to_string()
            }
        ));
        console.out(format!("coordinator     {}", group.coordinator_id));
        console.out("");
    }
    exit
}

/// The epoch a trigger produced, and the offsets it took.
pub fn report_trigger(response: &TriggerBarrierResponse, console: &mut Console<'_>) -> i32 {
    if response.error_code != 0 {
        return report_refused(console, response.error_code, response.error_message.as_deref());
    }
    console.out(format!("epoch  {}", response.epoch));
    console.out(format!("status {}", status_name(response.status)));
    for topic in &response.topics {
        for partition in &topic.partitions {
            console.out(format!(
                "{}\t{}\t{}",
                topic.topic, partition.partition, partition.offset
            ));
        }
    }
    // A partial cut is a published outcome, not a failure: the epoch is
    // consumed and the markers that did land cannot be withdrawn. Naming the
    // gaps is what lets a reader skip the epoch deterministically.
    for missing in &response.missing {
        console.err(format!("no marker: {}\t{}", missing.topic, missing.partition));
    }
    0
}

/// One line per partition of each retained cut.
pub fn report_list(response: &ListBarrierCutsResponse, console: &mut Console<'_>) -> i32 {
    if response.error_code != 0 {
        return report_refused(console, response.error_code, response.error_message.as_deref());
    }
    for cut in &response.cuts {
        console.out(format!(
            "epoch {} {} triggered {} completed {}",
            cut.epoch,
            status_name(cut.status),
            cut.triggered_at,
            cut.completed_at
        ));
        for topic in &cut.topics {
            for partition in &topic.partitions {
                console.out(format!(
                    "  {}\t{}\t{}",
                    topic.topic, partition.partition, partition.offset
                ));
            }
        }
        for missing in &cut.missing {
            console.out(format!("  {}\t{}\tno marker", missing.topic, missing.partition));
        }
    }
    0
}

/// What a verify found.
pub fn report_verify(outcome: &VerifyOutcome, console: &mut Console<'_>) -> i32 {
    for checked in &outcome.checked {
        console.out(format!("{}\t{}\t{}\tok", checked.0, checked.1, checked.2));
    }
    if outcome.mismatches.is_empty() {
        console.out(format!(
            "cut {} verified: {} markers are in the log",
            outcome.epoch,
            outcome.checked.len()
        ));
        return 0;
    }
    for mismatch in &outcome.mismatches {
        console.err(format!(
            "{}\t{}\t{}\t{}",
            mismatch.topic, mismatch.partition, mismatch.offset, mismatch.reason
        ));
    }
    console.err(format!(
        "cut {} does not match the log: {} of {} offsets are wrong",
        outcome.epoch,
        outcome.mismatches.len(),
        outcome.checked.len() + outcome.mismatches.len()
    ));
    EXIT_MISMATCH
}

/// The name of a cut status code.
fn status_name(status: i8) -> &'static str {
    if status == barrier::CUT_STATUS_COMPLETE {
        "complete"
    } else {
        "partial"
    }
}

#[cfg(test)]
mod scenarios;

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    #[test]
    fn a_status_code_reads_as_a_word() {
        check!(status_name(barrier::CUT_STATUS_COMPLETE) == "complete");
        check!(status_name(barrier::CUT_STATUS_PARTIAL) == "partial");
    }
}
