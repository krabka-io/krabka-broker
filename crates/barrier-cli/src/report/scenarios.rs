//! Step definitions for `tests/features/operator_messages`.
//!
//! These scenarios are about how the tool reads a broker's answer, so most of
//! them hand the reporter a response the broker could send rather than
//! arranging a broker into the state that sends it. A partial cut and an
//! injection already in flight are both hard to bring about on one node, and
//! what the tool prints for them does not depend on how they came about.

use {
    super::report_trigger,
    crate::{Cli, Console, broker_codes::BARRIER_INJECTION_IN_PROGRESS, run_with},
    clap::Parser as _,
    krabka_bdd::Given,
    krabka_protocol::krabka::barrier::{
        BarrierCutPartition, BarrierCutTopic, BarrierMissingPartition, CUT_STATUS_PARTIAL,
        TriggerBarrierResponse,
    },
    rstest_bdd_macros::{then, when},
    std::net::TcpListener,
    tokio::runtime,
};

/// What one run of the reporter printed, and the code it ended on.
#[derive(Debug, PartialEq, Eq)]
struct Rendered {
    exit: i32,
    stdout: String,
    stderr: String,
}

impl Rendered {
    /// Run `report` against two buffers and keep what it wrote.
    fn capture(report: impl FnOnce(&mut Console<'_>) -> i32) -> Self {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let exit = report(&mut Console::new(&mut out, &mut err));
        Self {
            exit,
            stdout: String::from_utf8(out).expect("utf-8 stdout"),
            stderr: String::from_utf8(err).expect("utf-8 stderr"),
        }
    }
}

struct OperatorMessages {
    /// What the tool printed, and the code it ended on.
    rendered: Given<Rendered>,
    /// The address the tool was pointed at.
    address: Given<String>,
}

#[rstest::fixture]
fn operator_messages() -> OperatorMessages {
    OperatorMessages {
        rendered: Given::new("rendered", "When the broker answers a trigger"),
        address: Given::new(
            "address",
            "When an operator triggers a cut against an address where no broker listens",
        ),
    }
}

/// Render one trigger response the way the tool does.
fn rendered_trigger(response: &TriggerBarrierResponse) -> Rendered {
    Rendered::capture(|console| report_trigger(response, console))
}

#[when(
    "the broker answers a trigger with a partial cut that has no marker in {topic} partition {partition}"
)]
fn partial_cut(operator_messages: &mut OperatorMessages, topic: String, partition: i32) {
    // The epoch, the other partition and its offset only give the cut a
    // shape; the gap is what the scenario is about.
    let response = TriggerBarrierResponse {
        epoch: 4,
        status: CUT_STATUS_PARTIAL,
        topics: vec![BarrierCutTopic {
            topic: topic.clone(),
            partitions: vec![BarrierCutPartition {
                partition: partition - 1,
                offset: 10,
                ..BarrierCutPartition::default()
            }],
            ..BarrierCutTopic::default()
        }],
        missing: vec![BarrierMissingPartition {
            topic,
            partition,
            ..BarrierMissingPartition::default()
        }],
        ..TriggerBarrierResponse::default()
    };
    operator_messages.rendered.set(rendered_trigger(&response));
}

#[then("the tool succeeds and reports no marker in {topic} partition {partition}")]
fn partial_cut_reported(operator_messages: &OperatorMessages, topic: String, partition: i32) {
    assert2::assert!(
        *operator_messages.rendered.get()
            == Rendered {
                exit: 0,
                stdout: format!("epoch  4\nstatus partial\n{topic}\t{}\t10\n", partition - 1),
                stderr: format!("no marker: {topic}\t{partition}\n"),
            }
    );
}

#[when("the broker refuses a trigger because an injection is already in flight")]
fn injection_in_flight(operator_messages: &mut OperatorMessages) {
    let response = TriggerBarrierResponse {
        error_code: BARRIER_INJECTION_IN_PROGRESS,
        ..TriggerBarrierResponse::default()
    };
    operator_messages.rendered.set(rendered_trigger(&response));
}

#[then(
    "the tool exits {exit} and tells the operator that an injection is already in flight, retry"
)]
fn told_to_retry(operator_messages: &OperatorMessages, exit: i32) {
    assert2::assert!(
        *operator_messages.rendered.get()
            == Rendered {
                exit,
                stdout: String::new(),
                stderr: "error 1000 (an injection is already in flight, retry)\n".to_owned(),
            }
    );
}

#[when("the broker refuses a trigger with error {code} and the reason {reason}")]
fn refused_with_reason(operator_messages: &mut OperatorMessages, code: i16, reason: String) {
    let response = TriggerBarrierResponse {
        error_code: code,
        error_message: Some(reason),
        ..TriggerBarrierResponse::default()
    };
    operator_messages.rendered.set(rendered_trigger(&response));
}

#[then("the tool exits {exit} and reports {message}")]
fn refusal_reported(operator_messages: &OperatorMessages, exit: i32, message: String) {
    assert2::assert!(
        *operator_messages.rendered.get()
            == Rendered {
                exit,
                stdout: String::new(),
                stderr: format!("{message}\n"),
            }
    );
}

#[when("an operator triggers a cut against an address where no broker listens")]
fn trigger_against_nothing(operator_messages: &mut OperatorMessages) {
    // Bind a port and let it go: nothing listens there afterwards, and no
    // other test can be handed it while this one holds the address.
    let address = TcpListener::bind("127.0.0.1:0")
        .and_then(|listener| listener.local_addr())
        .expect("a free port")
        .to_string();
    let cli = Cli::try_parse_from([
        "krabka-barrier",
        "--bootstrap-server",
        &address,
        "trigger",
        "--group",
        "orders-cut",
    ])
    .expect("the command line parses");
    let runtime = runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let rendered = Rendered::capture(|console| runtime.block_on(run_with(cli, console)));
    operator_messages.rendered.set(rendered);
    operator_messages.address.set(address);
}

#[then("the tool exits {exit} because the outcome of the request is unknown")]
fn unreachable_reported(operator_messages: &OperatorMessages, exit: i32) {
    let rendered = operator_messages.rendered.get();
    let address = operator_messages.address.get();
    assert2::check!(rendered.exit == exit);
    assert2::check!(rendered.stdout.is_empty());
    // The client connects on its first request, so an address with no
    // broker behind it fails that request rather than the connection setup.
    assert2::check!(
        rendered
            .stderr
            .starts_with("the request did not complete, so its outcome is unknown: "),
        "{rendered:?}"
    );
    assert2::check!(rendered.stderr.contains(address.as_str()), "{rendered:?}");
}

krabka_bdd::scenarios!(
    "tests/features/operator_messages",
    fixtures = [operator_messages: OperatorMessages]
);
