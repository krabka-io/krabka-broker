//! JVM cross-validation: the stock Apache Kafka admin tools read a KFC-9
//! refusal as the refusal it is.
//!
//! # This suite has never been executed
//!
//! It was written against the pattern that `jvm_barrier_markers` and the
//! `jvm_acceptance_*` suites established, and no case in it has ever run. The
//! machine it was written on has no working Docker daemon, so every case here
//! is a statement of what the behaviour must be rather than a report of what
//! it was observed to be. The first real execution has to happen on a
//! Docker-capable host, with the invocation below. A reviewer should treat a
//! first-run failure as a bug in this file until they have shown otherwise.
//!
//! ```text
//! cargo test -p krabka-broker --test jvm_acceptance_freeze -- --ignored --nocapture
//! ```
//!
//! # The claim
//!
//! KFC-9 adds two ways for a healthy, correctly configured cluster to refuse a
//! request that the caller is fully authorized to make: a topic write freeze,
//! and a break-glass two-person rule over the privileged transitions. Both
//! refuse with `POLICY_VIOLATION` (44), and the read-only `write.freeze` topic
//! config refuses an alter with `INVALID_CONFIG` (40).
//!
//! An error code is only worth what the client makes of it. `Errors.forCode`
//! maps 44 onto `PolicyViolationException` and 40 onto
//! `InvalidConfigurationException`, and it maps an unassigned code onto
//! `UnknownServerException`, which a JVM client classifies as retriable and a
//! JVM tool reports as an internal broker fault. That difference is the whole
//! reason KFC-9 reuses two existing Kafka codes rather than minting new ones,
//! and it is a claim about somebody else's code. This suite is the only place
//! the claim is checked against Apache Kafka's own client rather than against
//! krabka's.
//!
//! # What a weaker suite would miss
//!
//! Every other KFC-9 test drives krabka's client against krabka's broker. Such
//! a test passes just as happily on a private error code, on a code the JVM
//! calls retriable, and on a response that carries no `error_message` at all,
//! because krabka's client reads the number it was given. Three failures live
//! in that gap, and all three are invisible without a JVM in the loop.
//!
//! 1. **The exception class.** A JVM operator sees a class name, not a number.
//!    Each case asserts the fully-qualified exception the tool printed, so a
//!    code that stops mapping to `PolicyViolationException` fails here.
//! 2. **The broker's message.** KFC-9 promises that the refusal names the
//!    freeze scope, or the action and target the two-person rule wants an
//!    approval for. A response that dropped `error_message` still carries the
//!    right code, and leaves the on-call with nothing to act on. Each case
//!    asserts the broker's own sentence, reconstructed here, appears in what
//!    the tool printed.
//! 3. **The exit code.** A runbook branches on `$?`. A tool that reports a
//!    failure on stdout and exits zero is worse than one that fails loudly,
//!    and no in-process test can see it. This is why the producer case runs
//!    with `--sync`: the asynchronous console producer hands a failed send to
//!    a logging callback and still exits zero, so the synchronous path is the
//!    only one whose exit code means anything.
//!
//! The two positive controls carry the rest of the weight. A refusal that
//! refuses everything is not a feature, so the producer case produces to an
//! unfrozen control topic through the same tool and the same broker, and the
//! break-glass cases show the same command succeeding once an approval exists.
//! Without them a broker that refused every write would pass this file.
//!
//! # Networking
//!
//! The broker runs on the host and advertises `host.docker.internal`; the JVM
//! tools run in cp-kafka containers on the default bridge with
//! `--add-host=host.docker.internal:host-gateway`. [`jvm_acceptance`] documents
//! that arrangement, and why `--network host` is not used.
//!
//! Each case allocates its own listener pair through
//! [`support::JvmListeners::allocate`] rather than sharing the process-wide
//! set, so the four cases can run concurrently without racing for a port.
//!
//! # What this suite deliberately does not cover
//!
//! Operator signatures. `break_glass.signed_actions` is set empty in every
//! case here, so the approvals are unsigned. The canonical byte layout, the
//! attack table, and the verification rules are the in-process suites' work;
//! a container can say nothing about them that a unit test does not say
//! better. What a container can say is what the JVM tool prints and what it
//! exits with, and that is all this file asserts.
//!
//! # Layout
//!
//! `vocabulary` holds the names, topics, operators and refusal sentences the
//! cases pin; `jvm_tool` runs one CLI tool in a container and keeps its exit
//! status and its output; `host_broker` boots the broker those containers dial;
//! and `control_plane` does the krabka-private setup each case needs before a
//! tool runs. The cases are one file each: `frozen_produce` and
//! `freeze_config_key` for the write freeze, `break_glass_delete` and
//! `break_glass_election` for the two-person rule.

mod jvm_acceptance;
mod support;

// Cargo compiles this file as its own test binary, so the crate root's module
// directory is `tests/`. `#[path]` re-bases each declaration onto the sibling
// `jvm_acceptance_freeze/` directory, which keeps the parts out of `tests/`
// where every `.rs` file would become another test binary.
#[path = "jvm_acceptance_freeze/break_glass_delete.rs"]
mod break_glass_delete;
#[path = "jvm_acceptance_freeze/break_glass_election.rs"]
mod break_glass_election;
#[path = "jvm_acceptance_freeze/control_plane.rs"]
mod control_plane;
#[path = "jvm_acceptance_freeze/freeze_config_key.rs"]
mod freeze_config_key;
#[path = "jvm_acceptance_freeze/frozen_produce.rs"]
mod frozen_produce;
#[path = "jvm_acceptance_freeze/host_broker.rs"]
mod host_broker;
#[path = "jvm_acceptance_freeze/jvm_tool.rs"]
mod jvm_tool;
#[path = "jvm_acceptance_freeze/vocabulary.rs"]
mod vocabulary;
