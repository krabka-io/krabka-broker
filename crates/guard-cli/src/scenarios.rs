//! The Gherkin scenarios under `tests/features`, bound to the tool.
//!
//! They are living documentation of the operator's side of KFC-9: what an
//! operator asks for, and what the tool reports back. Each capability directory
//! has its own `scenarios!` call below.
//!
//! The steps live in the crate rather than in `tests/` because the driver
//! reuses [`run_command`](crate::command::run_command), the same seam
//! `signed_approval_interop` uses, with buffers in place of stdout and stderr,
//! so a Then reads the lines an operator would. A SASL client per operator is what lets alice,
//! bob and carol be three principals, which the two-person rule needs. The tool
//! itself can only connect over a plaintext listener, as one anonymous
//! principal, and `two_person_rule.feature` keeps that gap as an `@inprogress`
//! scenario. The one path that does go through
//! [`run_from_args`](crate::run_from_args) is the unreachable broker, because
//! that outcome is decided by the connect step `run_command` skips.
//!
//! - [`driver`] boots the cluster and runs the tool as a named operator.
//! - [`params`] parses the step words into domain values, once.
//! - [`transcript`] holds what one run wrote, and reads the list lines back.
//! - [`world`] is the scenario state.
//! - [`common_steps`] holds the steps every capability shares.
//! - [`freeze_steps`], [`break_glass_steps`], [`verification_steps`] and
//!   [`listing_steps`] hold the rest, grouped by domain area.

mod break_glass_steps;
mod common_steps;
mod driver;
mod freeze_steps;
mod listing_steps;
mod params;
mod transcript;
mod verification_steps;
mod world;

use self::world::{GuardScenario, guard};

krabka_bdd::scenarios!(
    "tests/features/freezing",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
krabka_bdd::scenarios!(
    "tests/features/thawing",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
krabka_bdd::scenarios!(
    "tests/features/signing",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
krabka_bdd::scenarios!(
    "tests/features/two_person_rule",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
krabka_bdd::scenarios!(
    "tests/features/registry_verification",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
krabka_bdd::scenarios!(
    "tests/features/outcomes",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
krabka_bdd::scenarios!(
    "tests/features/proposal_listing",
    fixtures = [guard: GuardScenario],
    runtime = "tokio-multi-thread",
    workers = 4,
);
