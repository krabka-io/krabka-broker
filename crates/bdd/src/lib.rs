//! Krabka's BDD layer over rstest-bdd.
//!
//! Placeholder crate docs, replaced once the pieces land.

mod given;
mod harness;
mod step_check;
mod step_param;

pub use {
    self::{
        given::Given,
        harness::{MultiThreadAttributePolicy, TokioMultiThreadContext, TokioMultiThreadHarness},
        step_param::StepParamError,
    },
    krabka_bdd_macros::{StepParam, scenarios},
};

/// What `scenarios!` expansions name. Not part of the API.
#[doc(hidden)]
pub mod __private {
    pub use {
        crate::step_check::{StepUse, check_steps},
        rstest_bdd::StepKeyword,
    };
}
