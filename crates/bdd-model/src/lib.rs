//! The Gherkin model behind `krabka-bdd`.
//!
//! Plain functions over parsed features, shared by the `krabka-bdd-macros`
//! proc macros and the `krabka-bdd` command-line tool.

pub mod baseline;
pub mod cucumber;
pub mod glossary;
pub mod lint;
pub mod model;
pub mod plan;
