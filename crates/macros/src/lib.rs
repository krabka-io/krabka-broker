//! Derive macros for the krabka broker.
//!
//! `krabka-macros` turns one annotated declaration into code that the broker
//! would otherwise write out by hand two or three times. It is built on
//! [`moxy`], which supplies the token, syntax-tree, attribute-parsing and
//! template layers.
//!
//! # `RegisterMetrics`
//!
//! `#[derive(RegisterMetrics)]` goes on a struct of `prometheus-client` metric
//! handles. It adds two private methods to the struct:
//!
//! - `fn unregistered() -> Self` constructs every field: with
//!   `Default::default()`, with a histogram over `buckets`, or with the `new`
//!   expression.
//! - `fn register(&self, registry: &mut Registry)` registers a clone of every
//!   field that is not `skip`, in field order. The order of the fields is the
//!   order of the families in the text exposition.
//!
//! Each field takes one optional `#[metric(...)]` attribute:
//!
//! - `help = "..."` — the help text. Every registered field needs one.
//! - `name = "..."` — the registered name. The default is the field name with
//!   a trailing `_total` removed, because `prometheus-client` appends `_total`
//!   to the name of a counter itself.
//! - `buckets = EXPR` — construct a `Histogram` over `EXPR`. On a
//!   `Family<_, Histogram>` field, construct the family so that each of its
//!   histograms uses `EXPR`.
//! - `new = EXPR` — construct the field with `EXPR`.
//! - `skip` — construct the field, but do not register it.
//!
//! ```ignore
//! #[derive(krabka_macros::RegisterMetrics)]
//! struct Metrics {
//!     #[metric(help = "Records received")]
//!     records_total: Counter,
//!     #[metric(help = "Request latency in seconds", buckets = [0.001, 0.01, 0.1])]
//!     latency_seconds: Family<ApiLabel, Histogram>,
//! }
//! ```

use moxy::{
    ast::{ItemStruct, ParseError},
    token::TokenStream,
};

mod meta;
mod metrics;

/// Derives `unregistered` and `register` for a struct of metric handles. The
/// crate documentation lists the field attributes.
#[moxy::derive(RegisterMetrics, attributes(metric))]
pub fn register_metrics(item: ItemStruct) -> Result<TokenStream, ParseError> {
    metrics::expand(item)
}

// New macros: put the implementation in its own module under `src/` and add
// its entry point below this line, one block per macro.
