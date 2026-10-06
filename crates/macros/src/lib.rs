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
    ast::{Attributed, Field, ItemStruct, ParseError},
    token::{Ident, LitStr, Spanner, TokenStream},
};

/// The arguments of one `#[metric(...)]` field attribute.
#[derive(moxy::FromMeta)]
struct MetricArgs {
    /// Leave the field out of the registration. It is still constructed.
    #[meta(default)]
    skip: bool,
    /// The registered name, when it is not the field name without `_total`.
    #[meta(default)]
    name: Option<String>,
    /// The help text, kept as the tokens of the literal so that the text
    /// reaches the registry exactly as rustc reads it.
    #[meta(default)]
    help: Option<Tokens>,
    /// The bucket boundaries of a histogram, or of each histogram of a family.
    #[meta(default)]
    buckets: Option<Tokens>,
    /// An expression that constructs the field, in place of `Default`.
    #[meta(default)]
    new: Option<Tokens>,
}

/// The value of a `key = <expression>` argument, as the expression's tokens.
struct Tokens(TokenStream);

impl moxy::ast::FromMeta for Tokens {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::Expr { expr, .. } => Ok(Self(expr.clone())),
            _ => Err(ParseError::new(
                meta.span(),
                "expected `key = <expression>`",
            )),
        }
    }
}

/// One field of the derived struct, with what the derive emits for it.
struct Metric {
    ident: Ident,
    constructor: TokenStream,
    registration: Option<(LitStr, TokenStream)>,
}

/// The name a metric field registers under when it names none itself.
///
/// `prometheus-client` appends `_total` to the name of every counter, so a
/// field that already ends in `_total` drops it. Any other field registers
/// under its own name.
fn default_name(field: &str) -> &str {
    field.strip_suffix("_total").unwrap_or(field)
}

/// Whether `field` is a `Family<..>` rather than a single metric.
fn is_family(field: &Field) -> bool {
    field
        .ty
        .as_path()
        .and_then(|ty| ty.path.last())
        .is_some_and(|segment| segment.ident == "Family")
}

fn metric(field: &Field) -> Result<Metric, ParseError> {
    let Some(ident) = field.ident.as_ref() else {
        return Err(ParseError::new(
            field.span(),
            "`RegisterMetrics` needs named fields",
        ));
    };
    let args = field
        .parse_meta::<MetricArgs>("metric")?
        .unwrap_or(MetricArgs {
            skip: false,
            name: None,
            help: None,
            buckets: None,
            new: None,
        });

    let constructor = match (args.new, args.buckets) {
        (Some(_), Some(buckets)) => {
            return Err(ParseError::new(
                buckets.0.span(),
                "give `new` or `buckets`, not both",
            ));
        }
        (Some(new), None) => new.0,
        (None, Some(buckets)) if is_family(field) => moxy::template! {
            ::prometheus_client::metrics::family::Family::new_with_constructor(|| {
                ::prometheus_client::metrics::histogram::Histogram::new({{ buckets.0 }})
            })
        },
        (None, Some(buckets)) => moxy::template! {
            ::prometheus_client::metrics::histogram::Histogram::new({{ buckets.0 }})
        },
        (None, None) => moxy::template! { ::core::default::Default::default() },
    };

    let registration = if args.skip {
        None
    } else {
        let Some(help) = args.help else {
            return Err(ParseError::new(
                ident.span(),
                "a registered metric needs `#[metric(help = \"...\")]`, or `skip`",
            ));
        };
        let name = args
            .name
            .unwrap_or_else(|| default_name(ident.text()).to_owned());
        Some((LitStr::new(&name, ident.span()), help.0))
    };

    Ok(Metric {
        ident: ident.clone(),
        constructor,
        registration,
    })
}

/// Derives `unregistered` and `register` for a struct of metric handles. The
/// crate documentation lists the field attributes.
#[moxy::derive(RegisterMetrics, attributes(metric))]
pub fn register_metrics(item: ItemStruct) -> Result<TokenStream, ParseError> {
    // Moved out field by field: moxy's `ItemFn` parser rejects a `let`
    // that destructures `item` with a struct pattern.
    let ident = item.ident;
    let generics = item.generics;
    let fields = item.fields;
    let Some(named) = fields.as_named() else {
        return Err(ParseError::new(
            ident.span(),
            "`RegisterMetrics` needs a struct with named fields",
        ));
    };
    let metrics = named
        .fields
        .iter()
        .map(metric)
        .collect::<Result<Vec<_>, _>>()?;

    let (impl_generics, type_generics, where_clause) = generics.split();
    Ok(moxy::template! {
        impl {{ impl_generics }} {{ ident }} {{ type_generics }} {{ where_clause }} {
            fn unregistered() -> Self {
                Self {
                    @for metric in &metrics {
                        {{ metric.ident }}: {{ metric.constructor }},
                    }
                }
            }

            fn register(&self, registry: &mut ::prometheus_client::registry::Registry) {
                @for metric in &metrics {
                    @if let Some((name, help)) = &metric.registration {
                        registry.register(
                            {{ name }},
                            {{ help }},
                            ::core::clone::Clone::clone(&self.{{ metric.ident }}),
                        );
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::default_name;

    #[test]
    fn default_name_drops_only_a_trailing_total() {
        for (field, name) in [
            ("isr_shrinks_total", "isr_shrinks"),
            ("partitions_total_led", "partitions_total_led"),
            ("topic_bytes_in", "topic_bytes_in"),
            ("total", "total"),
        ] {
            assert!(default_name(field) == name);
        }
    }
}
