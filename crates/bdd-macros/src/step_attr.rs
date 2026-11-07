//! `#[given]`, `#[when]`, `#[then]` and `#[step]`: step definitions from
//! Cucumber expressions.
//!
//! Each attribute takes one or more Cucumber expressions. They expand, through
//! [`cucumber::expand`], into literal rstest-bdd patterns. The first pattern
//! goes on the function itself as `#[::rstest_bdd_macros::given("...")]`;
//! every further pattern, and every further keyword of `#[step]`, gets a
//! `#[doc(hidden)]` forwarding function `__krabka_bdd_<fn>_<n>` with the same
//! signature, which calls the original.
//!
//! The expansion checks what the fork's step attribute would otherwise get
//! wrong silently or report far from the cause:
//!
//! - Every pattern compiles with `rstest_bdd_patterns`.
//! - The placeholders of every pattern name the non-fixture parameters in
//!   declaration order, so a step function reads like its step text.
//! - The function returns `()`, `Result<(), E>` or an `Attempt<T, E>`, and a
//!   pattern that says "attempts to" or "tries to" returns an `Attempt`.

use {
    krabka_bdd_model::cucumber::{self, ParamInfo},
    moxy::{
        ast::{
            Attributed, FnParam, GenericArgument, ItemFn, List, ParseError, Parser, PatType,
            PathArguments, Pattern, ReturnType, Token, Type,
        },
        token::{Ident, LitStr, Span, Spanner, ToTokenStream, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
    rstest_bdd_patterns::{
        build_regex_from_pattern,
        pattern::lexer::{Token as PatternToken, lex_pattern},
    },
};

/// Which step attribute is expanding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepAttribute {
    Given,
    When,
    Then,
    /// All three keywords.
    Step,
}

impl StepAttribute {
    /// The rstest-bdd attributes the step registers under, in order.
    const fn keywords(self) -> &'static [&'static str] {
        match self {
            Self::Given => &["given"],
            Self::When => &["when"],
            Self::Then => &["then"],
            Self::Step => &["given", "when", "then"],
        }
    }
}

/// How rstest-bdd binds one step-function parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParamKind {
    /// Looked up in the scenario context: a reference other than `&str`, or
    /// a parameter marked `#[from]` or `#[harness_context]`.
    Fixture,
    /// The step's data table: `#[datatable]`, or a parameter named
    /// `datatable`.
    DataTable,
    /// The step's doc string: a parameter named `docstring`.
    DocString,
    /// A `#[step_args]` struct that takes every placeholder.
    StepArgs,
    /// A placeholder value, bound to the placeholder of its name.
    Value,
}

/// `name` among `attributes`.
fn has_attribute(param: &PatType, name: &str) -> bool {
    param.attrs().iter().any(|attr| attr.path.is_ident(name))
}

/// How the fork binds `param`, called `name`.
fn classify(param: &PatType, name: &str) -> ParamKind {
    if has_attribute(param, "step_args") {
        ParamKind::StepArgs
    } else if has_attribute(param, "from") || has_attribute(param, "harness_context") {
        ParamKind::Fixture
    } else if has_attribute(param, "datatable") || name == "datatable" {
        ParamKind::DataTable
    } else if name == "docstring" {
        ParamKind::DocString
    } else if let Type::Reference(reference) = param.ty.as_ref()
        && !matches!(reference.elem.as_ref(), Type::Path(path) if path.path.is_ident("str"))
    {
        ParamKind::Fixture
    } else {
        ParamKind::Value
    }
}

/// One parameter of the step function.
struct StepParam<'a> {
    param: &'a PatType,
    ident: &'a Ident,
    kind: ParamKind,
}

/// The parameters of `item`, each a plain name.
fn step_params(item: &ItemFn) -> Result<Vec<StepParam<'_>>, ParseError> {
    item.sig
        .params
        .inner
        .inputs
        .iter()
        .map(|input| {
            let FnParam::Typed(param) = input else {
                return Err(ParseError::new(
                    input.span(),
                    "a step function takes no `self`: write it as a free function",
                ));
            };
            let Pattern::Ident(binding) = param.pat.as_ref() else {
                return Err(ParseError::new(
                    param.pat.span(),
                    "a step parameter binds a plain name: destructure it in the body",
                ));
            };
            if binding.by_ref.is_some() || binding.subpat.is_some() {
                return Err(ParseError::new(
                    param.pat.span(),
                    "a step parameter binds a plain name: destructure it in the body",
                ));
            }
            Ok(StepParam {
                param,
                ident: &binding.ident,
                kind: classify(param, binding.ident.text()),
            })
        })
        .collect()
}

/// What a step function returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReturnKind {
    /// `()`, written or not.
    Unit,
    /// `Result<(), E>`.
    UnitResult,
    /// `Attempt<T, E>`, which the runner stores for a later step.
    Attempt,
}

/// `ty` is `()`.
fn is_unit(ty: &Type) -> bool { matches!(ty, Type::Tuple(tuple) if tuple.elems.inner.is_empty()) }

/// What `output` returns, or why a step may not return it.
fn return_kind(output: &ReturnType) -> Result<ReturnKind, String> {
    let Some(ty) = output.as_type() else {
        return Ok(ReturnKind::Unit);
    };
    if is_unit(ty) {
        return Ok(ReturnKind::Unit);
    }
    if let Type::Path(path) = ty
        && let Some(last) = path.path.last()
    {
        match (last.ident.text(), &last.args) {
            ("Attempt", _) => return Ok(ReturnKind::Attempt),
            ("Result", PathArguments::AngleBracketed(arguments)) if matches!(arguments.args.first(), Some(GenericArgument::Type(ok)) if is_unit(ok)) =>
            {
                return Ok(ReturnKind::UnitResult);
            }
            _ => {}
        }
    }
    Err(format!(
        "a step returns `()`, `Result<(), E>` or `Attempt<T, E>`, not `{}`: keep other values \
         in the scenario's fixtures",
        ty.to_token_stream()
    ))
}

/// The pattern says the step attempts something, so it returns an `Attempt`.
fn says_attempt(pattern: &str) -> bool {
    pattern.contains("attempts to") || pattern.contains("tries to")
}

/// The placeholder names of `pattern`, in order.
fn placeholder_names(pattern: &str) -> Result<Vec<String>, String> {
    let tokens = lex_pattern(pattern)
        .map_err(|error| format!("step pattern `{pattern}` is invalid: {error}"))?;
    Ok(tokens
        .into_iter()
        .filter_map(|token| match token {
            PatternToken::Placeholder { name, .. } => Some(name),
            _ => None,
        })
        .collect())
}

/// `names` as a list of code spans.
fn name_list<S: AsRef<str>>(names: &[S]) -> String {
    let names: Vec<String> = names
        .iter()
        .map(|name| format!("`{}`", name.as_ref()))
        .collect();
    if names.is_empty() {
        "none".to_owned()
    } else {
        names.join(", ")
    }
}

/// Checks that `pattern` compiles and that its placeholders bind `values`,
/// the non-fixture parameters, by name and in order.
fn check_pattern(pattern: &str, values: &[ParamInfo]) -> Result<(), String> {
    build_regex_from_pattern(pattern)
        .map_err(|error| format!("step pattern `{pattern}` is invalid: {error}"))?;
    let placeholders = placeholder_names(pattern)?;
    let parameters: Vec<&str> = values.iter().map(ParamInfo::placeholder_name).collect();
    if placeholders == parameters {
        return Ok(());
    }
    Err(format!(
        "step pattern `{pattern}` has placeholders {} but the step function's non-fixture \
         parameters are {}: name each parameter after its placeholder, in the same order, and \
         take a fixture by reference or mark it `#[from]`",
        name_list(&placeholders),
        name_list(&parameters),
    ))
}

/// Every pattern `expressions` expand to, checked, in order.
///
/// `step_args` skips the placeholder check, because a `#[step_args]` struct
/// binds the placeholders by its field names.
fn step_patterns(
    expressions: &[String],
    values: &[ParamInfo],
    step_args: bool,
    returns: ReturnKind,
) -> Result<Vec<String>, String> {
    if expressions.is_empty() {
        return Err(
            "a step attribute takes one or more Cucumber expressions, such as \
             `#[given(\"a topic with {int} partitions\")]`"
                .to_owned(),
        );
    }
    let mut patterns: Vec<String> = Vec::new();
    for expression in expressions {
        for pattern in cucumber::expand(expression, values)? {
            if step_args {
                build_regex_from_pattern(&pattern)
                    .map_err(|error| format!("step pattern `{pattern}` is invalid: {error}"))?;
            } else {
                check_pattern(&pattern, values)?;
            }
            if says_attempt(&pattern) && returns != ReturnKind::Attempt {
                return Err(format!(
                    "step pattern `{pattern}` says \"attempts to\" or \"tries to\", so the step \
                     returns an `Attempt<T, E>` for a later step to check"
                ));
            }
            if patterns.contains(&pattern) {
                return Err(format!(
                    "step pattern `{pattern}` is listed twice: drop one of the expressions"
                ));
            }
            patterns.push(pattern);
        }
    }
    Ok(patterns)
}

/// The string literals of a step attribute's arguments.
fn expressions(meta: &TokenStream) -> Result<Vec<LitStr>, ParseError> {
    let parser = Parser::from_tokens(meta);
    let list = List::<LitStr, Token![,]>::parse_all(&parser).map_err(|error| {
        ParseError::new(
            error.span(),
            "a step attribute takes Cucumber expressions as string literals: \
             `#[given(\"a topic\", \"a stream\")]`",
        )
    })?;
    Ok(list.iter().cloned().collect())
}

/// What a step attribute adds around the function it decorates.
pub struct Expansion {
    /// The rstest-bdd attribute for the function itself.
    pub attribute: TokenStream,
    /// The forwarding functions for every further pattern and keyword.
    pub forwarders: TokenStream,
}

/// Expands `#[<attribute>(meta)]` on `item`.
pub fn expand(
    attribute: StepAttribute,
    meta: &TokenStream,
    item: &TokenStream,
) -> Result<Expansion, ParseError> {
    let literals = expressions(meta)?;
    let item: ItemFn = moxy::parse!(item as ItemFn)?;
    let span = literals
        .first()
        .map_or_else(|| item.sig.ident.span(), LitStr::span);
    let fail = |message: String| ParseError::new(span, message);

    let params = step_params(&item)?;
    let values: Vec<ParamInfo> = params
        .iter()
        .filter(|param| param.kind == ParamKind::Value)
        .map(|param| {
            ParamInfo::new(
                param.ident.text(),
                &param.param.ty.to_token_stream().to_string(),
            )
        })
        .collect();
    let step_args = params.iter().any(|param| param.kind == ParamKind::StepArgs);
    let returns = return_kind(&item.sig.output)
        .map_err(|message| ParseError::new(item.sig.output.span(), message))?;
    let sources: Vec<String> = literals
        .iter()
        .map(|literal| literal.value().to_owned())
        .collect();
    let patterns = step_patterns(&sources, &values, step_args, returns).map_err(fail)?;

    // The patterns carry a macro span: at the caller's span, Clippy would read
    // their placeholders as format arguments.
    let generated = Span::mixed_site();
    let mut registrations = patterns.iter().flat_map(|pattern| {
        attribute.keywords().iter().map(move |keyword| {
            (
                Ident::new(keyword).with_span(generated),
                LitStr::new(pattern, generated),
            )
        })
    });
    let Some((keyword, pattern)) = registrations.next() else {
        return Err(fail(
            "a step attribute registers at least one pattern".to_owned(),
        ));
    };
    let target = &item.sig.ident;
    let forwarders: Vec<(Ident, Ident, LitStr)> = registrations
        .enumerate()
        .map(|(index, (keyword, pattern))| {
            let name = format!("__krabka_bdd_{}_{}", target.text(), index + 1);
            (keyword, Ident::new(name).with_span(target.span()), pattern)
        })
        .collect();

    let vis = &item.vis;
    let asyncness = &item.sig.asyncness;
    let (generics, _, where_clause) = item.sig.generics.split();
    let output = &item.sig.output;
    let forwarders = moxy::template! {
        @for (keyword, name, pattern) in &forwarders {
            #[::rstest_bdd_macros::{{ keyword }}({{ pattern }})]
            #[doc(hidden)]
            {{ vis }} {{ asyncness }} fn {{ name }} {{ generics }} (
                @for param in &params {
                    {{ param.param.attrs }} {{ param.ident }}: {{ param.param.ty }},
                }
            ) {{ output }} {{ where_clause }} {
                {{ target }}(@for param in &params { {{ param.ident }}, })
                @if asyncness.is_some() { .await }
            }
        }
    };
    Ok(Expansion {
        attribute: moxy::template! { #[::rstest_bdd_macros::{{ keyword }}({{ pattern }})] },
        forwarders,
    })
}

/// Expands the attribute around the caller's native tokens, so the function
/// body keeps its own spans.
pub fn expand_native(
    attribute: StepAttribute,
    meta: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let expansion = expand(attribute, meta, &item.clone().into())?;
    let mut tokens = NativeTokenStream::from(expansion.attribute);
    tokens.extend(item);
    tokens.extend(NativeTokenStream::from(expansion.forwarders));
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            ParamKind, ReturnKind, StepAttribute, check_pattern, classify, expand, return_kind,
            step_params, step_patterns,
        },
        assert2::assert,
        krabka_bdd_model::cucumber::ParamInfo,
        moxy::{
            ast::{ItemFn, ReturnType},
            token::{ToTokenStream, TokenStream},
        },
        rstest::rstest,
    };

    /// `text` with `‹` and `›` for `{` and `}`, which keeps Clippy from
    /// reading the test patterns as format strings.
    fn braced(text: &str) -> String { text.replace('‹', "{").replace('›', "}") }

    fn values(declared: &[(&str, &str)]) -> Vec<ParamInfo> {
        declared
            .iter()
            .map(|(name, type_name)| ParamInfo::new(name, type_name))
            .collect()
    }

    fn kinds(source: &str) -> Vec<ParamKind> {
        let item: ItemFn = moxy::parse!({ source }).unwrap();
        step_params(&item)
            .unwrap()
            .iter()
            .map(|param| classify(param.param, param.ident.text()))
            .collect()
    }

    #[rstest]
    #[case::owned_value("fn s(count: u32) {}", &[ParamKind::Value])]
    #[case::borrowed_str("fn s(name: &str) {}", &[ParamKind::Value])]
    #[case::borrowed_str_lifetime("fn s<'a>(name: &'a str) {}", &[ParamKind::Value])]
    #[case::reference("fn s(world: &World) {}", &[ParamKind::Fixture])]
    #[case::mutable_reference("fn s(world: &mut World) {}", &[ParamKind::Fixture])]
    #[case::from("fn s(#[from] world: World) {}", &[ParamKind::Fixture])]
    #[case::from_named("fn s(#[from(other)] world: World) {}", &[ParamKind::Fixture])]
    #[case::harness_context("fn s(#[harness_context] ctx: Ctx) {}", &[ParamKind::Fixture])]
    #[case::datatable_attribute("fn s(#[datatable] rows: Rows) {}", &[ParamKind::DataTable])]
    #[case::datatable_name("fn s(datatable: Vec<Vec<String>>) {}", &[ParamKind::DataTable])]
    #[case::docstring("fn s(docstring: String) {}", &[ParamKind::DocString])]
    #[case::step_args("fn s(#[step_args] args: Args) {}", &[ParamKind::StepArgs])]
    #[case::mutable_binding("fn s(mut count: u32) {}", &[ParamKind::Value])]
    #[case::mixed(
        "fn s(world: &mut World, name: String, count: u8, datatable: Vec<Vec<String>>) {}",
        &[ParamKind::Fixture, ParamKind::Value, ParamKind::Value, ParamKind::DataTable]
    )]
    fn classifies_parameters_as_the_fork_binds_them(
        #[case] source: &str,
        #[case] expected: &[ParamKind],
    ) {
        assert!(kinds(source) == expected);
    }

    #[rstest]
    #[case::receiver(
        "fn s(&self) {}",
        "a step function takes no `self`: write it as a free function"
    )]
    #[case::tuple(
        "fn s((a, b): (u8, u8)) {}",
        "a step parameter binds a plain name: destructure it in the body"
    )]
    #[case::by_ref(
        "fn s(ref a: u8) {}",
        "a step parameter binds a plain name: destructure it in the body"
    )]
    fn rejects_unbindable_parameters(#[case] source: &str, #[case] message: &str) {
        let item: ItemFn = moxy::parse!({ source }).unwrap();
        assert!(let Err(error) = step_params(&item));
        assert!(error.to_string() == message);
    }

    fn output(source: &str) -> ReturnType {
        let item: ItemFn = moxy::parse!({ source }).unwrap();
        item.sig.output
    }

    #[rstest]
    #[case::none("fn s() {}", ReturnKind::Unit)]
    #[case::unit("fn s() -> () {}", ReturnKind::Unit)]
    #[case::result("fn s() -> Result<(), Error> {}", ReturnKind::UnitResult)]
    #[case::qualified_result("fn s() -> std::io::Result<()> {}", ReturnKind::UnitResult)]
    #[case::attempt("fn s() -> Attempt<Topic, Error> {}", ReturnKind::Attempt)]
    #[case::qualified_attempt("fn s() -> krabka_bdd::Attempt<(), Error> {}", ReturnKind::Attempt)]
    fn accepts_step_return_types(#[case] source: &str, #[case] expected: ReturnKind) {
        assert!(return_kind(&output(source)) == Ok(expected));
    }

    #[rstest]
    #[case::value("fn s() -> u32 {}", "u32")]
    #[case::value_result("fn s() -> Result<u32, Error> {}", "Result < u32 , Error >")]
    #[case::option("fn s() -> Option<()> {}", "Option < () >")]
    fn rejects_other_return_types(#[case] source: &str, #[case] shown: &str) {
        let message = format!(
            "a step returns `()`, `Result<(), E>` or `Attempt<T, E>`, not `{shown}`: keep other \
             values in the scenario's fixtures"
        );
        assert!(return_kind(&output(source)) == Err(message));
    }

    #[rstest]
    #[case::matching("‹name› has ‹count:u32› items", &[("name", "String"), ("count", "u32")])]
    #[case::underscore("‹count› items", &[("_count", "u32")])]
    #[case::no_placeholders("the broker starts", &[])]
    fn accepts_placeholders_in_parameter_order(
        #[case] pattern: &str,
        #[case] declared: &[(&str, &str)],
    ) {
        assert!(check_pattern(&braced(pattern), &values(declared)) == Ok(()));
    }

    #[rstest]
    #[case::swapped(
        "‹count› items in ‹name›",
        &[("name", "String"), ("count", "u32")],
        "`count`, `name`",
        "`name`, `count`"
    )]
    #[case::missing_parameter("‹count› items", &[], "`count`", "none")]
    #[case::extra_parameter("the broker starts", &[("count", "u32")], "none", "`count`")]
    #[case::misnamed("‹n› items", &[("count", "u32")], "`n`", "`count`")]
    fn rejects_placeholders_out_of_parameter_order(
        #[case] pattern: &str,
        #[case] declared: &[(&str, &str)],
        #[case] placeholders: &str,
        #[case] parameters: &str,
    ) {
        let pattern = braced(pattern);
        let message = format!(
            "step pattern `{pattern}` has placeholders {placeholders} but the step function's \
             non-fixture parameters are {parameters}: name each parameter after its \
             placeholder, in the same order, and take a fixture by reference or mark it \
             `#[from]`"
        );
        assert!(check_pattern(&pattern, &values(declared)) == Err(message));
    }

    #[test]
    fn rejects_an_invalid_pattern() {
        assert!(let Err(message) = check_pattern(&braced("a ‹n:› b"), &values(&[("n", "u8")])));
        assert!(message.starts_with(&braced("step pattern `a ‹n:› b` is invalid: ")));
    }

    fn sources(expressions: &[&str]) -> Vec<String> {
        expressions.iter().map(|text| braced(text)).collect()
    }

    #[rstest]
    #[case::one("a topic", &[], ReturnKind::Unit, &["a topic"])]
    #[case::cucumber(
        "a/an topic with ‹int› partition(s)",
        &[("partitions", "u32")],
        ReturnKind::Unit,
        &[
            "a topic with ‹partitions:u32› partition",
            "a topic with ‹partitions:u32› partitions",
            "an topic with ‹partitions:u32› partition",
            "an topic with ‹partitions:u32› partitions",
        ]
    )]
    #[case::attempt(
        "the client attempts to delete ‹string›",
        &[("topic", "String")],
        ReturnKind::Attempt,
        &["the client attempts to delete ‹topic:string›"]
    )]
    fn expands_one_expression(
        #[case] expression: &str,
        #[case] declared: &[(&str, &str)],
        #[case] returns: ReturnKind,
        #[case] expected: &[&str],
    ) {
        let patterns = step_patterns(&sources(&[expression]), &values(declared), false, returns);
        assert!(patterns == Ok(sources(expected)));
    }

    #[test]
    fn expands_several_expressions_in_order() {
        let patterns = step_patterns(
            &sources(&["a topic", "a stream"]),
            &[],
            false,
            ReturnKind::Unit,
        );
        assert!(patterns == Ok(sources(&["a topic", "a stream"])));
    }

    #[test]
    fn step_args_skip_the_order_check() {
        let patterns = step_patterns(&sources(&["‹a› and ‹b›"]), &[], true, ReturnKind::Unit);
        assert!(patterns == Ok(sources(&["‹a› and ‹b›"])));
    }

    #[rstest]
    #[case::no_expressions(
        &[],
        ReturnKind::Unit,
        "a step attribute takes one or more Cucumber expressions, such as \
         `#[given(\"a topic with ‹int› partitions\")]`"
    )]
    #[case::attempts_without_attempt(
        &["the client attempts to delete it"],
        ReturnKind::UnitResult,
        "step pattern `the client attempts to delete it` says \"attempts to\" or \"tries to\", \
         so the step returns an `Attempt<T, E>` for a later step to check"
    )]
    #[case::tries_without_attempt(
        &["the client tries to delete it"],
        ReturnKind::Unit,
        "step pattern `the client tries to delete it` says \"attempts to\" or \"tries to\", so \
         the step returns an `Attempt<T, E>` for a later step to check"
    )]
    #[case::listed_twice(
        &["a/an topic", "an topic"],
        ReturnKind::Unit,
        "step pattern `an topic` is listed twice: drop one of the expressions"
    )]
    #[case::cucumber_error(
        &["item()"],
        ReturnKind::Unit,
        "`item()` has empty optional text `()`: escape literal parentheses as `\\(\\)`"
    )]
    fn rejects_step_expressions(
        #[case] expressions: &[&str],
        #[case] returns: ReturnKind,
        #[case] message: &str,
    ) {
        let patterns = step_patterns(&sources(expressions), &[], false, returns);
        assert!(patterns == Err(braced(message)));
    }

    fn tokens(source: &str) -> TokenStream { source.parse().unwrap() }

    fn expand_source(
        attribute: StepAttribute,
        meta: &str,
        item: &str,
    ) -> Result<(String, String), String> {
        expand(attribute, &tokens(&braced(meta)), &tokens(item))
            .map(|expansion| {
                (
                    expansion.attribute.to_token_stream().to_string(),
                    expansion.forwarders.to_token_stream().to_string(),
                )
            })
            .map_err(|error| error.to_string())
    }

    /// `source` as moxy prints its tokens.
    fn printed(source: &str) -> String { tokens(&braced(source)).to_string() }

    #[test]
    fn one_pattern_needs_no_forwarder() {
        let expanded = expand_source(
            StepAttribute::Given,
            r#""a topic with ‹int› partitions""#,
            "fn topic(world: &mut World, partitions: u32) { world.create(partitions); }",
        );
        assert!(
            expanded
                == Ok((
                    printed(
                        r#"#[::rstest_bdd_macros::given("a topic with ‹partitions:u32› partitions")]"#
                    ),
                    String::new(),
                ))
        );
    }

    #[test]
    fn further_patterns_forward_to_the_function() {
        let expanded = expand_source(
            StepAttribute::When,
            r#""a/an topic", "a stream""#,
            "async fn create(#[from] world: World, mut docstring: String) -> Result<(), Error> { Ok(()) }",
        );
        let forwarders = printed(
            r#"
            #[::rstest_bdd_macros::when("an topic")]
            #[doc(hidden)]
            async fn __krabka_bdd_create_1(#[from] world: World, docstring: String,) -> Result<(), Error> {
                create(world, docstring,).await
            }
            #[::rstest_bdd_macros::when("a stream")]
            #[doc(hidden)]
            async fn __krabka_bdd_create_2(#[from] world: World, docstring: String,) -> Result<(), Error> {
                create(world, docstring,).await
            }
            "#,
        );
        assert!(
            expanded
                == Ok((
                    printed(r#"#[::rstest_bdd_macros::when("a topic")]"#),
                    forwarders
                ))
        );
    }

    #[test]
    fn step_registers_every_keyword() {
        let expanded = expand_source(
            StepAttribute::Step,
            r#""the broker is up""#,
            "pub fn up(world: &World) {}",
        );
        let forwarders = printed(
            r#"
            #[::rstest_bdd_macros::when("the broker is up")]
            #[doc(hidden)]
            pub fn __krabka_bdd_up_1(world: &World,) { up(world,) }
            #[::rstest_bdd_macros::then("the broker is up")]
            #[doc(hidden)]
            pub fn __krabka_bdd_up_2(world: &World,) { up(world,) }
            "#,
        );
        assert!(
            expanded
                == Ok((
                    printed(r#"#[::rstest_bdd_macros::given("the broker is up")]"#),
                    forwarders,
                ))
        );
    }

    #[rstest]
    #[case::not_a_literal(
        "a_topic",
        "fn s() {}",
        "a step attribute takes Cucumber expressions as string literals: \
         `#[given(\"a topic\", \"a stream\")]`"
    )]
    #[case::empty(
        "",
        "fn s() {}",
        "a step attribute takes one or more Cucumber expressions, such as \
         `#[given(\"a topic with ‹int› partitions\")]`"
    )]
    #[case::order(
        r#""‹count› items in ‹name›""#,
        "fn s(name: String, count: u32) {}",
        "step pattern `‹count:u32› items in ‹name›` has placeholders `count`, `name` but \
         the step function's non-fixture parameters are `name`, `count`: name each parameter \
         after its placeholder, in the same order, and take a fixture by reference or mark it \
         `#[from]`"
    )]
    #[case::return_type(
        r#""a topic""#,
        "fn s() -> u32 { 1 }",
        "a step returns `()`, `Result<(), E>` or `Attempt<T, E>`, not `u32`: keep other values \
         in the scenario's fixtures"
    )]
    fn rejects_malformed_attributes(#[case] meta: &str, #[case] item: &str, #[case] message: &str) {
        assert!(expand_source(StepAttribute::Then, meta, item) == Err(braced(message)));
    }
}
