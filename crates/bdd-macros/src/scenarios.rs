//! `scenarios!`: lint the features under a directory, then run them.
//!
//! The expansion is either one `compile_error!` per house-style finding, or
//! the caller's `rstest_bdd_macros::scenarios!` call with Krabka's defaults
//! plus a module holding the step-registry check.

use std::{
    env,
    fs,
    io,
    mem,
    path::{Path, PathBuf},
};

use gherkin::{Feature, GherkinEnv, tagexpr::TagOperation};
use moxy::{
    ast::ParseError,
    token::{Ident, LitStr, Span, TokenStream, TokenTree},
};

use crate::{
    lint::{self, Finding},
    model::{self, Keyword, StepUse},
};

/// The scenarios a gating run leaves out: reviewed but not automated, or
/// automated but not finished. The Gherkin style guide's tag table defines
/// both tags.
pub const DEFAULT_IGNORE_TAGS: &str = "@formulated or @inprogress";

/// The `runtime` value that selects the multi-thread Tokio harness.
const MULTI_THREAD_RUNTIME: &str = "tokio-multi-thread";

/// One `name = value` argument, or the bare directory literal.
struct Argument {
    name: Option<String>,
    tokens: Vec<TokenTree>,
}

/// What the expansion needs to know about the caller's arguments.
#[derive(Default)]
struct Arguments {
    /// The feature directory, relative to the crate's manifest.
    dir: Option<LitStr>,
    /// The caller's `tags` filter, which is forwarded and also narrows the
    /// step check.
    tags: Option<LitStr>,
    /// The caller's `ignore_tags`, in place of [`DEFAULT_IGNORE_TAGS`].
    ignore_tags: Option<LitStr>,
    /// `workers = N` for the multi-thread runtime.
    workers: Option<u128>,
    /// `runtime = "tokio-multi-thread"` was given.
    multi_thread: bool,
    /// Every argument rstest-bdd takes as it is, in the caller's order.
    forwarded: Vec<Vec<TokenTree>>,
}

/// Expands `scenarios!(...)`.
pub fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = parse_arguments(tokens)?;
    let dir = arguments
        .dir
        .as_ref()
        .ok_or_else(|| ParseError::new(Span::call_site(), "the feature directory is required"))?;
    let manifest = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .map_err(|error| ParseError::new(dir.span(), format!("CARGO_MANIFEST_DIR: {error}")))?;
    let include = tag_expression(arguments.tags.as_ref())?;
    let ignore = match &arguments.ignore_tags {
        Some(expression) => tag_expression(Some(expression))?,
        None => model::parse_tag_expression(DEFAULT_IGNORE_TAGS)
            .map(Some)
            .map_err(|message| ParseError::new(dir.span(), message))?,
    };

    let files = feature_files(&manifest.join(dir.value()))
        .map_err(|error| ParseError::new(dir.span(), format!("{}: {error}", dir.value())))?;
    let mut findings = Vec::new();
    let mut uses = Vec::new();
    for file in &files {
        let source = fs::read_to_string(file)
            .map_err(|error| ParseError::new(dir.span(), format!("{}: {error}", file.display())))?;
        let shown = display_path(file, &manifest);
        let file_findings = lint::lint(&shown, &source);
        if file_findings.is_empty()
            && let Ok(feature) = Feature::parse(&source, GherkinEnv::default())
        {
            uses.extend(model::step_uses(&shown, &feature, include.as_ref(), ignore.as_ref()));
        }
        findings.extend(file_findings);
    }
    if !findings.is_empty() {
        return Ok(compile_errors(&findings));
    }
    Ok(expansion(&arguments, dir, &uses))
}

/// Splits the arguments at their top-level commas and sorts out the ones
/// this macro reads.
fn parse_arguments(tokens: TokenStream) -> Result<Arguments, ParseError> {
    let mut arguments = Arguments::default();
    for argument in split_arguments(tokens) {
        match argument.name.as_deref() {
            None | Some("dir" | "path") => {
                arguments.dir = Some(string_value(&argument, "the feature directory")?);
                arguments.forwarded.push(argument_tokens(&argument));
            }
            Some("tags") => {
                arguments.tags = Some(string_value(&argument, "`tags`")?);
                arguments.forwarded.push(argument_tokens(&argument));
            }
            Some("ignore_tags") => {
                arguments.ignore_tags = Some(string_value(&argument, "`ignore_tags`")?);
            }
            Some("workers") => arguments.workers = Some(int_value(&argument)?),
            Some("runtime") if string_value(&argument, "`runtime`")?.value() == MULTI_THREAD_RUNTIME => {
                arguments.multi_thread = true;
            }
            Some(_) => arguments.forwarded.push(argument_tokens(&argument)),
        }
    }
    if arguments.workers.is_some() && !arguments.multi_thread {
        return Err(ParseError::new(
            Span::call_site(),
            format!("`workers` needs `runtime = \"{MULTI_THREAD_RUNTIME}\"`"),
        ));
    }
    Ok(arguments)
}

/// The arguments between top-level commas. Brackets and parentheses are
/// single groups, so the commas inside `fixtures = [..]` do not split.
fn split_arguments(tokens: TokenStream) -> Vec<Argument> {
    let mut arguments = Vec::new();
    let mut current = Vec::new();
    for token in tokens {
        if matches!(&token, TokenTree::Punct(punct) if punct.as_str() == ",") {
            arguments.push(mem::take(&mut current));
        } else {
            current.push(token);
        }
    }
    arguments.push(current);
    arguments
        .into_iter()
        .filter(|tokens| !tokens.is_empty())
        .map(|tokens| {
            let named = matches!(
                (tokens.first(), tokens.get(1)),
                (Some(TokenTree::Ident(_)), Some(TokenTree::Punct(punct))) if punct.as_str() == "="
            );
            let name = if named {
                tokens.first().and_then(TokenTree::text).map(str::to_owned)
            } else {
                None
            };
            Argument { name, tokens }
        })
        .collect()
}

/// The tokens of an argument as the caller wrote them.
fn argument_tokens(argument: &Argument) -> Vec<TokenTree> { argument.tokens.clone() }

/// The tokens after `name =`, or every token of a positional argument.
fn value_tokens(argument: &Argument) -> &[TokenTree] {
    if argument.name.is_some() {
        argument.tokens.get(2..).unwrap_or_default()
    } else {
        &argument.tokens
    }
}

fn first_span(argument: &Argument) -> Span {
    argument
        .tokens
        .first()
        .map_or_else(Span::call_site, TokenTree::span)
}

/// The string literal an argument holds.
fn string_value(argument: &Argument, what: &str) -> Result<LitStr, ParseError> {
    match value_tokens(argument) {
        [TokenTree::Literal(literal)] => literal
            .as_str()
            .cloned()
            .ok_or_else(|| ParseError::new(literal.span(), format!("{what} must be a string literal"))),
        _ => Err(ParseError::new(
            first_span(argument),
            format!("{what} must be a string literal"),
        )),
    }
}

/// The integer literal `workers = N` holds.
fn int_value(argument: &Argument) -> Result<u128, ParseError> {
    match value_tokens(argument) {
        [TokenTree::Literal(literal)] => literal
            .as_int()
            .map(|int| int.value())
            .filter(|workers| *workers > 0)
            .ok_or_else(|| ParseError::new(literal.span(), "`workers` must be a positive integer")),
        _ => Err(ParseError::new(
            first_span(argument),
            "`workers` must be a positive integer",
        )),
    }
}

fn tag_expression(expression: Option<&LitStr>) -> Result<Option<TagOperation>, ParseError> {
    expression
        .map(|literal| {
            model::parse_tag_expression(literal.value())
                .map_err(|message| ParseError::new(literal.span(), message))
        })
        .transpose()
}

/// Every `.feature` file under `dir`, in path order, as rstest-bdd finds them.
fn feature_files(dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next)? {
            let path = entry?.path();
            if path.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("feature"))
            {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

/// `file` relative to the manifest directory, with `/` separators.
fn display_path(file: &Path, manifest: &Path) -> String {
    let relative = file.strip_prefix(manifest).unwrap_or(file);
    relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// One `compile_error!` per finding.
fn compile_errors(findings: &[Finding]) -> TokenStream {
    let messages: Vec<LitStr> = findings
        .iter()
        .map(|finding| LitStr::new(&finding.to_string(), Span::call_site()))
        .collect();
    moxy::template! {
        @for message in &messages {
            ::core::compile_error!({{ message }});
        }
    }
}

/// The rstest-bdd call and the step check.
fn expansion(arguments: &Arguments, dir: &LitStr, uses: &[StepUse]) -> TokenStream {
    let forwarded: Vec<TokenStream> = arguments
        .forwarded
        .iter()
        .map(|tokens| tokens.iter().cloned().collect())
        .collect();
    let harness = arguments.multi_thread.then(|| {
        let workers: TokenStream = arguments
            .workers
            .unwrap_or_default()
            .to_string()
            .parse()
            .unwrap_or_default();
        moxy::template! {
            harness = ::krabka_bdd::TokioMultiThreadHarness<{{ workers }}>,
            attributes = ::krabka_bdd::MultiThreadAttributePolicy,
        }
    });
    let module = Ident::new(&format!("{}_steps", module_base(dir.value())));
    let locations: Vec<LitStr> = uses
        .iter()
        .map(|step| LitStr::new(&step.location, Span::call_site()))
        .collect();
    let keywords: Vec<Ident> = uses.iter().map(|step| keyword_ident(step.keyword)).collect();
    let texts: Vec<LitStr> = uses
        .iter()
        .map(|step| LitStr::new(&step.text, Span::call_site()))
        .collect();
    moxy::template! {
        ::rstest_bdd_macros::scenarios!(
            @for argument in &forwarded { {{ argument }}, }
            @if let Some(harness) = &harness { {{ harness }} }
        );

        /// Every step of these features matches exactly one step definition.
        mod {{ module }} {
            #[test]
            fn every_step_matches_one_definition() {
                ::krabka_bdd::__private::check_steps(&[
                    @for ((location, keyword), text) in locations.iter().zip(&keywords).zip(&texts) {
                        ::krabka_bdd::__private::StepUse {
                            location: {{ location }},
                            keyword: ::krabka_bdd::__private::StepKeyword::{{ keyword }},
                            text: {{ text }},
                        },
                    }
                ]);
            }
        }
    }
}

fn keyword_ident(keyword: Keyword) -> Ident {
    Ident::new(match keyword {
        Keyword::Given => "Given",
        Keyword::When => "When",
        Keyword::Then => "Then",
    })
}

/// The directory's last component as an identifier, as rstest-bdd names its
/// own module after it.
fn module_base(dir: &str) -> String {
    let base = Path::new(dir)
        .file_name()
        .map_or_else(|| "features".into(), |name| name.to_string_lossy());
    let ident: String = base
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() {
                character.to_ascii_lowercase()
            } else {
                '_'
            }
        })
        .collect();
    if ident.starts_with(|character: char| character.is_ascii_digit()) {
        format!("_{ident}")
    } else {
        ident
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use rstest::rstest;

    use super::module_base;

    #[rstest]
    #[case("tests/features/log_policy", "log_policy")]
    #[case("tests/features/Multi-Thread", "multi_thread")]
    #[case("tests/features/3x", "_3x")]
    fn module_base_is_an_identifier(#[case] dir: &str, #[case] expected: &str) {
        assert!(module_base(dir) == expected);
    }
}
