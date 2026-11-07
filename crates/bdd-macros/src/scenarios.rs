//! `scenarios!`: lint the features under a directory, then generate one test
//! per scenario and per outline row.
//!
//! The expansion is either one `compile_error!` per house-style error, or a
//! module tree with a `#[tokio::test]` for every scenario the call runs, each
//! handing a static `ScenarioSpec` and its fixtures to the krabka-bdd runner,
//! plus one registry-check test.

mod codegen;

pub use self::codegen::Runtime;
use {
    gherkin::{Feature, GherkinEnv, tagexpr::TagOperation},
    krabka_bdd_model::{
        glossary::Glossary,
        lint::{self, Finding, LintContext, Severity},
        model,
        plan::{self, Plan},
    },
    moxy::{
        ast::ParseError,
        token::{LitInt, LitStr, Span, TokenStream, TokenTree},
    },
    std::{
        env, fs, io, mem,
        path::{Path, PathBuf},
    },
};

/// One `name = value` argument, or the bare directory literal.
struct Argument {
    name: Option<String>,
    tokens: Vec<TokenTree>,
}

/// One `name: Type` entry of `fixtures = [..]`.
pub struct Fixture {
    /// The fixture's name, which is also the rstest fixture fn it calls.
    pub name: String,
    /// The fixture's type, as the caller wrote it.
    pub ty: TokenStream,
}

/// What the expansion reads from the caller's arguments.
#[derive(Default)]
pub struct Arguments {
    /// The feature directory, relative to the crate's manifest.
    dir: Option<LitStr>,
    /// `tags = "..."`: only the scenarios this expression matches run.
    tags: Option<LitStr>,
    /// `fixtures = [name: Type, ..]`.
    pub fixtures: Vec<Fixture>,
    /// `world = name`: the fixture whose `World` hooks run.
    pub world: Option<String>,
    /// `runtime = ".."` and `workers = N`.
    pub runtime: Runtime,
}

/// Expands `scenarios!(...)`.
pub fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = parse_arguments(tokens)?;
    let dir = arguments
        .dir
        .clone()
        .ok_or_else(|| ParseError::new(Span::call_site(), "the feature directory is required"))?;
    let manifest = env::var("CARGO_MANIFEST_DIR")
        .map(PathBuf::from)
        .map_err(|error| ParseError::new(dir.span(), format!("CARGO_MANIFEST_DIR: {error}")))?;
    let include = tag_expression(arguments.tags.as_ref())?;
    let glossary = Glossary::embedded()
        .map_err(|message| ParseError::new(dir.span(), format!("glossary.toml: {message}")))?;

    let files = feature_files(&manifest.join(dir.value()))
        .map_err(|error| ParseError::new(dir.span(), format!("{}: {error}", dir.value())))?;
    let mut errors = Vec::new();
    let mut plans = Vec::new();
    for file in &files {
        let source = fs::read_to_string(file)
            .map_err(|error| ParseError::new(dir.span(), format!("{}: {error}", file.display())))?;
        let shown = display_path(file, &manifest);
        let feature_dir = file.parent().map(Path::to_path_buf).unwrap_or_default();
        let exists = |relative: &str| feature_dir.join(relative).is_file();
        let context = LintContext {
            glossary,
            file_exists: &exists,
        };
        let file_errors: Vec<Finding> = lint::lint(&shown, &source, &context)
            .into_iter()
            .filter(|finding| finding.severity == Severity::Error)
            .collect();
        if file_errors.is_empty()
            && let Ok(feature) = Feature::parse(&source, GherkinEnv::default())
        {
            plans.push(filtered(plan::plan(&shown, &feature), include.as_ref()));
        }
        errors.extend(file_errors);
    }
    if !errors.is_empty() {
        return Ok(compile_errors(&errors));
    }
    if plans.iter().all(|plan| plan.scenarios.is_empty()) {
        return Err(ParseError::new(
            dir.span(),
            format!("no scenarios to run under `{}`", dir.value()),
        ));
    }
    if let Some(world) = &arguments.world
        && !arguments
            .fixtures
            .iter()
            .any(|fixture| &fixture.name == world)
    {
        return Err(ParseError::new(
            dir.span(),
            format!("`world = {world}` must name one of the `fixtures`"),
        ));
    }
    let tracked: Vec<String> = files
        .iter()
        .map(|file| file.to_string_lossy().into_owned())
        .collect();
    Ok(codegen::expansion(
        &arguments,
        &module_base(dir.value()),
        &plans,
        &tracked,
    ))
}

/// `plan` without the scenarios that `include` leaves out.
fn filtered(mut plan: Plan, include: Option<&TagOperation>) -> Plan {
    if let Some(include) = include {
        plan.scenarios
            .retain(|scenario| model::tags_match(include, &scenario.tags));
    }
    plan
}

/// Splits the arguments at their top-level commas and reads each.
fn parse_arguments(tokens: TokenStream) -> Result<Arguments, ParseError> {
    let mut arguments = Arguments::default();
    let mut runtime = None;
    let mut workers = None;
    for argument in split_arguments(tokens) {
        match argument.name.as_deref() {
            None | Some("dir" | "path") => {
                arguments.dir = Some(string_value(&argument, "the feature directory")?);
            }
            Some("tags") => arguments.tags = Some(string_value(&argument, "`tags`")?),
            Some("fixtures") => arguments.fixtures = fixtures(&argument)?,
            Some("world") => arguments.world = Some(ident_value(&argument, "`world`")?),
            Some("runtime") => runtime = Some(string_value(&argument, "`runtime`")?),
            Some("workers") => workers = Some(int_value(&argument)?),
            Some(other) => {
                return Err(ParseError::new(
                    first_span(&argument),
                    format!(
                        "unknown argument `{other}`: expected the directory, `tags`, \
                         `fixtures`, `world`, `runtime` or `workers`"
                    ),
                ));
            }
        }
    }
    arguments.runtime = Runtime::from_arguments(runtime.as_ref(), workers)?;
    Ok(arguments)
}

/// The arguments between top-level commas. Brackets and parentheses are
/// single groups, so the commas inside `fixtures = [..]` do not split.
fn split_arguments(tokens: TokenStream) -> Vec<Argument> {
    split_top_level(tokens.into_iter())
        .into_iter()
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

/// Splits tokens at the commas outside angle brackets, dropping empty parts.
fn split_top_level(tokens: impl Iterator<Item = TokenTree>) -> Vec<Vec<TokenTree>> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut depth = 0_usize;
    for token in tokens {
        if let TokenTree::Punct(punct) = &token {
            match punct.as_str() {
                "<" => depth += 1,
                ">" => depth = depth.saturating_sub(1),
                "," if depth == 0 => {
                    parts.push(mem::take(&mut current));
                    continue;
                }
                _ => {}
            }
        }
        current.push(token);
    }
    parts.push(current);
    parts.retain(|part| !part.is_empty());
    parts
}

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
        [TokenTree::Literal(literal)] => literal.as_str().cloned().ok_or_else(|| {
            ParseError::new(literal.span(), format!("{what} must be a string literal"))
        }),
        _ => Err(ParseError::new(
            first_span(argument),
            format!("{what} must be a string literal"),
        )),
    }
}

/// The identifier an argument holds.
fn ident_value(argument: &Argument, what: &str) -> Result<String, ParseError> {
    match value_tokens(argument) {
        [TokenTree::Ident(ident)] => Ok(ident.text().to_owned()),
        _ => Err(ParseError::new(
            first_span(argument),
            format!("{what} must be a fixture name"),
        )),
    }
}

/// The integer literal `workers = N` holds.
fn int_value(argument: &Argument) -> Result<u128, ParseError> {
    match value_tokens(argument) {
        [TokenTree::Literal(literal)] => literal
            .as_int()
            .map(LitInt::value)
            .filter(|workers| *workers > 0)
            .ok_or_else(|| ParseError::new(literal.span(), "`workers` must be a positive integer")),
        _ => Err(ParseError::new(
            first_span(argument),
            "`workers` must be a positive integer",
        )),
    }
}

/// The entries of `fixtures = [name: Type, ..]`.
fn fixtures(argument: &Argument) -> Result<Vec<Fixture>, ParseError> {
    let malformed = || {
        ParseError::new(
            first_span(argument),
            "`fixtures` must be `[name: Type, ..]`",
        )
    };
    let [TokenTree::Group(group)] = value_tokens(argument) else {
        return Err(malformed());
    };
    split_top_level(group.tokens.clone().into_iter())
        .into_iter()
        .map(|entry| match entry.as_slice() {
            [TokenTree::Ident(name), TokenTree::Punct(colon), ty @ ..]
                if colon.as_str() == ":" && !ty.is_empty() =>
            {
                Ok(Fixture {
                    name: name.text().to_owned(),
                    ty: ty.iter().cloned().collect(),
                })
            }
            _ => Err(malformed()),
        })
        .collect()
}

fn tag_expression(expression: Option<&LitStr>) -> Result<Option<TagOperation>, ParseError> {
    expression
        .map(|literal| {
            model::parse_tag_expression(literal.value())
                .map_err(|message| ParseError::new(literal.span(), message))
        })
        .transpose()
}

/// Every `.feature` file under `dir`, in path order.
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

/// The directory's last component as an identifier.
fn module_base(dir: &str) -> String {
    let base = Path::new(dir)
        .file_name()
        .map_or_else(|| "features".into(), |name| name.to_string_lossy());
    plan::ident(&base)
}

#[cfg(test)]
mod tests {
    use {
        super::{module_base, split_top_level},
        assert2::assert,
        moxy::token::TokenStream,
        rstest::rstest,
    };

    #[rstest]
    #[case("tests/features/log_policy", "log_policy")]
    #[case("tests/features/Multi-Thread", "multi_thread")]
    #[case("tests/features/3x", "_3x")]
    fn module_base_is_an_identifier(#[case] dir: &str, #[case] expected: &str) {
        assert!(module_base(dir) == expected);
    }

    #[rstest]
    #[case("a: A, b: B", 2)]
    #[case("map: HashMap<K, V>, b: B", 2)]
    #[case("a: Vec<Vec<u8>>,", 1)]
    fn split_top_level_keeps_generic_arguments_together(
        #[case] source: &str,
        #[case] parts: usize,
    ) {
        let tokens: TokenStream = source.parse().unwrap();
        assert!(split_top_level(tokens.into_iter()).len() == parts);
    }
}
