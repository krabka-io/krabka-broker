//! `cli_main!`: see the crate documentation.

use moxy::{
    ast::ParseError,
    token::{Span, TokenStream, TokenTree},
};

/// Splits `tokens` at its first top-level comma into the crate path and the
/// `#[tokio::main(...)]` arguments. A comma inside a group belongs to the
/// group, so only the outermost one splits.
fn split(tokens: &[TokenTree]) -> (&[TokenTree], &[TokenTree]) {
    match tokens.iter().position(TokenTree::is_punct_comma) {
        Some(comma) => (&tokens[..comma], &tokens[comma + 1..]),
        None => (tokens, &[]),
    }
}

/// Expands `cli_main!(crate_path, tokio arguments...)` into the binary's
/// `main`.
pub(crate) fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens = Vec::from(tokens);
    let (path, runtime) = split(&tokens);
    if path.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "`cli_main!` needs the path of the crate whose `run_from_args` it calls",
        ));
    }
    let path = TokenStream::from(path);
    let runtime = TokenStream::from(runtime);
    Ok(moxy::template! {
        @if runtime.is_empty() {
            #[::tokio::main]
        } @else {
            #[::tokio::main({{ runtime }})]
        }
        async fn main() {
            ::tracing_subscriber::fmt()
                .with_env_filter(
                    ::tracing_subscriber::EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| ::tracing_subscriber::EnvFilter::new("info")),
                )
                .init();
            ::std::process::exit({{ path }}::run_from_args(::std::env::args_os()).await);
        }
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use moxy::token::TokenStream;

    use super::split;

    /// `tokens` parsed, split, and each side printed without whitespace.
    fn split_text(tokens: &str) -> (String, String) {
        let tokens: TokenStream = tokens.parse().expect("tokens");
        let compact = |side: &[_]| {
            TokenStream::from(side)
                .to_string()
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>()
        };
        let (path, runtime) = split(&tokens);
        (compact(path), compact(runtime))
    }

    #[test]
    fn the_first_top_level_comma_ends_the_path() {
        for (tokens, path, runtime) in [
            ("krabka_format", "krabka_format", ""),
            ("krabka_format,", "krabka_format", ""),
            (
                "crate::cli, flavor = \"multi_thread\"",
                "crate::cli",
                "flavor=\"multi_thread\"",
            ),
            (
                "app, flavor = \"multi_thread\", worker_threads = 2",
                "app",
                "flavor=\"multi_thread\",worker_threads=2",
            ),
        ] {
            assert!(
                split_text(tokens) == (path.to_owned(), runtime.to_owned()),
                "{tokens}"
            );
        }
    }
}
