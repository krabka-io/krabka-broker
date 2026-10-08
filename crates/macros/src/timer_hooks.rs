//! Generate the timer registration and completion guards with caller-owned docs.

use moxy::{
    ast::ParseError,
    token::{Lit, Span, TokenStream, TokenTree},
};

pub(crate) fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut input = input.into_iter().peekable();
    let mut prefix = TokenStream::new();
    let mut output = TokenStream::new();
    let mut selected = Vec::new();
    while let Some(token) = input.next() {
        let Some(name) = token
            .as_ident()
            .filter(|name| *name == "arm" || *name == "fired")
        else {
            prefix.extend([token]);
            continue;
        };
        let name = name.clone();
        if selected
            .iter()
            .any(|selected: &moxy::token::Ident| selected.text() == name.text())
        {
            return Err(ParseError::new(
                name.span(),
                "timer hook selected more than once",
            ));
        }
        selected.push(name.clone());
        let Some(TokenTree::Group(arguments)) = input.next() else {
            return Err(ParseError::new(name.span(), "expected one failure message"));
        };
        let message = arguments.tokens;
        if !matches!(&*message, [TokenTree::Literal(Lit::Str(_))]) {
            return Err(ParseError::new(
                name.span(),
                "expected one literal failure message",
            ));
        }
        if !input.next().is_some_and(|token| token.is_punct_semi()) {
            return Err(ParseError::new(
                name.span(),
                "expected a semicolon after the timer hook",
            ));
        }
        let method = if name == "arm" {
            moxy::template! {
                {{ prefix }} fn arm(
                    timer: &dyn ::qubit_clock::Timer,
                    delay: ::std::time::Duration,
                    task: &'static str,
                ) -> Option<::qubit_clock::TimerFuture> {
                    match timer.after(delay) {
                        Ok(future) => Some(future),
                        Err(error) => {
                            ::tracing::error!(%error, task, {{ message }});
                            None
                        }
                    }
                }
            }
        } else {
            moxy::template! {
                {{ prefix }} fn fired(
                    outcome: Result<(), ::qubit_clock::TimeError>,
                    task: &'static str,
                ) -> bool {
                    match outcome {
                        Ok(()) => true,
                        Err(error) => {
                            ::tracing::error!(%error, task, {{ message }});
                            false
                        }
                    }
                }
            }
        };
        output.extend(method);
        prefix = TokenStream::new();
    }
    if selected.is_empty() || !prefix.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "expected documented arm(\"message\"); or fired(\"message\"); hooks",
        ));
    }
    Ok(output)
}
