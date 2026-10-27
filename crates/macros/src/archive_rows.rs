//! Share the archive walk while keeping Creusot's bindings in one expansion context.

use {
    crate::{fixtures::name, meta::arguments},
    moxy::{
        ast::ParseError,
        token::{Ident, Span, TokenStream},
    },
    std::mem,
};

pub fn expand(input: TokenStream) -> Result<TokenStream, ParseError> {
    let mut sections = Vec::new();
    let mut current = TokenStream::new();
    for token in input {
        // The fifth section, the result body, keeps its own semicolons.
        if token.is_punct_semi() && sections.len() < 4 {
            sections.push(mem::take(&mut current));
        } else {
            current.extend([token]);
        }
    }
    sections.push(current);
    let [signature, bindings, invariants, valid, after]: [TokenStream; 5] =
        sections.try_into().map_err(|_| {
            ParseError::new(
                Span::call_site(),
                "expected signature; bindings; invariants; validity; result body",
            )
        })?;
    let names = arguments(bindings, 5)?;
    let names = names
        .into_iter()
        .map(name)
        .collect::<Result<Vec<Ident>, _>>()?;
    let [entries, index, previous, first, second]: [Ident; 5] = names
        .try_into()
        .map_err(|_| ParseError::new(Span::call_site(), "expected five bindings"))?;
    if signature.is_empty() || valid.is_empty() || after.is_empty() {
        return Err(ParseError::new(
            Span::call_site(),
            "archive walk needs a signature, validity expression and result body",
        ));
    }
    let view: TokenStream = "@"
        .parse()
        .map_err(|_| ParseError::new(Span::call_site(), "Creusot view operator"))?;
    Ok(moxy::template! {
        {{ signature }} {
            let mut {{ index }} = 0usize;
            #[invariant({{ index }}{{ view }} <= {{ entries }}{{ view }}.len())]
            {{ invariants }}
            #[variant({{ entries }}{{ view }}.len() - {{ index }}{{ view }})]
            while {{ index }} < {{ entries }}.len() {
                let {{ previous }} = if {{ index }} == 0 { None } else { Some({{ entries }}[{{ index }} - 1]) };
                let {{ first }} = {{ entries }}[{{ index }}].key;
                let {{ second }} = {{ entries }}[{{ index }}].coordinate;
                if !({{ valid }}) {
                    return Err(());
                }
                {{ index }} += 1;
            }
            {{ after }}
        }
    })
}
