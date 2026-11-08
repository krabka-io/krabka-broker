//! Held-listener broker startup and its exact single-voter configuration.

use {
    crate::fixtures::CommaArguments,
    moxy::{
        ast::{ParseError, Visibility},
        token::{Span, TokenStream, TokenTree},
    },
};

/// Read epoch milliseconds with each caller's original checked conversions.
pub fn unix_millis(input: TokenStream) -> Result<TokenStream, ParseError> {
    let arguments = CommaArguments::new(input);
    let parts = arguments.parts();
    let [binding, epoch_context, conversion_context] = parts.as_slice() else {
        return Err(ParseError::new(
            arguments.span,
            "expected name and two panic contexts",
        ));
    };
    let [visibility @ .., TokenTree::Ident(name)] = *binding else {
        return Err(ParseError::new(arguments.span, "expected a function name"));
    };
    if epoch_context.is_empty() || conversion_context.is_empty() {
        return Err(ParseError::new(
            arguments.span,
            "expected two panic contexts",
        ));
    }
    let visibility = TokenStream::from(visibility);
    let visibility = moxy::parse!({ visibility } as Visibility)?;
    let epoch_context = TokenStream::from(*epoch_context);
    let conversion_context = TokenStream::from(*conversion_context);
    Ok(moxy::template! {
        /// Current wall-clock milliseconds since the Unix epoch.
        ///
        /// # Panics
        /// Panics if the clock precedes the epoch or milliseconds do not fit an i64.
        {{ visibility }} fn {{ name }}() -> i64 {
            i64::try_from(
                ::std::time::SystemTime::now()
                    .duration_since(::std::time::UNIX_EPOCH)
                    .expect({{ epoch_context }})
                    .as_millis(),
            ).expect({{ conversion_context }})
        }
    })
}

/// Preserve the Vec frame allocation and each fixture's signed length limit.
pub fn vector_request(input: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens = Vec::from(input);
    let [TokenTree::Ident(name), comma, TokenTree::Ident(length)] = tokens.as_slice() else {
        return Err(ParseError::new(Span::call_site(), "expected name, u32|i32"));
    };
    if !comma.is_punct_comma() || !(length == "u32" || length == "i32") {
        return Err(ParseError::new(
            length.span(),
            "expected u32 or i32 frame length",
        ));
    }
    Ok(moxy::template! {
        fn {{ name }}(
            api_key: i16, version: i16, correlation_id: i32, flexible: bool, body: &[u8],
        ) -> Vec<u8> {
            let mut frame = Vec::new();
            frame.extend_from_slice(&api_key.to_be_bytes());
            frame.extend_from_slice(&version.to_be_bytes());
            frame.extend_from_slice(&correlation_id.to_be_bytes());
            frame.extend_from_slice(&1i16.to_be_bytes());
            frame.push(b'c');
            if flexible { frame.push(0); }
            frame.extend_from_slice(body);
            let mut out = {{ length }}::try_from(frame.len()).unwrap().to_be_bytes().to_vec();
            out.extend_from_slice(&frame);
            out
        }
    })
}
