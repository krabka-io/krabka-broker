//! Public wire constants, Kafka's name and default message for each, and
//! ordered private guard tables from one declaration.

use {
    moxy::{
        ast::{Attributes, Parse, ParseError, Parser, Token},
        token::{Group, Ident, LitInt, LitStr, Spanner, ToTokens, TokenStream},
    },
    std::collections::HashSet,
};

struct Code {
    attrs: Attributes,
    name: Ident,
    value: TokenStream,
    /// Kafka's `Errors.message()` for the code. The release and trunk
    /// sections carry one on every row; the private section carries none.
    message: Option<LitStr>,
}

impl Code {
    fn parse(parser: &Parser, with_message: bool) -> Result<Self, ParseError> {
        let attrs = Attributes::parse(parser)?;
        let name = Ident::parse(parser)?;
        <Token![=]>::parse(parser)?;
        let negative = Option::<Token![-]>::parse(parser)?;
        let value = LitInt::parse(parser)?;
        let bound = if negative.is_some() { 32768 } else { 32767 };
        if value.value() > bound {
            return Err(ParseError::new(value.span(), "wire code must fit i16"));
        }
        let message = if with_message {
            <Token![=>]>::parse(parser)?;
            Some(LitStr::parse(parser)?)
        } else {
            None
        };
        <Token![;]>::parse(parser)?;
        Ok(Self {
            attrs,
            name,
            value: moxy::template! { {{ negative }} {{ value }} },
            message,
        })
    }
}

impl ToTokens for Code {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Self {
            attrs, name, value, ..
        } = self;
        output.extend(moxy::template! {
            {{ attrs }}
            pub const {{ name }}: i16 = {{ value }};
        });
    }
}

/// One guard-table row: the name, the code, and the message if the row has one.
struct GuardRow<'a>(&'a Code);

impl ToTokens for GuardRow<'_> {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Code { name, message, .. } = self.0;
        let message = match message {
            Some(message) => moxy::template! { Some({{ message }}) },
            None => moxy::template! { None },
        };
        output.extend(moxy::template! {
            NamedCode { name: stringify!({{ name }}), code: {{ name }}, message: {{ message }} },
        });
    }
}

/// One arm of the generated `declared_kafka_error` lookup.
struct LookupArm<'a>(&'a Code);

impl ToTokens for LookupArm<'_> {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Code { name, message, .. } = self.0;
        output.extend(moxy::template! {
            {{ name }} => Some(KafkaError {
                name: stringify!({{ name }}),
                message: {{ message }},
            }),
        });
    }
}

struct Section {
    table: Ident,
    codes: Vec<Code>,
}

impl ToTokens for Section {
    fn to_tokens(&self, output: &mut TokenStream) {
        let Self { table, codes } = self;
        let rows: Vec<_> = codes.iter().map(GuardRow).collect();
        output.extend(moxy::template! {
            @for code in codes { {{ code }} }

            /// Every constant in this block, with the name a guard-test failure
            /// reports and the message the row declares.
            #[cfg(test)]
            const {{ table }}: &[NamedCode] = &[
                @for row in &rows { {{ row }} }
            ];
        });
    }
}

pub fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let mut tables = HashSet::new();
    let mut names = HashSet::new();
    let mut sections = Vec::new();
    for expected in ["release", "trunk", "private"] {
        let label = Ident::parse(&parser)?;
        if label.text() != expected {
            return Err(ParseError::new(
                label.span(),
                format!("expected `{expected}` code section"),
            ));
        }
        let table = Ident::parse(&parser)?;
        if !tables.insert(table.text().to_owned()) {
            return Err(ParseError::new(
                table.span(),
                "guard table declared more than once",
            ));
        }
        let group = Group::parse(&parser)?;
        if !group.delim.is_brace() {
            return Err(ParseError::new(group.span(), "code section needs braces"));
        }
        let rows = Parser::from_tokens(&group.tokens);
        let mut codes = Vec::new();
        while !rows.is_empty() {
            let code = Code::parse(&rows, expected != "private")?;
            if !names.insert(code.name.text().to_owned()) {
                return Err(ParseError::new(
                    code.name.span(),
                    "wire constant declared more than once",
                ));
            }
            codes.push(code);
        }
        sections.push(Section { table, codes });
    }
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after the private code section"));
    }
    let arms: Vec<_> = sections
        .iter()
        .flat_map(|section| &section.codes)
        .filter(|code| code.message.is_some())
        .map(LookupArm)
        .collect();
    Ok(moxy::template! {
        @for section in &sections { {{ section }} }

        /// Kafka's `Errors.name()` and `Errors.message()` for a code the
        /// release or trunk section declares, or `None` for any other code.
        const fn declared_kafka_error(code: i16) -> Option<KafkaError> {
            match code {
                @for arm in &arms { {{ arm }} }
                _ => None,
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn signed_values_docs_messages_and_private_guard_order_survive_generation() {
        let input = "release RELEASE { #[doc = \"negative\"] NEGATIVE = -1 => \"Below.\"; MAX = 32767 => \"Top.\"; MIN = -32768 => \"Bottom.\"; } trunk TRUNK { FUTURE = 135 => \"Later.\"; } private PRIVATE { LOCAL = 1000; }".parse().unwrap();
        let generated = compact(&expand(&input).unwrap());
        assert!(generated.contains("#[doc=\"negative\"]pubconstNEGATIVE:i16=-1;"));
        assert!(generated.contains("pubconstMIN:i16=-32768;"));
        assert!(generated.matches("#[cfg(test)]const").count() == 3);
        let expected = "constRELEASE:&[NamedCode]=&[NamedCode{name:stringify!(NEGATIVE),code:NEGATIVE,message:Some(\"Below.\")},NamedCode{name:stringify!(MAX),code:MAX,message:Some(\"Top.\")},NamedCode{name:stringify!(MIN),code:MIN,message:Some(\"Bottom.\")},]";
        assert!(generated.contains(expected));
        assert!(generated.contains(
            "constPRIVATE:&[NamedCode]=&[NamedCode{name:stringify!(LOCAL),code:LOCAL,message:None},]"
        ));
        assert!(generated.contains(
            "FUTURE=>Some(KafkaError{name:stringify!(FUTURE),message:\"Later.\",}),_=>None,"
        ));
        assert!(!generated.contains("LOCAL=>"));
        assert!(!generated.contains("pubconstRELEASE"));
    }

    #[yare::parameterized(
        missing_private = { "release R {} trunk T {}" },
        out_of_order = { "trunk T {} release R {} private P {}" },
        duplicate_name = { "release R {} trunk R {} private P {}" },
        duplicate_value = { "release R { A = 0 => \"a\"; } trunk T { A = 1 => \"a\"; } private P {}" },
        too_positive = { "release R { A = 32768 => \"a\"; } trunk T {} private P {}" },
        too_negative = { "release R { A = -32769 => \"a\"; } trunk T {} private P {}" },
        float = { "release R { A = 1.5 => \"a\"; } trunk T {} private P {}" },
        expression = { "release R { A = 1 + 2 => \"a\"; } trunk T {} private P {}" },
        missing_semicolon = { "release R { A = 1 => \"a\" } trunk T {} private P {}" },
        missing_release_message = { "release R { A = 1; } trunk T {} private P {}" },
        missing_trunk_message = { "release R {} trunk T { A = 135; } private P {}" },
        private_message = { "release R {} trunk T {} private P { A = 1000 => \"a\"; }" },
        message_not_a_string = { "release R { A = 1 => 2; } trunk T {} private P {}" },
        bracketed = { "release R [] trunk T {} private P {}" },
        trailing = { "release R {} trunk T {} private P {} extra" },
    )]
    fn rejects_missing_sections_duplicates_bad_values_and_trailing_tokens(input: &str) {
        assert2::assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
    }
}
