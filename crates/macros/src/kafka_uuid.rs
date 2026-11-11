//! Display, parsing and string serde for the format crate's Kafka UUID wrappers.

use moxy::{
    ast::{ItemStruct, ParseError},
    token::TokenStream,
};

pub fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    use crate::meta::inner_type;

    inner_type(&item, "KafkaUuid")?;
    if !item.generics.params.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "`KafkaUuid` needs a non-generic UUID wrapper",
        ));
    }
    let ident = item.ident;
    Ok(moxy::template! {
        impl ::core::fmt::Display for {{ ident }} {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.pad(&encode(self.0))
            }
        }

        impl ::core::str::FromStr for {{ ident }} {
            type Err = KafkaUuidError;

            /// Parses the Kafka form only, as Kafka's `Uuid.fromString` does.
            fn from_str(input: &str) -> Result<Self, Self::Err> {
                decode(input).map(Self)
            }
        }

        impl ::serde::Serialize for {{ ident }} {
            fn serialize<S: ::serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.collect_str(self)
            }
        }

        impl<'de> ::serde::Deserialize<'de> for {{ ident }} {
            fn deserialize<D: ::serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                <::std::string::String as ::serde::Deserialize<'de>>::deserialize(deserializer)?
                    .parse()
                    .map_err(<D::Error as ::serde::de::Error>::custom)
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        assert2::assert,
        moxy::ast::{ItemStruct, ParseError},
    };

    #[derive(Default)]
    struct UuidDeriveScenario {
        source: String,
        expansion: Option<Result<(), ParseError>>,
    }

    #[rstest::fixture]
    fn uuid_derive() -> UuidDeriveScenario {
        UuidDeriveScenario {
            ..UuidDeriveScenario::default()
        }
    }

    #[rstest_bdd_macros::given("a UUID wrapper declared as")]
    fn wrapper_declaration(uuid_derive: &mut UuidDeriveScenario, docstring: String) {
        uuid_derive.source = docstring;
    }

    #[rstest_bdd_macros::when("the KafkaUuid derive is expanded")]
    fn derive_uuid(uuid_derive: &mut UuidDeriveScenario) {
        let source = &uuid_derive.source;
        let item: ItemStruct = moxy::parse!({ source }).expect("valid Rust struct source");
        uuid_derive.expansion = Some(expand(item).map(drop));
    }

    #[rstest_bdd_macros::then("the wrapper shape is rejected with {message}")]
    fn reject_wrapper(uuid_derive: &UuidDeriveScenario, message: String) {
        let error = uuid_derive
            .expansion
            .as_ref()
            .expect("derive expanded")
            .as_ref()
            .expect_err("unsupported wrapper rejected");
        assert!(error.to_string() == message, "{}", uuid_derive.source);
    }

    krabka_bdd::scenarios!(
        "tests/features/uuid_derive",
        fixtures = [uuid_derive: UuidDeriveScenario],
    );
}
