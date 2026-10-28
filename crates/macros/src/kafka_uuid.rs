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
    use {super::expand, crate::meta::compact, assert2::assert, moxy::ast::ItemStruct};

    #[cfg(test)]
    #[rstest_bdd_macros::then(
        "kafka uuid tests: refuses non newtypes and generic wrappers for {example}"
    )]
    fn bdd_verify_refuses_non_newtypes_and_generic_wrappers(example: String) {
        type Example<'a> = (&'a str,);
        let (input,): Example<'_> = match example.as_str() {
            "unit struct" => ("struct Id;",),
            "empty named struct" => ("struct Id {}",),
            "named field" => ("struct Id { value: Uuid }",),
            "multiple tuple fields" => ("struct Id(Uuid, Uuid);",),
            "const generic" => ("struct Id<const DOMAIN: u8>(Uuid);",),
            _ => panic!("unknown BDD example: {example}"),
        };

        let item: ItemStruct = moxy::parse!({ input }).unwrap();
        assert2::assert!(expand(item).is_err(), "{input}");
    }

    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/kafka_uuid.feature",
        name = "tests: refuses non newtypes and generic wrappers"
    )]
    fn refuses_non_newtypes_and_generic_wrappers(example: String) { let _ = example; }

    #[cfg(test)]
    #[rstest_bdd_macros::then(
        "kafka uuid tests: emits the four trait implementations for a single field wrapper"
    )]
    fn bdd_verify_emits_the_four_trait_implementations_for_a_single_field_wrapper() {
        let item: ItemStruct = moxy::parse!("pub struct Id(pub Uuid);").unwrap();
        let output = compact(&expand(item).unwrap());
        assert!(output.contains("DisplayforId"));
        assert!(output.contains("FromStrforId"));
        assert!(output.contains("SerializeforId"));
        assert!(output.contains("Deserialize<'de>forId"));
    }

    #[cfg(test)]
    #[rstest_bdd_macros::scenario(
        path = "tests/features/migrated/src/kafka_uuid.feature",
        name = "tests: emits the four trait implementations for a single field wrapper"
    )]
    fn emits_the_four_trait_implementations_for_a_single_field_wrapper() {}
}
