//! The tags and domain terms Krabka's feature files may use.
//!
//! `glossary.toml`, beside the crate manifest, is the one list. The lint
//! rejects a tag it does not define, and the tag table in the Gherkin style
//! guide is rendered from it with [`Glossary::tag_table_markdown`].

use {
    serde::Deserialize,
    std::{collections::BTreeMap, fmt::Write, sync::LazyLock},
};

/// The glossary this crate ships, parsed once.
static EMBEDDED: LazyLock<Result<Glossary, String>> =
    LazyLock::new(|| Glossary::parse(include_str!("../glossary.toml")));

/// Every tag and term a feature file may use.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Glossary {
    /// The tags, by name without their `@`.
    #[serde(default)]
    pub tags: BTreeMap<String, Entry>,
    /// The domain terms, by name.
    #[serde(default)]
    pub terms: BTreeMap<String, Term>,
}

/// One tag of the glossary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    /// What the tag says about a scenario, as one Markdown sentence or more.
    pub meaning: String,
}

/// One domain term of the glossary.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Term {
    pub definition: String,
}

impl Glossary {
    /// Parses a glossary from its TOML text.
    ///
    /// # Errors
    ///
    /// Returns a readable message when the text is not TOML or has a key the
    /// glossary does not define.
    pub fn parse(toml: &str) -> Result<Self, String> {
        toml::from_str(toml).map_err(|error| format!("the glossary does not parse: {error}"))
    }

    /// The glossary this crate embeds from its `glossary.toml`.
    ///
    /// # Errors
    ///
    /// Returns the parse error of the embedded file, which the crate's tests
    /// keep from shipping.
    pub fn embedded() -> Result<&'static Self, String> { EMBEDDED.as_ref().map_err(Clone::clone) }

    /// Reports whether the glossary defines `tag`, written without its `@`.
    #[must_use]
    pub fn has_tag(&self, tag: &str) -> bool { self.tags.contains_key(tag) }

    /// The tag table of the Gherkin style guide: a header row, the alignment
    /// row and one row per tag in name order, each line ending in a newline.
    #[must_use]
    pub fn tag_table_markdown(&self) -> String {
        let mut table = String::from("| Tag | Meaning |\n| :--- | :--- |\n");
        for (tag, entry) in &self.tags {
            let meaning = entry.meaning.replace('|', "\\|");
            // Writing to a `String` cannot fail.
            let _ = writeln!(table, "| `@{tag}` | {meaning} |");
        }
        table
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn tag(meaning: &str) -> Entry {
        Entry {
            meaning: meaning.to_owned(),
        }
    }

    #[test]
    fn parse_reads_tags_and_terms() {
        let glossary = Glossary::parse(
            "\
[tags.docker]
meaning = \"Needs a container.\"

[terms.offset]
definition = \"The position of a record.\"
",
        )
        .unwrap();
        let expected = Glossary {
            tags: BTreeMap::from([("docker".to_owned(), tag("Needs a container."))]),
            terms: BTreeMap::from([("offset".to_owned(), Term {
                definition: "The position of a record.".to_owned(),
            })]),
        };
        assert!(glossary == expected);
    }

    #[test]
    fn parse_rejects_an_unknown_key() {
        let error = Glossary::parse("[tags.docker]\nmeaning = \"x\"\nowner = \"y\"\n").unwrap_err();
        assert!(error.starts_with("the glossary does not parse:"));
    }

    #[test]
    fn embedded_glossary_parses_and_defines_the_lifecycle_tags() {
        let glossary = Glossary::embedded().unwrap();
        for name in [
            "docker",
            "journey",
            "formulated",
            "inprogress",
            "manual",
            "allow_skipped",
        ] {
            assert!(glossary.has_tag(name), "glossary lacks @{name}");
        }
    }

    #[test]
    fn tag_table_renders_one_row_per_tag_in_name_order() {
        let glossary = Glossary {
            tags: BTreeMap::from([
                ("journey".to_owned(), tag("An end-to-end flow.")),
                ("docker".to_owned(), tag("Needs a | container.")),
            ]),
            terms: BTreeMap::new(),
        };
        let expected = "\
| Tag | Meaning |
| :--- | :--- |
| `@docker` | Needs a \\| container. |
| `@journey` | An end-to-end flow. |
";
        assert!(glossary.tag_table_markdown() == expected);
    }
}
