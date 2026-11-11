//! Placeholder substitution for Scenario Outline step text.
//!
//! This module provides utilities for substituting `<placeholder>` tokens in
//! Gherkin step text with values from an Examples table row. This enables the
//! cucumber-rs-style parameterization where `Given I have <count> items` with
//! an Examples row `| count | = | 5 |` becomes `Given I have 5 items`.

use std::sync::LazyLock;

use regex::Regex;

/// Regex pattern matching `<placeholder>` tokens in step text.
///
/// Captures the placeholder name without the angle brackets, including spaces
/// and punctuation commonly used in Gherkin Examples headers.
pub(crate) static PLACEHOLDER_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Safe: The regex pattern is a compile-time constant and is valid.
    let Ok(regex) = Regex::new(r"<([^>\s][^>]*)>") else {
        unreachable!("placeholder regex is valid");
    };
    regex
});

/// Substitutes `<placeholder>` tokens in text with values from an Examples row.
///
/// Each placeholder token (e.g., `<count>`) is replaced with the corresponding
/// value from the row, matched by column header name. A placeholder that names
/// no column is left verbatim, as Cucumber does, so text such as `<br>` or
/// `<none>` in a step, docstring, or table cell survives substitution. The
/// text is scanned once, so a substituted value is never itself substituted.
///
/// # Arguments
///
/// * `text` - The text containing placeholders to substitute
/// * `headers` - Column headers from the Examples table
/// * `row` - Values for the current row, aligned with headers
///
/// # Examples
///
/// ```ignore
/// let headers = vec!["count".to_string(), "item".to_string()];
/// let row = vec!["5".to_string(), "apples".to_string()];
/// assert_eq!(substitute_placeholders("I have <count> <item>", &headers, &row), "I have 5 apples");
/// assert_eq!(substitute_placeholders("keep <other>", &headers, &row), "keep <other>");
/// ```
pub fn substitute_placeholders(text: &str, headers: &[String], row: &[String]) -> String {
    PLACEHOLDER_RE
        .replace_all(text, |caps: &regex::Captures<'_>| {
            let placeholder = caps.get(1).map_or("", |name| name.as_str());
            headers
                .iter()
                .position(|header| header == placeholder)
                .and_then(|idx| row.get(idx))
                .map_or_else(
                    || caps.get(0).map_or("", |whole| whole.as_str()).to_owned(),
                    Clone::clone,
                )
        })
        .into_owned()
}

/// Extracts all placeholder names from a text.
///
/// Returns a vector of placeholder names (without angle brackets) found in the
/// text, preserving the order of first occurrence.
#[cfg(test)]
fn extract_placeholder_names(text: &str) -> Vec<String> {
    PLACEHOLDER_RE
        .captures_iter(text)
        .map(|cap| cap[1].to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    //! Unit tests for parsing placeholder syntax.

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(
        "I have <count> items",
        vec!["count".to_owned()],
        vec!["5".to_owned()],
        "I have 5 items"
    )]
    #[case(
        "I have <count> <item>",
        vec!["count".to_owned(), "item".to_owned()],
        vec!["5".to_owned(), "apples".to_owned()],
        "I have 5 apples"
    )]
    #[case(
        "<val> plus <val> equals double <val>",
        vec!["val".to_owned()],
        vec!["3".to_owned()],
        "3 plus 3 equals double 3"
    )]
    #[case(
        "I have 5 items",
        vec!["count".to_owned()],
        vec!["10".to_owned()],
        "I have 5 items"
    )]
    #[case(
        "I have <count> items",
        vec!["count".to_owned()],
        vec![String::new()],
        "I have  items"
    )]
    fn substitute_placeholders_cases(
        #[case] text: &str,
        #[case] headers: Vec<String>,
        #[case] row: Vec<String>,
        #[case] expected: &str,
    ) {
        let result = substitute_placeholders(text, &headers, &row);
        assert_eq!(result, expected);
    }

    #[rstest]
    #[case::unknown("I have <undefined> items", "I have <undefined> items")]
    #[case::mixed("<count> of <unknown>", "5 of <unknown>")]
    #[case::markup("line<br>break", "line<br>break")]
    fn unknown_placeholders_stay_verbatim(#[case] text: &str, #[case] expected: &str) {
        let headers = vec!["count".to_owned()];
        let row = vec!["5".to_owned()];
        assert_eq!(substitute_placeholders(text, &headers, &row), expected);
    }

    #[test]
    fn substituted_values_are_not_substituted_again() {
        let headers = vec!["a".to_owned(), "b".to_owned()];
        let row = vec!["<b>".to_owned(), "x".to_owned()];
        assert_eq!(substitute_placeholders("<a> <b>", &headers, &row), "<b> x");
    }

    #[test]
    fn placeholder_with_underscores() {
        let text = "The <user_name> has <item_count> items";
        let headers = vec!["user_name".to_owned(), "item_count".to_owned()];
        let row = vec!["Alice".to_owned(), "42".to_owned()];

        let result = substitute_placeholders(text, &headers, &row);
        assert_eq!(result, "The Alice has 42 items");
    }

    #[test]
    fn placeholder_with_spaces_and_hyphens() {
        let text = "The <start count> includes <item-id>";
        let headers = vec!["start count".to_owned(), "item-id".to_owned()];
        let row = vec!["3".to_owned(), "apples".to_owned()];

        let result = substitute_placeholders(text, &headers, &row);
        assert_eq!(result, "The 3 includes apples");
    }

    #[test]
    fn extract_placeholder_names_finds_all() {
        let names = extract_placeholder_names("I have <count> <item> and <count> more");
        assert_eq!(names, vec!["count", "item", "count"]);
    }

    #[test]
    fn extract_placeholder_names_empty_when_none() {
        let names = extract_placeholder_names("No placeholders here");
        assert_eq!(names, Vec::<String>::new());
    }
}
