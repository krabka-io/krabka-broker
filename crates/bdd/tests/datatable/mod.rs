//! Data tables written as Gherkin text, for the suites that build one by
//! hand.

/// The cells of `text`, a Gherkin data table: one `| a | b |` row per line,
/// cells trimmed, blank lines skipped.
pub fn table(text: &str) -> Vec<Vec<String>> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let inner = line.strip_prefix('|').unwrap().strip_suffix('|').unwrap();
            inner
                .split('|')
                .map(|cell| cell.trim().to_owned())
                .collect()
        })
        .collect()
}
