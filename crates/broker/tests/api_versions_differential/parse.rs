//! Reader for `kafka-broker-api-versions` output.
//!
//! The JVM tool prints one block per broker. `NodeApiVersions::toString` builds
//! each block from two sources, and the difference between them is the whole
//! point of this parser:
//!
//! - every row the broker actually sent in its `ApiVersions` response, printed
//!   as `Name(key): <min> to <max> [usable: <n>]`, collapsed to
//!   `Name(key): <n>` when the two bounds are equal, and prefixed `UNKNOWN`
//!   instead of a name when the client's own `ApiKeys` enum has no entry for
//!   the key;
//! - one filler row per client API the broker did *not* send, printed as
//!   `Name(key): UNSUPPORTED` with no `usable` clause.
//!
//! So a row carries the broker's advertised range exactly when it is not the
//! `UNSUPPORTED` filler, and `[usable: UNSUPPORTED]` is a third thing again --
//! an advertised range that does not overlap the client's, which still tells us
//! what the broker advertised. [`parse_single_broker`] keeps the first kind and
//! drops the second.

use {krabka_protocol::owned::api_versions_response::ApiVersion, std::fmt};

/// The name the tool printed for a row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowName {
    /// The client's `ApiKeys` enum knows the key, so the tool printed its
    /// canonical Kafka name.
    Known(String),
    /// The broker advertised a key the client does not know, which the tool
    /// prints as `UNKNOWN(<key>)`.
    Unknown,
}

/// One advertised row: the printed name beside the version range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRow {
    /// What the tool called the key.
    pub(crate) name: RowName,
    /// The advertised range, in the same shape the broker sent it.
    pub(crate) api: ApiVersion,
}

/// Why an output could not be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Nothing in the output opens a broker block, so the tool never reached a
    /// broker.
    NoBrokerBlock,
    /// More than one broker answered. Every caller here drives a single node,
    /// so a second block means the wrong cluster was addressed.
    ManyBrokerBlocks(usize),
    /// A row named an API key but its version range did not parse.
    MalformedRange(String),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoBrokerBlock => write!(f, "no broker block in kafka-broker-api-versions output"),
            Self::ManyBrokerBlocks(count) => {
                write!(f, "expected one broker block, found {count}")
            }
            Self::MalformedRange(line) => write!(f, "unreadable version range in row: {line}"),
        }
    }
}

/// The line the tool writes to open a broker's block.
const BLOCK_HEADER_SUFFIX: &str = "-> (";

/// Read the advertised rows of a single-broker output, sorted by API key.
///
/// # Errors
///
/// Returns [`ParseError`] when the output holds no broker block, holds more
/// than one, or carries a row whose version range does not parse.
pub fn parse_single_broker(output: &str) -> Result<Vec<ParsedRow>, ParseError> {
    let blocks = output
        .lines()
        .filter(|line| line.trim_end().ends_with(BLOCK_HEADER_SUFFIX))
        .count();
    match blocks {
        0 => return Err(ParseError::NoBrokerBlock),
        1 => {}
        many => return Err(ParseError::ManyBrokerBlocks(many)),
    }

    let mut rows = Vec::new();
    for line in output.lines() {
        if let Some(row) = parse_row(line)? {
            rows.push(row);
        }
    }
    rows.sort_by_key(|row| row.api.api_key);
    Ok(rows)
}

/// The version ranges of `rows`, which is what a broker's `ApiVersions`
/// response carried.
pub fn advertised(rows: &[ParsedRow]) -> Vec<ApiVersion> {
    rows.iter().map(|row| row.api.clone()).collect()
}

/// Read one line, or `Ok(None)` when it is not an advertised row.
fn parse_row(line: &str) -> Result<Option<ParsedRow>, ParseError> {
    let trimmed = line.trim();
    let trimmed = trimmed.strip_suffix(',').unwrap_or(trimmed);
    // `Name(key): rest`. The block header (`host:port (id: 1 rack: null ...)
    // -> (`) and the closing `)` carry no such separator.
    let Some((head, rest)) = trimmed.split_once("): ") else {
        return Ok(None);
    };
    let Some((name, key)) = head.split_once('(') else {
        return Ok(None);
    };
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Ok(None);
    }
    let Ok(api_key) = key.parse::<i16>() else {
        return Ok(None);
    };

    // Drop the client-side ` [usable: ...]` clause. What is left is either the
    // broker's range or the `UNSUPPORTED` filler for a key it never sent.
    let range = rest.split(" [usable:").next().unwrap_or(rest).trim();
    if range == "UNSUPPORTED" {
        return Ok(None);
    }
    let (min_version, max_version) = if let Some((low, high)) = range.split_once(" to ") {
        (parse_version(low, line)?, parse_version(high, line)?)
    } else {
        let only = parse_version(range, line)?;
        (only, only)
    };

    let name = if name == "UNKNOWN" {
        RowName::Unknown
    } else {
        RowName::Known(name.to_owned())
    };
    Ok(Some(ParsedRow {
        name,
        api: ApiVersion {
            api_key,
            min_version,
            max_version,
            ..Default::default()
        },
    }))
}

fn parse_version(text: &str, line: &str) -> Result<i16, ParseError> {
    text.trim()
        .parse::<i16>()
        .map_err(|_| ParseError::MalformedRange(line.trim().to_owned()))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        rstest::fixture,
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    };

    /// A block shaped exactly like `mirror.gcr.io/apache/kafka:4.3.1`'s tool
    /// writes one, carrying every row kind at once: a two-bound range, a
    /// collapsed single version, the `UNSUPPORTED` filler for a key the broker
    /// never sent, an advertised range with no client overlap, and a key the
    /// client does not know.
    const BLOCK: &str = "\
host.docker.internal:41551 (id: 1 rack: null isFenced: false) -> (
\tProduce(0): 3 to 13 [usable: 13],
\tApiVersions(18): 0 to 4 [usable: 4],
\tListPartitionReassignments(46): 0 [usable: 0],
\tGetTelemetrySubscriptions(71): UNSUPPORTED,
\tShareFetch(78): 1 to 2 [usable: UNSUPPORTED],
\tUNKNOWN(1010): 0
)
";

    fn row(name: Option<&str>, api_key: i16, min_version: i16, max_version: i16) -> ParsedRow {
        ParsedRow {
            name: name.map_or(RowName::Unknown, |n| RowName::Known(n.to_owned())),
            api: ApiVersion {
                api_key,
                min_version,
                max_version,
                ..Default::default()
            },
        }
    }

    struct ToolOutput {
        output: String,
        parsed: Option<Result<Vec<ParsedRow>, ParseError>>,
        ranges: Option<Vec<ApiVersion>>,
    }

    #[fixture]
    fn output() -> ToolOutput {
        // Start this scenario with fresh inputs and observations.

        ToolOutput {
            output: String::new(),
            parsed: None,
            ranges: None,
        }
    }

    #[given("the JVM API versions tool prints:")]
    fn tool_output(output: &mut ToolOutput, docstring: String) { output.output = docstring; }

    #[given("the JVM tool prints {count} broker blocks")]
    fn repeated_blocks(output: &mut ToolOutput, count: usize) {
        output.output = BLOCK.repeat(count);
    }

    #[when("the output is read as a single broker response")]
    fn read_output(output: &mut ToolOutput) {
        output.parsed = Some(parse_single_broker(&output.output));
    }

    #[when("the parsed rows are projected into advertised wire ranges")]
    fn wire_ranges(output: &mut ToolOutput) {
        output.ranges = Some(advertised(
            output
                .parsed
                .as_ref()
                .expect("the output was read")
                .as_ref()
                .expect("a valid broker block"),
        ));
    }

    #[derive(DataTableRow)]
    struct ExpectedRow {
        name: String,
        key: i16,
        minimum: i16,
        maximum: i16,
    }

    #[derive(DataTableRow)]
    struct ExpectedRange {
        key: i16,
        minimum: i16,
        maximum: i16,
    }

    #[then("the advertised rows in API key order are:")]
    fn advertised_rows(output: &ToolOutput, #[datatable] expected: Rows<ExpectedRow>) {
        let expected: Vec<_> = expected
            .into_iter()
            .map(|expected| {
                row(
                    (expected.name != "UNKNOWN").then_some(expected.name.as_str()),
                    expected.key,
                    expected.minimum,
                    expected.maximum,
                )
            })
            .collect();
        assert!(output.parsed.as_ref() == Some(&Ok(expected)));
    }

    #[then("the advertised wire ranges are:")]
    fn projected_ranges(output: &ToolOutput, #[datatable] expected: Rows<ExpectedRange>) {
        let expected: Vec<_> = expected
            .into_iter()
            .map(|expected| ApiVersion {
                api_key: expected.key,
                min_version: expected.minimum,
                max_version: expected.maximum,
                ..Default::default()
            })
            .collect();
        assert!(output.ranges.as_ref() == Some(&expected));
    }

    #[then("parsing fails because no broker block was printed")]
    fn absent_block(output: &ToolOutput) {
        assert!(output.parsed.as_ref() == Some(&Err(ParseError::NoBrokerBlock)));
    }

    #[then("parsing fails because {count} broker blocks were printed")]
    fn multiple_blocks(output: &ToolOutput, count: usize) {
        assert!(output.parsed.as_ref() == Some(&Err(ParseError::ManyBrokerBlocks(count))));
    }

    #[then("parsing rejects the malformed row {line}")]
    fn malformed_range(output: &ToolOutput, line: String) {
        assert!(output.parsed.as_ref() == Some(&Err(ParseError::MalformedRange(line))));
    }

    scenarios!("tests/features/migrated/tests/api_versions_differential/parse.feature", fixtures = [output: ToolOutput]);
}
