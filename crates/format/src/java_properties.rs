//! Java's `Properties.load(InputStream)`, byte for byte.
//!
//! Kafka reads every properties file it is handed this way: `meta.properties`
//! through `PropertiesUtils.readPropertiesFile`, and a tool's
//! `--command-config` through `Utils.loadProps`. A reader that stands in for
//! either has to read the same keys and values, so there is one port here,
//! of `Properties.LineReader`, `load0` and `loadConvert`.

use {
    std::{collections::HashMap, mem},
    thiserror::Error,
};

/// A `\u` escape is not followed by four hexadecimal digits: the
/// `IllegalArgumentException` that `Properties.loadConvert` throws.
#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
#[error("Malformed \\uxxxx encoding.")]
pub struct MalformedEscape;

/// Kafka's `Properties.load(InputStream)`: the key and value of each entry.
/// A key that appears twice keeps its last value.
///
/// `load(InputStream)` reads ISO 8859-1, so each byte is the character of
/// the same value. An escape `\uXXXX` gives a UTF-16 code unit.
///
/// # Errors
///
/// Returns [`MalformedEscape`] for a `\u` that four hexadecimal digits do
/// not follow.
pub fn load(bytes: &[u8]) -> Result<HashMap<String, String>, MalformedEscape> {
    let mut props = HashMap::new();
    for line in logical_lines(bytes) {
        let RawEntry { key, value } = split_entry(&line);
        props.insert(unescape(key)?, unescape(value)?);
    }
    Ok(props)
}

/// The white space that `Properties.load` skips.
fn is_white_space(byte: u8) -> bool { matches!(byte, b' ' | b'\t' | b'\x0c') }

fn is_line_end(byte: u8) -> bool { matches!(byte, b'\n' | b'\r') }

/// The logical lines of a properties file, as `Properties.LineReader` reads
/// them.
///
/// A natural line ends at `\n`, `\r`, or `\r\n`. White space at the start of
/// a line is skipped, and a blank line is skipped. A line whose first other
/// character is `#` or `!` is a comment. A line that ends in an odd number of
/// backslashes goes on in the next line: the last backslash is dropped, and
/// so is the white space at the start of the next line.
fn logical_lines(input: &[u8]) -> Vec<Vec<u8>> {
    let mut lines = Vec::new();
    let mut line = Vec::new();
    let mut skip_white_space = true;
    let mut appended_line_begin = false;
    let mut preceding_backslash = false;
    let mut skip_lf = false;
    let mut bytes = input.iter().copied();
    while let Some(byte) = bytes.next() {
        if skip_lf {
            skip_lf = false;
            if byte == b'\n' {
                continue;
            }
        }
        if skip_white_space {
            if is_white_space(byte) || (!appended_line_begin && is_line_end(byte)) {
                continue;
            }
            skip_white_space = false;
            appended_line_begin = false;
        }
        if line.is_empty() && matches!(byte, b'#' | b'!') {
            if !bytes.by_ref().any(is_line_end) {
                break;
            }
            skip_white_space = true;
            continue;
        }
        if !is_line_end(byte) {
            line.push(byte);
            preceding_backslash = byte == b'\\' && !preceding_backslash;
        } else if line.is_empty() {
            skip_white_space = true;
        } else if preceding_backslash {
            line.pop();
            skip_white_space = true;
            appended_line_begin = true;
            preceding_backslash = false;
            skip_lf = byte == b'\r';
        } else {
            lines.push(mem::take(&mut line));
            skip_white_space = true;
        }
    }
    if !line.is_empty() {
        if preceding_backslash {
            line.pop();
        }
        lines.push(line);
    }
    lines
}

/// The raw key and value of one logical line, before their escapes resolve.
struct RawEntry<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

/// Splits a logical line into its key and its value, as `Properties.load0`
/// does.
///
/// The key ends at the first `=`, `:`, or white space that no backslash
/// escapes. The value starts after the white space that follows, with at
/// most one `=` or `:` among it.
fn split_entry(line: &[u8]) -> RawEntry<'_> {
    let mut key_len = 0;
    let mut value_start = line.len();
    let mut has_separator = false;
    let mut preceding_backslash = false;
    while let Some(&byte) = line.get(key_len) {
        if !preceding_backslash && matches!(byte, b'=' | b':') {
            value_start = key_len + 1;
            has_separator = true;
            break;
        }
        if !preceding_backslash && is_white_space(byte) {
            value_start = key_len + 1;
            break;
        }
        preceding_backslash = byte == b'\\' && !preceding_backslash;
        key_len += 1;
    }
    while let Some(&byte) = line.get(value_start) {
        if !is_white_space(byte) {
            if !has_separator && matches!(byte, b'=' | b':') {
                has_separator = true;
            } else {
                break;
            }
        }
        value_start += 1;
    }
    // Both scans stop inside `line`, so neither bound passes its end.
    RawEntry {
        key: line.get(..key_len).unwrap_or(line),
        value: line.get(value_start..).unwrap_or_default(),
    }
}

/// Resolves the escapes of a key or a value, as `Properties.loadConvert`
/// does: `\t`, `\r`, `\n`, `\f`, and `\uXXXX` are the characters they name,
/// and a backslash before any other character is dropped.
fn unescape(raw: &[u8]) -> Result<String, MalformedEscape> {
    let mut units: Vec<u16> = Vec::with_capacity(raw.len());
    let mut bytes = raw.iter().copied();
    while let Some(byte) = bytes.next() {
        if byte != b'\\' {
            units.push(u16::from(byte));
            continue;
        }
        let Some(escaped) = bytes.next() else {
            break;
        };
        let unit = match escaped {
            b'u' => {
                let mut value = 0u16;
                for _ in 0..4 {
                    let digit = bytes
                        .next()
                        .and_then(|digit| char::from(digit).to_digit(16))
                        .and_then(|digit| u16::try_from(digit).ok())
                        .ok_or(MalformedEscape)?;
                    value = (value << 4) | digit;
                }
                value
            }
            b't' => u16::from(b'\t'),
            b'r' => u16::from(b'\r'),
            b'n' => u16::from(b'\n'),
            b'f' => 0x0c,
            other => u16::from(other),
        };
        units.push(unit);
    }
    Ok(String::from_utf16_lossy(&units))
}
