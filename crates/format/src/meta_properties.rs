//! Kafka's `meta.properties`: the file that marks a formatted log directory
//! and records the identity of the node and of the directory.
//!
//! `kafka-storage format` writes the file into every directory it formats,
//! and the broker reads it from every directory on every boot. Kafka's tools
//! read it too: `kafka-metadata-quorum add-controller` takes the directory id
//! of the new controller from the `meta.properties` in its
//! `metadata.log.dir`. krabka writes and reads the same file, so a directory
//! that one side formats is a formatted directory to the other side.
//!
//! The file is a Java properties file, written by `Properties.store` through
//! Kafka's `PropertiesUtils.writePropertiesFile`. A `KRaft` node writes
//! version 1 (`MetaPropertiesVersion.V1`):
//!
//! ```text
//! #
//! #Thu Feb 29 12:34:56 UTC 2024
//! cluster.id=AQIDBAUGBwgJCgsMDQ4PEA
//! directory.id=U-hCdTzaSPyFB3swwc7Qdw
//! node.id=1
//! version=1
//! ```
//!
//! The first line is the empty comment that `writePropertiesFile` passes to
//! `store`. The second line is the time of the write, in the form of
//! `java.util.Date.toString`. The keys follow in their natural order, which
//! is the order `Properties.store` uses on Java 18 and later. [`MetaProperties`]
//! writes the same bytes, with the time in UTC. It reads what
//! `Properties.load` reads, so it reads the file of every Java version.

use {
    crate::ids::{ClusterId, DirectoryId, KafkaUuidError},
    std::{
        collections::HashMap,
        fs,
        io::{self, Write as _},
        mem,
        path::Path,
        time::{SystemTime, UNIX_EPOCH},
    },
    thiserror::Error,
    time::OffsetDateTime,
};

/// The file name, Kafka's `MetaPropertiesEnsemble.META_PROPERTIES_NAME`.
pub const META_PROPERTIES: &str = "meta.properties";

/// The name of the file while it is written, before the rename that
/// publishes it. `PropertiesUtils.writePropertiesFile` adds `.tmp`.
pub const META_PROPERTIES_TMP: &str = "meta.properties.tmp";

/// The version of the file that a `KRaft` node writes,
/// `MetaPropertiesVersion.V1`.
pub const META_PROPERTIES_VERSION: i32 = 1;

/// The property names, from Kafka's `MetaProperties`.
const VERSION_PROP: &str = "version";
const CLUSTER_ID_PROP: &str = "cluster.id";
const NODE_ID_PROP: &str = "node.id";
const DIRECTORY_ID_PROP: &str = "directory.id";

/// The content of a version 1 `meta.properties`.
///
/// Kafka's `MetaProperties` keeps the cluster id as a string. krabka keeps it
/// as 16 bytes, so a cluster id that is not a Kafka `Uuid` does not read.
/// The node id is Kafka's `int`. The directory id is optional when the file
/// is read, as in Kafka: a broker gives a directory without one an id at its
/// next start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaProperties {
    /// The cluster that the directory belongs to.
    pub cluster_id: ClusterId,
    /// The node that the directory belongs to, Kafka's `node.id`.
    pub node_id: i32,
    /// The directory's own id (KIP-858).
    pub directory_id: Option<DirectoryId>,
}

/// Why a `meta.properties` does not read.
///
/// Each message is the message of the exception that Kafka's
/// `PropertiesUtils.readPropertiesFile` and `MetaProperties.Builder` throw
/// for the same file, except where a variant says otherwise.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum MetaPropertiesError {
    /// The file is there, but it cannot be read.
    #[error(transparent)]
    Io(#[from] io::Error),

    /// A `\u` escape is not followed by four hexadecimal digits.
    #[error("Malformed \\uxxxx encoding.")]
    MalformedEscape,

    /// `version` is not a number.
    #[error("Invalid meta.properties version string '{0}'")]
    InvalidVersion(String),

    /// `version` is a number that Kafka does not define.
    #[error("Unknown meta.properties version number {0}")]
    UnknownVersion(i32),

    /// The file is version 0, which only a `ZooKeeper` broker writes. Kafka
    /// reads it. krabka does not: it is a krabka message.
    #[error(
        "Unsupported meta.properties version 0: krabka reads version 1, which a KRaft node writes"
    )]
    VersionZero,

    /// The file has no `node.id`.
    #[error("Failed to find node.id")]
    NoNodeId,

    /// `node.id` is not an `int`.
    #[error("Unable to read node.id as a base-10 number.")]
    NodeIdNotANumber,

    /// The file has no `cluster.id`.
    #[error("cluster.id was not found.")]
    NoClusterId,

    /// `cluster.id` is not a Kafka `Uuid`. Kafka accepts any string there, so
    /// this is a krabka message, in the form of Kafka's message for
    /// `directory.id`.
    #[error("Unable to read cluster.id as a Uuid: {0}")]
    ClusterIdNotAUuid(KafkaUuidError),

    /// `directory.id` is not a Kafka `Uuid`.
    #[error("Unable to read directory.id as a Uuid: {0}")]
    DirectoryIdNotAUuid(KafkaUuidError),
}

impl MetaProperties {
    /// Reads `<dir>/meta.properties`. Returns `None` when there is no such
    /// file, which is how Kafka's `MetaPropertiesEnsemble.Loader` finds an
    /// empty directory.
    ///
    /// # Errors
    ///
    /// Returns [`MetaPropertiesError::Io`] when the file is there but cannot
    /// be read, and another variant when it does not parse.
    pub fn read(dir: &Path) -> Result<Option<Self>, MetaPropertiesError> {
        match fs::read(dir.join(META_PROPERTIES)) {
            Ok(bytes) => Self::parse(&bytes).map(Some),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Parses the bytes of a `meta.properties`, as Kafka's
    /// `new MetaProperties.Builder(props).build()` does after
    /// `Properties.load`.
    ///
    /// The checks run in Kafka's order, so a file with several faults is
    /// reported with Kafka's first message.
    ///
    /// # Errors
    ///
    /// Returns the [`MetaPropertiesError`] for the first fault.
    pub fn parse(bytes: &[u8]) -> Result<Self, MetaPropertiesError> {
        let props = load_properties(bytes)?;
        let version = props.get(VERSION_PROP).map_or("0", String::as_str);
        // `MetaPropertiesVersion.fromNumberString` trims as `String.trim`
        // does, and `Integer.parseInt` does the rest.
        let number: i32 = version
            .trim_matches(|c: char| c <= ' ')
            .parse()
            .map_err(|_| MetaPropertiesError::InvalidVersion(version.to_owned()))?;
        match number {
            META_PROPERTIES_VERSION => {}
            0 => return Err(MetaPropertiesError::VersionZero),
            other => return Err(MetaPropertiesError::UnknownVersion(other)),
        }
        let node_id: i32 = props
            .get(NODE_ID_PROP)
            .ok_or(MetaPropertiesError::NoNodeId)?
            .parse()
            .map_err(|_| MetaPropertiesError::NodeIdNotANumber)?;
        let cluster_id = props.get(CLUSTER_ID_PROP);
        let directory_id: Option<DirectoryId> = props
            .get(DIRECTORY_ID_PROP)
            .map(|id| id.parse().map_err(MetaPropertiesError::DirectoryIdNotAUuid))
            .transpose()?;
        let cluster_id: ClusterId = cluster_id
            .ok_or(MetaPropertiesError::NoClusterId)?
            .parse()
            .map_err(MetaPropertiesError::ClusterIdNotAUuid)?;
        Ok(Self {
            cluster_id,
            node_id,
            directory_id,
        })
    }

    /// The text of the file that Kafka's `writePropertiesFile` writes for
    /// these ids at `now`, with `now` in UTC.
    ///
    /// No value needs an escape: a node id is digits and a sign, and Kafka's
    /// `Uuid` form uses only letters, digits, `-` and `_`, which
    /// `Properties.store` writes as they are.
    #[must_use]
    pub fn to_file_text(&self, now: SystemTime) -> String {
        let directory_id = self
            .directory_id
            .map_or_else(String::new, |id| format!("{DIRECTORY_ID_PROP}={id}\n"));
        format!(
            "#\n#{}\n{CLUSTER_ID_PROP}={}\n{directory_id}{NODE_ID_PROP}={}\n\
             {VERSION_PROP}={META_PROPERTIES_VERSION}\n",
            java_date(now),
            self.cluster_id,
            self.node_id,
        )
    }

    /// Writes `<dir>/meta.properties` as Kafka's `writePropertiesFile` does
    /// with `fsync` set: the text goes to `meta.properties.tmp`, which is
    /// synced and renamed into place, and then, on Unix, the directory is
    /// synced. A write that stops partway therefore never leaves a truncated
    /// file.
    ///
    /// # Errors
    ///
    /// Returns the first I/O error. A rename that fails removes the
    /// temporary file, as Kafka does.
    pub fn write(&self, dir: &Path) -> io::Result<()> { self.write_observed(dir, |_| Ok(())) }

    /// [`Self::write`], calling `after` with the path of each file once it
    /// is written: the temporary file, then `meta.properties`. An error from
    /// `after` stops the write there, which is how `krabka-format` injects a
    /// fault for its tests.
    pub(crate) fn write_observed(
        &self,
        dir: &Path,
        mut after: impl FnMut(&Path) -> io::Result<()>,
    ) -> io::Result<()> {
        let text = self.to_file_text(SystemTime::now());
        let tmp = dir.join(META_PROPERTIES_TMP);
        let mut file = fs::File::create(&tmp)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()?;
        drop(file);
        after(&tmp)?;
        let path = dir.join(META_PROPERTIES);
        if let Err(error) = fs::rename(&tmp, &path) {
            // Kafka deletes the temporary file and reports the rename.
            let _ = fs::remove_file(&tmp);
            return Err(error);
        }
        // Syncing the directory is what makes the rename durable, and only
        // Unix supports it through `std`: elsewhere, `wasm32-wasip1`
        // included, opening or syncing a directory fails. The sync is
        // skipped there, as `krabka-log` skips its own directory syncs.
        #[cfg(unix)]
        fs::File::open(dir)?.sync_all()?;
        after(&path)
    }
}

#[bon::builder]
pub fn node_id_mismatch_message(stored: i32, previous: i32, path: &Path) -> String {
    format!(
        "Stored node id {stored} doesn't match previous node id {previous} in {}. If you \
         moved your data, make sure your configured node id matches. If you intend \
         to create a new node, you should remove all data in your data directories.",
        path.display(),
    )
}

/// Kafka's `MetaPropertiesEnsemble.verify`, over the readable files of the
/// directories of one node, sorted by path.
///
/// The set agrees on one cluster id and one node id, and no two directories
/// share a directory id, and no directory id is reserved. `expected_cluster_id`
/// and `expected_node_id` are the ids that the set has to agree with, when
/// they are known. Returns the cluster id of the set: `expected_cluster_id`
/// when given, else the id of the first file, or `None` for an empty set.
///
/// # Errors
///
/// Returns Kafka's message, word for word, for the first disagreement.
pub fn verify_ensemble<'a>(
    formatted: impl IntoIterator<Item = (&'a Path, &'a MetaProperties)>,
    expected_cluster_id: Option<ClusterId>,
    expected_node_id: Option<i32>,
) -> Result<Option<ClusterId>, String> {
    let mut cluster_id = expected_cluster_id;
    let mut node_id = expected_node_id;
    let mut seen: HashMap<DirectoryId, &Path> = HashMap::new();
    for (dir, meta) in formatted {
        let path = dir.join(META_PROPERTIES);
        match cluster_id {
            None => cluster_id = Some(meta.cluster_id),
            Some(expected) if expected != meta.cluster_id => {
                return Err(format!(
                    "Invalid cluster.id in: {}. Expected {expected}, but read {}",
                    path.display(),
                    meta.cluster_id,
                ));
            }
            Some(_) => {}
        }
        match node_id {
            None => node_id = Some(meta.node_id),
            Some(expected) if expected != meta.node_id => {
                return Err(node_id_mismatch_message()
                    .stored(meta.node_id)
                    .previous(expected)
                    .path(&path)
                    .call());
            }
            Some(_) => {}
        }
        if let Some(directory_id) = meta.directory_id {
            if directory_id.is_reserved() {
                return Err(format!(
                    "Invalid reserved directory ID {directory_id} found in {}",
                    dir.display(),
                ));
            }
            if let Some(previous) = seen.insert(directory_id, dir) {
                // Kafka prints the `Optional` wrapper of the id here.
                return Err(format!(
                    "Duplicate directory ID Optional[{directory_id}] found. It was the ID of {}, \
                     but also of {}",
                    previous.display(),
                    dir.display(),
                ));
            }
        }
    }
    Ok(cluster_id)
}

/// Kafka's `Properties.load(InputStream)`: the key and value of each entry.
/// A key that appears twice keeps its last value.
///
/// `load(InputStream)` reads ISO 8859-1, so each byte is the character of
/// the same value. An escape `\uXXXX` gives a UTF-16 code unit.
fn load_properties(bytes: &[u8]) -> Result<HashMap<String, String>, MetaPropertiesError> {
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
fn unescape(raw: &[u8]) -> Result<String, MetaPropertiesError> {
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
                        .ok_or(MetaPropertiesError::MalformedEscape)?;
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

/// `java.util.Date.toString` in UTC: `EEE MMM dd HH:mm:ss zzz yyyy`.
///
/// `Properties.store` writes it as the comment on the second line. A time
/// before 1970 is written as 1970, which no clock that formats a directory
/// reads.
fn java_date(now: SystemTime) -> String {
    let date = OffsetDateTime::from(now.max(UNIX_EPOCH));
    // `EEE` and `MMM`: the first three letters of the English name.
    let short = |name: String| name.chars().take(3).collect::<String>();
    format!(
        "{} {} {:02} {:02}:{:02}:{:02} UTC {}",
        short(date.weekday().to_string()),
        short(date.month().to_string()),
        date.day(),
        date.hour(),
        date.minute(),
        date.second(),
        date.year(),
    )
}

#[cfg(test)]
mod tests {
    use {super::*, std::time::Duration, uuid::Uuid};

    const CLUSTER: &str = "AQIDBAUGBwgJCgsMDQ4PEA";
    const DIRECTORY: &str = "U-hCdTzaSPyFB3swwc7Qdw";

    fn at(seconds: u64) -> SystemTime { UNIX_EPOCH + Duration::from_secs(seconds) }

    fn meta(node_id: i32, directory_id: Option<&str>) -> MetaProperties {
        MetaProperties {
            cluster_id: CLUSTER.parse().expect("cluster id"),
            node_id,
            directory_id: directory_id.map(|id| id.parse().expect("directory id")),
        }
    }

    /// `Date.toString` in UTC, checked against `date -u` for each instant.
    #[yare::parameterized(
        epoch = { 0, "Thu Jan 01 00:00:00 UTC 1970" },
        leap_day_2000 = { 951_782_400, "Tue Feb 29 00:00:00 UTC 2000" },
        kafka_epoch_example = { 1_000_000_000, "Sun Sep 09 01:46:40 UTC 2001" },
        leap_day_2024 = { 1_709_210_096, "Thu Feb 29 12:34:56 UTC 2024" },
        year_2099_end = { 4_102_444_799, "Thu Dec 31 23:59:59 UTC 2099" },
        zero_padded = { 1_767_596_889, "Mon Jan 05 07:08:09 UTC 2026" },
    )]
    fn the_date_comment_is_java_date_to_string(seconds: u64, want: &str) {
        assert2::check!(java_date(at(seconds)) == want, "{seconds}");
    }

    #[test]
    fn pre_epoch_date_clamps_to_epoch() {
        assert2::check!(
            java_date(UNIX_EPOCH - Duration::from_secs(1)) == "Thu Jan 01 00:00:00 UTC 1970"
        );
    }

    #[derive(Default)]
    struct MetaFileScenario {
        properties: Option<MetaProperties>,
        timestamp: u64,
        text: String,
        encoded: String,
        parsed: Option<Result<MetaProperties, String>>,
        files: Vec<(String, MetaProperties)>,
        expected_cluster: Option<ClusterId>,
        expected_node: Option<i32>,
        verified: Option<Result<Option<ClusterId>, String>>,
    }
    #[rstest::fixture]
    fn meta_file() -> MetaFileScenario {
        let text = String::new();
        MetaFileScenario {
            text,
            ..MetaFileScenario::default()
        }
    }
    #[rstest_bdd_macros::given(
        "cluster AQIDBAUGBwgJCgsMDQ4PEA node {node} and directory ID {directory}"
    )]
    fn meta_file_ids(meta_file: &mut MetaFileScenario, node: i32, directory: String) {
        meta_file.properties = Some(meta(
            node,
            (directory != "omitted").then_some(directory.as_str()),
        ));
    }
    #[rstest_bdd_macros::given("meta.properties is written at Unix second {timestamp}")]
    fn meta_file_timestamp(meta_file: &mut MetaFileScenario, timestamp: u64) {
        meta_file.timestamp = timestamp;
    }
    #[rstest_bdd_macros::when("the Kafka meta.properties file text is generated")]
    fn write_meta_file_text(meta_file: &mut MetaFileScenario) {
        meta_file.encoded = meta_file
            .properties
            .as_ref()
            .expect("metadata IDs given")
            .to_file_text(at(meta_file.timestamp));
    }
    #[rstest_bdd_macros::then("the meta.properties file is exactly")]
    fn exact_meta_file_text(meta_file: &MetaFileScenario, docstring: String) {
        assert2::check!(meta_file.encoded == format!("{}\n", docstring.trim_matches('\n')));
    }

    #[rstest_bdd_macros::given("meta.properties file contents")]
    fn existing_meta_file_text(meta_file: &mut MetaFileScenario, docstring: String) {
        meta_file.text = format!("{}\n", docstring.trim_matches('\n'));
    }
    #[rstest_bdd_macros::given("a meta.properties file with a trailing space after node ID 1")]
    fn trailing_meta_node_space(meta_file: &mut MetaFileScenario) {
        meta_file.text = "version=1\nnode.id=1 \ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n".into();
    }
    #[rstest_bdd_macros::when("the meta.properties file is parsed")]
    fn parse_meta_file_text(meta_file: &mut MetaFileScenario) {
        meta_file.parsed = Some(
            MetaProperties::parse(meta_file.text.as_bytes()).map_err(|error| error.to_string()),
        );
    }
    #[rstest_bdd_macros::then("the meta.properties error is {error}")]
    fn exact_meta_parse_error(meta_file: &MetaFileScenario, error: String) {
        assert2::check!(meta_file.parsed == Some(Err(error)), "{:?}", meta_file.text);
    }

    #[rstest_bdd_macros::given("directory verification expects cluster {cluster} and node {node}")]
    fn expected_directory_ids(meta_file: &mut MetaFileScenario, cluster: String, node: String) {
        meta_file.expected_cluster = (cluster != "discover").then(|| {
            ClusterId(Uuid::from_u128(
                cluster.parse().expect("expected cluster UUID integer"),
            ))
        });
        meta_file.expected_node =
            (node != "discover").then(|| node.parse().expect("expected node ID"));
    }
    #[rstest_bdd_macros::given("formatted directory metadata")]
    fn existing_directory_metadata(meta_file: &mut MetaFileScenario, datatable: Vec<Vec<String>>) {
        meta_file.files = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [path, cluster, node, directory] = row.as_slice() else {
                    panic!("directory path cluster node and directory ID columns required")
                };
                (path.clone(), MetaProperties {
                    cluster_id: ClusterId(Uuid::from_u128(
                        cluster.parse().expect("cluster UUID integer"),
                    )),
                    node_id: node.parse().expect("stored node ID"),
                    directory_id: (directory != "omitted").then(|| {
                        DirectoryId(Uuid::from_u128(
                            directory.parse().expect("directory UUID integer"),
                        ))
                    }),
                })
            })
            .collect();
    }
    #[rstest_bdd_macros::when("the directory metadata ensemble is verified")]
    fn verify_directory_metadata(meta_file: &mut MetaFileScenario) {
        meta_file.verified = Some(verify_ensemble(
            meta_file
                .files
                .iter()
                .map(|(path, properties)| (Path::new(path), properties)),
            meta_file.expected_cluster,
            meta_file.expected_node,
        ));
    }
    #[rstest_bdd_macros::then("the verified directory cluster is {cluster}")]
    fn directory_cluster_id(meta_file: &MetaFileScenario, cluster: String) {
        let expected = (cluster != "unknown").then(|| {
            ClusterId(Uuid::from_u128(
                cluster.parse().expect("cluster UUID integer"),
            ))
        });
        assert2::check!(meta_file.verified == Some(Ok(expected)));
    }
    #[rstest_bdd_macros::then("directory verification refuses {reason}")]
    fn directory_metadata_error(meta_file: &MetaFileScenario, reason: String) {
        let expected = match reason.as_str() {
            "a cluster mismatch" => format!(
                "Invalid cluster.id in: {}. Expected {}, but read {}",
                Path::new("/a").join(META_PROPERTIES).display(),
                ClusterId(Uuid::from_u128(0xc2)),
                ClusterId(Uuid::from_u128(0xc1))
            ),
            "a configured node mismatch" => node_id_mismatch_message()
                .stored(2)
                .previous(1)
                .path(&Path::new("/a").join(META_PROPERTIES))
                .call(),
            "disagreeing stored node IDs" => format!(
                "Stored node id 4 doesn't match previous node id 3 in {}. If you moved your data, make sure your configured node id matches. If you intend to create a new node, you should remove all data in your data directories.",
                Path::new("/b").join(META_PROPERTIES).display()
            ),
            "a reserved directory ID" => {
                "Invalid reserved directory ID AAAAAAAAAAAAAAAAAAAAAg found in /a".into()
            }
            "a duplicate directory ID" => format!(
                "Duplicate directory ID Optional[{}] found. It was the ID of /a, but also of /b",
                DirectoryId(Uuid::from_u128(0x1000))
            ),
            _ => panic!("unknown directory conflict {reason}"),
        };
        assert2::check!(meta_file.verified == Some(Err(expected)));
    }
    rstest_bdd_macros::scenarios!("tests/features/meta_properties_contract", fixtures = [meta_file: MetaFileScenario]);

    /// Every file krabka writes reads back as the same ids.
    #[yare::parameterized(
        ordinary = { meta(0, Some(DIRECTORY)) },
        reserved_directory_id = { meta(1, Some("AAAAAAAAAAAAAAAAAAAAZA")) },
        high_bit_directory_id = { meta(2_147_483_647, Some("_____________________w")) },
        no_directory_id = { meta(7, None) },
        random_directory_id = { MetaProperties {
                cluster_id: ClusterId(Uuid::from_u128(0)),
                node_id: 3,
                directory_id: Some(DirectoryId::random()),
            } },
    )]
    fn the_written_file_parses_back_to_the_same_ids(written: MetaProperties) {
        let tmp = tempfile::tempdir().expect("tempdir");
        written.write(tmp.path()).expect("write");
        assert2::check!(MetaProperties::read(tmp.path()).expect("read") == Some(written));
        assert2::check!(!tmp.path().join(META_PROPERTIES_TMP).exists());
    }

    #[test]
    fn an_empty_directory_has_no_meta_properties() {
        let empty = tempfile::tempdir().expect("tempdir");
        assert2::check!(MetaProperties::read(empty.path()).expect("read").is_none());
    }

    /// `Properties.load` reads more than `store` writes: the file of an
    /// older Java, `:` and white space as separators, comments, line
    /// continuations, escapes, and `\r\n`. A key given twice keeps its last
    /// value.
    #[yare::parameterized(
        kafka_hash_order = { "#\n#Thu Feb 29 12:34:56 UTC 2024\nnode.id=1\nversion=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n" },
        no_comments = { "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
        colon_and_whitespace_separators = { "version : 1\n  cluster.id:AQIDBAUGBwgJCgsMDQ4PEA\nnode.id 1\n\
             \tdirectory.id \t= U-hCdTzaSPyFB3swwc7Qdw" },
        comments_and_blank_lines = { "! a comment\n\n   # another\nversion=1\n\n\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\n\
             node.id=1\ndirectory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
        crlf_and_cr_endings = { "#\r\nversion=1\r\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\rnode.id=1\r\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\r\n" },
        continued_line = { "version=1\ncluster.id=AQIDBAUGBwgJ\\\n    CgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\\\r\n" },
        escaped_key_and_value = { "vers\\ion=\\u0031\ncluster\\.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyF\\B3swwc7Qdw\n" },
        duplicate_key_keeps_last = { "version=1\nnode.id=9\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
        unknown_key_is_ignored = { "version=1\ncluster.id=AQIDBAUGBwgJCgsMDQ4PEA\nnode.id=1\nbroker.id=4\n\
             directory.id=U-hCdTzaSPyFB3swwc7Qdw\n" },
    )]
    fn the_reader_accepts_what_properties_load_accepts(text: &str) {
        let want = meta(1, Some(DIRECTORY));
        assert2::check!(
            MetaProperties::parse(text.as_bytes()).map_err(|e| e.to_string()) == Ok(want),
            "{text:?}"
        );
    }
}
