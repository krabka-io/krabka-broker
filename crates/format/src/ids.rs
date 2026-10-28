//! The two identity UUIDs that `format` writes, in Kafka's string form.
//!
//! `format` makes or accepts a **cluster id**, and makes a **directory id** for
//! each log directory. The directory id of the metadata directory is also the
//! node's KIP-853 voter identity. Both ids go into `meta.properties`, and the
//! cluster id also goes into the bootstrap manifest.
//!
//! Kafka prints an `org.apache.kafka.common.Uuid` as 22 characters of unpadded
//! base64url, for example `AQIDBAUGBwgJCgsMDQ4PEA`, not as the hyphenated hex
//! form that [`uuid::Uuid`] prints. [`ClusterId`] and [`DirectoryId`] use the
//! Kafka form for [`Display`](core::fmt::Display), [`FromStr`](core::str::FromStr),
//! and serde. An id that `format` writes can thus go into a Kafka tool, and an
//! id from a Kafka cluster can go into `format`.
//!
//! [`FromStr`](core::str::FromStr) accepts the same strings as Kafka 4.3.1's
//! `Uuid.fromString`:
//! 22 base64url characters, optionally followed by `==`, with the unused low
//! bits of the last character ignored. Parsing does not refuse the two
//! reserved ids, because Kafka does not: `kafka-storage format -t
//! AAAAAAAAAAAAAAAAAAAAAA` succeeds on `apache/kafka:4.3.1`. The command line
//! also accepts the 36-character hyphenated form, through
//! [`ClusterId::parse_cli`] and [`DirectoryId::parse_cli`]. A file is read in
//! the Kafka form only.
//!
//! Both ids wrap a `uuid::Uuid`, so a bare-`Uuid` signature such as
//! `write_meta_properties(_, cluster, dir)` would compile with the two
//! arguments transposed. A distinct newtype for each id makes the compiler
//! reject that mix-up.

use {
    base64::{
        Engine as _,
        alphabet::URL_SAFE,
        engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig},
    },
    derive_more::{From, Into},
    thiserror::Error,
    uuid::Uuid,
};

/// The longest input, in UTF-16 code units, that Kafka tries to decode.
const MAX_ENCODED_LEN: usize = 24;

/// Unpadded base64url that, as the JDK decoder does, ignores the unused low
/// bits of the last character.
const KAFKA_BASE64: GeneralPurpose = GeneralPurpose::new(
    &URL_SAFE,
    GeneralPurposeConfig::new()
        .with_encode_padding(false)
        .with_decode_allow_trailing_bits(true)
        .with_decode_padding_mode(DecodePaddingMode::RequireNone),
);

/// A Kafka cluster id: the identity that every node of a cluster shares.
///
/// This id is distinct from [`DirectoryId`], which is per log directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, From, Into, krabka_macros::KafkaUuid)]
pub struct ClusterId(pub Uuid);

/// A log directory's stable id (KIP-858). The metadata directory's id is also
/// the node's KIP-853 voter identity.
///
/// `format` writes this id to `meta.properties`, and the broker reads it back
/// on every boot. The type is distinct from [`ClusterId`], so a call site
/// cannot transpose the two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, From, Into, krabka_macros::KafkaUuid)]
pub struct DirectoryId(pub Uuid);

impl ClusterId {
    /// Returns a random id, as Kafka's `Uuid.randomUuid` does.
    ///
    /// The result is never one of Kafka's two reserved ids, and its string
    /// form never starts with `-`, which a command-line parser would read as
    /// an option.
    #[must_use]
    pub fn random() -> Self { Self(random_uuid()) }

    /// Parses a command-line value: the Kafka form, or the hyphenated form.
    ///
    /// # Errors
    ///
    /// Returns [`KafkaUuidError::Hyphenated`] for an input that has the shape
    /// of the hyphenated form but is not hex. Otherwise returns the error of
    /// [`FromStr`](core::str::FromStr).
    pub fn parse_cli(input: &str) -> Result<Self, KafkaUuidError> {
        parse_kafka_or_hyphenated(input).map(Self)
    }
}

impl DirectoryId {
    /// Returns a random directory id, as Kafka's `DirectoryId.random` does.
    ///
    /// The result obeys the rules of [`ClusterId::random`], and it is never
    /// one of the 100 lowest ids, which Kafka reserves as directory-id
    /// sentinels.
    #[must_use]
    pub fn random() -> Self {
        Self(first_accepted(|id| {
            is_reserved_directory_id(id) || starts_with_dash(id)
        }))
    }

    /// Returns a random directory id that is not in `used`, as Kafka's
    /// `MetaPropertiesEnsemble.Copier.generateValidDirectoryId` returns one
    /// that no other directory of the node holds.
    #[must_use]
    pub fn random_unused(used: &[Self]) -> Self {
        loop {
            let id = Self::random();
            if !used.contains(&id) {
                return id;
            }
        }
    }

    /// Returns `true` for one of the 100 lowest ids, which Kafka reserves as
    /// directory ids (Kafka's `DirectoryId.reserved`).
    #[must_use]
    pub fn is_reserved(self) -> bool { is_reserved_directory_id(self.0) }

    /// Parses a command-line value: the Kafka form, or the hyphenated form.
    ///
    /// # Errors
    ///
    /// As [`ClusterId::parse_cli`].
    pub fn parse_cli(input: &str) -> Result<Self, KafkaUuidError> {
        parse_kafka_or_hyphenated(input).map(Self)
    }
}

/// Returns a random id, as Kafka's `Uuid.randomUuid` does: never the zero id
/// or the id with value one, and never one whose string form starts with `-`,
/// which a command-line parser would read as an option.
///
/// [`ClusterId::random`] uses it. So does the broker for a new topic id, which
/// Kafka's `ReplicationControlManager.createTopic` draws the same way.
#[must_use]
pub fn random_uuid() -> Uuid { first_accepted(|id| is_reserved(id) || starts_with_dash(id)) }

/// Kafka's `Uuid.RESERVED`: the zero id and the id with value one.
fn is_reserved(id: Uuid) -> bool { id.as_u128() <= 1 }

/// Kafka's `DirectoryId.reserved`: the most significant half is zero, and the
/// least significant half, as a signed integer, is below 100.
fn is_reserved_directory_id(id: Uuid) -> bool {
    let (most, least) = id.as_u64_pair();
    most == 0 && least.cast_signed() < 100
}

/// Returns `true` when the first base64url character is `-`, which stands for
/// the six-bit value 62.
fn starts_with_dash(id: Uuid) -> bool { id.as_bytes()[0] >> 2 == 62 }

/// Draws version 4 ids until one is not rejected.
fn first_accepted(rejected: impl Fn(Uuid) -> bool) -> Uuid {
    loop {
        let candidate = Uuid::new_v4();
        if !rejected(candidate) {
            return candidate;
        }
    }
}

/// Kafka's `Uuid.toString`: unpadded base64url of the 16 bytes.
fn encode(id: Uuid) -> String { KAFKA_BASE64.encode(id.as_bytes()) }

/// Kafka's `Uuid.fromString`.
///
/// Kafka accepts exactly 22 base64url characters, optionally followed by
/// `==`. It measures the length of a Java string, in UTF-16 code units, and
/// refuses more than 24 before it decodes anything.
fn decode(input: &str) -> Result<Uuid, KafkaUuidError> {
    if input.encode_utf16().count() > MAX_ENCODED_LEN {
        return Err(KafkaUuidError::TooLong {
            prefix: java_prefix(input, MAX_ENCODED_LEN),
        });
    }
    let unpadded = input.strip_suffix("==").unwrap_or(input);
    let bytes = KAFKA_BASE64
        .decode(unpadded)
        .map_err(|error| KafkaUuidError::NotBase64 {
            input: input.to_owned(),
            reason: error.to_string(),
        })?;
    let Ok(uuid) = <[u8; 16]>::try_from(bytes.as_slice()) else {
        return Err(KafkaUuidError::WrongLength {
            input: input.to_owned(),
            decoded_len: bytes.len(),
        });
    };
    Ok(Uuid::from_bytes(uuid))
}

/// Parses the hyphenated form when the input has its shape, and the Kafka
/// form otherwise.
fn parse_kafka_or_hyphenated(input: &str) -> Result<Uuid, KafkaUuidError> {
    let bytes = input.as_bytes();
    if bytes.len() == 36 && [8, 13, 18, 23].iter().all(|&i| bytes.get(i) == Some(&b'-')) {
        return Uuid::try_parse(input).map_err(|error| KafkaUuidError::Hyphenated {
            input: input.to_owned(),
            reason: error.to_string(),
        });
    }
    decode(input)
}

/// Returns the first `max` UTF-16 code units of `input`, as Java's
/// `substring(0, max)` does. A cut that splits a surrogate pair keeps the lone
/// high surrogate, which the JVM prints as `?`.
fn java_prefix(input: &str, max: usize) -> String {
    let mut prefix = String::new();
    let mut units = 0;
    for c in input.chars() {
        let width = c.len_utf16();
        if units + width > max {
            if units < max {
                prefix.push('?');
            }
            break;
        }
        prefix.push(c);
        units += width;
    }
    prefix
}

/// A string that is not a Kafka `Uuid`.
///
/// [`TooLong`](Self::TooLong) and [`WrongLength`](Self::WrongLength) carry the
/// message of the `IllegalArgumentException` that Kafka's `Uuid.fromString`
/// throws for the same input. Kafka's other messages come from
/// `java.util.Base64`; this type names the problem in its own words there.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum KafkaUuidError {
    /// The input is longer than 24 UTF-16 code units.
    #[error("Input string with prefix `{prefix}` is too long to be decoded as a base64 UUID")]
    TooLong {
        /// The first 24 UTF-16 code units of the input.
        prefix: String,
    },

    /// The input is base64url, but not of 16 bytes.
    #[error(
        "Input string `{input}` decoded as {decoded_len} bytes, which is not equal to the \
         expected 16 bytes of a base64-encoded UUID"
    )]
    WrongLength {
        /// The input.
        input: String,
        /// The number of bytes that the input decodes to.
        decoded_len: usize,
    },

    /// The input is not base64url.
    #[error("Input string `{input}` is not base64url: {reason}")]
    NotBase64 {
        /// The input.
        input: String,
        /// What the decoder refused.
        reason: String,
    },

    /// The input has the shape of the hyphenated hex form but is not hex.
    /// Only the `parse_cli` functions return it.
    #[error("Input string `{input}` is not a valid hyphenated UUID: {reason}")]
    Hyphenated {
        /// The input.
        input: String,
        /// What the hex parser refused.
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check, std::iter};

    #[derive(Default)]
    struct UuidTextScenario {
        input: String,
        cluster: Option<Result<String, String>>,
        directory: Option<Result<String, String>>,
        id: Uuid,
        json: String,
        decoded_cluster: Option<Result<ClusterId, String>>,
        decoded_directory: Option<Result<DirectoryId, String>>,
    }
    #[rstest::fixture]
    fn uuid_text() -> UuidTextScenario {
        let input = String::new();
        UuidTextScenario {
            input,
            ..UuidTextScenario::default()
        }
    }
    #[rstest_bdd_macros::given("Kafka UUID input {input}")]
    fn kafka_uuid_text(uuid_text: &mut UuidTextScenario, input: String) {
        uuid_text.input = if input == "empty" {
            String::new()
        } else {
            input
        };
    }
    #[rstest_bdd_macros::when("the input is parsed and rendered as cluster and directory UUIDs")]
    fn parse_render_uuid(uuid_text: &mut UuidTextScenario) {
        uuid_text.cluster = Some(
            uuid_text
                .input
                .parse::<ClusterId>()
                .map(|id| id.to_string())
                .map_err(|error| error.to_string()),
        );
        uuid_text.directory = Some(
            uuid_text
                .input
                .parse::<DirectoryId>()
                .map(|id| id.to_string())
                .map_err(|error| error.to_string()),
        );
    }
    #[rstest_bdd_macros::then("both UUID types render the canonical text {canonical}")]
    fn canonical_uuid_text(uuid_text: &UuidTextScenario, canonical: String) {
        check!(uuid_text.cluster == Some(Ok(canonical.clone())));
        check!(uuid_text.directory == Some(Ok(canonical)));
    }
    #[rstest_bdd_macros::then("both UUID types report {error}")]
    fn rejected_uuid_text(uuid_text: &UuidTextScenario, error: String) {
        check!(
            uuid_text.cluster == Some(Err(error.clone())),
            "{:?}",
            uuid_text.input
        );
        check!(
            uuid_text.directory == Some(Err(error)),
            "{:?} as a directory id",
            uuid_text.input
        );
    }
    #[rstest_bdd_macros::given("cluster UUID bytes 0102030405060708090a0b0c0d0e0f10")]
    fn cluster_uuid_bytes(uuid_text: &mut UuidTextScenario) {
        uuid_text.id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
    }
    #[rstest_bdd_macros::when(
        "cluster UUID JSON is saved and cluster and directory UUID strings are loaded"
    )]
    fn uuid_json_codec(uuid_text: &mut UuidTextScenario) {
        uuid_text.json =
            serde_json::to_string(&ClusterId(uuid_text.id)).expect("encode cluster UUID");
        uuid_text.decoded_cluster = Some(
            serde_json::from_str(r#""AQIDBAUGBwgJCgsMDQ4PEA""#).map_err(|error| error.to_string()),
        );
        uuid_text.decoded_directory = Some(
            serde_json::from_str(r#""01020304-0506-0708-090a-0b0c0d0e0f10""#)
                .map_err(|error| error.to_string()),
        );
    }
    #[rstest_bdd_macros::then(
        "JSON preserves the Kafka UUID string and rejects hyphenated directory UUIDs"
    )]
    fn uuid_json_contract(uuid_text: &UuidTextScenario) {
        check!(uuid_text.json == r#""AQIDBAUGBwgJCgsMDQ4PEA""#);
        check!(uuid_text.decoded_cluster == Some(Ok(ClusterId(uuid_text.id))));
        check!(
            uuid_text
                .decoded_directory
                .as_ref()
                .expect("directory UUID decoded")
                .is_err()
        );
    }

    #[derive(Default)]
    struct UuidRulesScenario {
        id: Uuid,
        sample_sizes: (usize, usize),
        reserved: Option<(bool, bool)>,
        plain: Vec<Uuid>,
        clusters: Vec<ClusterId>,
        directories: Vec<DirectoryId>,
        leading_bytes: Vec<Uuid>,
        dash_results: Vec<(bool, bool)>,
    }
    #[rstest::fixture]
    fn uuid_rules() -> UuidRulesScenario {
        let id = Uuid::nil();
        UuidRulesScenario {
            id,
            ..UuidRulesScenario::default()
        }
    }
    #[rstest_bdd_macros::given("a UUID with high word {high} and low word {low}")]
    fn uuid_words(uuid_rules: &mut UuidRulesScenario, high: u64, low: u64) {
        uuid_rules.id = Uuid::from_u64_pair(high, low);
    }
    #[rstest_bdd_macros::when("cluster and directory UUID reservation rules are evaluated")]
    fn uuid_reservations(uuid_rules: &mut UuidRulesScenario) {
        uuid_rules.reserved = Some((
            is_reserved(uuid_rules.id),
            is_reserved_directory_id(uuid_rules.id),
        ));
    }
    #[rstest_bdd_macros::then(
        "cluster reservation is {cluster} and directory reservation is {directory}"
    )]
    fn expected_reservations(uuid_rules: &UuidRulesScenario, cluster: bool, directory: bool) {
        check!(
            uuid_rules.reserved == Some((cluster, directory)),
            "{}",
            uuid_rules.id
        );
    }
    #[rstest_bdd_macros::given(
        "random UUID sample sizes {plain} plain and {wrappers} for each wrapper"
    )]
    fn random_uuid_rules(uuid_rules: &mut UuidRulesScenario, plain: usize, wrappers: usize) {
        uuid_rules.sample_sizes = (plain, wrappers);
    }
    #[rstest_bdd_macros::when("the random UUID samples are generated")]
    fn draw_random_uuids(uuid_rules: &mut UuidRulesScenario) {
        uuid_rules.plain = iter::repeat_with(random_uuid)
            .take(uuid_rules.sample_sizes.0)
            .collect();
        uuid_rules.clusters = iter::repeat_with(ClusterId::random)
            .take(uuid_rules.sample_sizes.1)
            .collect();
        uuid_rules.directories = iter::repeat_with(DirectoryId::random)
            .take(uuid_rules.sample_sizes.1)
            .collect();
    }
    #[rstest_bdd_macros::then("all generated UUIDs avoid their reserved values and leading dashes")]
    fn generated_uuid_rules(uuid_rules: &UuidRulesScenario) {
        check!(uuid_rules.plain.len() == 5000);
        check!(uuid_rules.clusters.len() == 500);
        check!(uuid_rules.directories.len() == 500);
        for plain in &uuid_rules.plain {
            check!(!is_reserved(*plain));
            check!(!encode(*plain).starts_with('-'));
        }
        for cluster in &uuid_rules.clusters {
            check!(!is_reserved(cluster.0));
            check!(!cluster.to_string().starts_with('-'));
        }
        for directory in &uuid_rules.directories {
            check!(!directory.is_reserved());
            check!(!directory.to_string().starts_with('-'));
        }
    }
    #[rstest_bdd_macros::given("UUIDs spanning every possible leading byte")]
    fn all_uuid_leading_bytes(uuid_rules: &mut UuidRulesScenario) {
        uuid_rules.leading_bytes = (0..=u8::MAX)
            .map(|first| {
                let mut bytes = [0u8; 16];
                bytes[0] = first;
                Uuid::from_bytes(bytes)
            })
            .collect();
    }
    #[rstest_bdd_macros::when(
        "their leading-dash classification and base64 encodings are computed"
    )]
    fn classify_uuid_dash(uuid_rules: &mut UuidRulesScenario) {
        uuid_rules.dash_results = uuid_rules
            .leading_bytes
            .iter()
            .map(|id| (starts_with_dash(*id), encode(*id).starts_with('-')))
            .collect();
    }
    #[rstest_bdd_macros::then(
        "the leading-dash classifier agrees with Kafka encoding for every byte"
    )]
    fn all_uuid_dash_results(uuid_rules: &UuidRulesScenario) {
        check!(uuid_rules.dash_results.len() == 256);
        for (index, (classified, encoded)) in uuid_rules.dash_results.iter().enumerate() {
            check!(classified == encoded, "{index:#x}");
        }
    }
    rstest_bdd_macros::scenarios!("tests/features/kafka_uuid_contract", fixtures = [uuid_text: UuidTextScenario, uuid_rules: UuidRulesScenario]);

    /// The command line also takes the hyphenated form, and only a string of
    /// exactly that shape is read as hex.
    #[yare::parameterized(
        kafka_form = { "AQIDBAUGBwgJCgsMDQ4PEA" },
        lowercase_hex = { "01020304-0506-0708-090a-0b0c0d0e0f10" },
        uppercase_hex = { "01020304-0506-0708-090A-0B0C0D0E0F10" },
    )]
    fn the_command_line_accepts_both_forms(input: &str) {
        let id = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);
        assert2::check!(ClusterId::parse_cli(input) == Ok(ClusterId(id)), "{input}");
        assert2::check!(
            DirectoryId::parse_cli(input) == Ok(DirectoryId(id)),
            "{input}"
        );
    }

    #[test]
    fn the_command_line_refuses_malformed_hex_and_other_uuid_spellings() {
        let not_hex = "01020304-0506-0708-090a-0b0c0d0e0f1g";
        check!(matches!(
            ClusterId::parse_cli(not_hex),
            Err(KafkaUuidError::Hyphenated { input, .. }) if input == not_hex
        ));
        // The other spellings `uuid` accepts are not the hyphenated form.
        check!(matches!(
            ClusterId::parse_cli("0102030405060708090a0b0c0d0e0f10"),
            Err(KafkaUuidError::TooLong { .. })
        ));
    }
}
