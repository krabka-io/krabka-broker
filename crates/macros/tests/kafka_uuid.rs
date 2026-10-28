//! The derive delegates its string representation, parser and errors to the local codec.
#![cfg(test)]

use assert2::assert;

type KafkaUuidError = &'static str;

fn encode(value: u32) -> String { format!("kafka-{value}") }

fn decode(input: &str) -> Result<u32, KafkaUuidError> {
    input
        .strip_prefix("kafka-")
        .ok_or("expected Kafka UUID")?
        .parse()
        .map_err(|_| "invalid Kafka UUID")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::KafkaUuid)]
struct ClusterId(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_macros::KafkaUuid)]
struct DirectoryId(u32);

struct DisplayScenario {
    cluster: ClusterId,
    directory: DirectoryId,
    rendered: Vec<String>,
}

#[rstest::fixture]
fn display() -> DisplayScenario {
    // Start this scenario with fresh inputs and observations.

    DisplayScenario {
        cluster: ClusterId(0),
        directory: DirectoryId(0),
        rendered: Vec::new(),
    }
}

#[rstest_bdd_macros::given("cluster and directory UUID wrappers contain {value}")]
fn same_uuid_value(display: &mut DisplayScenario, value: u32) {
    display.cluster = ClusterId(value);
    display.directory = DirectoryId(value);
}

#[rstest_bdd_macros::when("UUIDs are displayed with default width fill alignment and precision")]
fn format_uuid(display: &mut DisplayScenario) {
    let cluster = display.cluster;
    let directory = display.directory;
    display.rendered = vec![
        cluster.to_string(),
        directory.to_string(),
        format!("{cluster:>10}"),
        format!("{directory:*<10}"),
        format!("{cluster:_^10}"),
        format!("{directory:.5}"),
    ];
}

#[rstest_bdd_macros::then("both UUID wrappers delegate text and formatting to their local codec")]
fn formatted_uuid(display: &DisplayScenario) {
    let rendered = &display.rendered;
    assert!(rendered[0] == "kafka-7");
    assert!(rendered[1] == rendered[0]);
    assert!(rendered[2] == "   kafka-7");
    assert!(rendered[3] == "kafka-7***");
    assert!(rendered[4] == "_kafka-7__");
    assert!(rendered[5] == "kafka");
}

rstest_bdd_macros::scenarios!("tests/features/uuid_representation", fixtures = [display: DisplayScenario, codec: CodecScenario]);

struct CodecScenario {
    cluster: ClusterId,
    directory: DirectoryId,
    parsed_cluster: Option<Result<ClusterId, KafkaUuidError>>,
    parsed_directory: Option<Result<DirectoryId, KafkaUuidError>>,
    wrong_prefix: Option<Result<ClusterId, KafkaUuidError>>,
    wrong_number: Option<Result<DirectoryId, KafkaUuidError>>,
    cluster_json: String,
    directory_json: String,
    decoded_cluster: Option<Result<ClusterId, String>>,
    decoded_directory: Option<Result<DirectoryId, String>>,
    numeric_json: Option<Result<ClusterId, String>>,
    malformed_json: Option<Result<DirectoryId, String>>,
}

#[rstest::fixture]
fn codec() -> CodecScenario {
    // Start this scenario with fresh inputs and observations.

    CodecScenario {
        cluster: ClusterId(0),
        directory: DirectoryId(0),
        parsed_cluster: None,
        parsed_directory: None,
        wrong_prefix: None,
        wrong_number: None,
        cluster_json: String::new(),
        directory_json: String::new(),
        decoded_cluster: None,
        decoded_directory: None,
        numeric_json: None,
        malformed_json: None,
    }
}

#[rstest_bdd_macros::given("cluster UUID {cluster} and directory UUID {directory}")]
fn uuid_values(codec: &mut CodecScenario, cluster: u32, directory: u32) {
    codec.cluster = ClusterId(cluster);
    codec.directory = DirectoryId(directory);
}

#[rstest_bdd_macros::when("UUID strings and JSON are encoded and decoded")]
fn uuid_codecs(codec: &mut CodecScenario) {
    codec.parsed_cluster = Some("kafka-7".parse());
    codec.parsed_directory = Some("kafka-9".parse());
    codec.wrong_prefix = Some("hyphenated".parse());
    codec.wrong_number = Some("kafka-many".parse());
    codec.cluster_json = serde_json::to_string(&codec.cluster).expect("cluster UUID JSON");
    codec.directory_json = serde_json::to_string(&codec.directory).expect("directory UUID JSON");
    codec.decoded_cluster =
        Some(serde_json::from_str(r#""kafka-7""#).map_err(|error| error.to_string()));
    codec.decoded_directory =
        Some(serde_json::from_str(r#""kafka-9""#).map_err(|error| error.to_string()));
    codec.numeric_json = Some(serde_json::from_str("7").map_err(|error| error.to_string()));
    codec.malformed_json =
        Some(serde_json::from_str(r#""kafka-many""#).map_err(|error| error.to_string()));
}

#[rstest_bdd_macros::then("UUID strings and JSON retain their values and local codec errors")]
fn codec_results(codec: &CodecScenario) {
    assert!(codec.parsed_cluster == Some(Ok(ClusterId(7))));
    assert!(codec.parsed_directory == Some(Ok(DirectoryId(9))));
    assert!(codec.wrong_prefix == Some(Err("expected Kafka UUID")));
    assert!(codec.wrong_number == Some(Err("invalid Kafka UUID")));
    assert!(codec.cluster_json == r#""kafka-7""#);
    assert!(codec.directory_json == r#""kafka-9""#);
    assert!(codec.decoded_cluster == Some(Ok(ClusterId(7))));
    assert!(codec.decoded_directory == Some(Ok(DirectoryId(9))));
    assert!(
        codec
            .numeric_json
            .as_ref()
            .expect("numeric JSON decoded")
            .is_err()
    );
    let error = codec
        .malformed_json
        .as_ref()
        .expect("malformed JSON decoded")
        .as_ref()
        .unwrap_err();
    assert!(error.starts_with("invalid Kafka UUID"));
}
