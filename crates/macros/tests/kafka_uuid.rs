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
    let text = format!("kafka-{}", display.cluster.0);
    let expected = [
        text.clone(),
        format!("kafka-{}", display.directory.0),
        format!("{text:>10}"),
        format!("{text:*<10}"),
        format!("{text:_^10}"),
        format!("{text:.5}"),
    ];
    assert!(display.rendered == expected);
}

krabka_bdd::scenarios!(
    "tests/features/uuid_representation",
    fixtures = [display: DisplayScenario, codec: CodecScenario],
);

struct CodecScenario {
    cluster: ClusterId,
    directory: DirectoryId,
    parsed_cluster: Option<Result<ClusterId, KafkaUuidError>>,
    parsed_directory: Option<Result<DirectoryId, KafkaUuidError>>,
    wrong_prefix: Option<Result<ClusterId, KafkaUuidError>>,
    wrong_number: Option<Result<DirectoryId, KafkaUuidError>>,
    cluster_json: Option<Result<String, serde_json::Error>>,
    directory_json: Option<Result<String, serde_json::Error>>,
    decoded_cluster: Option<Result<ClusterId, serde_json::Error>>,
    decoded_directory: Option<Result<DirectoryId, serde_json::Error>>,
    numeric_json: Option<Result<ClusterId, serde_json::Error>>,
    malformed_json: Option<Result<DirectoryId, serde_json::Error>>,
}

#[rstest::fixture]
fn codec() -> CodecScenario {
    CodecScenario {
        cluster: ClusterId(0),
        directory: DirectoryId(0),
        parsed_cluster: None,
        parsed_directory: None,
        wrong_prefix: None,
        wrong_number: None,
        cluster_json: None,
        directory_json: None,
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
    let cluster_text = format!("kafka-{}", codec.cluster.0);
    let directory_text = format!("kafka-{}", codec.directory.0);
    codec.parsed_cluster = Some(cluster_text.parse());
    codec.parsed_directory = Some(directory_text.parse());
    codec.wrong_prefix = Some("hyphenated".parse());
    codec.wrong_number = Some("kafka-many".parse());
    codec.cluster_json = Some(serde_json::to_string(&codec.cluster));
    codec.directory_json = Some(serde_json::to_string(&codec.directory));
    codec.decoded_cluster = Some(serde_json::from_str(&format!("\"{cluster_text}\"")));
    codec.decoded_directory = Some(serde_json::from_str(&format!("\"{directory_text}\"")));
    codec.numeric_json = Some(serde_json::from_str("7"));
    codec.malformed_json = Some(serde_json::from_str(r#""kafka-many""#));
}

#[rstest_bdd_macros::then("UUID strings and JSON retain their values and local codec errors")]
fn codec_results(codec: &CodecScenario) {
    assert!(codec.parsed_cluster == Some(Ok(codec.cluster)));
    assert!(codec.parsed_directory == Some(Ok(codec.directory)));
    assert!(codec.wrong_prefix == Some(Err("expected Kafka UUID")));
    assert!(codec.wrong_number == Some(Err("invalid Kafka UUID")));
    assert!(let Some(Ok(cluster_json)) = codec.cluster_json.as_ref());
    assert!(let Some(Ok(directory_json)) = codec.directory_json.as_ref());
    assert!(
        (cluster_json, directory_json)
            == (
                &format!("\"kafka-{}\"", codec.cluster.0),
                &format!("\"kafka-{}\"", codec.directory.0)
            )
    );
    assert!(let Some(Ok(decoded_cluster)) = codec.decoded_cluster.as_ref());
    assert!(let Some(Ok(decoded_directory)) = codec.decoded_directory.as_ref());
    assert!((*decoded_cluster, *decoded_directory) == (codec.cluster, codec.directory));
    assert!(let Some(Err(numeric_error)) = codec.numeric_json.as_ref());
    assert!(numeric_error.is_data());
    assert!(let Some(Err(malformed_error)) = codec.malformed_json.as_ref());
    assert!(malformed_error.is_data());
    assert!(
        malformed_error
            .to_string()
            .starts_with("invalid Kafka UUID")
    );
}
