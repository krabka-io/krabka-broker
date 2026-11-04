//! The error-version boundary measured with the released 4.4 broker.
use {super::*, assert2::assert};

struct LegacyDelete {
    version: i16,
    input_code: i16,
    result: Option<DeletableGroupResult>,
}

#[rstest::fixture]
fn legacy_delete() -> LegacyDelete {
    LegacyDelete {
        version: 0,
        input_code: 0,
        result: None,
    }
}

#[rstest_bdd_macros::given("DeleteGroups version {version} with coordinator error {code}")]
fn deletion_input(legacy_delete: &mut LegacyDelete, version: i16, code: i16) {
    legacy_delete.version = version;
    legacy_delete.input_code = code;
}

#[rstest_bdd_macros::when("the handler renders the group deletion result")]
fn render_result(legacy_delete: &mut LegacyDelete) {
    legacy_delete.result = Some(deletion_result(
        "app".into(),
        legacy_delete.input_code,
        legacy_delete.version,
    ));
}

#[rstest_bdd_macros::then("the result has error {code} and message {message}")]
fn expected_result(legacy_delete: &LegacyDelete, code: i16, message: String) {
    let row = legacy_delete.result.as_ref().expect("rendered result");
    let expected = (message == "generic")
        .then_some("Topology description plugin failed to delete the topology.");
    assert!(
        *row == DeletableGroupResult {
            group_id: "app".into(),
            error_code: code,
            error_message: expected.map(str::to_owned),
            ..Default::default()
        }
    );
}

rstest_bdd_macros::scenarios!("tests/features/handler_legacy_delete.feature", fixtures = [legacy_delete: LegacyDelete]);
