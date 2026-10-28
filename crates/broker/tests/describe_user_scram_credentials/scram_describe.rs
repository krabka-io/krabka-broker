//! Authorized SCRAM descriptions report seeded unknown and duplicate users.
use {
    crate::scram_cluster::{ScramDescriptions, scram},
    assert2::assert,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::DataTableRow,
};
#[rstest_bdd_macros::then("alice has a successful credential row containing mechanism 2")]
fn alice_scram_credential_reported(scram: &ScramDescriptions) {
    let (_, users) = scram.response.as_ref().expect("SCRAM users described");
    let alice = users
        .iter()
        .find(|(user, _, _)| user == "alice")
        .expect("alice must appear in response");
    assert!(alice.1 == 0);
    assert!(alice.2.iter().any(|(mechanism, _)| *mechanism == 2));
}
#[rstest_bdd_macros::then("ghost has resource not found error 91")]
fn missing_scram_user_reported(scram: &ScramDescriptions) {
    let (_, users) = scram.response.as_ref().expect("SCRAM users described");
    let ghost = users
        .iter()
        .find(|(user, _, _)| user == "ghost")
        .expect("ghost must appear in response");
    assert!(ghost.1 == 91);
}
#[derive(DataTableRow)]
struct ExpectedScramUser {
    user: String,
    error: i16,
    credentials: String,
}
#[rstest_bdd_macros::then("duplicate names collapse to these exact per-user results:")]
fn duplicate_scram_users_reported(
    scram: &ScramDescriptions,
    #[datatable] rows: Rows<ExpectedScramUser>,
) {
    let (_, users) = scram.response.as_ref().expect("SCRAM users described");
    let mut expected: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let credentials: Vec<(i8, i32)> = if row.credentials == "none" {
                Vec::new()
            } else {
                row.credentials
                    .split(',')
                    .map(|pair| {
                        let (mechanism, iterations) =
                            pair.split_once(':').expect("mechanism:iterations");
                        (
                            mechanism.parse().expect("mechanism code"),
                            iterations.parse().expect("iteration count"),
                        )
                    })
                    .collect()
            };
            (row.user, row.error, credentials)
        })
        .collect();
    expected.sort_by(|left, right| left.0.cmp(&right.0));
    let mut actual = users.clone();
    actual.sort_by(|left, right| left.0.cmp(&right.0));
    assert!(actual == expected);
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_user_scram_credentials/scram_describe.feature",
    name = "describe all users round trip"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_all_users_round_trip(scram: ScramDescriptions) { scram.shutdown().await; }
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_user_scram_credentials/scram_describe.feature",
    name = "describe unknown user returns error"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_unknown_user_returns_error(scram: ScramDescriptions) { scram.shutdown().await; }
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/describe_user_scram_credentials/scram_describe.feature",
    name = "describe duplicate requested user returns single duplicate resource row"
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn describe_duplicate_requested_user_returns_single_duplicate_resource_row(
    scram: ScramDescriptions,
) {
    scram.shutdown().await;
}
