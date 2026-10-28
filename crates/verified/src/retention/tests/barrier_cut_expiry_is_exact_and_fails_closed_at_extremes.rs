use {
    super::*,
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    std::vec,
};

#[derive(Default)]
struct RetentionPasses {
    published: i64,
    retained_cuts: i32,
    barrier_expired: Option<bool>,
    size_debt: Option<u64>,
    local: Vec<LocalRetentionSegment>,
    remote: Vec<RemoteRetentionSegment>,
    local_prefix: Option<usize>,
    remote_prefixes: Option<(usize, usize)>,
}

#[fixture]
fn retention() -> RetentionPasses {
    // Start this scenario with fresh inputs and observations.

    RetentionPasses {
        barrier_expired: None,
        ..RetentionPasses::default()
    }
}

fn cut_id(value: &str) -> i64 {
    match value {
        "minimum" => i64::MIN,
        "minimum-plus-one" => i64::MIN + 1,
        "maximum" => i64::MAX,
        _ => value.parse().expect("barrier cut ID is an integer"),
    }
}

#[given("published barrier cut {published} with {retained} retained cuts")]
fn published_cut(retention: &mut RetentionPasses, published: String, retained: i32) {
    retention.published = cut_id(&published);
    retention.retained_cuts = retained;
}

#[when("held barrier cut {held} is evaluated for expiry")]
fn held_cut_expiry(retention: &mut RetentionPasses, held: String) {
    retention.barrier_expired = Some(barrier_cut_expired(
        retention.published,
        retention.retained_cuts,
        cut_id(&held),
    ));
}

#[then("the held barrier cut has expired: {expired}")]
fn expired_cut(retention: &RetentionPasses, expired: bool) {
    check!(retention.barrier_expired == Some(expired));
}

fn size_debt(value: &str) -> Option<u64> {
    match value {
        "none" => None,
        "maximum" => Some(u64::MAX),
        _ => Some(value.parse().expect("size debt is an unsigned byte count")),
    }
}

#[given("a local retention size debt of {bytes} bytes")]
fn local_debt(retention: &mut RetentionPasses, bytes: String) {
    retention.size_debt = size_debt(&bytes);
}

#[given("a remote retention size debt of {bytes} bytes")]
fn remote_debt(retention: &mut RetentionPasses, bytes: String) {
    retention.size_debt = size_debt(&bytes);
}

#[derive(DataTableRow)]
struct LocalSegmentRow {
    size: u64,
    blocked: bool,
    expired: bool,
}

#[given("these local segments are ordered oldest first:")]
fn local_segments(retention: &mut RetentionPasses, #[datatable] rows: Rows<LocalSegmentRow>) {
    retention.local = rows
        .into_iter()
        .map(|row| {
            if row.blocked {
                blocked(row.expired, row.size)
            } else if row.expired {
                expired(row.size)
            } else {
                fresh(row.size)
            }
        })
        .collect();
}

#[when("Kafka local retention applies its ordered deletion passes")]
fn local_deletion(retention: &mut RetentionPasses) {
    retention.local_prefix = Some(local_retention_prefix(
        &retention.local,
        retention.size_debt,
    ));
}

#[then("the oldest {count} local segments are removed")]
fn removed_local_prefix(retention: &RetentionPasses, count: usize) {
    check!(retention.local_prefix == Some(count));
}

#[derive(DataTableRow)]
struct RemoteSegmentRow {
    size: u64,
    blocked: bool,
    floor_breached: bool,
    time_expired: bool,
}

#[given("these remote segments are ordered oldest first:")]
fn remote_segments(retention: &mut RetentionPasses, #[datatable] rows: Rows<RemoteSegmentRow>) {
    retention.remote = rows
        .into_iter()
        .map(|row| RemoteRetentionSegment {
            blocked: row.blocked,
            ..remote(row.floor_breached, row.time_expired, row.size)
        })
        .collect();
}

#[when("Kafka remote retention evaluates delete-enabled and delete-disabled tiers")]
fn remote_deletion(retention: &mut RetentionPasses) {
    let debt = retention
        .size_debt
        .expect("remote retention has a size debt");
    retention.remote_prefixes = Some((
        remote_retention_prefix(true, &retention.remote, debt),
        remote_retention_prefix(false, &retention.remote, debt),
    ));
}

#[then(
    "the delete-enabled tier removes {count} oldest remote segments and the delete-disabled tier removes none"
)]
fn removed_remote_prefix(retention: &RetentionPasses, count: usize) {
    check!(retention.remote_prefixes == Some((count, 0)));
}

scenarios!("tests/features/migrated/src/retention/tests/barrier_cut_expiry_is_exact_and_fails_closed_at_extremes.feature", fixtures = [retention: RetentionPasses]);
