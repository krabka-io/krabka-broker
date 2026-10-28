//! The remote-log quota measures Kafka's `SimpleRate` over its sample
//! windows, answers `QuotaUtils.throttleTime`, keeps its samples across a new
//! bound.

use {super::*, assert2::check};

#[derive(Default)]
struct RemoteLogQuota {
    quota: Option<RlmQuotaManager>,
    throttle_ms: Option<i64>,
    acquisition: Option<Result<(), i64>>,
}

#[rstest::fixture]
fn remote_log_quota() -> RemoteLogQuota {
    // Each rate history and live quota bound belong to one independent manager.
    RemoteLogQuota::default()
}

#[rstest_bdd_macros::given(
    "a remote-log quota permits {rate} bytes per second with Kafka sample windows"
)]
fn supplied_remote_log_quota(remote_log_quota: &mut RemoteLogQuota, rate: i64) {
    remote_log_quota.quota = Some(RlmQuotaManager::new(rate, QuotaWindow::default()));
}

#[rstest_bdd_macros::when("the manager records {bytes} remote-log bytes at millisecond {at}")]
fn record_remote_log_quota(remote_log_quota: &RemoteLogQuota, bytes: u64, at: i64) {
    remote_log_quota
        .quota
        .as_ref()
        .expect("quota manager was configured")
        .record_at(bytes, at);
}

#[rstest_bdd_macros::when("the manager samples the remote-log throttle at millisecond {at}")]
fn sample_remote_log_quota(remote_log_quota: &mut RemoteLogQuota, at: i64) {
    remote_log_quota.throttle_ms = Some(
        remote_log_quota
            .quota
            .as_ref()
            .expect("quota manager was configured")
            .throttle_time_ms_at(at),
    );
}

#[rstest_bdd_macros::when("the remote-log byte-rate bound is changed to {rate}")]
fn update_remote_log_quota(remote_log_quota: &RemoteLogQuota, rate: i64) {
    remote_log_quota
        .quota
        .as_ref()
        .expect("quota manager was configured")
        .update_quota(rate);
}

#[rstest_bdd_macros::when(
    "the manager tries to acquire {bytes} remote-log bytes at millisecond {at}"
)]
fn acquire_remote_log_quota(remote_log_quota: &mut RemoteLogQuota, bytes: u64, at: i64) {
    remote_log_quota.acquisition = Some(
        remote_log_quota
            .quota
            .as_ref()
            .expect("quota manager was configured")
            .try_acquire_at(bytes, at),
    );
}

#[rstest_bdd_macros::then("the remote-log throttle is {milliseconds} milliseconds")]
fn expected_remote_log_throttle(remote_log_quota: &RemoteLogQuota, milliseconds: i64) {
    check!(remote_log_quota.throttle_ms == Some(milliseconds));
}

#[rstest_bdd_macros::then("the remote-log acquisition is admitted")]
fn admitted_remote_log_acquisition(remote_log_quota: &RemoteLogQuota) {
    check!(remote_log_quota.acquisition == Some(Ok(())));
}

#[rstest_bdd_macros::then("the remote-log acquisition is refused for {milliseconds} milliseconds")]
fn refused_remote_log_acquisition(remote_log_quota: &RemoteLogQuota, milliseconds: i64) {
    check!(remote_log_quota.acquisition == Some(Err(milliseconds)));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/remote_log_manager/quota/tests.feature",
    fixtures = [remote_log_quota: RemoteLogQuota]
);

#[test]
fn an_unlimited_quota_never_throttles() {
    let quota = RlmQuotaManager::unlimited();
    quota.record_at(u64::MAX, 0);
    check!(quota.throttle_time_ms_at(0) == 0);
    check!(quota.bytes_per_second() == i64::MAX);
}
