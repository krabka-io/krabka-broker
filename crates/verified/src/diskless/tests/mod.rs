use {super::*, assert2::check};

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct RetentionMillis(i64);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct RetentionBytes(u64);

#[derive(derive_more::From, derive_more::Into, Clone, Copy, Default)]
struct LogicalOffset(i64);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
struct UnixMillis(i64);

#[bon::builder]
fn policy(
    #[builder(into)] retention: Option<RetentionMillis>,
    #[builder(into)] bytes: Option<RetentionBytes>,
    #[builder(into, default)] start: LogicalOffset,
    #[builder(into, default = UnixMillis (1_000))] now: UnixMillis,
) -> DisklessRetentionPolicy {
    DisklessRetentionPolicy {
        retention_ms: retention.map(|duration| duration.0),
        retention_bytes: bytes.map(|limit| limit.0),
        log_start_offset: start.0,
        now_ms: now.0,
    }
}

mod reclaim_needs_the_grace_period_and_no_reference;

mod logical_range_selects_the_cover_or_the_successor_after_a_gap;
