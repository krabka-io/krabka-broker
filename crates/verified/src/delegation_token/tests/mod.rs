use {super::*, assert2::check};

fn created(max_timestamp_ms: i64, initial_expiry_ms: i64) -> TokenCreateDecision {
    TokenCreateDecision::Create(TokenDeadlines {
        max_timestamp_ms,
        initial_expiry_ms,
    })
}

mod token_visibility_requires_owner_filter_plus_a_relationship;

mod token_mutations_are_generation_bound_live_and_idempotent;

fn token_millis(value: &str) -> i64 {
    match value {
        "maximum" => i64::MAX,
        "minimum" => i64::MIN,
        _ => value.parse().expect("a signed millisecond value"),
    }
}
