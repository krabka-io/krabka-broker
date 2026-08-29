//! Shared unit-test helpers for the telemetry crate.
//!
//! The module holds the injected environment lookup that both the `config` and
//! the `subscriber` tests use, so neither test module reads the real process
//! environment.

pub fn env_from<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
    move |k: &str| {
        pairs
            .iter()
            .find(|(key, _)| *key == k)
            .map(|(_, v)| (*v).to_owned())
    }
}
