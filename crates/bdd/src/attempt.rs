//! The result of a step that attempts an action whose failure a later step
//! checks.

/// What a When step that "attempts to" or "tries to" do something hands to the
/// Then steps after it.
///
/// A step that may fail on purpose returns an `Attempt` rather than failing
/// the scenario. The runner stores the returned value in the scenario context
/// in place of the fixture of the same type, so a later Then that takes that
/// fixture reads it. The scenario declares the fixture with its default,
/// [`Attempt::NotExecuted`].
///
/// # Examples
///
/// ```
/// use {assert2::assert, krabka_bdd::Attempt};
///
/// let attempt: Attempt<u32, String> =
///     Attempt::of(|| "x".parse::<u32>().map_err(|e| e.to_string()));
/// attempt.assert_failed_with(|error| error.contains("invalid digit"));
/// assert!(!attempt.is_not_executed());
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Attempt<T, E> {
    /// No step has made the attempt.
    #[default]
    NotExecuted,
    /// The attempt succeeded with this value.
    Succeeded(T),
    /// The attempt failed with this error.
    Failed(E),
}

impl<T, E> Attempt<T, E> {
    /// Runs `f` and records its result.
    pub fn of(f: impl FnOnce() -> Result<T, E>) -> Self { Self::from_result(f()) }

    /// Records `result`.
    pub fn from_result(result: Result<T, E>) -> Self {
        match result {
            Ok(value) => Self::Succeeded(value),
            Err(error) => Self::Failed(error),
        }
    }

    /// Returns the value of a successful attempt.
    ///
    /// # Panics
    ///
    /// Panics, naming the state, when the attempt failed or was not made.
    #[must_use]
    #[track_caller]
    pub fn succeeded(&self) -> &T {
        match self {
            Self::Succeeded(value) => value,
            Self::Failed(_) | Self::NotExecuted => unexpected("succeeded", self.state()),
        }
    }

    /// Returns the error of a failed attempt.
    ///
    /// # Panics
    ///
    /// Panics, naming the state, when the attempt succeeded or was not made.
    #[must_use]
    #[track_caller]
    pub fn failed(&self) -> &E {
        match self {
            Self::Failed(error) => error,
            Self::Succeeded(_) | Self::NotExecuted => unexpected("failed", self.state()),
        }
    }

    /// Checks that the attempt failed with an error that `expected` accepts.
    ///
    /// # Panics
    ///
    /// Panics when the attempt did not fail, or when `expected` returns
    /// `false` for its error.
    #[track_caller]
    pub fn assert_failed_with(&self, expected: impl FnOnce(&E) -> bool) {
        if !expected(self.failed()) {
            panic!("the attempt failed, but not with the expected error");
        }
    }

    /// Reports whether no step has made the attempt.
    #[must_use]
    pub const fn is_not_executed(&self) -> bool { matches!(self, Self::NotExecuted) }

    /// The state's name in panic messages.
    const fn state(&self) -> &'static str {
        match self {
            Self::NotExecuted => "was not executed",
            Self::Succeeded(_) => "succeeded",
            Self::Failed(_) => "failed",
        }
    }
}

#[track_caller]
fn unexpected(wanted: &str, actual: &str) -> ! {
    panic!("expected the attempt to have {wanted}, but it {actual}");
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest, rstest_bdd::panic_message, std::panic};

    type Probe = Attempt<u32, &'static str>;

    #[rstest]
    #[case::ok(Ok(3), Probe::Succeeded(3))]
    #[case::err(Err("no"), Probe::Failed("no"))]
    fn from_result_records_the_result(
        #[case] result: Result<u32, &'static str>,
        #[case] expected: Probe,
    ) {
        assert!(Probe::from_result(result) == expected);
        assert!(Probe::of(|| result) == expected);
    }

    #[test]
    fn default_is_not_executed() {
        assert!(Probe::default() == Probe::NotExecuted);
        assert!(Probe::default().is_not_executed());
        assert!(!Probe::Failed("x").is_not_executed());
    }

    #[test]
    fn accessors_return_the_matching_payload() {
        assert!(*Probe::Succeeded(4).succeeded() == 4);
        assert!(*Probe::Failed("bad").failed() == "bad");
        Probe::Failed("bad").assert_failed_with(|error| *error == "bad");
    }

    fn panic_of(f: impl FnOnce() + panic::UnwindSafe) -> String {
        let payload = panic::catch_unwind(f).expect_err("the call panics");
        panic_message(payload.as_ref())
    }

    #[rstest]
    #[case::succeeded_on_failed(
        || { let _ = Probe::Failed("x").succeeded(); },
        "expected the attempt to have succeeded, but it failed",
    )]
    #[case::succeeded_on_not_executed(
        || { let _ = Probe::NotExecuted.succeeded(); },
        "expected the attempt to have succeeded, but it was not executed",
    )]
    #[case::failed_on_succeeded(
        || { let _ = Probe::Succeeded(1).failed(); },
        "expected the attempt to have failed, but it succeeded",
    )]
    #[case::assert_failed_with_on_not_executed(
        || Probe::NotExecuted.assert_failed_with(|_| true),
        "expected the attempt to have failed, but it was not executed",
    )]
    #[case::assert_failed_with_wrong_error(
        || Probe::Failed("x").assert_failed_with(|error| *error == "y"),
        "the attempt failed, but not with the expected error",
    )]
    fn wrong_state_panics_naming_it(#[case] call: fn(), #[case] expected: &str) {
        assert!(panic_of(call) == expected);
    }
}
