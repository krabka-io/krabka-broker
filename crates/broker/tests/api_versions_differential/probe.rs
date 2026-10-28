//! Deadlines for the readiness polling this suite does against a container.
//!
//! Waiting for a broker to come up means running something that talks to it and
//! asking whether it worked. The something here is a JVM tool, and the failure
//! that matters is the one where the broker never answers: `AdminClient` sits on
//! its own `default.api.timeout.ms` -- a minute in a stock client -- before the
//! tool exits non-zero, and a wedged `docker exec` need not exit at all. A retry
//! loop that blocks on each attempt therefore bounds nothing, and the "give up
//! after a minute" it looks like is really "give up after an hour, or never".
//!
//! So both halves of the loop carry a deadline: [`wait_bounded`] kills a child
//! that outruns the budget it was given, and [`retry_until`] hands each attempt
//! only the budget that is left. The whole wait then ends when the clock says
//! so, whatever the attempt inside it is doing.

use std::{
    process::{Child, ExitStatus},
    thread,
    time::{Duration, Instant},
};

/// How often [`wait_bounded`] looks at a child that has not exited yet.
///
/// Short enough that killing an overrun probe is prompt, long enough that a
/// minute of waiting is not a minute of spinning.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Wait for `child` to exit, for at most `budget`.
///
/// Returns its exit status when it exits in time. Otherwise the child is killed
/// and reaped -- so the caller leaks neither a process nor a zombie -- and the
/// result is `None`.
///
/// # Panics
///
/// Panics when the child cannot be waited on at all, which means the process
/// handle is broken rather than the child being slow.
pub fn wait_bounded(child: &mut Child, budget: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + budget;
    loop {
        if let Some(status) = child.try_wait().expect("wait on child process") {
            return Some(status);
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            return None;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Run `probe` until it reports ready, or until `budget` runs out.
///
/// Each attempt is handed the budget that remains, and is expected to bound
/// itself by it; `gap` is the pause between a failed attempt and the next one.
/// Returns how many attempts it took to become ready, or `Err` with the number
/// that were made before the deadline passed.
pub fn retry_until(
    budget: Duration,
    gap: Duration,
    mut probe: impl FnMut(Duration) -> bool,
) -> Result<u32, u32> {
    let deadline = Instant::now() + budget;
    let mut attempts = 0;
    loop {
        attempts += 1;
        if probe(deadline.saturating_duration_since(Instant::now())) {
            return Ok(attempts);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(attempts);
        }
        thread::sleep(gap.min(remaining));
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{retry_until, wait_bounded},
        assert2::assert,
        std::{
            process::{Child, Command, ExitStatus, Stdio},
            thread,
            time::{Duration, Instant},
        },
    };

    /// Spawn a shell that runs `script`, with its output discarded.
    fn spawn(script: &str) -> Child {
        Command::new("sh")
            .args(["-c", script])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sh")
    }

    struct ProbeBudget {
        child: Option<Child>,
        status: rstest_bdd::Slot<Option<ExitStatus>>,
        elapsed: Option<Duration>,
        budget: Duration,
        ready_after: Option<u32>,
        blocks: bool,
        seen: u32,
        attempts: Option<Result<u32, u32>>,
    }
    #[rstest::fixture]
    fn probe() -> ProbeBudget {
        // Start this scenario with fresh inputs and observations.

        ProbeBudget {
            child: None,
            status: rstest_bdd::Slot::default(),
            elapsed: None,
            budget: Duration::ZERO,
            ready_after: None,
            blocks: false,
            seen: 0,
            attempts: None,
        }
    }
    #[rstest_bdd_macros::given("a child process executing {script}")]
    fn probe_child(probe: &mut ProbeBudget, script: String) { probe.child = Some(spawn(&script)); }
    #[rstest_bdd_macros::given("the child has already exited")]
    fn probe_child_exited(probe: &mut ProbeBudget) {
        probe
            .child
            .as_mut()
            .expect("child spawned")
            .wait()
            .expect("wait on child process");
    }
    #[rstest_bdd_macros::when("the child is waited on with a {millis} millisecond budget")]
    fn bounded_child_wait(probe: &mut ProbeBudget, millis: u64) {
        let started = Instant::now();
        probe.status.set(wait_bounded(
            probe.child.as_mut().expect("child spawned"),
            Duration::from_millis(millis),
        ));
        probe.elapsed = Some(started.elapsed());
    }
    #[rstest_bdd_macros::then("the child reports successful exit {successful}")]
    fn probe_exit_status(probe: &ProbeBudget, successful: bool) {
        let status = probe.status.get().expect("child waited on");
        assert!(status.map(|status| status.success()) == Some(successful));
    }
    #[rstest_bdd_macros::then("the overrun child is killed and reaped within 30 seconds")]
    fn overrun_child_reaped(probe: &mut ProbeBudget) {
        assert!(probe.status.get().expect("child waited on").is_none());
        assert!(probe.elapsed.expect("wait measured") < Duration::from_secs(30));
        assert!(
            probe
                .child
                .as_mut()
                .expect("child spawned")
                .try_wait()
                .expect("wait on child process")
                .is_some()
        );
    }
    #[rstest_bdd_macros::given("a readiness probe that becomes ready on attempt {attempt}")]
    fn eventual_readiness(probe: &mut ProbeBudget, attempt: u32) {
        probe.ready_after = Some(attempt);
    }
    #[rstest_bdd_macros::given("a readiness probe that never becomes ready")]
    fn unavailable_probe(probe: &mut ProbeBudget) { probe.ready_after = None; }
    #[rstest_bdd_macros::given("each probe blocks for all its remaining budget")]
    fn blocking_probe(probe: &mut ProbeBudget) { probe.blocks = true; }
    #[rstest_bdd_macros::when(
        "readiness is retried for {millis} milliseconds with a {gap} millisecond gap"
    )]
    fn retry_probe(probe: &mut ProbeBudget, millis: u64, gap: u64) {
        probe.budget = Duration::from_millis(millis);
        let started = Instant::now();
        let ready_after = probe.ready_after;
        let blocks = probe.blocks;
        let seen = &mut probe.seen;
        probe.attempts = Some(retry_until(
            probe.budget,
            Duration::from_millis(gap),
            |remaining| {
                *seen += 1;
                if blocks {
                    thread::sleep(remaining);
                }
                ready_after == Some(*seen)
            },
        ));
        probe.elapsed = Some(started.elapsed());
    }
    #[rstest_bdd_macros::then("readiness succeeds after exactly {attempts} attempts")]
    fn ready_attempt_count(probe: &ProbeBudget, attempts: u32) {
        assert!(probe.attempts == Some(Ok(attempts)));
        assert!(probe.seen == attempts);
    }
    #[rstest_bdd_macros::then("the blocking probe exhausts its budget within ten budgets")]
    fn blocking_deadline(probe: &ProbeBudget) {
        assert!(
            probe
                .attempts
                .as_ref()
                .expect("readiness attempted")
                .is_err()
        );
        assert!(probe.elapsed.expect("retry measured") < probe.budget * 10);
    }
    #[rstest_bdd_macros::then("readiness fails at the deadline within 30 seconds")]
    fn readiness_deadline(probe: &ProbeBudget) {
        assert!(
            probe
                .attempts
                .as_ref()
                .expect("readiness attempted")
                .is_err()
        );
        let elapsed = probe.elapsed.expect("retry measured");
        assert!(elapsed >= Duration::from_millis(200));
        assert!(elapsed < Duration::from_secs(30));
    }
    rstest_bdd_macros::scenarios!("tests/features/migrated/tests/api_versions_differential/probe.feature", fixtures = [probe: ProbeBudget]);
}
