//! The fault schedule a contract expands to.
//!
//! A fault plan is a template: a first offset, an optional period and a jitter.
//! Expanding it against the contract's seed gives the concrete occurrences,
//! each with a start, an end and the recoveries it is measured for. The same
//! contract always expands to the same schedule, so a failing run can be
//! repeated fault for fault.
//!
//! Faults are bounded in two ways here. They never overlap: an occurrence may
//! start only once the previous one has ended and every recovery it is measured
//! for has had its full deadline. And none runs into the settle window at the
//! end of the load, so reconciliation always reads a cluster that has had time
//! to recover.

use {
    super::contract::{
        Contract, ContractError, FaultKind, RecoveryCheck, RecoveryDeadlines, Target, human,
    },
    krabka_units::prelude::*,
    serde::{Deserialize, Serialize},
};

/// One occurrence of a fault plan.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduledFault {
    /// `<plan>#<n>`, counting occurrences of the plan from zero.
    pub id: String,
    pub plan: String,
    pub kind: FaultKind,
    /// The plan's target, with `any` already resolved from the seed.
    pub target: Target,
    /// Offset from the start of the load.
    #[serde(with = "krabka_units::serde_units::human::time")]
    pub start: Time,
    #[serde(with = "krabka_units::serde_units::human::time")]
    pub duration: Time,
    pub checks: Vec<RecoveryCheck>,
}

impl ScheduledFault {
    /// When the fault's cleanup runs, from the start of the load.
    #[must_use]
    pub fn end(&self) -> Time { self.start + self.duration }

    /// The longest of the contract's deadlines among the recoveries this
    /// fault is measured for: how long after [`Self::end`] the cluster may
    /// still be recovering from it.
    ///
    /// It is the contract's own deadline, never a self-check override: the
    /// override is a judging device, and the cluster is given its real
    /// deadline to recover.
    #[must_use]
    pub fn recovery_budget(&self, deadlines: &RecoveryDeadlines) -> Time {
        self.checks
            .iter()
            .map(|check| check.deadline(deadlines))
            .fold(Time::ZERO, |longest, deadline| {
                if deadline > longest {
                    deadline
                } else {
                    longest
                }
            })
    }
}

/// `SplitMix64`, the generator `java.util.SplittableRandom` uses: one 64-bit
/// state, no dependency, and the same stream on every platform.
fn splitmix64(state: u64) -> u64 {
    let mut z = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// FNV-1a over a plan's name, so each plan draws its own stream from one seed
/// and reordering plans in the file does not move another plan's faults.
fn fnv1a(text: &str) -> u64 {
    text.bytes().fold(0xCBF2_9CE4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01B3)
    })
}

/// The draw for occurrence `n` of `plan` under `seed`.
fn draw(seed: u64, plan: &str, n: u64) -> u64 { splitmix64(seed ^ fnv1a(plan) ^ splitmix64(n)) }

/// A uniform fraction in `[0, 1)` from a draw: its top 53 bits over `2^53`.
fn unit_fraction(draw: u64) -> f64 {
    // 53 bits fit an f64 mantissa exactly, so neither conversion rounds.
    let top = u32::try_from(draw >> 32).unwrap_or(u32::MAX);
    let low = u32::try_from((draw >> 11) & 0x1F_FFFF).unwrap_or(0);
    (f64::from(top) * f64::from(1_u32 << 21) + f64::from(low)) / 9_007_199_254_740_992.0
}

/// Expand every fault plan of `contract` into its occurrences, in start order.
///
/// # Errors
///
/// Returns every pair of occurrences that overlap (counting each one's
/// recovery deadlines), and every plan whose first occurrence would still be
/// running or recovering inside the settle window. A later occurrence that
/// would do so is not an error: the plan's repeats stop before it.
pub fn expand(contract: &Contract) -> Result<Vec<ScheduledFault>, Vec<ContractError>> {
    let quiet_from = contract.duration - contract.settle;
    let mut schedule = Vec::new();
    let mut errors = Vec::new();
    for plan in &contract.faults {
        // A period that does not advance would expand to the same slot forever.
        if plan.every.is_some_and(|every| every <= Time::ZERO) {
            errors.push(ContractError(format!(
                "fault `{}` repeats every {}; a period must be positive",
                plan.name,
                human(plan.every.unwrap_or(Time::ZERO))
            )));
            continue;
        }
        for n in 0_u64.. {
            let slot = match plan.every {
                Some(every) => plan.first_at + every * u64_to_f64(n),
                None if n == 0 => plan.first_at,
                None => break,
            };
            let roll = draw(contract.seed, &plan.name, n);
            let start = slot + plan.jitter * unit_fraction(roll);
            let checks = plan.fault.checks(plan.target);
            let fault = ScheduledFault {
                id: format!("{}#{n}", plan.name),
                plan: plan.name.clone(),
                kind: plan.fault.clone(),
                target: plan.target,
                start,
                duration: plan.duration,
                checks,
            };
            // A repeat that would still be running or recovering when the
            // settle window starts ends the plan. A first occurrence that
            // does not fit is kept, so the validation below reports it: that
            // plan cannot fit the run at all.
            let recovered_at = fault.end() + fault.recovery_budget(&contract.deadlines);
            if n > 0 && (start >= quiet_from || recovered_at > quiet_from) {
                break;
            }
            let target = match plan.target {
                Target::Any => Target::Broker(
                    u32::try_from(splitmix64(roll) % u64::from(contract.cluster.brokers.max(1)))
                        .unwrap_or(0),
                ),
                other => other,
            };
            schedule.push(ScheduledFault { target, ..fault });
            if plan.every.is_none() {
                break;
            }
        }
    }
    schedule.sort_by(|a, b| {
        a.start
            .partial_cmp(&b.start)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    for fault in &schedule {
        if fault.end() + fault.recovery_budget(&contract.deadlines) > quiet_from {
            errors.push(ContractError(format!(
                "fault `{}` ends at {} and may recover until {}, inside the settle window that starts at {}",
                fault.id,
                human(fault.end()),
                human(fault.end() + fault.recovery_budget(&contract.deadlines)),
                human(quiet_from)
            )));
        }
    }
    for pair in schedule.windows(2) {
        let [earlier, later] = pair else { continue };
        let free_at = earlier.end() + earlier.recovery_budget(&contract.deadlines);
        if later.start < free_at {
            errors.push(ContractError(format!(
                "fault `{}` starts at {} and overlaps `{}`, which with its recovery deadlines runs until {}",
                later.id,
                human(later.start),
                earlier.id,
                human(free_at)
            )));
        }
    }
    if errors.is_empty() {
        Ok(schedule)
    } else {
        Err(errors)
    }
}

fn u64_to_f64(n: u64) -> f64 { f64::from(u32::try_from(n).unwrap_or(u32::MAX)) }

#[cfg(test)]
mod tests {
    use {
        super::{expand, unit_fraction},
        crate::soak::contract::{Contract, FaultKind, RecoveryCheck, Target, fixture},
        assert2::{assert, check},
        krabka_units::prelude::*,
    };

    fn periodic(seed: u64) -> Contract {
        let mut contract = fixture::self_check();
        contract.seed = seed;
        contract.duration = hours(2);
        contract.settle = minutes(10);
        contract.faults.truncate(1);
        contract.faults[0].target = Target::Any;
        contract.faults[0].every = Some(minutes(10));
        contract.faults[0].jitter = minutes(2);
        contract.self_check = None;
        contract
    }

    #[test]
    fn the_fixture_expands_to_its_two_faults_in_order() {
        let schedule = expand(&fixture::self_check()).unwrap();
        let ids: Vec<&str> = schedule.iter().map(|fault| fault.id.as_str()).collect();
        check!(ids == ["short-pause#0", "leader-restart#0"]);
        check!(schedule[0].start == secs(30));
        check!(schedule[0].checks == [RecoveryCheck::CatchUpAfterPause]);
        check!(schedule[1].end() == secs(95));
        check!(schedule[1].target == Target::Leader);
    }

    #[test]
    fn one_seed_always_expands_to_one_schedule() {
        check!(expand(&periodic(1392)).unwrap() == expand(&periodic(1392)).unwrap());
    }

    #[test]
    fn another_seed_moves_the_faults() {
        let starts = |seed| -> Vec<Time> {
            expand(&periodic(seed))
                .unwrap()
                .iter()
                .map(|f| f.start)
                .collect()
        };
        check!(starts(1) != starts(2));
    }

    #[test]
    fn a_periodic_plan_fills_the_run_up_to_the_settle_window() {
        let schedule = expand(&periodic(1392)).unwrap();
        // Slots at 30s, 10m30s, ..., 1h40m30s; the 1h50m30s slot is past the
        // settle window at 1h50m.
        check!(schedule.len() == 11);
        for (n, fault) in schedule.iter().enumerate() {
            let slot = secs(30) + minutes(10) * f64::from(u32::try_from(n).unwrap());
            check!(fault.start >= slot);
            check!(fault.start < slot + minutes(2));
            check!(matches!(fault.target, Target::Broker(index) if index < 3));
        }
    }

    #[test]
    fn any_resolves_to_more_than_one_broker_over_a_long_run() {
        let mut contract = periodic(1392);
        contract.duration = hours(24);
        let targets: std::collections::BTreeSet<String> = expand(&contract)
            .unwrap()
            .iter()
            .map(|fault| format!("{:?}", fault.target))
            .collect();
        check!(targets.len() > 1);
    }

    #[test]
    fn a_zero_period_is_refused_rather_than_expanded_forever() {
        let mut contract = periodic(1392);
        contract.faults[0].every = Some(Time::ZERO);
        contract.faults[0].jitter = Time::ZERO;
        let errors = expand(&contract).unwrap_err();
        check!(errors.len() == 1);
        check!(errors[0].0.contains("a period must be positive"));
    }

    #[test]
    fn a_period_shorter_than_the_recovery_budget_is_refused() {
        let mut contract = periodic(1392);
        contract.faults[0].fault = FaultKind::Restart { graceful: true };
        contract.faults[0].every = Some(secs(40));
        contract.faults[0].jitter = Time::ZERO;
        let errors = expand(&contract).unwrap_err();
        assert!(errors.iter().any(|error| error.0.contains("overlaps")));
    }

    #[yare::parameterized(
        zero = { 0, 0.0 },
        max = { u64::MAX, 1.0 - 1.0 / 9_007_199_254_740_992.0 },
        half = { 1 << 63, 0.5 },
    )]
    fn draws_map_to_fractions_below_one(draw: u64, expected: f64) {
        check!(unit_fraction(draw).to_bits() == expected.to_bits());
    }

    /// A plan whose slot at 1h49m40s, 20s before the settle window at 1h50m,
    /// starts inside the window's recovery budget for every seed: the pause
    /// lasts 1s and its catch-up deadline is 30s.
    fn straddling(seed: u64) -> Contract {
        let mut contract = periodic(seed);
        contract.faults[0].first_at = minutes(9) + secs(40);
        contract.faults[0].jitter = secs(10);
        contract
    }

    #[yare::parameterized(
        seed_1 = { 1 },
        seed_7 = { 7 },
        seed_1392 = { 1392 },
    )]
    fn a_repeat_that_would_recover_inside_the_settle_window_is_dropped(seed: u64) {
        let contract = straddling(seed);
        let quiet_from = contract.duration - contract.settle;
        let schedule = expand(&contract).unwrap();
        // Slots at 9m40s, 19m40s, ..., 1h39m40s; the 1h49m40s slot is dropped.
        check!(schedule.len() == 10);
        let last = schedule.last().unwrap();
        check!(last.end() + last.recovery_budget(&contract.deadlines) <= quiet_from);
        // The contract's own validation finds nothing wrong with the schedule.
        // (The fixture without its self-check section is refused for that.)
        let errors = contract.validate();
        check!(
            errors
                .iter()
                .all(|error| !error.0.contains("settle window")),
            "{errors:?}"
        );
    }

    #[yare::parameterized(
        one_shot = { None },
        periodic = { Some(minutes(10)) },
    )]
    fn a_plan_whose_first_occurrence_cannot_fit_is_refused(every: Option<Time>) {
        let mut contract = straddling(1392);
        contract.faults[0].first_at = hours(1) + minutes(49) + secs(40);
        contract.faults[0].every = every;
        let errors = expand(&contract).unwrap_err();
        check!(errors.len() == 1);
        check!(
            errors[0].0.contains("inside the settle window"),
            "{:?}",
            errors
        );
    }
}
