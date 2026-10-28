//! Whether a series of resource samples is level, or is climbing.
//!
//! This is the judgement the soak lane exists to make, so it is a pure function
//! over a list of numbers rather than something the sampling loop decides
//! inline. A soak that crashes is easy to read; a soak that finishes green
//! while its resident set grew by a third is the one that costs an operator a
//! weekend, and the only way to be sure this file catches that is to feed it
//! series whose shape is known.
//!
//! Two rules, applied to every series:
//!
//! 1. **A ceiling.** No sample may exceed a stated bound. This is the absolute
//!    statement -- "a krabka broker under this load stays under 1.5 GiB" -- and
//!    the one an operator can size a machine from.
//! 2. **A trend over the second half.** The first half of a soak is warm-up:
//!    caches fill, the fetch-session table reaches its steady size, the first
//!    segments roll. What has to be flat is what happens *after* that, so the
//!    trend is fitted over the samples from the run's midpoint onwards and
//!    nothing earlier.
//!
//! The trend is a least-squares slope, not a first-to-last difference, because
//! the series this judges are sawtooths by construction: a log directory under
//! retention grows until the cleaner deletes a segment and then drops, over and
//! over. A first-to-last comparison of such a series says whatever the phase of
//! the last sample happens to be. A slope over a whole number of teeth is zero,
//! and over a partial one is small -- which is why the tolerance is a fraction
//! of the series' own mean rather than an absolute number of bytes, and why
//! the segment-roll sawtooth scenario is one of the tests below.

use std::{fmt, time::Duration};

/// Samples the second half must hold before a slope means anything.
///
/// Eight is two-and-a-bit teeth of the fastest sawtooth this lane produces. A
/// series with fewer is not judged level -- it is reported as unjudgeable, and
/// the suite treats that as a failure, because a soak that sampled four times
/// has proved nothing and must not be allowed to look green.
const MIN_TREND_SAMPLES: usize = 8;

/// One observation of one resource.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Point {
    /// Time since the load started.
    pub(crate) elapsed: Duration,
    pub(crate) value: f64,
}

impl Point {
    pub(crate) fn new(elapsed: Duration, value: f64) -> Self { Self { elapsed, value } }
}

/// What a series is allowed to do.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bound {
    /// No sample may exceed this.
    pub(crate) ceiling: f64,
    /// The rise the fitted slope may account for across the second half, as a
    /// fraction of that half's mean.
    pub(crate) drift_fraction: f64,
    /// A floor under the denominator of that fraction, so a series that sits
    /// near zero -- an idle gauge, a counter nobody moved -- is not failed by
    /// arithmetic noise on a mean of 3.
    pub(crate) floor: f64,
}

/// A named list of observations of one resource.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    /// What was sampled, e.g. `broker-2 rss`.
    pub(crate) name: String,
    /// What the numbers are, e.g. `bytes`.
    pub(crate) unit: &'static str,
    pub(crate) points: Vec<Point>,
}

impl Series {
    pub(crate) fn new(name: impl Into<String>, unit: &'static str) -> Self {
        Self {
            name: name.into(),
            unit,
            points: Vec::new(),
        }
    }

    pub(crate) fn push(&mut self, elapsed: Duration, value: f64) {
        self.points.push(Point::new(elapsed, value));
    }

    /// The samples from the run's midpoint onwards.
    ///
    /// Midpoint by elapsed time rather than by index, so a sampling loop that
    /// slipped -- a slow `/metrics` scrape, a busy runner -- still splits the
    /// run where the clock says, not where the vector happens to be halved.
    fn second_half(&self) -> &[Point] {
        let (Some(first), Some(last)) = (self.points.first(), self.points.last()) else {
            return &[];
        };
        let span = last.elapsed.saturating_sub(first.elapsed);
        let midpoint = first.elapsed + span / 2;
        let start = self
            .points
            .iter()
            .position(|p| p.elapsed >= midpoint)
            .unwrap_or(self.points.len());
        &self.points[start..]
    }
}

/// What [`judge`] concluded about one series.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Under the ceiling and flat over the second half.
    Level {
        series: String,
        peak: Point,
        /// The rise the second-half slope accounts for, in the series' unit.
        rise: f64,
    },
    /// A sample exceeded the ceiling. Names the worst one.
    Ceiling {
        series: String,
        unit: &'static str,
        worst: Point,
        ceiling: f64,
    },
    /// The second half trends upward by more than the tolerance.
    Drift {
        series: String,
        unit: &'static str,
        /// First sample of the judged half.
        from: Point,
        /// Last sample of the judged half.
        to: Point,
        /// Rise the fitted slope accounts for across that half.
        rise: f64,
        /// What the rise was compared against.
        allowed: f64,
    },
    /// Too few samples in the second half to fit anything to.
    TooFewSamples { series: String, judged: usize },
}

impl Verdict {
    /// Whether this verdict fails the soak.
    pub(crate) fn is_failure(&self) -> bool { !matches!(self, Self::Level { .. }) }
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Level { series, peak, rise } => write!(
                f,
                "{series}: level -- peak {:.0} at {:?}, second-half rise {rise:+.0}",
                peak.value, peak.elapsed
            ),
            Self::Ceiling {
                series,
                unit,
                worst,
                ceiling,
            } => write!(
                f,
                "{series}: sample at {:?} is {:.0} {unit}, over the {ceiling:.0} {unit} ceiling",
                worst.elapsed, worst.value
            ),
            Self::Drift {
                series,
                unit,
                from,
                to,
                rise,
                allowed,
            } => write!(
                f,
                "{series}: trends upward over the second half -- {:.0} {unit} at {:?} to \
                 {:.0} {unit} at {:?}, a fitted rise of {rise:+.0} {unit} against an \
                 allowance of {allowed:.0}",
                from.value, from.elapsed, to.value, to.elapsed
            ),
            Self::TooFewSamples { series, judged } => write!(
                f,
                "{series}: {judged} samples in the second half, fewer than the \
                 {MIN_TREND_SAMPLES} a trend can be fitted to"
            ),
        }
    }
}

/// A sample count as a float.
///
/// Counts here are in the thousands at most, so the conversion is exact. A run
/// that somehow took more than `u32::MAX` samples would have arithmetic
/// problems well before this one.
fn count(n: usize) -> f64 { f64::from(u32::try_from(n).unwrap_or(u32::MAX)) }

/// The least-squares slope of `points`, in value per second.
///
/// `None` when every sample landed at the same instant, which leaves nothing to
/// regress against.
fn slope_per_second(points: &[Point]) -> Option<f64> {
    let n = count(points.len());
    let xs: Vec<f64> = points.iter().map(|p| p.elapsed.as_secs_f64()).collect();
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = points.iter().map(|p| p.value).sum::<f64>() / n;
    let mut covariance = 0.0;
    let mut variance = 0.0;
    for (x, point) in xs.iter().zip(points) {
        covariance = (x - mean_x).mul_add(point.value - mean_y, covariance);
        variance = (x - mean_x).mul_add(x - mean_x, variance);
    }
    if variance <= f64::EPSILON {
        return None;
    }
    Some(covariance / variance)
}

/// Judge one series against one bound.
///
/// The ceiling is checked over every sample, warm-up included: a broker that
/// touched 4 GiB while filling its caches is over the ceiling whenever it did
/// it. The trend is checked over the second half alone, for the reason in the
/// module documentation.
pub fn judge(series: &Series, bound: Bound) -> Verdict {
    let worst = series
        .points
        .iter()
        .copied()
        .max_by(|a, b| a.value.total_cmp(&b.value));
    let Some(worst) = worst else {
        return Verdict::TooFewSamples {
            series: series.name.clone(),
            judged: 0,
        };
    };
    if worst.value > bound.ceiling {
        return Verdict::Ceiling {
            series: series.name.clone(),
            unit: series.unit,
            worst,
            ceiling: bound.ceiling,
        };
    }

    let judged = series.second_half();
    if judged.len() < MIN_TREND_SAMPLES {
        return Verdict::TooFewSamples {
            series: series.name.clone(),
            judged: judged.len(),
        };
    }
    let (Some(from), Some(to)) = (judged.first().copied(), judged.last().copied()) else {
        return Verdict::TooFewSamples {
            series: series.name.clone(),
            judged: judged.len(),
        };
    };
    let span = to.elapsed.saturating_sub(from.elapsed).as_secs_f64();
    let rise = slope_per_second(judged).unwrap_or(0.0) * span;
    let mean = judged.iter().map(|p| p.value).sum::<f64>() / count(judged.len());
    let allowed = bound.drift_fraction * mean.max(bound.floor);
    if rise > allowed {
        return Verdict::Drift {
            series: series.name.clone(),
            unit: series.unit,
            from,
            to,
            rise,
            allowed,
        };
    }
    Verdict::Level {
        series: series.name.clone(),
        peak: worst,
        rise,
    }
}

/// A series and the bound it is judged against.
pub struct BoundedSeries {
    pub(crate) series: Series,
    pub(crate) bound: Bound,
}

/// Judge every series, returning one verdict each in the order given.
pub fn judge_all(series: &[BoundedSeries]) -> Vec<Verdict> {
    series
        .iter()
        .map(|BoundedSeries { series, bound }| judge(series, *bound))
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
        rstest::fixture,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    fn near(a: f64, b: f64) -> bool { (a - b).abs() < 1.0e-9 }

    fn series(values: &[f64]) -> Series {
        let mut series = Series::new("probe", "widgets");
        for (index, value) in values.iter().enumerate() {
            let index = u64::try_from(index).expect("a test series is short");
            series.push(Duration::from_secs(10 * index), *value);
        }
        series
    }

    fn sawtooth(teeth: usize, period: usize, base: f64, height: f64, per_tooth: f64) -> Vec<f64> {
        let mut values = Vec::new();
        for tooth in 0..teeth {
            for step in 0..period {
                let ramp = height * count(step) / count(period);
                values.push(per_tooth.mul_add(count(tooth), base) + ramp);
            }
        }
        values
    }

    struct DriftScenario {
        observations: Vec<Series>,
        bound: Bound,
        verdicts: Vec<Verdict>,
    }

    #[fixture]
    fn drift() -> DriftScenario {
        // Resource observations and verdicts are private to each scenario.
        DriftScenario {
            observations: Vec::new(),
            bound: Bound {
                ceiling: 1_000.0,
                drift_fraction: 0.20,
                floor: 10.0,
            },
            verdicts: Vec::new(),
        }
    }

    #[given("resource bounds allow ceiling {ceiling} drift fraction {fraction} and floor {floor}")]
    fn configured_bound(drift: &mut DriftScenario, ceiling: f64, fraction: f64, floor: f64) {
        drift.bound = Bound {
            ceiling,
            drift_fraction: fraction,
            floor,
        };
    }

    #[given("{samples} resource samples are constant at {value}")]
    fn constant_observations(drift: &mut DriftScenario, samples: usize, value: f64) {
        drift.observations = vec![series(&vec![value; samples])];
    }

    #[given("{samples} resource samples repeat values {values}")]
    fn cyclic_observations(drift: &mut DriftScenario, samples: usize, values: String) {
        let cycle: Vec<f64> = values
            .split(',')
            .map(|value| value.parse().expect("a numeric resource sample"))
            .collect();
        let values: Vec<_> = (0..samples)
            .map(|index| cycle[index % cycle.len()])
            .collect();
        drift.observations = vec![series(&values)];
    }

    #[given("{samples} resource samples start at {base} and change by {change}")]
    fn linear_observations(drift: &mut DriftScenario, samples: i32, base: f64, change: f64) {
        let values: Vec<_> = (0..samples)
            .map(|index| change.mul_add(f64::from(index), base))
            .collect();
        drift.observations = vec![series(&values)];
    }

    #[given(
        "ten retention sawteeth have six samples with base 400 amplitude 120 and baseline rise {rise}"
    )]
    fn retention_observations(drift: &mut DriftScenario, rise: f64) {
        drift.observations = vec![series(&sawtooth(10, 6, 400.0, 120.0, rise))];
    }

    #[given("resource sample {index} is replaced by {value}")]
    fn anomalous_observation(drift: &mut DriftScenario, index: usize, value: f64) {
        drift.observations[0].points[index].value = value;
    }

    #[given("named rising and flat resource series occur in that order")]
    fn ordered_resource_series(drift: &mut DriftScenario) {
        let values: Vec<_> = (0..40)
            .map(|index| 8.0f64.mul_add(f64::from(index), 300.0))
            .collect();
        let mut rising = series(&values);
        rising.name = "rising".into();
        let mut flat = series(&[500.0; 40]);
        flat.name = "flat".into();
        drift.observations = vec![rising, flat];
    }

    #[when("the soak judge evaluates the resource series")]
    fn judge_observations(drift: &mut DriftScenario) {
        drift.verdicts = vec![judge(&drift.observations[0], drift.bound)];
    }

    #[when("the soak judge evaluates all named series together")]
    fn judge_named_observations(drift: &mut DriftScenario) {
        let bounded: Vec<_> = drift
            .observations
            .iter()
            .cloned()
            .map(|series| BoundedSeries {
                series,
                bound: drift.bound,
            })
            .collect();
        drift.verdicts = judge_all(&bounded);
    }

    #[then("the resource series does not fail its bound")]
    fn resource_is_level(drift: &DriftScenario) {
        check!(!drift.verdicts[0].is_failure());
    }

    #[then("the flat resource verdict is Level with absolute fitted rise below one")]
    fn flat_resource_level(drift: &DriftScenario) {
        assert!(let Verdict::Level { .. } = &drift.verdicts[0]);
        let Verdict::Level { rise, .. } = &drift.verdicts[0] else {
            unreachable!("checked above")
        };
        check!(rise.abs() < 1.0);
    }

    #[then("the rising verdict describes drift from seconds 200 through 390 exceeding 140 widgets")]
    fn rising_resource_drift(drift: &DriftScenario) {
        let judged = &drift.verdicts[0];
        assert!(let Verdict::Drift { .. } = judged);
        let Verdict::Drift { from, to, rise, .. } = judged else {
            unreachable!("checked above")
        };
        check!(from.elapsed == Duration::from_secs(200));
        check!(to.elapsed == Duration::from_secs(390));
        check!(*rise > 140.0, "the second half rises by ~152 widgets");
        check!(judged.to_string().contains("trends upward"));
    }

    #[then("the rising retention baseline fails with a Drift verdict")]
    fn rising_retention_drift(drift: &DriftScenario) {
        let judged = &drift.verdicts[0];
        check!(judged.is_failure(), "{judged}");
        assert!(let Verdict::Drift { .. } = judged);
    }

    #[then(
        "the ceiling verdict identifies 1400 widgets at second 30 and names the 1000 widget ceiling"
    )]
    fn ceiling_breach_identified(drift: &DriftScenario) {
        let judged = &drift.verdicts[0];
        assert!(let Verdict::Ceiling { .. } = judged);
        let Verdict::Ceiling { worst, .. } = judged else {
            unreachable!("checked above")
        };
        check!(worst.elapsed == Duration::from_secs(30));
        check!(near(worst.value, 1_400.0));
        check!(judged.to_string().contains("over the 1000 widgets ceiling"));
    }

    #[then("the resource series fails its bound")]
    fn resource_fails(drift: &DriftScenario) {
        check!(drift.verdicts[0].is_failure());
    }

    #[then("the verdict rejects too few second-half observations")]
    fn insufficient_observations(drift: &DriftScenario) {
        let judged = &drift.verdicts[0];
        assert!(let Verdict::TooFewSamples { .. } = judged);
        let Verdict::TooFewSamples {
            judged: observations,
            ..
        } = judged
        else {
            unreachable!("checked above")
        };
        check!(*observations < MIN_TREND_SAMPLES);
    }

    #[then("rising fails and flat passes with named verdicts in input order")]
    fn ordered_verdicts(drift: &DriftScenario) {
        check!(drift.verdicts.len() == 2);
        check!(drift.verdicts[0].is_failure());
        check!(!drift.verdicts[1].is_failure());
        check!(drift.verdicts[0].to_string().starts_with("rising:"));
        check!(drift.verdicts[1].to_string().starts_with("flat:"));
    }

    scenarios!("tests/features/migrated/tests/soak/drift.feature", fixtures = [drift: DriftScenario]);
}
