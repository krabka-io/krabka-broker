//! The statistics both lanes share: a median over repeated runs, the relative
//! median absolute deviation around it, and the tolerance calibrated from the
//! two sides' spreads.

/// The median of `values`; an even count averages the middle two. An empty
/// `values` has no median, and gives NaN.
#[must_use]
pub fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    let lower = middle.checked_sub(1).and_then(|lower| values.get(lower));
    match (lower, values.get(middle)) {
        (Some(&lower), Some(&upper)) if values.len().is_multiple_of(2) => {
            f64::midpoint(lower, upper)
        }
        (_, Some(&upper)) => upper,
        (_, None) => f64::NAN,
    }
}

/// The median absolute deviation of `values` around `center`, as a ratio of
/// `center`.
#[must_use]
pub fn relative_mad(values: &[f64], center: f64) -> f64 {
    median(values.iter().map(|value| (value - center).abs()).collect()) / center
}

/// The relative change a comparison allows: `multiplier` times the larger of
/// the two sides' spreads, but never less than `floor`.
#[must_use]
pub fn calibrated_tolerance(
    floor: f64,
    multiplier: f64,
    reference_spread: f64,
    candidate_spread: f64,
) -> f64 {
    floor.max(multiplier * reference_spread.max(candidate_spread))
}
