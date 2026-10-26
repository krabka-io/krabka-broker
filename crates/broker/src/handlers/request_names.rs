//! Topic names that recur in an admin request, in their first-row order.

use std::collections::{HashMap, HashSet};
pub(super) fn duplicate_names<'a>(names: impl Clone + IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut counts = HashMap::<&str, usize>::new();
    for name in names.clone() {
        *counts.entry(name).or_default() += 1;
    }
    let mut seen = HashSet::new();
    names
        .into_iter()
        .filter(|name| counts.get(name).is_some_and(|&count| count > 1) && seen.insert(*name))
        .map(str::to_owned)
        .collect()
}
