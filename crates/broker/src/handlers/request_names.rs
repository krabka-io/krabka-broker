//! Topic names that recur in an admin request, in their first-row order.

pub(super) fn duplicate_names<'a>(names: impl Clone + IntoIterator<Item = &'a str>) -> Vec<String> {
    let mut counts = std::collections::HashMap::<&str, usize>::new();
    for name in names.clone() {
        *counts.entry(name).or_default() += 1;
    }
    let mut seen = std::collections::HashSet::new();
    names
        .into_iter()
        .filter(|name| counts[name] > 1 && seen.insert(*name))
        .map(str::to_owned)
        .collect()
}
