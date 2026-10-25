//! The expansion of the task counts into task lists.
//!
//! A task is `(subtopology_id, partition)`. [`super::configure_topics`]
//! decides the number of tasks of each subtopology.

use std::collections::BTreeMap;

/// Expands the per-subtopology task counts into the full set of tasks.
///
/// Each subtopology gets the partition list `0..num_tasks`.
#[must_use]
pub fn task_set(num_tasks: &BTreeMap<String, i32>) -> BTreeMap<String, Vec<i32>> {
    num_tasks
        .iter()
        .map(|(sub, &n)| (sub.clone(), (0..n.max(0)).collect()))
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn task_set_enumerates_zero_to_n() {
        let mut num_tasks = BTreeMap::new();
        num_tasks.insert("0".to_owned(), 3);
        num_tasks.insert("1".to_owned(), 0);
        let set = task_set(&num_tasks);
        assert!(set["0"] == vec![0, 1, 2]);
        assert!(set["1"].is_empty());
    }
}
