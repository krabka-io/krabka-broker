//! The filesystem seam the commands read and write through, and the path
//! arithmetic over it.
//!
//! Every path here is absolute, with `/` separators and no `.` or `..`
//! component, so the commands do their path math as plain strings and a unit
//! test can stand an in-memory tree in for the disk.

/// What a path names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Dir,
}

/// The filesystem operations the commands need.
pub trait FileSystem {
    /// What `path` names, or `None` when nothing is there.
    fn kind(&self, path: &str) -> Option<EntryKind>;

    /// The names of the entries in the directory `dir`, in any order.
    ///
    /// # Errors
    ///
    /// Returns a readable message when the directory cannot be listed.
    fn list(&self, dir: &str) -> Result<Vec<String>, String>;

    /// The UTF-8 text of the file at `path`.
    ///
    /// # Errors
    ///
    /// Returns a readable message when the file cannot be read.
    fn read(&self, path: &str) -> Result<String, String>;

    /// Replaces the file at `path` with `contents`.
    ///
    /// # Errors
    ///
    /// Returns a readable message when the file cannot be written.
    fn write(&self, path: &str, contents: &str) -> Result<(), String>;
}

/// `input` as an absolute, normalized path: a relative `input` is taken from
/// `cwd`, and `.` and `..` components are resolved lexically.
#[must_use]
pub fn absolute(cwd: &str, input: &str) -> String {
    let joined = if input.starts_with('/') {
        input.to_owned()
    } else {
        format!("{cwd}/{input}")
    };
    let mut parts: Vec<&str> = Vec::new();
    for part in joined.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    format!("/{}", parts.join("/"))
}

/// `name` inside the directory `dir`.
#[must_use]
pub fn join(dir: &str, name: &str) -> String {
    if dir == "/" {
        format!("/{name}")
    } else {
        format!("{dir}/{name}")
    }
}

/// The directory that holds `path`, or `None` for the root.
#[must_use]
pub fn parent(path: &str) -> Option<&str> {
    if path == "/" {
        return None;
    }
    path.rsplit_once('/')
        .map(|(dir, _)| if dir.is_empty() { "/" } else { dir })
}

/// The last component of `path`.
#[must_use]
pub fn file_name(path: &str) -> &str { path.rsplit_once('/').map_or(path, |(_, name)| name) }

/// `to` written relative to the directory `from_dir`, with `..` where it
/// leaves `from_dir`, or `.` when the two are the same.
#[must_use]
pub fn relative(from_dir: &str, to: &str) -> String {
    let from: Vec<&str> = components(from_dir).collect();
    let target: Vec<&str> = components(to).collect();
    let common = from
        .iter()
        .zip(&target)
        .take_while(|(left, right)| left == right)
        .count();
    let mut parts: Vec<&str> = vec![".."; from.len() - common];
    parts.extend(target.iter().skip(common));
    if parts.is_empty() {
        ".".to_owned()
    } else {
        parts.join("/")
    }
}

fn components(path: &str) -> impl Iterator<Item = &str> {
    path.split('/').filter(|part| !part.is_empty())
}

#[cfg(test)]
pub mod memory {
    use {
        super::{EntryKind, FileSystem},
        std::{cell::RefCell, collections::BTreeMap},
    };

    /// A tree of files held in memory; a directory exists when a file sits
    /// under it.
    #[derive(Debug, Default)]
    pub struct MemoryFs {
        pub files: RefCell<BTreeMap<String, String>>,
    }

    impl MemoryFs {
        pub fn new<'a>(files: impl IntoIterator<Item = (&'a str, &'a str)>) -> Self {
            Self {
                files: RefCell::new(
                    files
                        .into_iter()
                        .map(|(path, text)| (path.to_owned(), text.to_owned()))
                        .collect(),
                ),
            }
        }

        pub fn file(&self, path: &str) -> Option<String> { self.files.borrow().get(path).cloned() }
    }

    impl FileSystem for MemoryFs {
        fn kind(&self, path: &str) -> Option<EntryKind> {
            let files = self.files.borrow();
            if files.contains_key(path) {
                return Some(EntryKind::File);
            }
            let prefix = if path == "/" {
                "/".to_owned()
            } else {
                format!("{path}/")
            };
            files
                .keys()
                .any(|file| file.starts_with(&prefix))
                .then_some(EntryKind::Dir)
        }

        fn list(&self, dir: &str) -> Result<Vec<String>, String> {
            let prefix = if dir == "/" {
                "/".to_owned()
            } else {
                format!("{dir}/")
            };
            let mut names: Vec<String> = self
                .files
                .borrow()
                .keys()
                .filter_map(|file| file.strip_prefix(&prefix))
                .map(|rest| rest.split('/').next().unwrap_or(rest).to_owned())
                .collect();
            names.dedup();
            if names.is_empty() {
                return Err(format!("cannot list {dir}"));
            }
            Ok(names)
        }

        fn read(&self, path: &str) -> Result<String, String> {
            self.file(path).ok_or_else(|| format!("cannot read {path}"))
        }

        fn write(&self, path: &str, contents: &str) -> Result<(), String> {
            self.files
                .borrow_mut()
                .insert(path.to_owned(), contents.to_owned());
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, rstest::rstest};

    #[rstest]
    #[case::relative_input("/repo", "crates/log", "/repo/crates/log")]
    #[case::dot_and_trailing_slash("/repo", "./crates/log/", "/repo/crates/log")]
    #[case::parent_component("/repo/crates", "../docs", "/repo/docs")]
    #[case::absolute_input("/repo", "/elsewhere/./x", "/elsewhere/x")]
    #[case::cwd_itself("/repo", ".", "/repo")]
    #[case::above_the_root("/", "../..", "/")]
    fn absolute_resolves_against_the_cwd(
        #[case] cwd: &str,
        #[case] input: &str,
        #[case] expected: &str,
    ) {
        assert!(absolute(cwd, input) == expected);
    }

    #[rstest]
    #[case::sibling_file("/repo", "/repo/crates/a.feature", "crates/a.feature")]
    #[case::same_dir("/repo/crates", "/repo/crates", ".")]
    #[case::up_and_over("/repo/docs/bdd", "/repo/crates/a.feature", "../../crates/a.feature")]
    #[case::from_the_root("/", "/repo/a", "repo/a")]
    fn relative_walks_up_then_down(#[case] from: &str, #[case] to: &str, #[case] expected: &str) {
        assert!(relative(from, to) == expected);
    }

    #[rstest]
    #[case::nested("/repo/crates", Some("/repo"))]
    #[case::top_level("/repo", Some("/"))]
    #[case::root("/", None)]
    fn parent_of_a_path(#[case] path: &str, #[case] expected: Option<&str>) {
        assert!(parent(path) == expected);
    }

    #[test]
    fn join_and_file_name() {
        assert!(join("/", "repo") == "/repo");
        assert!(join("/repo", "Cargo.toml") == "/repo/Cargo.toml");
        assert!(file_name("/repo/a.feature") == "a.feature");
    }
}
