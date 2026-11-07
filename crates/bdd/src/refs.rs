//! Introduce and Refer: a registry of the entities a scenario has introduced.
//!
//! A step that says "a topic" introduces an entity, and a later step that says
//! "the topic" refers back to it. [`Ref`] keeps that bookkeeping out of the
//! step definitions: [`Ref::introduce`] records an entity under its noun and
//! makes it the current one, and [`Ref::the`] returns the current one. A step
//! that names its entity, as in "the topic orders", uses
//! [`Ref::introduce_named`] and [`Ref::named`]. A step that picks one entity
//! out of several, as in "the oldest segment" or "the latest leader epoch",
//! uses an Entity Selector: [`Ref::register_selector`] registers a function
//! per noun, and [`Ref::select`] applies it.
//!
//! Each lookup panics with a message a scenario author can act on: which step
//! is missing, which names were introduced, or which selectors exist. The
//! `try_` variants return the same message as a [`RefError`].

use std::{collections::BTreeMap, error::Error, fmt};

/// A function that picks one entity out of every entity introduced under a
/// noun, oldest first, and returns its index.
pub type Selector<T> = fn(&[T]) -> Option<usize>;

/// The entities a scenario has introduced, by noun.
///
/// Hold one in the scenario context, so each scenario starts with none.
///
/// # Examples
///
/// ```
/// use {assert2::assert, krabka_bdd::Ref};
///
/// let mut segments: Ref<u64> = Ref::default();
/// segments.register_selector("segment", "oldest", |all| (!all.is_empty()).then_some(0));
/// segments.introduce("segment", 0);
/// segments.introduce_named("segment", "active", 4096);
///
/// assert!(*segments.the("segment") == 4096);
/// assert!(*segments.named("segment", "active") == 4096);
/// assert!(*segments.select("segment", "oldest") == 0);
/// ```
#[derive(Debug)]
pub struct Ref<T> {
    kinds: BTreeMap<String, Kind<T>>,
}

/// The entities and selectors of one noun.
#[derive(Debug)]
struct Kind<T> {
    values: Vec<T>,
    names: BTreeMap<String, usize>,
    current: Option<usize>,
    selectors: BTreeMap<String, Selector<T>>,
}

impl<T> Default for Kind<T> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            names: BTreeMap::new(),
            current: None,
            selectors: BTreeMap::new(),
        }
    }
}

impl<T> Kind<T> {
    /// Appends `value` and makes it the current entity.
    fn push(&mut self, value: T) -> &T {
        self.current = Some(self.values.len());
        self.values.push_mut(value)
    }
}

impl<T> Default for Ref<T> {
    fn default() -> Self {
        Self {
            kinds: BTreeMap::new(),
        }
    }
}

impl<T> Ref<T> {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Records `value` under `noun`, as a step that says "a topic" does, and
    /// makes it the current `noun`.
    pub fn introduce(&mut self, noun: &str, value: T) -> &T {
        self.kinds.entry(noun.to_owned()).or_default().push(value)
    }

    /// Records `value` under `noun` and `name`, as a step that says "a topic
    /// named orders" does, and makes it the current `noun`.
    ///
    /// # Panics
    ///
    /// Panics when a `noun` named `name` has already been introduced.
    #[track_caller]
    pub fn introduce_named(&mut self, noun: &str, name: &str, value: T) -> &T {
        unwrap(self.try_introduce_named(noun, name, value))
    }

    /// Records `value` under `noun` and `name`, like
    /// [`introduce_named`](Self::introduce_named).
    ///
    /// # Errors
    ///
    /// Returns [`RefError::DuplicateName`] when a `noun` named `name` has
    /// already been introduced.
    pub fn try_introduce_named(
        &mut self,
        noun: &str,
        name: &str,
        value: T,
    ) -> Result<&T, RefError> {
        let kind = self.kinds.entry(noun.to_owned()).or_default();
        if kind.names.contains_key(name) {
            return Err(RefError::DuplicateName {
                noun: noun.to_owned(),
                name: name.to_owned(),
            });
        }
        kind.names.insert(name.to_owned(), kind.values.len());
        Ok(kind.push(value))
    }

    /// Returns the current `noun`, as a step that says "the topic" does: the
    /// one introduced last.
    ///
    /// # Panics
    ///
    /// Panics, naming the step to add, when no `noun` has been introduced.
    #[must_use]
    #[track_caller]
    pub fn the(&self, noun: &str) -> &T { unwrap(self.try_the(noun)) }

    /// Returns the current `noun` mutably.
    ///
    /// # Panics
    ///
    /// Panics, naming the step to add, when no `noun` has been introduced.
    #[must_use]
    #[track_caller]
    pub fn the_mut(&mut self, noun: &str) -> &mut T {
        let index = unwrap(self.current_index(noun));
        unwrap(self.value_mut(noun, index))
    }

    /// Returns the current `noun`, like [`the`](Self::the).
    ///
    /// # Errors
    ///
    /// Returns [`RefError::NotIntroduced`] when no `noun` has been introduced.
    pub fn try_the(&self, noun: &str) -> Result<&T, RefError> {
        let index = self.current_index(noun)?;
        self.value(noun, index)
    }

    /// Returns the `noun` named `name`, as a step that says "the topic orders"
    /// does.
    ///
    /// # Panics
    ///
    /// Panics, listing the introduced names, when no `noun` named `name` has
    /// been introduced.
    #[must_use]
    #[track_caller]
    pub fn named(&self, noun: &str, name: &str) -> &T { unwrap(self.try_named(noun, name)) }

    /// Returns the `noun` named `name` mutably.
    ///
    /// # Panics
    ///
    /// Panics, listing the introduced names, when no `noun` named `name` has
    /// been introduced.
    #[must_use]
    #[track_caller]
    pub fn named_mut(&mut self, noun: &str, name: &str) -> &mut T {
        let index = unwrap(self.named_index(noun, name));
        unwrap(self.value_mut(noun, index))
    }

    /// Returns the `noun` named `name`, like [`named`](Self::named).
    ///
    /// # Errors
    ///
    /// Returns [`RefError::NameNotIntroduced`], with the introduced names,
    /// when no `noun` named `name` has been introduced.
    pub fn try_named(&self, noun: &str, name: &str) -> Result<&T, RefError> {
        let index = self.named_index(noun, name)?;
        self.value(noun, index)
    }

    /// Registers `select` as the `selector` for `noun`, such as `oldest` for
    /// `segment`, replacing an earlier one of that name.
    pub fn register_selector(&mut self, noun: &str, selector: &str, select: Selector<T>) {
        self.kinds
            .entry(noun.to_owned())
            .or_default()
            .selectors
            .insert(selector.to_owned(), select);
    }

    /// Returns the `noun` that `selector` picks, as a step that says "the
    /// oldest segment" does.
    ///
    /// # Panics
    ///
    /// Panics when `selector` is not registered for `noun` (listing the ones
    /// that are), when no `noun` has been introduced, or when the selector
    /// picks none of them.
    #[must_use]
    #[track_caller]
    pub fn select(&self, noun: &str, selector: &str) -> &T {
        unwrap(self.try_select(noun, selector))
    }

    /// Returns the `noun` that `selector` picks, like [`select`](Self::select).
    ///
    /// # Errors
    ///
    /// Returns [`RefError::UnknownSelector`] when `selector` is not registered
    /// for `noun`, [`RefError::NotIntroduced`] when no `noun` has been
    /// introduced, and [`RefError::SelectedNothing`] when the selector picks
    /// none of them.
    pub fn try_select(&self, noun: &str, selector: &str) -> Result<&T, RefError> {
        let kind = self.kinds.get(noun);
        let Some(select) = kind.and_then(|kind| kind.selectors.get(selector)) else {
            return Err(RefError::UnknownSelector {
                noun: noun.to_owned(),
                selector: selector.to_owned(),
                selectors: kind
                    .map(|kind| kind.selectors.keys().cloned().collect())
                    .unwrap_or_default(),
            });
        };
        let values = self.all(noun);
        if values.is_empty() {
            return Err(not_introduced(noun));
        }
        select(values)
            .and_then(|index| values.get(index))
            .ok_or_else(|| RefError::SelectedNothing {
                noun: noun.to_owned(),
                selector: selector.to_owned(),
                introduced: values.len(),
            })
    }

    /// Returns every `noun` introduced so far, oldest first.
    #[must_use]
    pub fn all(&self, noun: &str) -> &[T] {
        self.kinds
            .get(noun)
            .map_or(&[], |kind| kind.values.as_slice())
    }

    fn current_index(&self, noun: &str) -> Result<usize, RefError> {
        self.kinds
            .get(noun)
            .and_then(|kind| kind.current)
            .ok_or_else(|| not_introduced(noun))
    }

    fn named_index(&self, noun: &str, name: &str) -> Result<usize, RefError> {
        let kind = self.kinds.get(noun);
        kind.and_then(|kind| kind.names.get(name).copied())
            .ok_or_else(|| RefError::NameNotIntroduced {
                noun: noun.to_owned(),
                name: name.to_owned(),
                names: kind
                    .map(|kind| kind.names.keys().cloned().collect())
                    .unwrap_or_default(),
            })
    }

    fn value(&self, noun: &str, index: usize) -> Result<&T, RefError> {
        self.all(noun)
            .get(index)
            .ok_or_else(|| not_introduced(noun))
    }

    fn value_mut(&mut self, noun: &str, index: usize) -> Result<&mut T, RefError> {
        self.kinds
            .get_mut(noun)
            .and_then(|kind| kind.values.get_mut(index))
            .ok_or_else(|| not_introduced(noun))
    }
}

fn not_introduced(noun: &str) -> RefError {
    RefError::NotIntroduced {
        noun: noun.to_owned(),
    }
}

#[track_caller]
fn unwrap<V>(result: Result<V, RefError>) -> V {
    match result {
        Ok(value) => value,
        Err(error) => panic!("{error}"),
    }
}

/// Why a [`Ref`] lookup found no entity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefError {
    /// No entity of the noun has been introduced.
    NotIntroduced {
        /// The noun, such as `topic`.
        noun: String,
    },
    /// No entity of the noun has been introduced under the name.
    NameNotIntroduced {
        /// The noun, such as `topic`.
        noun: String,
        /// The name looked up, such as `orders`.
        name: String,
        /// The names that have been introduced, sorted.
        names: Vec<String>,
    },
    /// An entity of the noun has already been introduced under the name.
    DuplicateName {
        /// The noun, such as `topic`.
        noun: String,
        /// The name introduced twice.
        name: String,
    },
    /// The selector is not registered for the noun.
    UnknownSelector {
        /// The noun, such as `segment`.
        noun: String,
        /// The selector looked up, such as `oldest`.
        selector: String,
        /// The selectors registered for the noun, sorted.
        selectors: Vec<String>,
    },
    /// The selector picked none of the introduced entities.
    SelectedNothing {
        /// The noun, such as `segment`.
        noun: String,
        /// The selector that picked nothing.
        selector: String,
        /// How many entities of the noun have been introduced.
        introduced: usize,
    },
}

/// `a` or `an`, by the first letter of `noun`.
fn article(noun: &str) -> &'static str {
    if noun.starts_with(['a', 'e', 'i', 'o', 'u']) {
        "an"
    } else {
        "a"
    }
}

/// `names` as `` `a`, `b` ``.
fn listed(names: &[String]) -> String {
    names
        .iter()
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ")
}

impl fmt::Display for RefError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotIntroduced { noun } => write!(
                formatter,
                "no {noun} has been introduced: add a step like `Given {} {noun} ...` before this \
                 step",
                article(noun)
            ),
            Self::NameNotIntroduced { noun, name, names } if names.is_empty() => write!(
                formatter,
                "no {noun} named {name} has been introduced: add a step like `Given {} {noun} \
                 named {name} ...` before this step",
                article(noun)
            ),
            Self::NameNotIntroduced { noun, name, names } => write!(
                formatter,
                "no {noun} named {name} has been introduced: the named {noun} entities are {}",
                listed(names)
            ),
            Self::DuplicateName { noun, name } => write!(
                formatter,
                "{} {noun} named {name} has already been introduced: give each {noun} its own \
                 name",
                article(noun)
            ),
            Self::UnknownSelector {
                noun,
                selector,
                selectors,
            } if selectors.is_empty() => write!(
                formatter,
                "no {noun} selector `{selector}`: no selector is registered for {noun}; register \
                 one with `Ref::register_selector`"
            ),
            Self::UnknownSelector {
                noun,
                selector,
                selectors,
            } => write!(
                formatter,
                "no {noun} selector `{selector}`: the {noun} selectors are {}",
                listed(selectors)
            ),
            Self::SelectedNothing {
                noun,
                selector,
                introduced,
            } => write!(
                formatter,
                "the {noun} selector `{selector}` picked none of the {introduced} introduced \
                 {noun} entities"
            ),
        }
    }
}

impl Error for RefError {}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        rstest::rstest,
        rstest_bdd::panic_message,
        std::panic::{AssertUnwindSafe, catch_unwind},
    };

    /// A segment: its base offset and its size in bytes.
    type Segment = (u64, u64);

    fn largest(all: &[Segment]) -> Option<usize> {
        all.iter()
            .enumerate()
            .max_by_key(|(_, (_, size))| *size)
            .map(|(index, _)| index)
    }

    fn oldest(all: &[Segment]) -> Option<usize> { (!all.is_empty()).then_some(0) }

    fn segments() -> Ref<Segment> {
        let mut segments = Ref::new();
        segments.register_selector("segment", "oldest", oldest);
        segments.register_selector("segment", "largest", largest);
        segments.register_selector("segment", "missing", |_| None);
        segments.register_selector("segment", "past the end", |all| Some(all.len()));
        segments.introduce("segment", (0, 100));
        segments.introduce_named("segment", "active", (100, 50));
        segments.introduce("segment", (150, 300));
        segments
    }

    #[test]
    fn the_returns_the_entity_introduced_last() {
        let mut topics = Ref::new();
        assert!(*topics.introduce("topic", "orders") == "orders");
        assert!(*topics.introduce_named("topic", "payments", "payments") == "payments");
        assert!(*topics.the("topic") == "payments");
        assert!(topics.all("topic") == ["orders", "payments"]);
    }

    #[test]
    fn nouns_are_kept_apart() {
        let mut entities = Ref::new();
        entities.introduce("topic", 1);
        entities.introduce("consumer group", 2);
        assert!((*entities.the("topic"), *entities.the("consumer group")) == (1, 2));
        assert!(entities.all("partition").is_empty());
    }

    #[test]
    fn mutable_lookups_change_the_entity() {
        let mut segments = segments();
        segments.the_mut("segment").1 += 1;
        segments.named_mut("segment", "active").1 += 1;
        assert!(segments.all("segment") == [(0, 100), (100, 51), (150, 301)]);
    }

    #[rstest]
    #[case::oldest("oldest", (0, 100))]
    #[case::largest("largest", (150, 300))]
    fn select_applies_the_registered_selector(#[case] selector: &str, #[case] expected: Segment) {
        assert!(*segments().select("segment", selector) == expected);
    }

    fn selector_without_entities() -> Result<(), RefError> {
        let mut segments = Ref::<Segment>::new();
        segments.register_selector("segment", "oldest", oldest);
        segments.try_select("segment", "oldest").map(|_| ())
    }

    #[rstest]
    #[case::not_introduced(
        Ref::<Segment>::new().try_the("topic").map(|_| ()),
        RefError::NotIntroduced { noun: "topic".to_owned() },
    )]
    #[case::name_never_introduced(
        Ref::<Segment>::new().try_named("topic", "orders").map(|_| ()),
        RefError::NameNotIntroduced {
            noun: "topic".to_owned(),
            name: "orders".to_owned(),
            names: Vec::new(),
        },
    )]
    #[case::name_not_introduced(
        segments().try_named("segment", "closed").map(|_| ()),
        RefError::NameNotIntroduced {
            noun: "segment".to_owned(),
            name: "closed".to_owned(),
            names: vec!["active".to_owned()],
        },
    )]
    #[case::duplicate_name(
        segments().try_introduce_named("segment", "active", (9, 9)).map(|_| ()),
        RefError::DuplicateName { noun: "segment".to_owned(), name: "active".to_owned() },
    )]
    #[case::unknown_selector(
        segments().try_select("segment", "newest").map(|_| ()),
        RefError::UnknownSelector {
            noun: "segment".to_owned(),
            selector: "newest".to_owned(),
            selectors: vec![
                "largest".to_owned(),
                "missing".to_owned(),
                "oldest".to_owned(),
                "past the end".to_owned(),
            ],
        },
    )]
    #[case::no_selectors(
        segments().try_select("topic", "oldest").map(|_| ()),
        RefError::UnknownSelector {
            noun: "topic".to_owned(),
            selector: "oldest".to_owned(),
            selectors: Vec::new(),
        },
    )]
    #[case::selector_without_entities(
        selector_without_entities(),
        RefError::NotIntroduced { noun: "segment".to_owned() },
    )]
    #[case::selected_nothing(
        segments().try_select("segment", "missing").map(|_| ()),
        RefError::SelectedNothing {
            noun: "segment".to_owned(),
            selector: "missing".to_owned(),
            introduced: 3,
        },
    )]
    #[case::selected_past_the_end(
        segments().try_select("segment", "past the end").map(|_| ()),
        RefError::SelectedNothing {
            noun: "segment".to_owned(),
            selector: "past the end".to_owned(),
            introduced: 3,
        },
    )]
    fn a_failed_lookup_says_why(#[case] result: Result<(), RefError>, #[case] expected: RefError) {
        assert!(result == Err(expected));
    }

    #[rstest]
    #[case::not_introduced(
        RefError::NotIntroduced { noun: "topic".to_owned() },
        "no topic has been introduced: add a step like `Given a topic ...` before this step",
    )]
    #[case::not_introduced_vowel(
        RefError::NotIntroduced { noun: "offset".to_owned() },
        "no offset has been introduced: add a step like `Given an offset ...` before this step",
    )]
    #[case::name_never_introduced(
        RefError::NameNotIntroduced {
            noun: "topic".to_owned(),
            name: "orders".to_owned(),
            names: Vec::new(),
        },
        "no topic named orders has been introduced: add a step like `Given a topic named orders \
         ...` before this step",
    )]
    #[case::name_not_introduced(
        RefError::NameNotIntroduced {
            noun: "topic".to_owned(),
            name: "orders".to_owned(),
            names: vec!["payments".to_owned(), "refunds".to_owned()],
        },
        "no topic named orders has been introduced: the named topic entities are `payments`, \
         `refunds`",
    )]
    #[case::duplicate_name(
        RefError::DuplicateName { noun: "topic".to_owned(), name: "orders".to_owned() },
        "a topic named orders has already been introduced: give each topic its own name",
    )]
    #[case::no_selectors(
        RefError::UnknownSelector {
            noun: "segment".to_owned(),
            selector: "oldest".to_owned(),
            selectors: Vec::new(),
        },
        "no segment selector `oldest`: no selector is registered for segment; register one \
         with `Ref::register_selector`",
    )]
    #[case::unknown_selector(
        RefError::UnknownSelector {
            noun: "segment".to_owned(),
            selector: "newest".to_owned(),
            selectors: vec!["largest".to_owned(), "oldest".to_owned()],
        },
        "no segment selector `newest`: the segment selectors are `largest`, `oldest`",
    )]
    #[case::selected_nothing(
        RefError::SelectedNothing {
            noun: "segment".to_owned(),
            selector: "oldest".to_owned(),
            introduced: 2,
        },
        "the segment selector `oldest` picked none of the 2 introduced segment entities",
    )]
    fn the_message_reads_as_a_fix(#[case] error: RefError, #[case] expected: &str) {
        assert!(error.to_string() == expected);
    }

    fn panic_of(f: impl FnOnce()) -> String {
        let payload = catch_unwind(AssertUnwindSafe(f)).expect_err("the lookup panics");
        panic_message(payload.as_ref())
    }

    #[rstest]
    #[case::the(|| { let _ = Ref::<Segment>::new().the("topic"); })]
    #[case::the_mut(|| { let _ = Ref::<Segment>::new().the_mut("topic"); })]
    #[case::select(|| {
        let mut topics = Ref::<Segment>::new();
        topics.register_selector("topic", "oldest", oldest);
        let _ = topics.select("topic", "oldest");
    })]
    fn a_panicking_lookup_panics_with_the_message(#[case] lookup: fn()) {
        assert!(
            panic_of(lookup)
                == "no topic has been introduced: add a step like `Given a topic ...` before \
                    this step"
        );
    }

    #[rstest]
    #[case::named(|| { let _ = segments().named("segment", "closed"); })]
    #[case::named_mut(|| { let _ = segments().named_mut("segment", "closed"); })]
    fn a_panicking_name_lookup_lists_the_names(#[case] lookup: fn()) {
        assert!(
            panic_of(lookup)
                == "no segment named closed has been introduced: the named segment entities are \
                    `active`"
        );
    }

    #[test]
    fn introducing_a_name_twice_panics() {
        assert!(
            panic_of(|| {
                let _ = segments().introduce_named("segment", "active", (0, 0));
            }) == "a segment named active has already been introduced: give each segment its \
                   own name"
        );
    }
}
