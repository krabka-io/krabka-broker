//! The look-behinds of a pattern: Java's study of them, and what krabka can
//! read as Java does.
//!
//! `Pattern` studies the nodes of a look-behind for the most text they can
//! match, and refuses the pattern where that is not valid: `Look-behind group
//! does not have an obvious maximum length`. The study follows the nodes Java
//! builds, and its `int` arithmetic:
//!
//! - A backreference, and a repeat of a group with an alternation or a range
//!   of counts in it (`Loop`), have no maximum.
//! - Any other counted repeat (`Curly`, or `GroupCurly` for a group) adds the
//!   maximum of its atom times its maximum count, which is `MAX_REPS` when it
//!   has none, and has no maximum where that overflows past the maximum
//!   before it. So `(?<=(?:ab)*)` has none, and `(?<=(?:a)*)` has one.
//! - A greedy `*`, `+` or `{n,}` of a single char or class
//!   (`CharPropertyGreedy`) adds `MAX_REPS` without that check, so
//!   `(?<=a*b*)` has a maximum.
//! - An alternation (`Branch`) takes the greatest maximum of its branches,
//!   and studies what follows it from nothing before it adds them.
//!
//! `fancy_regex` takes some of the look-behinds Java refuses, such as
//! `(?<=(a)\1)`, so the pattern is checked against these rules before it is
//! compiled.
//!
//! Java's `Behind` and `NotBehind` then try the starts from `i - rmin` down
//! to `max(i - rmax, 0)`, with `rmax` the maximum as the study left it. Where
//! two `CharPropertyGreedy` nodes wrap that maximum, the window is not every
//! start, and `(?<=a*b*)c` matches nowhere. [`check_look_behinds`] accepts
//! a wrapped maximum only where the window is every start the body can match
//! from, and refuses the rest.
//!
//! `fancy_regex` builds a look-behind that is not of one length as a reverse
//! DFA with no size limit, so `(?<=x{1,1000000000})` would take minutes to
//! compile. [`check_look_behinds`] refuses a look-behind the `regex` crate
//! finds too large at its default size limit.

use {
    super::backtrack::deterministic,
    fancy_regex::{Assertion, Expr, LookAround},
    regex::{Error as RegexError, RegexBuilder},
};

/// Java's `Pattern.MAX_REPS`, the count of an unbounded repeat.
const MAX_REPS: i32 = i32::MAX;

/// The least wrapped maximum, in UTF-16 units, that is accepted with an
/// input limit. No rule value krabka reads comes near 2^30 units.
const LEAST_INPUT_LIMIT: i32 = 1 << 30;

/// Why [`check_look_behinds`] refuses a pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Refusal {
    /// Java's own refusal: the study finds no maximum length.
    NoObviousMaximum,
    /// The maximum length wraps in Java's `int`, and Java's window then
    /// leaves out starts that krabka tries.
    MaximumOverflows,
    /// The body is too large for the `regex` crate to build at its default
    /// size limit.
    TooLarge,
}

impl Refusal {
    /// What the refused pattern has, after its quoted source.
    pub(super) const fn reason(self) -> &'static str {
        match self {
            Self::NoObviousMaximum => "has a look-behind group with no obvious maximum length",
            Self::MaximumOverflows => {
                "has a look-behind group whose maximum length overflows Java's int, which krabka \
                 cannot read as Java does"
            }
            Self::TooLarge => "has a look-behind group too large to compile",
        }
    }
}

/// The part of Java's `TreeInfo` the check reads.
#[derive(Debug, Clone, Copy)]
struct TreeInfo {
    /// `maxLength`, in Java's wrapping `int` arithmetic.
    max_length: i32,
    /// `maxLength` as the same steps give it without the wrap.
    exact_max_length: i128,
    max_valid: bool,
}

impl TreeInfo {
    const fn new() -> Self {
        Self {
            max_length: 0,
            exact_max_length: 0,
            max_valid: true,
        }
    }

    /// Adds `length` to the maximum, as Java's `+=` does.
    fn add(&mut self, length: i32, exact: i128) {
        self.max_length = self.max_length.wrapping_add(length);
        self.exact_max_length = self.exact_max_length.saturating_add(exact);
    }
}

/// Checks every look-behind in `expr`, the tree of `pattern` as rewritten.
///
/// # Errors
///
/// [`Refusal::NoObviousMaximum`] where Java refuses a look-behind, and
/// [`Refusal::MaximumOverflows`] where its maximum wraps and krabka would
/// match at a start Java does not try.
pub(super) fn check_look_behinds(pattern: &str, expr: &Expr) -> Result<LookBehinds, Refusal> {
    let mut bodies = Vec::new();
    look_behind_bodies(expr, &mut bodies);
    let studied: Vec<(&Expr, TreeInfo)> = bodies
        .into_iter()
        .map(|body| {
            let mut info = TreeInfo::new();
            study(&[body], &mut info);
            (body, info)
        })
        .collect();
    if studied.iter().any(|(_, info)| !info.max_valid) {
        return Err(Refusal::NoObviousMaximum);
    }
    let mut input_limit: Option<usize> = None;
    for (body, info) in &studied {
        if let Some(limit) = wrapped_window(pattern, body, info)? {
            input_limit = Some(input_limit.map_or(limit, |least| least.min(limit)));
        }
    }
    Ok(LookBehinds {
        input_limit,
        too_large: studied.iter().any(|(body, _)| too_large(body)),
    })
}

/// What [`check_look_behinds`] finds in a pattern Java reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct LookBehinds {
    /// The most UTF-16 units of input the pattern is read for as Java reads
    /// it, where a wrapped maximum bounds that, and `None` where nothing does.
    pub(super) input_limit: Option<usize>,
    /// Whether a look-behind is too large for `fancy_regex`, which would
    /// build it with no size limit: [`Refusal::TooLarge`].
    pub(super) too_large: bool,
}

/// Adds the body of each look-behind in `expr` to `bodies`, outer ones
/// first.
fn look_behind_bodies<'a>(expr: &'a Expr, bodies: &mut Vec<&'a Expr>) {
    match expr {
        Expr::LookAround(child, kind) => {
            if matches!(kind, LookAround::LookBehind | LookAround::LookBehindNeg) {
                bodies.push(child);
            }
            look_behind_bodies(child, bodies);
        }
        Expr::Repeat { child, .. } | Expr::AtomicGroup(child) => look_behind_bodies(child, bodies),
        Expr::Group(child) => look_behind_bodies(child, bodies),
        Expr::Concat(children) | Expr::Alt(children) => {
            for child in children {
                look_behind_bodies(child, bodies);
            }
        }
        _ => {}
    }
}

/// Java's window for the look-behind `body`, whose study is `info`, where
/// its maximum wraps. `Behind` tries the starts from `i - rmin` down to
/// `max(i - rmax, 0)`:
///
/// - `rmax` is not negative: where the body matches the empty string at any
///   position, both Java and krabka find that match at the start `i`.
///   Otherwise, where `rmax` is at least 2^30, the window is every start for
///   an input of at most `rmax` UTF-16 units, and `rmax` is the input limit.
///   A smaller `rmax` leaves out starts the body can match from.
/// - `rmax` is negative: `i - rmax` overflows to below 0 for `i` past
///   `K = i32::MAX + rmax`, and the window is every start there. At `i` up to
///   `K` it is empty, which leaves nothing out where the body is longer than
///   `K`.
///
/// Returns the input limit, or `None` for none.
///
/// # Errors
///
/// [`Refusal::MaximumOverflows`] where the window leaves out a start the
/// body can match from, or where the pattern has a char past U+FFFF: Java
/// then counts the window in code points with `BehindS`, which this does not
/// model.
fn wrapped_window(pattern: &str, body: &Expr, info: &TreeInfo) -> Result<Option<usize>, Refusal> {
    let rmax = info.max_length;
    if i128::from(rmax) == info.exact_max_length {
        return Ok(None);
    }
    if pattern
        .chars()
        .any(|character| u32::from(character) > 0xFFFF)
    {
        return Err(Refusal::MaximumOverflows);
    }
    if rmax >= 0 {
        if matches_empty_anywhere(body) {
            return Ok(None);
        }
        if rmax >= LEAST_INPUT_LIMIT {
            return Ok(usize::try_from(rmax).ok());
        }
        return Err(Refusal::MaximumOverflows);
    }
    // `K`, the last `i` at which the window is empty. `rmax` is negative, so
    // the sum does not overflow.
    let last_empty = i32::MAX + rmax;
    if i64::try_from(min_length(body)).unwrap_or(i64::MAX) > i64::from(last_empty) {
        Ok(None)
    } else {
        Err(Refusal::MaximumOverflows)
    }
}

/// A lower bound on the UTF-16 units `expr` matches: one for each char of a
/// literal, and for a char, a class or `\R`.
fn min_length(expr: &Expr) -> usize {
    match expr {
        Expr::Literal { val, .. } => val.chars().count(),
        Expr::Any { .. } | Expr::Delegate { .. } | Expr::GeneralNewline { .. } => 1,
        Expr::Concat(children) => children
            .iter()
            .fold(0, |sum, child| sum.saturating_add(min_length(child))),
        Expr::Alt(children) => children.iter().map(min_length).min().unwrap_or(0),
        Expr::Repeat { child, lo, .. } => lo.saturating_mul(min_length(child)),
        Expr::Group(child) => min_length(child),
        Expr::AtomicGroup(child) => min_length(child),
        _ => 0,
    }
}

/// Whether `expr` matches the empty string at every position: Java's
/// look-behind matcher, which backtracks until the match ends at `i`, then
/// finds it at the start `i`. A look-around, an assertion or an atomic group
/// may not, and is not taken.
fn matches_empty_anywhere(expr: &Expr) -> bool {
    match expr {
        Expr::Empty | Expr::Repeat { lo: 0, .. } => true,
        Expr::Literal { val, .. } => val.is_empty(),
        Expr::Repeat { child, .. } => matches_empty_anywhere(child),
        Expr::Concat(children) => children.iter().all(matches_empty_anywhere),
        Expr::Alt(children) => children.iter().any(matches_empty_anywhere),
        Expr::Group(child) => matches_empty_anywhere(child),
        _ => false,
    }
}

/// Whether a part of the look-behind `body` that `fancy_regex` hands to a
/// reverse DFA is too large for the `regex` crate at its default size limit.
/// `fancy_regex` sets no limit on that DFA, so it would build it however
/// large. It delegates each alternative of an alternation, else the body.
/// Of a sequence with a node it cannot delegate, it delegates each run of
/// nodes it can. Where the `regex` crate fails for another reason,
/// `fancy_regex` reports that.
fn too_large(body: &Expr) -> bool {
    let alternatives = match body {
        Expr::Alt(children) => children.iter().collect(),
        _ => vec![body],
    };
    let mut delegated: Vec<String> = Vec::new();
    for alternative in alternatives {
        if soft(alternative) {
            let mut source = String::new();
            alternative.to_str(&mut source, 1);
            delegated.push(source);
        } else if let Expr::Concat(children) = alternative {
            for run in children.split(|child| !soft(child)) {
                if !run.is_empty() {
                    let mut source = String::new();
                    for child in run {
                        child.to_str(&mut source, 1);
                    }
                    delegated.push(source);
                }
            }
        }
    }
    delegated.iter().any(|source| {
        matches!(
            RegexBuilder::new(source).build(),
            Err(RegexError::CompiledTooBig(_))
        )
    })
}

/// Whether `expr` is made only of nodes `Expr::to_str` writes, which
/// `fancy_regex` may hand to the `regex` crate. `to_str` panics on the rest.
fn soft(expr: &Expr) -> bool {
    match expr {
        Expr::Empty
        | Expr::Any { .. }
        | Expr::Literal { .. }
        | Expr::Delegate { .. }
        | Expr::Assertion(
            Assertion::StartText
            | Assertion::EndText
            | Assertion::StartLine { .. }
            | Assertion::EndLine { .. },
        ) => true,
        Expr::Concat(children) | Expr::Alt(children) => children.iter().all(soft),
        Expr::Group(child) => soft(child),
        Expr::Repeat { child, .. } => soft(child),
        _ => false,
    }
}

/// Java's `Node.study` of `chain`, the nodes from here to the end of the
/// look-behind, into `info`.
fn study(chain: &[&Expr], info: &mut TreeInfo) {
    let Some((&first, rest)) = chain.split_first() else {
        return;
    };
    match first {
        Expr::Concat(children) => return inline(children.iter(), rest, info),
        Expr::Group(child) => return inline(std::iter::once(&**child), rest, info),
        Expr::AtomicGroup(child) => return inline(std::iter::once(&**child), rest, info),
        Expr::Alt(children) => return branch(children.iter(), rest, info),
        Expr::Repeat {
            child,
            lo,
            hi,
            greedy,
        } => {
            if (*lo, *hi) == (0, 1) && is_group(child) {
                return branch([&**child, &Expr::Empty].into_iter(), rest, info);
            }
            if !repeat(child, *lo, *hi, *greedy, info) {
                return;
            }
        }
        Expr::Literal { val, .. } => add(info, val.chars().count()),
        Expr::Any { .. } | Expr::Delegate { .. } => add(info, 1),
        Expr::GeneralNewline { .. } => add(info, 2),
        Expr::Backref { .. } => info.max_valid = false,
        _ => {}
    }
    study(rest, info);
}

/// Studies `children` in place of the node that holds them, a sequence or a
/// group, and then `rest`.
fn inline<'a>(children: impl Iterator<Item = &'a Expr>, rest: &[&'a Expr], info: &mut TreeInfo) {
    let joined: Vec<&Expr> = children.chain(rest.iter().copied()).collect();
    study(&joined, info);
}

/// The study of a node of `length` chars, such as `Slice` or `CharProperty`.
fn add(info: &mut TreeInfo, length: usize) {
    info.add(
        i32::try_from(length).unwrap_or(MAX_REPS),
        i128::try_from(length).unwrap_or(i128::MAX),
    );
}

/// Java's `Branch.study`, for the alternatives `children` and the nodes
/// `rest` after them.
fn branch<'a>(children: impl Iterator<Item = &'a Expr>, rest: &[&'a Expr], info: &mut TreeInfo) {
    let (mut max_length, mut exact_max_length, mut max_valid) = (-1_i32, -1_i128, info.max_valid);
    for child in children {
        let mut alternative = TreeInfo::new();
        study(&[child], &mut alternative);
        max_length = max_length.max(alternative.max_length);
        exact_max_length = exact_max_length.max(alternative.exact_max_length);
        max_valid &= alternative.max_valid;
    }
    let mut before = *info;
    before.add(max_length, exact_max_length);
    *info = TreeInfo::new();
    study(rest, info);
    info.add(before.max_length, before.exact_max_length);
    info.max_valid &= max_valid;
}

/// Java's study of a repeat of `child`, other than a `?` of a group. Whether
/// the study goes on to the nodes after it, which a `Loop` stops.
fn repeat(child: &Expr, lo: usize, hi: usize, greedy: bool, info: &mut TreeInfo) -> bool {
    let unbounded = hi == usize::MAX;
    if (lo, hi) == (0, 1) {
        // `Ques`: the atom's maximum, once.
        study(&[child], info);
    } else if greedy && unbounded && is_char_property(child) {
        // `CharPropertyGreedy`.
        if info.max_valid {
            info.add(MAX_REPS, i128::from(MAX_REPS));
        }
    } else if is_group(child) && !deterministic(child) && !zero_width(child) {
        // `Loop`.
        info.max_valid = false;
        return false;
    } else {
        // `Curly` and `GroupCurly`.
        let count = i32::try_from(hi).unwrap_or(MAX_REPS);
        let mut atom = TreeInfo::new();
        study(&[child], &mut atom);
        if info.max_valid && atom.max_valid {
            let total = atom
                .max_length
                .wrapping_mul(count)
                .wrapping_add(info.max_length);
            if total < info.max_length {
                info.max_valid = false;
            }
            info.max_length = total;
            info.exact_max_length = atom
                .exact_max_length
                .saturating_mul(i128::from(count))
                .saturating_add(info.exact_max_length);
        } else {
            info.max_valid = false;
        }
    }
    true
}

/// Whether Java reads `child` as one `CharProperty`: a single char, a class
/// or `.`.
fn is_char_property(child: &Expr) -> bool {
    match child {
        Expr::Literal { val, .. } => val.chars().count() == 1,
        Expr::Any { .. } | Expr::Delegate { .. } => true,
        _ => false,
    }
}

/// Whether `expr` only ever matches the empty string. The rewrite writes some
/// single Java nodes, such as `$` or `\b`, as an alternation of look-arounds,
/// which Java repeats as the one node it is, of length 0.
fn zero_width(expr: &Expr) -> bool {
    match expr {
        Expr::Empty
        | Expr::Assertion(_)
        | Expr::ContinueFromPreviousMatchEnd
        | Expr::LookAround(..) => true,
        Expr::Concat(children) | Expr::Alt(children) => children.iter().all(zero_width),
        Expr::Group(child) => zero_width(child),
        _ => false,
    }
}

/// Whether Java reads `child` as a group, which a quantifier repeats with a
/// `Branch`, `GroupCurly` or `Loop`.
fn is_group(child: &Expr) -> bool {
    match child {
        Expr::Group(_) | Expr::Concat(_) | Expr::Alt(_) => true,
        Expr::Literal { val, .. } => val.chars().count() > 1,
        _ => false,
    }
}
