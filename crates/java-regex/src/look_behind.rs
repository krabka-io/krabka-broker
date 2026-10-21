//! The look-behinds of a pattern: Java's study of them, and what krabka can
//! read as Java does.
//!
//! `Pattern` studies the nodes of a look-behind for the least and the most
//! text they can match, and refuses the pattern where the most is not valid:
//! `Look-behind group does not have an obvious maximum length`. The study
//! follows the nodes Java builds, and its `int` arithmetic:
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
//! - An alternation (`Branch`) takes the least minimum and the greatest
//!   maximum of its branches, and studies what follows it from nothing before
//!   it adds them.
//! - The minimum adds the minimum count of a repeat times the minimum of its
//!   atom, and nothing for a `?` (`Ques`). Where a counted repeat takes it
//!   past Java's `int`, it is `0xFFFFFFF`, and a greedy repeat of a char adds
//!   its count with no check. A char, a class, a literal and `\R` add the
//!   chars they match at least, and a look-around adds nothing.
//!
//! `fancy_regex` takes some of the look-behinds Java refuses, such as
//! `(?<=(a)\1)`, so the pattern is checked against these rules before it is
//! compiled.
//!
//! Java's `Behind` and `NotBehind` then try the starts from `i - rmin` down
//! to `max(i - rmax, 0)`, with `rmin` and `rmax` the minimum and the maximum
//! as the study left them: a [`JavaWindow`]. Where the Java pattern has a
//! supplementary char from the look-behind's `(` to its end, `BehindS` and
//! `NotBehindS` count that window in code points. Where two
//! `CharPropertyGreedy` nodes wrap the maximum, the window is not every
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
    /// `minLength`, in Java's wrapping `int` arithmetic.
    min_length: i32,
    /// `maxLength`, in Java's wrapping `int` arithmetic.
    max_length: i32,
    /// `minLength` as the same steps give it without the wrap: a lower bound
    /// on the chars the body matches.
    exact_min_length: i128,
    /// `maxLength` as the same steps give it without the wrap.
    exact_max_length: i128,
    max_valid: bool,
}

impl TreeInfo {
    const fn new() -> Self {
        Self {
            min_length: 0,
            max_length: 0,
            exact_min_length: 0,
            exact_max_length: 0,
            max_valid: true,
        }
    }

    /// Adds `length` to the minimum, as Java's `+=` does.
    fn add_min(&mut self, length: i32, exact: i128) {
        self.min_length = self.min_length.wrapping_add(length);
        self.exact_min_length = self.exact_min_length.saturating_add(exact);
    }

    /// Adds `length` to the maximum, as Java's `+=` does.
    fn add_max(&mut self, length: i32, exact: i128) {
        self.max_length = self.max_length.wrapping_add(length);
        self.exact_max_length = self.exact_max_length.saturating_add(exact);
    }
}

/// Checks every look-behind in `expr`, the tree of a pattern as rewritten.
/// `code_points` holds, for each look-behind in the order
/// [`LookBehinds::windows`] lists them, whether Java counts its window in
/// code points. A look-behind past the end of it counts in UTF-16 units.
///
/// # Errors
///
/// [`Refusal::NoObviousMaximum`] where Java refuses a look-behind, and
/// [`Refusal::MaximumOverflows`] where its maximum wraps and krabka would
/// match at a start Java does not try.
pub(super) fn check_look_behinds(
    expr: &Expr,
    code_points: &[bool],
) -> Result<LookBehinds, Refusal> {
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
    let windows: Vec<JavaWindow> = studied
        .iter()
        .enumerate()
        .map(|(index, (_, info))| JavaWindow {
            min: info.min_length,
            max: info.max_length,
            code_points: code_points.get(index).copied().unwrap_or(false),
        })
        .collect();
    let mut input_limit: Option<usize> = None;
    for ((body, info), window) in studied.iter().zip(&windows) {
        if let Some(limit) = wrapped_window(body, info, window.code_points)? {
            input_limit = Some(input_limit.map_or(limit, |least| least.min(limit)));
        }
    }
    Ok(LookBehinds {
        input_limit,
        too_large: studied.iter().any(|(body, _)| too_large(body)),
        windows,
    })
}

/// The starts Java's `Behind` (or `BehindS`) tries for one look-behind: from
/// `i - min` down to `max(i - max, 0)`, in Java's `int` arithmetic, where `min`
/// and `max` are its `TreeInfo` lengths. In UTF-16 units, or with
/// `code_points` in code points.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JavaWindow {
    pub(crate) min: i32,
    pub(crate) max: i32,
    pub(crate) code_points: bool,
}

/// What [`check_look_behinds`] finds in a pattern Java reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct LookBehinds {
    /// Java's window of each look-behind, in the order `fancy_regex` parses
    /// them, outer ones first.
    pub(super) windows: Vec<JavaWindow>,
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
/// body can match from, or where Java counts the window in code points
/// (`code_points`) with `BehindS`, which this does not model.
fn wrapped_window(
    body: &Expr,
    info: &TreeInfo,
    code_points: bool,
) -> Result<Option<usize>, Refusal> {
    let rmax = info.max_length;
    if i128::from(rmax) == info.exact_max_length {
        return Ok(None);
    }
    if code_points {
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
    if info.exact_min_length > i128::from(last_empty) {
        Ok(None)
    } else {
        Err(Refusal::MaximumOverflows)
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
        Expr::GeneralNewline { .. } => {
            // `LineEnding`, of one char or of `\r\n`.
            info.add_min(1, 1);
            info.add_max(2, 2);
        }
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
    let exact = i128::try_from(length).unwrap_or(i128::MAX);
    info.add_min(java_int(length), exact);
    info.add_max(java_int(length), exact);
}

/// A length or a count as Java's `int` holds it. Java refuses a count past
/// `i32::MAX`.
fn java_int(value: usize) -> i32 { i32::try_from(value).unwrap_or(MAX_REPS) }

/// Java's `Branch.study`, for the alternatives `children` and the nodes
/// `rest` after them.
fn branch<'a>(children: impl Iterator<Item = &'a Expr>, rest: &[&'a Expr], info: &mut TreeInfo) {
    // Java starts the minimum at `Integer.MAX_VALUE` and the maximum at -1.
    let mut branches = TreeInfo {
        min_length: i32::MAX,
        max_length: -1,
        exact_min_length: i128::MAX,
        exact_max_length: -1,
        max_valid: info.max_valid,
    };
    for child in children {
        let mut alternative = TreeInfo::new();
        study(&[child], &mut alternative);
        branches.min_length = branches.min_length.min(alternative.min_length);
        branches.max_length = branches.max_length.max(alternative.max_length);
        branches.exact_min_length = branches.exact_min_length.min(alternative.exact_min_length);
        branches.exact_max_length = branches.exact_max_length.max(alternative.exact_max_length);
        branches.max_valid &= alternative.max_valid;
    }
    let mut before = *info;
    before.add_min(branches.min_length, branches.exact_min_length);
    before.add_max(branches.max_length, branches.exact_max_length);
    *info = TreeInfo::new();
    study(rest, info);
    info.add_min(before.min_length, before.exact_min_length);
    info.add_max(before.max_length, before.exact_max_length);
    info.max_valid &= branches.max_valid;
}

/// Java's study of a repeat of `child`, other than a `?` of a group. Whether
/// the study goes on to the nodes after it, which a `Loop` stops.
fn repeat(child: &Expr, lo: usize, hi: usize, greedy: bool, info: &mut TreeInfo) -> bool {
    let unbounded = hi == usize::MAX;
    if (lo, hi) == (0, 1) {
        // `Ques`: the atom's maximum, once, and none of its minimum.
        let (min_length, exact_min_length) = (info.min_length, info.exact_min_length);
        study(&[child], info);
        (info.min_length, info.exact_min_length) = (min_length, exact_min_length);
    } else if greedy && unbounded && is_char_property(child) {
        // `CharPropertyGreedy`, which adds `cmin` with no check.
        info.add_min(java_int(lo), i128::from(java_int(lo)));
        if info.max_valid {
            info.add_max(MAX_REPS, i128::from(MAX_REPS));
        }
    } else if is_group(child) && !deterministic(child) && !zero_width(child) {
        // `Loop`.
        info.max_valid = false;
        return false;
    } else {
        // `Curly` and `GroupCurly`.
        let (cmin, cmax) = (java_int(lo), java_int(hi));
        let mut atom = TreeInfo::new();
        study(&[child], &mut atom);
        let least = atom
            .min_length
            .wrapping_mul(cmin)
            .wrapping_add(info.min_length);
        // Java's "arbitrary large number" where the minimum wraps below the
        // one before.
        info.min_length = if least < info.min_length {
            0xFFF_FFFF
        } else {
            least
        };
        info.exact_min_length = atom
            .exact_min_length
            .saturating_mul(i128::from(cmin))
            .saturating_add(info.exact_min_length);
        if info.max_valid && atom.max_valid {
            let total = atom
                .max_length
                .wrapping_mul(cmax)
                .wrapping_add(info.max_length);
            if total < info.max_length {
                info.max_valid = false;
            }
            info.max_length = total;
            info.exact_max_length = atom
                .exact_max_length
                .saturating_mul(i128::from(cmax))
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

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// What [`check_look_behinds`] gives for `pattern`, in `fancy_regex`
    /// syntax, with `code_points` for its look-behinds.
    fn look_behinds(pattern: &str, code_points: &[bool]) -> Result<LookBehinds, Refusal> {
        let tree = Expr::parse_tree(pattern).unwrap_or_else(|_| panic!("{pattern:?} parses"));
        check_look_behinds(&tree.expr, code_points)
    }

    /// A window counted in UTF-16 units.
    const fn units(min: i32, max: i32) -> JavaWindow {
        JavaWindow {
            min,
            max,
            code_points: false,
        }
    }

    /// Each look-behind's window is Java's `rmin` and `rmax`, as the `Behind`
    /// node JDK 25.0.4 builds for the same Java pattern holds them.
    #[yare::parameterized(
        char_property = { "(?<=a)", &[units(1, 1)] },
        ques = { "(?<=ab?)", &[units(1, 2)] },
        curly = { "(?<=a{2,5})", &[units(2, 5)] },
        line_ending = { r"(?<=.\R)", &[units(2, 3)] },
        branch = { "(?<=a|bcd)", &[units(1, 3)] },
        group_curly = { "(?<=(?:ab){2})", &[units(4, 4)] },
        char_property_greedy = { "(?<=a*)", &[units(0, i32::MAX)] },
        // Three greedy repeats wrap the maximum back past 0, and the body
        // matches the empty string anywhere, so the window is accepted.
        char_property_greedy_wrapped = { "(?<=a*b*c*)", &[units(0, 2_147_483_645)] },
        char_property_greedy_wrapped_negative = { "(?<=a*bc)", &[units(2, i32::MIN + 1)] },
        char_property_greedy_minimum = { "(?<=a{3,}b)", &[units(4, i32::MIN)] },
        look_ahead = { "(?<=(?=x)y)", &[units(1, 1)] },
        nested_look_behind = { "(?<=a(?<=b)c)", &[units(2, 2), units(1, 1)] },
        negated_branch = { "(?<!ab|c)", &[units(1, 2)] },
        atomic_group = { "(?<=(?>ab)c?)", &[units(2, 3)] },
        group_ques = { "(?<=a(?:bc)?)", &[units(1, 3)] },
        supplementary_char = { r"(?<=\x{1F600})", &[units(1, 1)] },
        // `a{1073741824}` four times wraps the maximum to 0, and twice wraps
        // the minimum, which Java then sets to `0xFFFFFFF`.
        wrapped_minimum = { "(?<=(?:a{1073741824}){2,4}b*)", &[units(0xFFF_FFFF, i32::MAX)] },
    )]
    fn a_window_is_the_minimum_and_maximum_java_studies(pattern: &str, expected: &[JavaWindow]) {
        let windows = look_behinds(pattern, &[]).map(|found| found.windows);
        check!(windows.as_deref() == Ok(expected));
    }

    /// A look-behind's `code_points` flag is its window's, and a look-behind
    /// past the end of the slice counts in UTF-16 units. A wrapped maximum
    /// counted in code points is refused, and one counted in units is read
    /// with its input limit. On JDK 25.0.4, `"xy".replaceAll("😀?(?<=a*b*c*x)y",
    /// "<$0>")` is `x<y>`, and `😀x(?<=a*b*c*x)y` matches `😀xy`: the char
    /// before the look-behind leaves it counted in units.
    #[yare::parameterized(
        // `rmax` is -2, and the window leaves out starts in either count.
        wrapped_to_minus_two = { "(?<=a*b*)", &[false], Err(Refusal::MaximumOverflows) },
        wrapped_in_code_points = { "(?<=a*b*c*x)", &[true], Err(Refusal::MaximumOverflows) },
        wrapped_in_units = {
            "(?<=a*b*c*x)",
            &[false],
            Ok(LookBehinds {
                windows: vec![units(1, 2_147_483_646)],
                input_limit: Some(2_147_483_646),
                too_large: false,
            }),
        },
        wrapped_with_no_flag = {
            "(?<=a*b*c*x)",
            &[],
            Ok(LookBehinds {
                windows: vec![units(1, 2_147_483_646)],
                input_limit: Some(2_147_483_646),
                too_large: false,
            }),
        },
        nested_with_one_flag = {
            "(?<=a(?<=b)c)",
            &[true],
            Ok(LookBehinds {
                windows: vec![
                    JavaWindow {
                        min: 2,
                        max: 2,
                        code_points: true,
                    },
                    units(1, 1),
                ],
                input_limit: None,
                too_large: false,
            }),
        },
        wrapped_minimum = {
            "(?<=(?:a{1073741824}){2,4}b*)",
            &[false],
            Ok(LookBehinds {
                windows: vec![units(0xFFF_FFFF, i32::MAX)],
                input_limit: Some(2_147_483_647),
                too_large: true,
            }),
        },
    )]
    fn a_window_counts_as_its_flag_says(
        pattern: &str,
        code_points: &[bool],
        expected: Result<LookBehinds, Refusal>,
    ) {
        check!(look_behinds(pattern, code_points) == expected);
    }
}
