//! Java's `Look-behind group does not have an obvious maximum length`.
//!
//! `Pattern` studies the nodes of a look-behind for the most text they can
//! match, and refuses the pattern where that is not valid. The study follows
//! the nodes Java builds, and its `int` arithmetic:
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

use {
    super::backtrack::deterministic,
    fancy_regex::{Expr, LookAround},
};

/// Java's `Pattern.MAX_REPS`, the count of an unbounded repeat.
const MAX_REPS: i32 = i32::MAX;

/// The part of Java's `TreeInfo` the check reads.
#[derive(Debug, Clone, Copy)]
struct TreeInfo {
    max_length: i32,
    max_valid: bool,
}

impl TreeInfo {
    const fn new() -> Self {
        Self {
            max_length: 0,
            max_valid: true,
        }
    }
}

/// Whether every look-behind in `expr`, the tree of a rewritten pattern, has
/// a maximum length that Java's study finds.
pub(super) fn look_behinds_bounded(expr: &Expr) -> bool {
    match expr {
        Expr::LookAround(child, kind) => {
            let bounded = !matches!(kind, LookAround::LookBehind | LookAround::LookBehindNeg) || {
                let mut info = TreeInfo::new();
                study(&[child], &mut info);
                info.max_valid
            };
            bounded && look_behinds_bounded(child)
        }
        Expr::Repeat { child, .. } | Expr::AtomicGroup(child) => look_behinds_bounded(child),
        Expr::Group(child) => look_behinds_bounded(child),
        Expr::Concat(children) | Expr::Alt(children) => children.iter().all(look_behinds_bounded),
        _ => true,
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

fn add(info: &mut TreeInfo, length: usize) {
    let length = i32::try_from(length).unwrap_or(MAX_REPS);
    info.max_length = info.max_length.wrapping_add(length);
}

/// Java's `Branch.study`, for the alternatives `children` and the nodes
/// `rest` after them.
fn branch<'a>(children: impl Iterator<Item = &'a Expr>, rest: &[&'a Expr], info: &mut TreeInfo) {
    let (mut max_length, mut max_valid) = (-1_i32, info.max_valid);
    for child in children {
        let mut alternative = TreeInfo::new();
        study(&[child], &mut alternative);
        max_length = max_length.max(alternative.max_length);
        max_valid &= alternative.max_valid;
    }
    let before = info.max_length.wrapping_add(max_length);
    *info = TreeInfo::new();
    study(rest, info);
    info.max_length = info.max_length.wrapping_add(before);
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
            info.max_length = info.max_length.wrapping_add(MAX_REPS);
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
