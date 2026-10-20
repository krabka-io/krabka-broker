//! A backtracking matcher for the patterns that `fancy_regex` cannot match as
//! `java.util.regex` does.
//!
//! [`Program`] takes the tree that `fancy_regex` parses from the rewritten
//! pattern and runs it on a backtracking machine of its own. It reads seven
//! things as Java does and `fancy_regex` does not:
//!
//! - A backreference under a case flag, which the rewrite writes as
//!   `(?i:\1)`, compares as `CIBackRef.match` does, by
//!   [`BackrefFold::chars_match`]. `fancy_regex` compares by its own fold
//!   (`translate::java_fold` lists where they differ).
//! - `\R` is Java's `LineEnding`, which gives back the `\n` of `\r\n` when the
//!   rest of the pattern needs it: `\R\n` matches `\r\n`. `fancy_regex`'s
//!   `\R` is atomic. Under a quantifier of its own Java takes the first way
//!   only, as its `Ques` and `Curly` take each iteration's atom, so `\R?\n`
//!   and `\R{2}` do not match `\r\n` there either. Inside a repeated group
//!   it gives back as anywhere else: `(\R\n)*` matches `\r\n`.
//! - A group that matched inside a look-around or an atomic group, which a
//!   possessive quantifier is, keeps its text when the match backtracks out
//!   of it: Java's `GroupTail` restores a group only when the nodes after it
//!   fail, and those of a look-around end at its condition. So
//!   `(?:(?=(a))ax|a)\1` matches `aa`. `Matcher.search` clears the groups
//!   once, so such a group also stays set for the next start position.
//! - Java sets a group when it ends, so a backreference inside the group it
//!   refers to sees the text the group had before: `(a|b\1)+` does not
//!   match `abb`. `fancy_regex` sets the start when the group opens.
//! - A repeat of a group that `TreeInfo.study` finds deterministic, with no
//!   alternation and no quantifier with a range of counts in it, is Java's
//!   `GroupCurly`. It matches each iteration the first way only, and nothing
//!   after the repeat backtracks into one. The groups an iteration sets keep
//!   their text when the match backtracks out of the repeat, so
//!   `(?:((a))*y|a)\2` matches `aa`. The repeated group itself does not when
//!   the quantifier follows its `)`, and `(?:(a)*y|a)\1` does not match `aa`.
//!   Once the least count of iterations is made, one that matches nothing
//!   fails and gives back the text of the repeated group, so `()*\1` does
//!   not match the empty string and `(())*\2` does. `fancy_regex` undoes
//!   every group it backtracks past, and keeps an iteration that matches
//!   nothing.
//! - Any other repeat of a group with a group in it, which can match
//!   nothing, keeps the groups its last iteration set when that iteration
//!   matched nothing: Java's `Loop` ends the repeat there without undoing
//!   them, so `"aa".replaceAll("(a*)*", "[$1]")` is `[][]`. `fancy_regex`
//!   takes no such iteration.
//! - `\G` is `LastMatch`, where the last match ended, and the first search
//!   started. After a match that is empty, the next search starts one char
//!   on and `\G` stays where that match ended, so `"aa".replaceAll("\\G",
//!   "-")` is `-aa`. `fancy_regex`'s `\G` is where each search starts.
//!
//! Every other node matches as `fancy_regex` matches it, and a pattern that
//! has none of the seven keeps `fancy_regex`. A tree with a node the machine
//! does not know is not compiled, and the pattern keeps `fancy_regex`'s
//! answer. The rewrite of a pattern that `Pattern.compile` takes writes none.
//!
//! The machine keeps its alternatives on a stack of its own, so a long input
//! cannot overflow the stack of the thread, and it gives up after as many
//! steps as `fancy_regex` takes backtracking steps by default: each backtrack
//! and each iteration of a repeat is one, so a repeat of something that
//! matches nothing cannot run a billion times. A look-around, an
//! atomic group and an iteration of a `GroupCurly` run on the same stack,
//! above the frames of the block they are in, so none of them allocates.
//!
//! A look-behind tries the starts of Java's `Behind`, or of `BehindS` where
//! the pattern has a supplementary char from the look-behind on: the
//! [`JavaWindow`] that the look-behind study gives it, counted back from
//! where it is checked in UTF-16 units, or for `BehindS` in code points.
//! `Behind` counts a class or `.` as one unit, so `a.(?<=a.)x` does not
//! match `a😀x`: the only start it tries is the high surrogate of the `😀`.
//! It also tries a start between the two units of a surrogate pair, where
//! Java reads the lone low surrogate, which `.` and a negated class match:
//! `.(?<=.)x` matches `😀x`, and `.(?<=\x{1F600})x` does not. A position is
//! therefore a byte offset into the input, or the offset two bytes into a
//! supplementary char, between its high and low surrogate, which is never a
//! char boundary. A group may start or end there. `fancy_regex` reads the
//! text before a look-behind by chars.

use {
    crate::{look_behind::JavaWindow, translate::BackrefFold},
    fancy_regex::{Assertion, Expr, LookAround},
    regex::Regex,
    regex_syntax::ast::{
        Ast, ClassBracketed, ClassSet, ClassSetBinaryOpKind, ClassSetItem, ClassUnicode,
        ClassUnicodeKind, parse::Parser,
    },
    std::{ops::Range, slice::Iter},
};

/// The backtracking steps one search may take, `fancy_regex`'s default
/// `backtrack_limit`.
const BACKTRACK_LIMIT: usize = 1_000_000;

/// A search the machine did not answer, which Java answers by a result or an
/// exception, and a caller rejects.
#[derive(Debug, thiserror::Error)]
pub(super) enum SearchError {
    #[error("the search ran past the backtrack limit of {BACKTRACK_LIMIT} steps")]
    BacktrackLimit,
}

/// A pattern compiled for the machine.
#[derive(Debug)]
pub(super) struct Program {
    /// The instructions of the pattern, then those of each look-around,
    /// atomic group and iteration of a `GroupCurly`. Each block ends in
    /// [`Inst::Match`].
    blocks: Vec<Vec<Inst>>,
    /// Two slots for each group, group 0 first, then the private slots of
    /// the groups and repeats.
    slot_count: usize,
    /// The groups, group 0 included.
    groups: usize,
    /// Whether the pattern has a node that `fancy_regex` reads otherwise.
    differs: bool,
}

/// One instruction of the machine. A jump target is an index into the block
/// the instruction is in.
#[derive(Debug)]
enum Inst {
    /// This char.
    Char(char),
    /// One char of the class.
    Class(Class),
    /// One char, other than `\n` unless `newline`.
    Any {
        newline: bool,
    },
    /// `\R`: `\r\n`, or one of `\n`, `\x0B`, `\x0C`, `\r`, U+0085, U+2028 and
    /// U+2029. With `give_back`, `\r\n` backtracks to `\r`.
    Linebreak {
        give_back: bool,
    },
    /// Go on at `first`, and at `second` when that fails.
    Split {
        first: usize,
        second: usize,
    },
    Jump(usize),
    /// Record the position in the slot.
    Save(usize),
    /// `group` ends here, and started at the position in `open`. Java sets a
    /// group only when it ends, so a reference inside it sees the text it
    /// had before.
    Close {
        group: usize,
        open: usize,
    },
    /// The text that `group` matched: as it is, or under `fold`.
    Backref {
        group: usize,
        fold: Option<BackrefFold>,
    },
    Assert(Check),
    /// The block matches here, or with `behind` it matches text that ends
    /// here and starts where [`Behind`] says, or with `negated` it does not.
    Look {
        block: usize,
        behind: Option<Behind>,
        negated: bool,
    },
    /// The first way the block matches here, which nothing backtracks into.
    Atomic(usize),
    /// One iteration of Java's `GroupCurly`: the first way the block matches
    /// here, which nothing backtracks into. The groups it sets keep their
    /// text, but `group`, the repeated group when the quantifier follows its
    /// `)`, gets back the text it had when the match backtracks past the
    /// iteration.
    Iteration {
        block: usize,
        group: Option<usize>,
    },
    /// A repeat has made no iteration yet.
    RepeatStart {
        count: usize,
    },
    /// The head of a repeat: iterate at the next instruction, or leave.
    Repeat {
        count: usize,
        lo: usize,
        hi: usize,
        greedy: bool,
        exit: usize,
    },
    /// The end of an iteration that started at the position in `start`. One
    /// that matched nothing counts toward `lo`, as in Java and `fancy_regex`.
    /// Once `lo` iterations are made, such an iteration leaves the repeat and
    /// keeps the groups it set, as Java's `Loop` does, so that it cannot loop
    /// for ever. With `group_curly`, one past the `lo`th fails instead, as
    /// Java's `GroupCurly` takes none.
    RepeatEnd {
        count: usize,
        lo: usize,
        start: usize,
        head: usize,
        exit: usize,
        group_curly: bool,
    },
    Match,
}

/// What `Character.codePointAt` reads at a position: a char, or between the
/// two units of a surrogate pair the lone low surrogate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Unit {
    Char(char),
    LowSurrogate(u16),
}

/// One char that a regex, compiled as `\A(?:class)\z`, matches.
///
/// The answer for each ASCII char is worked out when the class is compiled,
/// by the same regex, so that one under a case flag reads as it does. Only a
/// char past ASCII runs the regex. The `regex` crate cannot test a lone
/// surrogate, so the answer for those is worked out from the text of the
/// class: [`class_has_surrogates`].
#[derive(Debug)]
struct Class {
    /// Bit `c` is whether the regex matches the ASCII char `c`.
    ascii: u128,
    /// Whether the class matches a lone low surrogate.
    surrogates: bool,
    regex: Regex,
}

impl Class {
    /// The class that `class`, in `regex` syntax, is, or `None` when `regex`
    /// does not compile it.
    fn new(class: &str) -> Option<Self> {
        let regex = Regex::new(&format!("\\A(?:{class})\\z")).ok()?;
        let ascii = (0..128_u8)
            .filter(|&byte| regex.is_match(char::from(byte).encode_utf8(&mut [0; 4])))
            .fold(0, |table, byte| table | 1 << byte);
        Some(Self {
            ascii,
            surrogates: class_has_surrogates(class),
            regex,
        })
    }

    fn matches(&self, c: char) -> bool {
        if c.is_ascii() {
            self.ascii >> u32::from(c) & 1 == 1
        } else {
            self.regex.is_match(c.encode_utf8(&mut [0; 4]))
        }
    }

    fn matches_unit(&self, unit: Unit) -> bool {
        match unit {
            Unit::Char(c) => self.matches(c),
            Unit::LowSurrogate(_) => self.surrogates,
        }
    }
}

/// Whether the class `class`, in `regex` syntax as the rewrite writes it,
/// matches the lone low surrogates in Java, which it matches all or none of.
///
/// The text cannot name a surrogate, as no `char` is one, so a range spans
/// the surrogates whole or misses them, and the answer is worked out from
/// the parts of the class:
///
/// - A literal matches none, and a range all that it spans.
/// - An ASCII class, and Perl's `\d`, `\s` and `\w`, match none.
/// - A Unicode class matches them for the general categories `Cs` and `C`,
///   for `Assigned`, and for the script `Zzzz`, as `Script` or
///   `Script_Extensions`, and for no other. `Any` is `fancy_regex`'s
///   `\P{Cs}`: [`category_has_surrogates`].
/// - Negation, and the set operations of a bracketed class, apply as they do
///   to a char. A flag changes nothing for a surrogate.
///
/// A text that does not parse as one class matches none.
fn class_has_surrogates(class: &str) -> bool {
    Parser::new()
        .parse(class)
        .ok()
        .and_then(|ast| ast_has_surrogates(&ast))
        .unwrap_or(false)
}

fn ast_has_surrogates(ast: &Ast) -> Option<bool> {
    match ast {
        Ast::Literal(_) => Some(false),
        Ast::Dot(_) => Some(true),
        Ast::ClassPerl(class) => Some(class.negated),
        Ast::ClassUnicode(class) => Some(unicode_has_surrogates(class)),
        Ast::ClassBracketed(class) => Some(bracketed_has_surrogates(class)),
        Ast::Group(group) if !group.is_capturing() => ast_has_surrogates(&group.ast),
        Ast::Concat(concat) => {
            let mut classes = concat
                .asts
                .iter()
                .filter(|ast| !matches!(ast, Ast::Flags(_)));
            match (classes.next(), classes.next()) {
                (Some(class), None) => ast_has_surrogates(class),
                _ => None,
            }
        }
        _ => None,
    }
}

fn unicode_has_surrogates(class: &ClassUnicode) -> bool {
    let named = |name: &str| {
        let name = normalize_property_name(name);
        category_has_surrogates(&name) || script_has_surrogates(&name)
    };
    let surrogates = match &class.kind {
        ClassUnicodeKind::OneLetter(letter) => named(&letter.to_string()),
        ClassUnicodeKind::Named(name) => named(name),
        ClassUnicodeKind::NamedValue { name, value, .. } => {
            let value = normalize_property_name(value);
            match normalize_property_name(name).as_str() {
                "gc" | "generalcategory" => category_has_surrogates(&value),
                "sc" | "script" | "scx" | "scriptextensions" => script_has_surrogates(&value),
                _ => false,
            }
        }
    };
    surrogates != class.is_negated()
}

fn bracketed_has_surrogates(class: &ClassBracketed) -> bool {
    set_has_surrogates(&class.kind) != class.negated
}

fn set_has_surrogates(set: &ClassSet) -> bool {
    match set {
        ClassSet::Item(item) => item_has_surrogates(item),
        ClassSet::BinaryOp(op) => {
            let (lhs, rhs) = (set_has_surrogates(&op.lhs), set_has_surrogates(&op.rhs));
            match op.kind {
                ClassSetBinaryOpKind::Intersection => lhs && rhs,
                ClassSetBinaryOpKind::Difference => lhs && !rhs,
                ClassSetBinaryOpKind::SymmetricDifference => lhs != rhs,
            }
        }
    }
}

fn item_has_surrogates(item: &ClassSetItem) -> bool {
    match item {
        ClassSetItem::Empty(_) | ClassSetItem::Literal(_) => false,
        ClassSetItem::Range(range) => {
            (u32::from(range.start.c)..=u32::from(range.end.c)).contains(&0xDC00)
        }
        ClassSetItem::Ascii(class) => class.negated,
        ClassSetItem::Perl(class) => class.negated,
        ClassSetItem::Unicode(class) => unicode_has_surrogates(class),
        ClassSetItem::Bracketed(class) => bracketed_has_surrogates(class),
        ClassSetItem::Union(union) => union.items.iter().any(item_has_surrogates),
    }
}

/// A Unicode property name or value as the `regex` crate matches it: with
/// no `is` before it, no space, `_` or `-`, and no char past ASCII, in lower
/// case. `regex_syntax` reads `isc` as itself, not as `c`.
fn normalize_property_name(name: &str) -> String {
    let (rest, is) = match name.get(..2) {
        Some(prefix) if prefix.eq_ignore_ascii_case("is") => (&name[2..], true),
        _ => (name, false),
    };
    let normalized: String = rest
        .chars()
        .filter(|c| c.is_ascii() && !matches!(c, ' ' | '_' | '-'))
        .map(|c| c.to_ascii_lowercase())
        .collect();
    if is && normalized == "c" {
        "isc".to_owned()
    } else {
        normalized
    }
}

/// Whether the general category `name`, normalized, has the surrogates:
/// `Cs` and `C`, and the `regex` crate's `Assigned`.
///
/// `fancy_regex` writes `\p{Cs}` as `\P{any}`, and `\P{Cs}` as `\p{any}`,
/// which match the same chars, so `Any` counts as the class without the
/// surrogates. The rewrite writes no `\p{Any}` of its own: Java's is refused.
fn category_has_surrogates(name: &str) -> bool {
    matches!(name, "cs" | "surrogate" | "c" | "other" | "assigned")
}

/// Whether the script `name`, normalized, has the surrogates: `Unknown`.
fn script_has_surrogates(name: &str) -> bool { matches!(name, "zzzz" | "unknown") }

/// The starts a look-behind tries.
#[derive(Clone, Copy, Debug)]
enum Behind {
    /// Java's window, as `Behind` or `BehindS` tries it.
    Window(JavaWindow),
    /// The starts at a char boundary within the lengths its body can match,
    /// for a look-behind that [`Program::new`] is given no window for.
    Lengths(Lengths),
}

/// The least and the most UTF-16 units that a look-behind's body can match,
/// with `max` `None` when there is no most. These are bounds of what the
/// body matches on the machine, so no start outside them can match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Lengths {
    min: usize,
    max: Option<usize>,
}

impl Lengths {
    const fn exactly(units: usize) -> Self {
        Self {
            min: units,
            max: Some(units),
        }
    }

    /// The text of `self` then that of `other`.
    fn then(self, other: Self) -> Self {
        Self {
            min: self.min.saturating_add(other.min),
            max: self.max.zip(other.max).map(|(a, b)| a.saturating_add(b)),
        }
    }

    /// The text of `self` or that of `other`.
    fn or(self, other: Self) -> Self {
        Self {
            min: self.min.min(other.min),
            max: self.max.zip(other.max).map(|(a, b)| a.max(b)),
        }
    }

    /// Bounds of what `expr` can match on the machine. A char the machine
    /// matches as one may be a supplementary one, two units. A node the
    /// machine does not know, and a backreference, has no most.
    fn of(expr: &Expr) -> Self {
        const UNBOUNDED: Lengths = Lengths { min: 0, max: None };
        match expr {
            Expr::Empty
            | Expr::Assertion(_)
            | Expr::LookAround(..)
            | Expr::ContinueFromPreviousMatchEnd => Self::exactly(0),
            Expr::Literal { val, .. } => Self::exactly(val.encode_utf16().count()),
            Expr::Any { .. } | Expr::Delegate { .. } | Expr::GeneralNewline { .. } => Self {
                min: 1,
                max: Some(2),
            },
            Expr::Concat(children) => children
                .iter()
                .map(Self::of)
                .fold(Self::exactly(0), Self::then),
            Expr::Alt(children) => children
                .iter()
                .map(Self::of)
                .reduce(Self::or)
                .unwrap_or(Self::exactly(0)),
            Expr::Group(child) => Self::of(child),
            Expr::AtomicGroup(child) => Self::of(child),
            Expr::Repeat { child, lo, hi, .. } => {
                let child = Self::of(child);
                Self {
                    min: child.min.saturating_mul(*lo),
                    max: child
                        .max
                        .filter(|_| *hi != usize::MAX)
                        .map(|max| max.saturating_mul(*hi)),
                }
            }
            _ => UNBOUNDED,
        }
    }
}

/// A zero-width test, with the meaning `fancy_regex` gives it.
#[derive(Debug)]
enum Check {
    StartText,
    EndText,
    /// `\G`: where the last match ended, or the first search started.
    LastMatchEnd,
    /// `\Z`: the end, or only `\n`s after here.
    EndTextBeforeNewlines,
    /// Under `(?m)`: the start, or after `\n`.
    StartLine,
    /// Under `(?m)`: the end, or before `\n`.
    EndLine,
    /// `\b`, or `\B` when `negated`, under `(?U)`: between a Unicode word
    /// char, which `word` matches, and one that is not.
    WordBoundary {
        word: Class,
        negated: bool,
    },
}

impl Program {
    /// Compiles the pattern that `translated` is, in `fancy_regex` syntax, with
    /// `groups` groups, group 0 included, `folds` the fold of each of its
    /// backreferences under a case flag in order, and `linebreak_atoms`
    /// whether a quantifier binds to each of its `\R`s alone, in order, and
    /// `windows` Java's window of each look-behind in the order they open,
    /// outer ones first. A look-behind past the last window tries the starts
    /// at a char boundary within the lengths its body can match. `None` for a
    /// pattern the machine does not know a node of.
    pub(super) fn new(
        translated: &str,
        groups: usize,
        folds: &[BackrefFold],
        linebreak_atoms: &[bool],
        group_repeats: &[bool],
        windows: &[JavaWindow],
    ) -> Option<Self> {
        let tree = Expr::parse_tree(translated).ok()?;
        let mut compiler = Compiler {
            blocks: vec![Vec::new()],
            slot_count: 2 * groups,
            next_group: 1,
            folds: folds.iter(),
            linebreak_atoms: linebreak_atoms.iter(),
            group_repeats,
            windows: windows.iter(),
            nested: false,
            first_way: false,
            open_groups: Vec::new(),
            differs: false,
        };
        let mut main = Vec::new();
        compiler.compile(&tree.expr, &mut main)?;
        main.push(Inst::Match);
        compiler.blocks[0] = main;
        // Every group, fold and `\R` the rewrite wrote is in the tree.
        if compiler.next_group != groups
            || compiler.folds.next().is_some()
            || compiler.linebreak_atoms.next().is_some()
        {
            return None;
        }
        Some(Self {
            blocks: compiler.blocks,
            slot_count: compiler.slot_count,
            groups,
            differs: compiler.differs,
        })
    }

    /// Whether the pattern has a backreference under a case flag or inside the
    /// group it refers to, a `\R`, a group inside a look-around or an atomic
    /// group, a `GroupCurly` with a group in its iterations other than the
    /// repeated one or with a repeated group that can match nothing, another
    /// repeat of a group with a group in it that can match nothing, or a
    /// `\G`, which `fancy_regex` does not read as Java does.
    pub(super) fn differs_from_fancy(&self) -> bool { self.differs }

    /// `Matcher.matches()`: whether some way of matching from the start spans
    /// the whole input.
    pub(super) fn matches(&self, input: &str) -> Result<bool, SearchError> {
        let mut slots = vec![None; self.slot_count];
        let mut machine = Machine::new(self, input, 0);
        Ok(machine.run(0, 0, Some(input.len()), &mut slots)?.is_some())
    }

    /// `Matcher.find()` after a match that ended at `last`, or the first one
    /// with `last` at `from`: the first match that starts at `from` or after
    /// it, as the span of each group, group 0 the whole match.
    pub(super) fn find(
        &self,
        input: &str,
        from: usize,
        last: usize,
    ) -> Result<Option<Vec<Option<Range<usize>>>>, SearchError> {
        let mut machine = Machine::new(self, input, last);
        let starts = input[from..]
            .char_indices()
            .map(|(offset, _)| from + offset)
            .chain([input.len()]);
        // One set of slots for every start, as `Matcher.search` clears the
        // groups once: a group that kept its text when an earlier start
        // failed still has it.
        let mut slots = vec![None; self.slot_count];
        for start in starts {
            if let Some(end) = machine.run(0, start, None, &mut slots)? {
                slots[0] = Some(start);
                slots[1] = Some(end);
                return Ok(Some(
                    slots
                        .chunks(2)
                        .take(self.groups)
                        .map(|pair| Some(pair[0]?..pair[1]?))
                        .collect(),
                ));
            }
        }
        Ok(None)
    }
}

/// The state of one [`Program::new`].
struct Compiler<'a> {
    blocks: Vec<Vec<Inst>>,
    slot_count: usize,
    /// The number of the next group, in the order the groups open.
    next_group: usize,
    /// The folds of the references under a case flag not reached yet.
    folds: Iter<'a, BackrefFold>,
    /// For each `\R` not reached yet, whether a quantifier binds to it alone.
    linebreak_atoms: Iter<'a, bool>,
    /// For each group by number, whether Java repeats it with a `GroupCurly`
    /// that sets the group itself: `JavaTranslation::group_repeats`.
    group_repeats: &'a [bool],
    /// The windows of the look-behinds not reached yet.
    windows: Iter<'a, JavaWindow>,
    /// Inside a look-around or an atomic group.
    nested: bool,
    /// Inside the atom of a quantifier that Java matches the first way only.
    first_way: bool,
    /// The groups that the node being compiled is inside.
    open_groups: Vec<usize>,
    /// See [`Program::differs_from_fancy`].
    differs: bool,
}

impl Compiler<'_> {
    /// A new slot, after those of the groups.
    fn slot(&mut self) -> usize {
        self.slot_count += 1;
        self.slot_count - 1
    }

    /// Compiles `expr` into the block of a look-around or an atomic group, and
    /// gives its index.
    fn nested_block(&mut self, expr: &Expr) -> Option<usize> {
        let outer = self.nested;
        self.nested = true;
        let block = self.block(expr)?;
        self.nested = outer;
        Some(block)
    }

    /// Compiles `expr` into a block of its own, and gives its index. A block
    /// backtracks inside itself, as the condition of a Java look-around and
    /// the atom of a `GroupCurly` do, whatever quantifier it is under.
    fn block(&mut self, expr: &Expr) -> Option<usize> {
        let outer = self.first_way;
        self.first_way = false;
        let mut code = Vec::new();
        self.compile(expr, &mut code)?;
        self.first_way = outer;
        code.push(Inst::Match);
        self.blocks.push(code);
        Some(self.blocks.len() - 1)
    }

    fn compile(&mut self, expr: &Expr, code: &mut Vec<Inst>) -> Option<()> {
        match expr {
            Expr::Empty => {}
            Expr::Any {
                newline,
                crlf: false,
            } => code.push(Inst::Any { newline: *newline }),
            Expr::GeneralNewline { .. } => {
                self.linebreak_atoms.next()?;
                self.differs = true;
                code.push(Inst::Linebreak {
                    give_back: !self.first_way,
                });
            }
            Expr::ContinueFromPreviousMatchEnd => {
                self.differs = true;
                code.push(Inst::Assert(Check::LastMatchEnd));
            }
            Expr::Assertion(assertion) => code.push(Inst::Assert(check(*assertion)?)),
            Expr::Literal { val, casei: false } => code.extend(val.chars().map(Inst::Char)),
            Expr::Concat(children) => {
                for child in children {
                    self.compile(child, code)?;
                }
            }
            Expr::Alt(children) => self.alternation(children, code)?,
            Expr::Group(child) => {
                let group = self.next_group;
                self.next_group += 1;
                self.differs |= self.nested;
                let open = self.slot();
                code.push(Inst::Save(open));
                self.open_groups.push(group);
                self.compile(child, code)?;
                self.open_groups.pop();
                code.push(Inst::Close { group, open });
            }
            Expr::LookAround(child, kind) => {
                let (behind, negated) = match kind {
                    LookAround::LookAhead => (false, false),
                    LookAround::LookAheadNeg => (false, true),
                    LookAround::LookBehind => (true, false),
                    LookAround::LookBehindNeg => (true, true),
                };
                // Before the body, whose look-behinds come after this one.
                let behind = behind.then(|| {
                    self.windows.next().map_or_else(
                        || Behind::Lengths(Lengths::of(child)),
                        |&window| Behind::Window(window),
                    )
                });
                let block = self.nested_block(child)?;
                code.push(Inst::Look {
                    block,
                    behind,
                    negated,
                });
            }
            Expr::Repeat {
                child,
                lo,
                hi,
                greedy,
            } => self.repeat(child, *lo, *hi, *greedy, code)?,
            Expr::Delegate {
                inner,
                casei: false,
            } => {
                code.push(Inst::Class(Class::new(inner)?));
            }
            Expr::Backref { group, casei } => {
                self.differs |= *casei || self.open_groups.contains(group);
                let fold = if *casei {
                    Some(*self.folds.next()?)
                } else {
                    None
                };
                code.push(Inst::Backref {
                    group: *group,
                    fold,
                });
            }
            Expr::AtomicGroup(child) => {
                let block = self.nested_block(child)?;
                code.push(Inst::Atomic(block));
            }
            _ => return None,
        }
        Some(())
    }

    fn alternation(&mut self, children: &[Expr], code: &mut Vec<Inst>) -> Option<()> {
        let Some((last, rest)) = children.split_last() else {
            return Some(());
        };
        let mut jumps = Vec::new();
        for child in rest {
            let split = code.len();
            code.push(Inst::Split {
                first: split + 1,
                second: 0,
            });
            self.compile(child, code)?;
            jumps.push(code.len());
            code.push(Inst::Jump(0));
            let next = code.len();
            if let Inst::Split { second, .. } = &mut code[split] {
                *second = next;
            }
        }
        self.compile(last, code)?;
        let end = code.len();
        for jump in jumps {
            code[jump] = Inst::Jump(end);
        }
        Some(())
    }

    fn repeat(
        &mut self,
        child: &Expr,
        lo: usize,
        hi: usize,
        greedy: bool,
        code: &mut Vec<Inst>,
    ) -> Option<()> {
        let (count, start) = (self.slot(), self.slot());
        // `{0,1}` is Java's `Ques`, a `Branch` for a group, and a group's
        // other quantifiers a `GroupCurly` when `TreeInfo.study` finds it
        // deterministic. Of the trees a Java group gives, only these three can
        // hold a group or a `\R`. A repeat is one only as the body of a group,
        // `(?:x{2})*`: Java refuses `x{2}*` as a dangling quantifier.
        let group_curly = (lo, hi) != (0, 1)
            && matches!(
                child,
                Expr::Group(_) | Expr::Concat(_) | Expr::Repeat { .. }
            )
            && deterministic(child);
        let repeated = match child {
            Expr::Group(body) if group_curly => self
                .group_repeats
                .get(self.next_group)
                .copied()
                .unwrap_or(false)
                .then_some((self.next_group, &**body)),
            _ => None,
        };
        self.differs |= match repeated {
            // The repeated group gives back the text of an iteration that
            // matched nothing, and the groups inside it keep theirs.
            Some((_, body)) => can_be_empty(child) || has_group(body),
            None if group_curly => has_group(child),
            // Java's `Loop` keeps the groups of a last iteration that matched
            // nothing, which `fancy_regex` never takes.
            None => (lo, hi) != (0, 1) && has_group(child) && can_be_empty(child),
        };
        code.push(Inst::RepeatStart { count });
        let head = code.len();
        code.push(Inst::Repeat {
            count,
            lo,
            hi,
            greedy,
            exit: 0,
        });
        code.push(Inst::Save(start));
        if group_curly {
            let block = self.block(child)?;
            code.push(Inst::Iteration {
                block,
                group: repeated.map(|(group, _)| group),
            });
        } else {
            let outer = self.first_way;
            // A `\R` that the quantifier binds to alone, as against `(?:\R)`.
            let linebreak_atom = matches!(child, Expr::GeneralNewline { .. })
                && self.linebreak_atoms.as_slice().first() == Some(&true);
            self.first_way |= takes_first_way(child, lo, hi, linebreak_atom);
            self.compile(child, code)?;
            self.first_way = outer;
        }
        let exit = code.len() + 1;
        code.push(Inst::RepeatEnd {
            count,
            lo,
            start,
            head,
            exit,
            group_curly,
        });
        if let Inst::Repeat { exit: target, .. } = &mut code[head] {
            *target = exit;
        }
        Some(())
    }
}

/// Whether Java matches each iteration of a repeat of `child` the first way
/// only, which matters for a `\R` in it and nothing else.
///
/// A `?` of a lone atom is Java's `Ques`, and any other quantifier of one is
/// `Curly`. Both take the atom's first way. A quantifier of a group is a
/// `Branch` for `?`, which backtracks into the group. For the others it is a
/// `GroupCurly`, which takes the first way, when `TreeInfo.study` finds the
/// group deterministic, and otherwise a `Loop`, which backtracks. The tree of
/// `(?:\R)?` is that of `\R?`, so `linebreak_atom`, from the rewrite, says
/// which one a repeat of a `\R` is.
fn takes_first_way(child: &Expr, lo: usize, hi: usize, linebreak_atom: bool) -> bool {
    match child {
        _ if linebreak_atom => true,
        _ if (lo, hi) == (0, 1) => false,
        _ => deterministic(child),
    }
}

/// `TreeInfo.deterministic` after Java studies `expr`: no alternation, and
/// no quantifier with a range of counts. A look-around is not studied.
pub(super) fn deterministic(expr: &Expr) -> bool {
    match expr {
        Expr::Alt(_) => false,
        Expr::Repeat { child, lo, hi, .. } => lo == hi && deterministic(child),
        Expr::Concat(children) => children.iter().all(deterministic),
        Expr::Group(child) => deterministic(child),
        Expr::AtomicGroup(child) => deterministic(child),
        _ => true,
    }
}

/// Whether `expr` has a capturing group in it.
fn has_group(expr: &Expr) -> bool {
    match expr {
        Expr::Group(_) => true,
        Expr::Concat(children) | Expr::Alt(children) => children.iter().any(has_group),
        Expr::Repeat { child, .. } | Expr::LookAround(child, _) | Expr::AtomicGroup(child) => {
            has_group(child)
        }
        _ => false,
    }
}

/// Whether `expr` may match the empty string. A node the machine does not
/// know may.
fn can_be_empty(expr: &Expr) -> bool {
    match expr {
        Expr::Literal { val, .. } => val.is_empty(),
        Expr::Any { .. } | Expr::Delegate { .. } | Expr::GeneralNewline { .. } => false,
        Expr::Concat(children) => children.iter().all(can_be_empty),
        Expr::Alt(children) => children.iter().any(can_be_empty),
        Expr::Group(child) => can_be_empty(child),
        Expr::AtomicGroup(child) => can_be_empty(child),
        Expr::Repeat { child, lo, .. } => *lo == 0 || can_be_empty(child),
        _ => true,
    }
}

/// The [`Check`] for an assertion, or `None` for one the machine does not
/// know.
fn check(assertion: Assertion) -> Option<Check> {
    Some(match assertion {
        Assertion::StartText => Check::StartText,
        Assertion::EndText => Check::EndText,
        Assertion::EndTextIgnoreTrailingNewlines { crlf: false } => Check::EndTextBeforeNewlines,
        Assertion::StartLine { crlf: false } => Check::StartLine,
        Assertion::EndLine { crlf: false } => Check::EndLine,
        Assertion::WordBoundary | Assertion::NotWordBoundary => Check::WordBoundary {
            word: Class::new(r"\w")?,
            negated: matches!(assertion, Assertion::NotWordBoundary),
        },
        _ => return None,
    })
}

/// An entry of the machine's backtracking stack.
#[derive(Clone, Copy)]
enum Frame {
    /// Go on at `pc` and `pos`.
    Alternative { pc: usize, pos: usize },
    /// Put the value back in the slot.
    Undo { slot: usize, value: Option<usize> },
}

/// One search of a [`Program`] over an input.
struct Machine<'a> {
    program: &'a Program,
    input: &'a str,
    /// Where the last match ended, which `\G` matches.
    last_match_end: usize,
    /// The backtracking steps left.
    budget: usize,
    /// The frames of every block that runs, each nested run above those of
    /// the block it is in.
    stack: Vec<Frame>,
    /// For each slot, the last [`Machine::keep`] that found a frame for it,
    /// by the number in `keeps`.
    kept: Vec<usize>,
    /// The [`Machine::keep`]s so far.
    keeps: usize,
}

impl<'a> Machine<'a> {
    fn new(program: &'a Program, input: &'a str, last_match_end: usize) -> Self {
        Self {
            program,
            input,
            last_match_end,
            budget: BACKTRACK_LIMIT,
            stack: Vec::new(),
            kept: vec![0; program.slot_count],
            keeps: 0,
        }
    }

    /// Runs `block` at `at`, and gives the position the first way of matching
    /// ends at, with the slots it set. With `end`, a way of matching that ends
    /// elsewhere is not one. The frames of the run go on the stack above
    /// those it had. On no match they are gone, and the slots are as they
    /// were but for the groups that a nested run set.
    fn run(
        &mut self,
        block: usize,
        at: usize,
        end: Option<usize>,
        slots: &mut [Option<usize>],
    ) -> Result<Option<usize>, SearchError> {
        let program = self.program;
        let code = &program.blocks[block];
        let base = self.stack.len();
        let (mut pc, mut pos) = (0, at);
        loop {
            let next = match &code[pc] {
                Inst::Char(expected) => self
                    .next_unit(pos)
                    .filter(|&(actual, _)| actual == Unit::Char(*expected))
                    .map(|(_, after)| (pc + 1, after)),
                Inst::Class(class) => self
                    .next_unit(pos)
                    .filter(|&(actual, _)| class.matches_unit(actual))
                    .map(|(_, after)| (pc + 1, after)),
                Inst::Any { newline } => self
                    .next_unit(pos)
                    .filter(|&(actual, _)| *newline || actual != Unit::Char('\n'))
                    .map(|(_, after)| (pc + 1, after)),
                &Inst::Linebreak { give_back } => {
                    if self
                        .input
                        .get(pos..)
                        .is_some_and(|after| after.starts_with("\r\n"))
                    {
                        if give_back {
                            self.stack.push(Frame::Alternative {
                                pc: pc + 1,
                                pos: pos + 1,
                            });
                        }
                        Some((pc + 1, pos + 2))
                    } else {
                        self.next_unit(pos)
                            .filter(|(actual, _)| {
                                matches!(
                                    actual,
                                    Unit::Char(
                                        '\n' | '\x0B'
                                            | '\x0C'
                                            | '\r'
                                            | '\u{85}'
                                            | '\u{2028}'
                                            | '\u{2029}'
                                    )
                                )
                            })
                            .map(|(_, after)| (pc + 1, after))
                    }
                }
                &Inst::Split { first, second } => {
                    self.stack.push(Frame::Alternative { pc: second, pos });
                    Some((first, pos))
                }
                &Inst::Jump(target) => Some((target, pos)),
                &Inst::Save(slot) => {
                    set(&mut self.stack, slots, slot, Some(pos));
                    Some((pc + 1, pos))
                }
                &Inst::Close { group, open } => {
                    set(&mut self.stack, slots, 2 * group, slots[open]);
                    set(&mut self.stack, slots, 2 * group + 1, Some(pos));
                    Some((pc + 1, pos))
                }
                &Inst::Backref { group, fold } => slots[2 * group]
                    .zip(slots[2 * group + 1])
                    .and_then(|(start, stop)| self.backref(pos, start..stop, fold))
                    .map(|after| (pc + 1, after)),
                Inst::Assert(check) => self.holds(check, pos).then_some((pc + 1, pos)),
                &Inst::Look {
                    block,
                    behind,
                    negated,
                } => {
                    let nested = self.stack.len();
                    let found = match behind {
                        Some(behind) => self.behind(block, pos, behind, slots)?,
                        None => self.run(block, pos, None, slots)?.is_some(),
                    };
                    self.keep(nested, slots, None);
                    (found != negated).then_some((pc + 1, pos))
                }
                &Inst::Atomic(block) => {
                    let nested = self.stack.len();
                    let after = self.run(block, pos, None, slots)?;
                    self.keep(nested, slots, None);
                    after.map(|after| (pc + 1, after))
                }
                &Inst::Iteration { block, group } => {
                    let nested = self.stack.len();
                    let after = self.run(block, pos, None, slots)?;
                    self.keep(nested, slots, group);
                    after.map(|after| (pc + 1, after))
                }
                &Inst::RepeatStart { count } => {
                    set(&mut self.stack, slots, count, Some(0));
                    Some((pc + 1, pos))
                }
                &Inst::Repeat {
                    count,
                    lo,
                    hi,
                    greedy,
                    exit,
                } => {
                    let done = slots[count].unwrap_or(0);
                    if done < lo {
                        Some((pc + 1, pos))
                    } else if done >= hi {
                        Some((exit, pos))
                    } else if greedy {
                        self.stack.push(Frame::Alternative { pc: exit, pos });
                        Some((pc + 1, pos))
                    } else {
                        self.stack.push(Frame::Alternative { pc: pc + 1, pos });
                        Some((exit, pos))
                    }
                }
                &Inst::RepeatEnd {
                    count,
                    lo,
                    start,
                    head,
                    exit,
                    group_curly,
                } => {
                    // An iteration is a step too, so that a repeat of something that
                    // matches nothing, such as `(){1000000000}`, stops at the limit
                    // and does not grow the stack by a billion frames.
                    self.spend()?;
                    let done = slots[count].unwrap_or(0) + 1;
                    set(&mut self.stack, slots, count, Some(done));
                    let empty = slots[start] == Some(pos);
                    match (group_curly, empty) {
                        (true, true) if done > lo => None,
                        (false, true) if done >= lo => Some((exit, pos)),
                        _ => Some((head, pos)),
                    }
                }
                Inst::Match if end.is_none_or(|end| end == pos) => return Ok(Some(pos)),
                Inst::Match => None,
            };
            (pc, pos) = match next {
                Some(next) => next,
                None => match self.backtrack(base, slots)? {
                    Some(next) => next,
                    None => return Ok(None),
                },
            };
        }
    }

    /// Pops the stack down to `base`, undoing the slots, to the next
    /// alternative, or `None` when there is none left above `base`.
    fn backtrack(
        &mut self,
        base: usize,
        slots: &mut [Option<usize>],
    ) -> Result<Option<(usize, usize)>, SearchError> {
        while self.stack.len() > base {
            match self.stack.pop() {
                Some(Frame::Undo { slot, value }) => slots[slot] = value,
                Some(Frame::Alternative { pc, pos }) => {
                    self.spend()?;
                    return Ok(Some((pc, pos)));
                }
                None => break,
            }
        }
        Ok(None)
    }

    /// Takes one backtracking step from the budget.
    fn spend(&mut self) -> Result<(), SearchError> {
        self.budget = self
            .budget
            .checked_sub(1)
            .ok_or(SearchError::BacktrackLimit)?;
        Ok(())
    }

    /// A look-behind: whether `block` matches text that ends at `pos`, trying
    /// the nearest start first, as Java's `Behind` and `BehindS` do.
    ///
    /// `Behind` tries `j` from `i - rmin` down to `max(i - rmax, 0)`, one
    /// UTF-16 unit at a time, with `i` the unit at `pos` and the arithmetic
    /// Java's `int`. `BehindS` counts `rmin` and `rmax` back from `i` in code
    /// points instead, by `countChars`, which counts forward for a negative
    /// length, and steps `j` back a code point at a time, but one unit from
    /// `from` itself.
    fn behind(
        &mut self,
        block: usize,
        pos: usize,
        behind: Behind,
        slots: &mut [Option<usize>],
    ) -> Result<bool, SearchError> {
        let window = match behind {
            Behind::Window(window) => window,
            Behind::Lengths(lengths) => return self.behind_within(block, pos, lengths, slots),
        };
        // The units back from `pos` of the nearest start and the farthest.
        // A start before the input is not tried.
        let (nearest, farthest) = self.window_units(pos, window);
        let Some(mut start) = self.units_back(pos, nearest) else {
            return Ok(false);
        };
        let mut back = nearest;
        while back <= farthest {
            self.spend()?;
            if self.run(block, start, Some(pos), slots)?.is_some() {
                return Ok(true);
            }
            // `j > from`: before the farthest start, and not at the first
            // unit of the input.
            let step = if window.code_points && back < farthest && start > 0 {
                i64::from(self.count_chars(start, -1))
            } else {
                1
            };
            let Some(next) = self.units_back(start, step) else {
                break;
            };
            (start, back) = (next, back + step);
        }
        Ok(false)
    }

    /// The units back from `pos` that Java's `window` tries the starts
    /// between: `i - j` for its first `j`, and for `from`.
    fn window_units(&self, pos: usize, window: JavaWindow) -> (i64, i64) {
        let JavaWindow {
            min,
            max,
            code_points,
        } = window;
        if code_points {
            // `countChars` gives no negative count.
            return (
                i64::from(self.count_chars(pos, min.wrapping_neg())),
                i64::from(self.count_chars(pos, max.wrapping_neg())),
            );
        }
        if min >= 0 && max >= 0 {
            return (i64::from(min), i64::from(max));
        }
        // A negative length, which Java's study can wrap to, counts from
        // past `i`, where the value of `i` matters.
        let i = self.utf16_index(pos);
        let from = i.wrapping_sub(max).max(0);
        // No start past `pos` can match text that ends there.
        let first = i.wrapping_sub(min).min(i);
        (
            i64::from(i) - i64::from(first),
            i64::from(i) - i64::from(from),
        )
    }

    /// The look-behind of [`Behind::Lengths`]: the starts at a char boundary
    /// within `lengths` of `pos`, in UTF-16 units, the nearest first.
    fn behind_within(
        &mut self,
        block: usize,
        pos: usize,
        lengths: Lengths,
        slots: &mut [Option<usize>],
    ) -> Result<bool, SearchError> {
        let (mut start, mut units) = (pos, 0);
        while lengths.max.is_none_or(|max| units <= max) {
            if units >= lengths.min && self.input.is_char_boundary(start) {
                self.spend()?;
                if self.run(block, start, Some(pos), slots)?.is_some() {
                    return Ok(true);
                }
            }
            let Some((_, before)) = unit_before(self.input, start) else {
                break;
            };
            (start, units) = (before, units + 1);
        }
        Ok(false)
    }

    /// The position `units` UTF-16 units before `pos`, or `None` when that
    /// is before the input.
    fn units_back(&self, pos: usize, units: i64) -> Option<usize> {
        let mut at = pos;
        for _ in 0..units {
            at = unit_before(self.input, at)?.1;
        }
        Some(at)
    }

    /// Java's `countChars(seq, i, length)` at `pos`: the UTF-16 units of the
    /// `length` code points after `pos`, or for a negative `length` of the
    /// `-length` code points before it, as far as the input goes.
    fn count_chars(&self, pos: usize, length: i32) -> i32 {
        let input = self.input;
        if length == 1 && unit_at(input, pos).is_some_and(|(unit, _)| !is_high_surrogate(unit)) {
            return 1;
        }
        let (mut at, mut units) = (pos, 0_i32);
        if length >= 0 {
            for _ in 0..length {
                let Some((unit, after)) = unit_at(input, at) else {
                    break;
                };
                (at, units) = (after, units + 1);
                if is_high_surrogate(unit)
                    && let Some((next, after)) = unit_at(input, at)
                    && is_low_surrogate(next)
                {
                    (at, units) = (after, units + 1);
                }
            }
        } else {
            for _ in 0..length.wrapping_neg() {
                let Some((unit, before)) = unit_before(input, at) else {
                    break;
                };
                (at, units) = (before, units + 1);
                if is_low_surrogate(unit)
                    && let Some((previous, before)) = unit_before(input, at)
                    && is_high_surrogate(previous)
                {
                    (at, units) = (before, units + 1);
                }
            }
        }
        units
    }

    /// The UTF-16 index of `pos`, Java's `i`.
    fn utf16_index(&self, pos: usize) -> i32 {
        let (boundary, inside) = if self.input.is_char_boundary(pos) {
            (pos, 0)
        } else {
            (pos - 2, 1)
        };
        let units: usize = self.input[..boundary].chars().map(char::len_utf16).sum();
        i32::try_from(units + inside).unwrap_or(i32::MAX)
    }

    /// After a nested run that started with the stack at `base`, a
    /// look-around, an atomic group or an iteration: drops the frames the
    /// run left, but for the old value of each private slot it changed, and
    /// of `group` if it set that, so that backtracking past it undoes the
    /// change. The other groups it set keep their text, as Java's do; it
    /// matched whether or not a negated look-around then fails.
    ///
    /// The first frame for a slot above `base` holds the value the slot had
    /// before the run, as the frames of a slot that a nested run set were
    /// kept by this when it ended.
    fn keep(&mut self, base: usize, slots: &[Option<usize>], group: Option<usize>) {
        self.keeps += 1;
        let private = 2 * self.program.groups;
        let mut kept = base;
        for index in base..self.stack.len() {
            if let Frame::Undo { slot, value } = self.stack[index]
                && (slot >= private || group.is_some_and(|group| slot / 2 == group))
                && self.kept[slot] != self.keeps
            {
                self.kept[slot] = self.keeps;
                if slots[slot] != value {
                    self.stack[kept] = Frame::Undo { slot, value };
                    kept += 1;
                }
            }
        }
        self.stack.truncate(kept);
    }

    /// A reference at `pos` to the text in `group`, as `BackRef.match` reads
    /// it, or under `fold` as `CIBackRef.match` does: the position after the
    /// text it matches, or `None`.
    ///
    /// Where `pos` or an end of the group is inside a pair, `BackRef`
    /// compares UTF-16 units, as Java does. `CIBackRef` there compares code
    /// points as `codePointAt` reads them, which the machine does not port:
    /// it matches an empty group only.
    fn backref(&self, pos: usize, group: Range<usize>, fold: Option<BackrefFold>) -> Option<usize> {
        let input = self.input;
        if group.is_empty() {
            return Some(pos);
        }
        let whole = [pos, group.start, group.end]
            .into_iter()
            .all(|at| input.is_char_boundary(at));
        match fold {
            None if whole => input[pos..]
                .starts_with(&input[group.clone()])
                .then_some(pos + group.len()),
            None => {
                let (mut at, mut from) = (pos, group.start);
                while from < group.end {
                    let (expected, next_from) = unit_at(input, from)?;
                    let (actual, next_at) = unit_at(input, at)?;
                    if actual != expected {
                        return None;
                    }
                    (at, from) = (next_at, next_from);
                }
                Some(at)
            }
            Some(fold) if whole => folded_backref(input, pos, group, fold),
            Some(_) => None,
        }
    }

    /// What Java reads at `pos`, and the position after it.
    fn next_unit(&self, pos: usize) -> Option<(Unit, usize)> {
        if self.input.is_char_boundary(pos) {
            let c = self.input[pos..].chars().next()?;
            Some((Unit::Char(c), pos + c.len_utf8()))
        } else {
            let (unit, after) = unit_at(self.input, pos)?;
            Some((Unit::LowSurrogate(unit), after))
        }
    }

    fn holds(&self, check: &Check, pos: usize) -> bool {
        // Between the units of a pair no text starts or ends, neither unit is
        // a line terminator, and neither is a word char, as `Bound.check`
        // reads them.
        if !self.input.is_char_boundary(pos) {
            return match check {
                Check::LastMatchEnd => pos == self.last_match_end,
                Check::WordBoundary { negated, .. } => *negated,
                _ => false,
            };
        }
        let (before, after) = self.input.split_at(pos);
        match check {
            Check::StartText => before.is_empty(),
            Check::EndText => after.is_empty(),
            Check::LastMatchEnd => pos == self.last_match_end,
            Check::EndTextBeforeNewlines => after.bytes().all(|byte| byte == b'\n'),
            Check::StartLine => before.is_empty() || before.ends_with('\n'),
            Check::EndLine => after.is_empty() || after.starts_with('\n'),
            Check::WordBoundary { word, negated } => {
                let is_word = |c: Option<char>| c.is_some_and(|c| word.matches(c));
                (is_word(before.chars().next_back()) != is_word(after.chars().next())) != *negated
            }
        }
    }
}

/// The UTF-16 unit at `pos`, and the position after it.
fn unit_at(input: &str, pos: usize) -> Option<(u16, usize)> {
    if input.is_char_boundary(pos) {
        let c = input[pos..].chars().next()?;
        let [first, _] = utf16_units(c);
        let after = if c.len_utf16() == 2 {
            pos + 2
        } else {
            pos + c.len_utf8()
        };
        Some((first, after))
    } else {
        let c = input[pos - 2..].chars().next()?;
        Some((utf16_units(c)[1], pos + 2))
    }
}

/// The UTF-16 unit before `pos`, and the position it starts at.
fn unit_before(input: &str, pos: usize) -> Option<(u16, usize)> {
    if input.is_char_boundary(pos) {
        let c = input[..pos].chars().next_back()?;
        let [first, second] = utf16_units(c);
        Some(if c.len_utf16() == 2 {
            (second, pos - 2)
        } else {
            (first, pos - c.len_utf8())
        })
    } else {
        let c = input[pos - 2..].chars().next()?;
        Some((utf16_units(c)[0], pos - 2))
    }
}

/// The UTF-16 units of `c`, the second 0 for a char of one.
fn utf16_units(c: char) -> [u16; 2] {
    let mut units = [0; 2];
    c.encode_utf16(&mut units);
    units
}

const fn is_high_surrogate(unit: u16) -> bool { matches!(unit, 0xD800..=0xDBFF) }

const fn is_low_surrogate(unit: u16) -> bool { matches!(unit, 0xDC00..=0xDFFF) }

/// Sets a slot, and records its old value on the stack.
fn set(stack: &mut Vec<Frame>, slots: &mut [Option<usize>], slot: usize, value: Option<usize>) {
    stack.push(Frame::Undo {
        slot,
        value: slots[slot],
    });
    slots[slot] = value;
}

/// `CIBackRef.match` at `at`, for the text in `group`: the position after the
/// text it matches, or `None`.
///
/// Java compares the two texts one code point for one, as JDK 21 and later
/// do, the JDK the cp-kafka images run. JDK 17 counted the text in UTF-16
/// units, compared one more pair past both texts for each supplementary char
/// in it, and threw where that pair was past the input. No case mapping
/// takes a supplementary char to one that is not, so the texts that match are
/// as long in UTF-16 units as in code points, and Java's `i + groupSizeChars`
/// is the end of the last pair.
fn folded_backref(input: &str, at: usize, group: Range<usize>, fold: BackrefFold) -> Option<usize> {
    let mut rest = input[at..].chars();
    let mut after = at;
    for c2 in input[group].chars() {
        let c1 = rest.next()?;
        if !fold.chars_match(c2, c1) {
            return None;
        }
        after += c1.len_utf8();
    }
    Some(after)
}

#[cfg(test)]
mod tests {
    use {super::*, crate::next_search_start, assert2::check};

    /// One whole-input match of a pattern in `fancy_regex` syntax, with the
    /// `group_repeats` the rewrite gives it, one per group and group 0 first.
    struct Case<'a> {
        pattern: &'a str,
        input: &'a str,
        group_repeats: &'a [bool],
        expected: bool,
    }

    /// A [`Case`] of `pattern` against `input`.
    const fn row<'a>(
        pattern: &'a str,
        input: &'a str,
        group_repeats: &'a [bool],
        expected: bool,
    ) -> Case<'a> {
        Case {
            pattern,
            input,
            group_repeats,
            expected,
        }
    }

    /// The program of `pattern`, whose `\R`s have the `linebreak_atoms` given.
    fn program(pattern: &str, group_repeats: &[bool], linebreak_atoms: &[bool]) -> Program {
        let compiled = Program::new(
            pattern,
            group_repeats.len(),
            &[],
            linebreak_atoms,
            group_repeats,
            &[],
        );
        compiled.unwrap_or_else(|| panic!("{pattern:?} does not compile"))
    }

    fn check_matches<'a>(cases: impl IntoIterator<Item = Case<'a>>, linebreak_atoms: &[bool]) {
        for Case {
            pattern,
            input,
            group_repeats,
            expected,
        } in cases
        {
            let program = program(pattern, group_repeats, linebreak_atoms);
            check!(
                program.matches(input).ok() == Some(expected),
                "{pattern:?} against {input:?}"
            );
        }
    }

    /// Java's `GroupCurly` repeats a deterministic group: it takes no
    /// iteration past the least count that matches nothing, and gives the
    /// repeated group back the text it had before. `{0,1}` is a `Branch`, and
    /// a group that is not deterministic a `Loop`, which keeps such an
    /// iteration. Each row is `Pattern.matches` on JDK 25.0.4.
    #[test]
    fn a_group_curly_takes_no_empty_iteration_past_its_least_count() {
        const REPEATED: &[bool] = &[false, true];
        const OUTER_REPEATED: &[bool] = &[false, true, false];
        const NONE_REPEATED: &[bool] = &[false, false];
        // (pattern, input, group_repeats, matches)
        let cases = [
            row(r"()*\1", "", REPEATED, false),
            row(r"()+\1", "", REPEATED, true),
            row(r"(){2}\1", "", REPEATED, true),
            row(r"(){0,3}\1", "", REPEATED, false),
            row(r"()*?\1", "", REPEATED, false),
            row(r"(?:())*\1", "", NONE_REPEATED, true),
            row(r"(())*\2", "", OUTER_REPEATED, true),
            row(r"(())*\1", "", OUTER_REPEATED, false),
            row(r"(())*?\2", "", OUTER_REPEATED, false),
            row(r"(())*?\1", "", OUTER_REPEATED, false),
            row(r"(())*?x\2", "x", OUTER_REPEATED, false),
            row(r"(()){0,1}\1", "", OUTER_REPEATED, true),
            row(r"(()){0,2}\1", "", OUTER_REPEATED, false),
            row(r"(()){1,3}\1", "", OUTER_REPEATED, true),
            row(r"((?:)){2,}\1", "", REPEATED, true),
            row(r"x()*\1", "x", REPEATED, false),
            row(r"(\b)*\1", "", REPEATED, false),
            row(r"(a|)*\1", "aa", REPEATED, true),
            row(r"(b*)*\1", "", REPEATED, true),
            row(r"(x?)*\1", "", REPEATED, true),
        ];
        check_matches(cases, &[]);
    }

    /// The groups inside an iteration of a `GroupCurly` keep their text when
    /// the match backtracks out of the repeat, and the repeated group does
    /// not. A possessive quantifier is an atomic group around the repeat, and
    /// keeps them all. A `Loop` keeps none. Each row is `Pattern.matches` on
    /// JDK 25.0.4.
    #[test]
    fn the_groups_inside_a_group_curly_keep_their_text_when_the_match_backtracks() {
        const REPEATED: &[bool] = &[false, true];
        const OUTER_REPEATED: &[bool] = &[false, true, false];
        const NONE_REPEATED: &[bool] = &[false, false];
        const NONE_OF_TWO_REPEATED: &[bool] = &[false, false, false];
        // (pattern, input, group_repeats, matches)
        let cases = [
            row(r"(?:(?:(a))*y|a)\1", "aa", NONE_REPEATED, true),
            row(r"(?:(())*?y|x)\2", "x", OUTER_REPEATED, true),
            row(r"(?:(())*?y|x)\1", "x", OUTER_REPEATED, false),
            row(r"(())*+\1", "", NONE_OF_TWO_REPEATED, true),
            row(r"(?:((a))*y|a)\2", "aa", OUTER_REPEATED, true),
            row(r"(?:(a)*y|a)\1", "aa", REPEATED, false),
            row(r"(?:((a))*?y|a)\2", "aa", OUTER_REPEATED, true),
            row(r"(?:((a))+y|a)\2", "aa", OUTER_REPEATED, true),
            row(r"(?:(?>((a))*)y|a)\2", "aa", OUTER_REPEATED, true),
            row(r"(?:((a))*+y|a)\2", "aa", NONE_OF_TWO_REPEATED, true),
            row(r"(?:((a)|b)*y|a)\2", "aa", OUTER_REPEATED, false),
            row(r"(?:(?:(a){2})*y|aa)\1", "aaa", REPEATED, true),
        ];
        check_matches(cases, &[]);
    }

    /// The atom of a `GroupCurly` backtracks inside itself, so its `\R` gives
    /// back the `\n` of `\r\n`. A quantifier of a `\R` alone is a `Curly`,
    /// which takes the first way, and so is one of `(?:\R)`, whose tree is the
    /// same. Each row is `Pattern.matches` on JDK 25.0.4.
    #[yare::parameterized(
        group = { r"(\R\n)*", "\r\n", &[false, true], false, true },
        non_capturing_group = { r"(?:\R\n)*", "\r\n", &[false], false, true },
        after_a_char = { r"(?:a\R\n)*", "a\r\n", &[false], false, true },
        in_a_counted_group = { r"(?:(?:\R\n){2})*", "\r\n\r\n", &[false], false, true },
        alone = { r"\R*\n", "\r\n", &[false], true, false },
        alone_in_a_group = { r"(?:\R)*\n", "\r\n", &[false], false, false },
    )]
    fn a_linebreak_in_a_group_curly_gives_back_its_newline(
        pattern: &str,
        input: &str,
        group_repeats: &[bool],
        linebreak_atom: bool,
        expected: bool,
    ) {
        check_matches([row(pattern, input, group_repeats, expected)], &[
            linebreak_atom,
        ]);
    }

    /// A `GroupCurly` keeps `fancy_regex` unless a group inside its atom or
    /// an empty iteration of its repeated group shows.
    #[yare::parameterized(
        one_char_group = { r"(a)*", &[false, true], false },
        two_char_group = { r"(ab)+", &[false, true], false },
        restored_group = { r"(?:(a)*y|a)\1", &[false, true], false },
        empty_group = { r"()*", &[false, true], true },
        nested_group = { r"(?:((a))*y|a)\2", &[false, true, false], true },
    )]
    fn a_group_curly_differs_from_fancy_only_where_it_shows(
        pattern: &str,
        group_repeats: &[bool],
        expected: bool,
    ) {
        check!(program(pattern, group_repeats, &[]).differs_from_fancy() == expected);
    }

    /// A look-behind tries only the starts its body can match from, so one
    /// checked at each char of a long input answers in time, where trying
    /// every start before it would run past the backtrack limit. Each row is
    /// `Pattern.matches` on JDK 25.0.4.
    #[test]
    fn a_look_behind_on_a_long_input_answers() {
        let xs = "x".repeat(20_000);
        let after_y = format!("y{xs}");
        // (pattern, input, linebreak_atoms, matches)
        let cases = [
            (r"y(?:(?<!z)x|\R)*", after_y.as_str(), &[false][..], true),
            (r"x*(?<!x)", &xs, &[], false),
            (r"x*(?<=y)", &xs, &[], false),
            (r"x*(?<!y|x{1,3})", &xs, &[], false),
            (r"x*(?<!x{1,3}y)", &xs, &[], true),
            (r"x*(?<=x{2,4})", &xs, &[], true),
        ];
        for (pattern, input, linebreak_atoms, expected) in cases {
            check_matches([row(pattern, input, &[false], expected)], linebreak_atoms);
        }
    }

    /// A char of a look-behind's body may be a supplementary one, two UTF-16
    /// units, so a start that far back is tried. Each row is
    /// `Pattern.matches` on JDK 25.0.4.
    #[yare::parameterized(
        any = { r".(?<=.)x", "😀x", true },
        class = { r".(?<=[^a])x", "😀x", true },
        negated = { r".(?<!.)x", "😀x", false },
        variable_length = { r"a.(?<=a.{0,2})x", "a😀x", true },
    )]
    fn a_look_behind_reaches_back_over_a_supplementary_char(
        pattern: &str,
        input: &str,
        expected: bool,
    ) {
        check_matches([row(pattern, input, &[false], expected)], &[]);
    }

    /// The UTF-16 lengths a look-behind's body can match.
    #[yare::parameterized(
        empty = { "", 0, Some(0) },
        literal = { "ab😀", 4, Some(4) },
        any = { ".", 1, Some(2) },
        class = { "[a-z]", 1, Some(2) },
        linebreak = { r"\R", 1, Some(2) },
        concat = { "a.b", 3, Some(4) },
        alternation = { "ab|c|", 0, Some(2) },
        group = { "(ab)", 2, Some(2) },
        atomic_group = { "(?>ab)", 2, Some(2) },
        counted = { "(?:ab){2,3}", 4, Some(6) },
        unbounded = { "a+", 1, None },
        assertions = { r"^\b(?=abc)(?<!x)a$", 1, Some(1) },
        backref = { r"(a)\1", 1, None },
    )]
    fn the_lengths_of_a_look_behind_bound_its_body(pattern: &str, min: usize, max: Option<usize>) {
        let tree = Expr::parse_tree(pattern).unwrap_or_else(|_| panic!("{pattern:?} parses"));
        check!(Lengths::of(&tree.expr) == Lengths { min, max });
    }

    /// The table of a class answers for each ASCII char as its regex does,
    /// under a case flag too, and the regex answers past ASCII.
    #[yare::parameterized(
        range = { "[a-z0-9._-]" },
        negated = { "[^a-z]" },
        case_insensitive = { "(?i:[a-z])" },
        case_insensitive_kelvin = { "(?i:k)" },
        word = { r"\w" },
        unicode_class = { r"\p{Lu}" },
    )]
    fn a_class_matches_each_char_as_its_regex_does(class: &str) {
        let compiled = Class::new(class).unwrap_or_else(|| panic!("{class:?} compiles"));
        let regex = Regex::new(&format!(r"\A(?:{class})\z"))
            .unwrap_or_else(|_| panic!("{class:?} compiles"));
        for c in ('\0'..='\u{3FF}').chain(['\u{212A}', '\u{1F600}']) {
            check!(
                compiled.matches(c) == regex.is_match(c.encode_utf8(&mut [0; 4])),
                "{class:?} against {c:?}"
            );
        }
    }

    /// Java's `.`, as the rewrite writes it.
    const DOT: &str = r"[^\n\r\x{85}\x{2028}\x{2029}]";

    /// The window of Java's `Behind`, in UTF-16 units.
    const fn units(min: i32, max: i32) -> JavaWindow {
        JavaWindow {
            min,
            max,
            code_points: false,
        }
    }

    /// The window of Java's `BehindS`, in code points.
    const fn code_points(min: i32, max: i32) -> JavaWindow {
        JavaWindow {
            min,
            max,
            code_points: true,
        }
    }

    /// The program of `pattern`, in `fancy_regex` syntax with each `.` in it
    /// Java's, with `groups` groups, group 0 included, and the `windows` of
    /// its look-behinds.
    fn windowed(pattern: &str, groups: usize, windows: &[JavaWindow]) -> Program {
        let pattern = pattern.replace('.', DOT);
        let compiled = Program::new(&pattern, groups, &[], &[], &vec![false; groups], windows);
        compiled.unwrap_or_else(|| panic!("{pattern:?} does not compile"))
    }

    /// That `pattern`, with one group and the look-behind `windows` given,
    /// marks `input` as `expected`, `replaceAll("<$0>")` on JDK 25.0.4 does.
    fn check_marks(pattern: &str, windows: &[JavaWindow], input: &str, expected: &str) {
        check!(
            mark_matches(&windowed(pattern, 1, windows), input) == expected,
            "{pattern:?} against {input:?}"
        );
    }

    /// `Matcher.replaceAll("<$0>")` of `program` on `input`.
    fn mark_matches(program: &Program, input: &str) -> String {
        let (mut out, mut copied, mut from) = (String::new(), 0, 0);
        while from <= input.len() {
            let groups = match program.find(input, from, copied) {
                Ok(Some(groups)) => groups,
                Ok(None) => break,
                Err(error) => panic!("{error}"),
            };
            let Some(whole) = groups[0].clone() else {
                break;
            };
            out.push_str(&input[copied..whole.start]);
            out.push('<');
            out.push_str(&input[whole.clone()]);
            out.push('>');
            copied = whole.end;
            from = next_search_start(input, &whole);
        }
        out.push_str(&input[copied..]);
        out
    }

    /// Java's `Behind` counts a class or `.` as one UTF-16 unit, so its
    /// window leaves out a start before a supplementary char that the body
    /// would match whole, and tries a start between the units of a pair,
    /// where `.` and a negated class match the lone low surrogate and a class
    /// of supplementary chars does not. Each row is `replaceAll("<$0>")` on
    /// JDK 25.0.4, with the window of each look-behind from Java's study.
    #[yare::parameterized(
        dot_after_a_char = { r"a.(?<=a.)x", &[units(2, 2)], "a😀x", "a😀x" },
        two_dots_over_two_pairs = { r".(?<=..)x", &[units(2, 2)], "😀😀x", "😀😀x" },
        two_dots_over_a_pair_and_a_char = { r".(?<=..)x", &[units(2, 2)], "😀bx", "😀<bx>" },
        two_dots_alone = { r"(?<=..)x", &[units(2, 2)], "a😀x", "a😀x" },
        two_dots_alone_over_bmp = { r"(?<=..)x", &[units(2, 2)], "😀bx", "😀b<x>" },
        counted_dots = { r"(?<=.{2})x", &[units(2, 2)], "😀x𝄞x", "😀x𝄞x" },
        dots_with_a_range_of_counts = { r"(?<=.{1,2})x", &[units(1, 2)], "😀x𝄞x", "😀<x>𝄞<x>" },
        symbol_class = { r"(?<=\p{So})x", &[units(1, 1)], "😀x𝄞x", "😀x𝄞x" },
        dot_then_symbol_class = { r".(?<=\p{So})x", &[units(1, 1)], "😀x", "😀x" },
        supplementary_escape = { r".(?<=\x{1F600})x", &[units(1, 1)], "😀😀x", "😀😀x" },
        supplementary_class = { r"(?<=[\x{1F600}])x", &[units(1, 1)], "a😀x", "a😀x" },
        negated_symbol_class = { r"(?<!\p{So})x", &[units(1, 1)], "😀x𝄞x", "😀<x>𝄞<x>" },
        dot_then_negated_symbol_class = { r".(?<!\p{So})x", &[units(1, 1)], "😀😀x", "😀<😀x>" },
        char_then_dot = { r"(?<=a.)x", &[units(2, 2)], "a😀x", "a😀x" },
        negated_char_then_dot = { r"(?<!a.)x", &[units(2, 2)], "a😀x", "a😀<x>" },
        start_then_dot = { r"(?<=^.)x", &[units(1, 1)], "😀x", "😀x" },
        start_then_two_dots = { r"(?<=^..)x", &[units(2, 2)], "😀bx", "😀bx" },
        dot_then_escape = { r"(?<=.\x{1F600})x", &[units(2, 2)], "😀😀x", "😀😀x" },
        two_negated_classes = { r"(?<=[^a][^a])x", &[units(2, 2)], "😀x𝄞x", "😀x𝄞x" },
        two_negated_classes_over_bmp = { r"(?<=[^a][^a])x", &[units(2, 2)], "😀bx", "😀b<x>" },
        dot_inside_a_pair = { r".(?<=.)x", &[units(1, 1)], "😀x𝄞x", "<😀x><𝄞x>" },
        dot_alone_inside_a_pair = { r"(?<=.)x", &[units(1, 1)], "a😀x", "a😀<x>" },
        negated_class_inside_a_pair = { r".(?<=[^a])x", &[units(1, 1)], "a😀x", "a<😀x>" },
        non_word_inside_a_pair = { r"(?<=[^0-9A-Za-z_])x", &[units(1, 1)], "😀x", "😀<x>" },
        non_letter_inside_a_pair = { r"(?<=\P{L})x", &[units(1, 1)], "😀x", "😀<x>" },
        letter_inside_a_pair = { r"(?<=\p{L})x", &[units(1, 1)], "😀x", "😀x" },
        surrogate_range_inside_a_pair = { r"(?<=[\x{D000}-\x{E000}])x", &[units(1, 1)], "😀x", "😀<x>" },
        surrogate_category_inside_a_pair = { r"(?<=\p{Cs})x", &[units(1, 1)], "😀x", "😀<x>" },
        not_surrogate_inside_a_pair = { r"(?<=\P{Cs})x", &[units(1, 1)], "😀x", "😀x" },
        intersection_inside_a_pair = { r"(?<=[[[a]&&[[^b]]]])x", &[units(1, 1)], "😀x", "😀x" },
        negated_intersection_inside_a_pair = { r"(?<=[^[[a]&&[[^b]]]])x", &[units(1, 1)], "😀x", "😀<x>" },
        negated_dot = { r"(?<!.)x", &[units(1, 1)], "😀x", "😀x" },
        dot_then_negated_dot = { r".(?<!.)x", &[units(1, 1)], "😀x", "😀x" },
        greedy_class = { r"(?<=[^x]+)x", &[units(1, i32::MAX)], "😀x𝄞x", "😀<x>𝄞<x>" },
        empty_after_a_dot = { r"(?<=.)", &[units(1, 1)], "😀x𝄞x", "😀<>x<>𝄞<>x<>" },
        two_look_behinds = { r"(?<=.)(?<=.)x", &[units(1, 1), units(1, 1)], "a😀x", "a😀<x>" },
        nested_look_behind_before = { r"(?<=(?<=.).)x", &[units(1, 1), units(1, 1)], "😀x", "😀x" },
        nested_look_behind_after = { r"(?<=.(?<=.))x", &[units(1, 1), units(1, 1)], "😀x", "😀<x>" },
        look_ahead_inside_a_pair = { r"(?<=(?=.).)x", &[units(1, 1)], "😀x", "😀<x>" },
        look_ahead_at_x_inside_a_pair = { r"(?<=(?=x).)x", &[units(1, 1)], "😀x", "😀x" },
        start_of_text_inside_a_pair = { r"(?<=\A.)x", &[units(1, 1)], "😀x", "😀x" },
        end_of_text_inside_a_pair = { r"(?<=\z.)x", &[units(1, 1)], "😀x", "😀x" },
        word_boundary_inside_a_pair = { r"(?<=\b.)x", &[units(1, 1)], "😀x", "😀x" },
        word_boundary_after_a_pair = { r"(?<=\b.)x", &[units(1, 1)], "😀bx", "😀b<x>" },
        not_word_boundary_inside_a_pair = { r"(?<=\B.)x", &[units(1, 1)], "a😀x", "a😀<x>" },
        not_word_boundary_after_a_pair = { r"(?<=\B.)x", &[units(1, 1)], "😀bx", "😀bx" },
    )]
    fn a_look_behind_tries_the_starts_of_javas_window(
        pattern: &str,
        windows: &[JavaWindow],
        input: &str,
        expected: &str,
    ) {
        check_marks(pattern, windows, input, expected);
    }

    /// The rewrite writes Java's `\b`, `\B`, `$` and `^` under `(?m)` with
    /// look-arounds, whose look-behinds read a start inside a pair as
    /// `Bound.check`, `Dollar` and `Caret` do: neither unit is a word char
    /// or a line terminator. Each row is `replaceAll("<$0>")` on JDK 25.0.4.
    #[yare::parameterized(
        word_boundary = {
            r"(?<=(?:(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|(?<![0-9A-Za-z_])(?=[0-9A-Za-z_])).)x",
            "b😀x",
            "b😀x",
        },
        word_boundary_after_a_pair = {
            r"(?<=(?:(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|(?<![0-9A-Za-z_])(?=[0-9A-Za-z_])).)x",
            "😀bx",
            "😀b<x>",
        },
        not_word_boundary = {
            r"(?<=(?:(?<=[0-9A-Za-z_])(?=[0-9A-Za-z_])|(?<![0-9A-Za-z_])(?![0-9A-Za-z_])).)x",
            "😀x𝄞x",
            "😀<x>𝄞<x>",
        },
        end_of_line = {
            r"(?<=(?:(?=(?:\r\n|(?<!\r)\n|[\r\x{85}\x{2028}\x{2029}])?\z)).)x",
            "😀x",
            "😀x",
        },
        start_of_line = {
            r"(?<=(?:(?!\z)(?:\A|(?<=[\n\x{85}\x{2028}\x{2029}])|(?<=\r)(?!\n))).)x",
            "a😀x",
            "a😀x",
        },
    )]
    fn a_rewritten_assertion_inside_a_pair_reads_as_javas(
        pattern: &str,
        input: &str,
        expected: &str,
    ) {
        let look_behinds = pattern.matches("(?<").count();
        let windows = vec![units(1, 1); look_behinds];
        check!(
            mark_matches(&windowed(pattern, 1, &windows), input) == expected,
            "{pattern:?} against {input:?}"
        );
    }

    /// Where the pattern has a supplementary char from a look-behind on,
    /// Java's `BehindS` counts its window back in code points and steps by
    /// them. Each row is `replaceAll("<$0>")` on JDK 25.0.4.
    #[yare::parameterized(
        pair = { r"(?<=😀)x", &[code_points(1, 1)], "😀x𝄞x", "😀<x>𝄞x" },
        pair_before_a_char = { r"(?<=😀)x", &[code_points(1, 1)], "😀bx", "😀bx" },
        dot_then_pair = { r".(?<=😀)x", &[code_points(1, 1)], "😀😀x", "😀<😀x>" },
        char_then_pair = { r"(?<=a😀)x", &[code_points(2, 2)], "a😀x", "a😀<x>" },
        pair_then_dot = { r"(?<=😀.)x", &[code_points(2, 2)], "😀😀x", "😀😀<x>" },
        pair_then_dot_over_bmp = { r"(?<=😀.)x", &[code_points(2, 2)], "😀a😀x", "😀a😀x" },
        dot_then_pair_alone = { r"(?<=.😀)x", &[code_points(2, 2)], "a😀x", "a😀<x>" },
        dot_then_pair_at_the_start = { r"(?<=.😀)x", &[code_points(2, 2)], "😀x", "😀x" },
        negated_class_then_pair = { r"(?<=[^a]😀)x", &[code_points(2, 2)], "😀😀x", "😀😀<x>" },
        negated_class_then_pair_over_a = { r"(?<=[^a]😀)x", &[code_points(2, 2)], "😀a😀x", "😀a😀x" },
        negated_pair = { r"(?<!😀)x", &[code_points(1, 1)], "😀x𝄞x", "😀x𝄞<x>" },
        negated_dot_then_pair = { r"(?<!.😀)x", &[code_points(2, 2)], "😀x", "😀<x>" },
        negated_dot_then_pair_after_a = { r"(?<!.😀)x", &[code_points(2, 2)], "a😀x", "a😀x" },
        two_dots_then_pair = { r"(?<=..😀)x", &[code_points(3, 3)], "😀a😀x", "😀a😀<x>" },
        two_dots_then_pair_over_pairs = { r"(?<=..😀)x", &[code_points(3, 3)], "😀😀x", "😀😀x" },
        counted_dots_then_pair = { r"(?<=.{1,2}😀)x", &[code_points(2, 3)], "😀a😀x", "😀a😀<x>" },
        counted_dots_then_pair_at_the_start = { r"(?<=.{1,2}😀)x", &[code_points(2, 3)], "😀x", "😀x" },
        alternation = { r"(?<=😀|a)x", &[code_points(1, 1)], "😀x𝄞x", "😀<x>𝄞x" },
        two_look_behinds = { r"(?<=😀)(?<=.)x", &[code_points(1, 1), code_points(1, 1)], "a😀x", "a😀<x>" },
    )]
    fn a_look_behind_with_a_supplementary_char_counts_code_points(
        pattern: &str,
        windows: &[JavaWindow],
        input: &str,
        expected: &str,
    ) {
        check_marks(pattern, windows, input, expected);
    }

    /// A group inside a look-behind may start or end between the units of a
    /// pair: byte 2 of a 4-byte char. A reference to it compares UTF-16
    /// units, so an empty one matches and one of a low surrogate does not.
    /// Each row is the first `Matcher.find()` on JDK 25.0.4, its spans in
    /// UTF-16 units taken to bytes, and no spans for no match.
    #[yare::parameterized(
        dot_starts_inside_a_pair = { r"(?<=(.))x", "😀x", &[Some(4..5), Some(2..4)] },
        dot_after_a_pair_and_a_char = { r"(?<=(.))x", "😀x𝄞x", &[Some(4..5), Some(2..4)] },
        empty_group_inside_a_pair = { r"(?<=().)x", "😀x", &[Some(4..5), Some(2..2)] },
        reference_to_an_empty_group = { r"(?<=().)x\1", "😀x", &[Some(4..5), Some(2..2)] },
        reference_to_a_low_surrogate = { r"(?<=(.))x\1", "😀x", &[] },
        group_before_the_match = { r"(?<=(.)).", "😀a😀x", &[Some(4..5), Some(2..4)] },
        group_of_a_char = { r"(?<=(.)).", "a😀x", &[Some(1..5), Some(0..1)] },
    )]
    fn a_look_behind_sets_a_group_inside_a_pair(
        pattern: &str,
        input: &str,
        expected: &[Option<Range<usize>>],
    ) {
        let program = windowed(pattern, 2, &[units(1, 1)]);
        let found = program.find(input, 0, 0).ok().flatten().unwrap_or_default();
        check!(found == expected, "{pattern:?} against {input:?}");
    }

    /// Java's `countChars`: the UTF-16 units of so many code points after a
    /// position, or before it for a negative count, as far as the input
    /// goes. Byte 2 of a pair is between its units.
    #[yare::parameterized(
        back_over_a_pair = { "😀x", 4, -1, 2 },
        back_over_a_pair_and_a_char = { "😀x", 5, -2, 3 },
        back_from_inside_a_pair = { "😀x", 2, -1, 1 },
        back_past_the_start = { "a😀", 5, -5, 3 },
        back_from_the_start = { "😀", 0, -1, 0 },
        back_none = { "😀", 4, 0, 0 },
        forward_over_a_pair = { "😀x", 0, 1, 2 },
        forward_from_inside_a_pair = { "😀x", 2, 1, 1 },
        forward_past_the_end = { "😀x", 0, 5, 3 },
        most_negative = { "😀x", 5, i32::MIN, 0 },
    )]
    fn count_chars_counts_as_javas(input: &str, pos: usize, length: i32, expected: i32) {
        let program = windowed("", 1, &[]);
        check!(Machine::new(&program, input, 0).count_chars(pos, length) == expected);
    }

    /// Java's `int` arithmetic for a window that the study wrapped below 0:
    /// a start past `i` is not tried, and `from` is `max(i - rmax, 0)`.
    #[yare::parameterized(
        rmin_below_zero = { units(-1, 1), "ab", true },
        rmax_below_zero = { units(0, -1), "ab", false },
        rmax_wraps = { units(0, i32::MIN + 1), "ab", true },
    )]
    fn a_wrapped_window_counts_as_javas_int(window: JavaWindow, input: &str, expected: bool) {
        // `a` before the end, or nothing: `(?<=a|)` at the end of `ab`
        // matches only from the start `i`, 0 units back.
        let program = windowed(r"ab(?<=b|)", 1, &[window]);
        check!(program.matches(input).ok() == Some(expected));
    }

    /// The lone low surrogate that Java reads between the units of a pair
    /// against each class as the rewrite writes it, and `fancy_regex` then
    /// hands it on: `(?<=class)x` with the window of `Behind`, against `😀x`,
    /// tries only the start between the units of the `😀`. Each row is
    /// `Pattern.compile(class).matcher("\uDC00").matches()` on JDK 25.0.4,
    /// which `"\uDFFF"` answers the same, for the Java class in the name.
    #[yare::parameterized(
        dot = { ".", true },
        negated_char = { "[^a]", true },
        negated_char_under_a_case_flag = { "[^aA]", true },
        non_word = { "[^0-9A-Za-z_]", true },
        non_space = { r"[^ \t\n\x0B\x0C\r]", true },
        non_digit = { "[^0-9]", true },
        word = { "[0-9A-Za-z_]", false },
        digit = { "[0-9]", false },
        space = { r"[ \t\n\x0B\x0C\r]", false },
        unicode_word = { r"\w", false },
        unicode_non_word = { r"\W", true },
        non_horizontal_space = {
            r"[^\t\x20\xA0\x{1680}\x{180E}\x{2000}-\x{200A}\x{202F}\x{205F}\x{3000}]",
            true,
        },
        horizontal_space = {
            r"[\t\x20\xA0\x{1680}\x{180E}\x{2000}-\x{200A}\x{202F}\x{205F}\x{3000}]",
            false,
        },
        non_vertical_space = { r"[^\n-\r\x{85}\x{2028}\x{2029}]", true },
        vertical_space = { r"[\n-\r\x{85}\x{2028}\x{2029}]", false },
        non_letter = { r"\P{L}", true },
        letter = { r"\p{L}", false },
        other_letter = { r"\p{Lo}", false },
        other_symbol = { r"\p{So}", false },
        surrogate = { r"\p{Cs}", true },
        not_surrogate = { r"\P{Cs}", false },
        other = { r"\p{C}", true },
        not_other = { r"\P{C}", false },
        unassigned = { r"\p{Cn}", false },
        private_use = { r"\p{Co}", false },
        control = { r"\p{IsControl}", false },
        assigned = { r"\p{IsAssigned}", true },
        not_assigned = { r"\P{IsAssigned}", false },
        common_script = { r"\p{IsCommon}", false },
        latin_script = { r"\p{IsLatin}", false },
        alphabetic = { r"\p{IsAlphabetic}", false },
        unicode_alpha = { r"[\p{Alphabetic}]", false },
        white_space = { r"\p{IsWhite_Space}", false },
        punctuation = { r"\p{IsPunctuation}", false },
        emoji = { r"\p{IsEmoji}", false },
        ascii = { r"\p{ASCII}", false },
        not_ascii = { r"\P{ASCII}", true },
        range_across_the_surrogates = { r"[\x{D000}-\x{E000}]", true },
        negated_range_across_the_surrogates = { r"[^\x{D000}-\x{E000}]", false },
        range_past_the_surrogates = { r"[\x{E000}-\x{F000}]", false },
        negated_supplementary = { r"[^\x{1F600}]", true },
        supplementary = { r"[\x{1F600}]", false },
        supplementary_escape = { r"\x{1F600}", false },
        intersection = { r"[[[a]&&[[^b]]]]", false },
        negated_intersection = { r"[^[[a]&&[[^b]]]]", true },
        negated_range_intersection = { r"[^[[a-z]&&[[^m]]]]", true },
        intersection_with_surrogates = { r"[[[\p{L}]&&[\p{Cs}]]]", false },
        surrogates_in_a_class = { r"[[[\P{L}]&&[\p{Cs}]]]", true },
        intersection_of_negations = { r"[[[\P{L}]&&[[^a]]]]", true },
        negated_union = { r"[^\p{L}\p{N}]", true },
        union_with_a_negation = { r"[[^0-9A-Za-z_]0-9]", true },
        upper_under_a_case_flag = { r"[\p{Lu}\p{Ll}\p{Lt}]", false },
    )]
    fn a_class_matches_a_lone_low_surrogate_as_javas(class: &str, expected: bool) {
        let program = windowed(&format!("(?<={class})x"), 1, &[units(1, 1)]);
        let marked = if expected { "😀<x>" } else { "😀x" };
        check!(mark_matches(&program, "😀x") == marked, "{class:?}");
    }

    /// A class whose `fancy_regex` text the `regex` crate refuses is not
    /// compiled, but its text is read by the same rules. Each row is
    /// `Pattern.compile(class).matcher("\uDC00").matches()` on JDK 25.0.4,
    /// for the Java class in the name.
    #[yare::parameterized(
        surrogate_by_category = { r"\p{gc=Cs}", true },
        surrogate_by_long_category = { r"\p{general_category=Cs}", true },
        unknown_script = { r"\p{IsUnknown}", true },
        unknown_script_code = { r"\p{IsZzzz}", true },
        unknown_script_by_property = { r"\p{sc=Zzzz}", true },
        unknown_script_by_long_property = { r"\p{script=Unknown}", true },
    )]
    fn a_class_the_regex_crate_refuses_is_read_for_surrogates(class: &str, expected: bool) {
        let tree = Expr::parse_tree(class).unwrap_or_else(|_| panic!("{class:?} parses"));
        let Expr::Delegate { inner, .. } = tree.expr else {
            panic!("{class:?} is not a class");
        };
        check!(Class::new(&inner).is_none(), "{inner:?}");
        check!(class_has_surrogates(&inner) == expected, "{inner:?}");
    }

    /// The parts of a class that the rewrite does not write are read by the
    /// same rules, and a text that is not one class matches no surrogate.
    #[yare::parameterized(
        group_with_a_flag = { r"(?i:[^a])", true },
        flag_before = { r"(?i)[^a]", true },
        perl_digit = { r"\d", false },
        perl_non_space = { r"\S", true },
        one_letter = { r"\pC", true },
        not_equal = { r"\p{gc!=Cs}", false },
        negated_not_equal = { r"\P{gc!=Cs}", true },
        script_extensions = { r"\p{scx=Zzzz}", true },
        fancy_not_surrogate = { r"\p{any}", false },
        fancy_surrogate = { r"\P{any}", true },
        difference = { r"[\p{Cs}--a]", true },
        difference_of_all = { r"[\p{Cs}--\p{C}]", false },
        symmetric_difference = { r"[\p{Cs}~~a]", true },
        symmetric_difference_of_both = { r"[\p{Cs}~~\p{C}]", false },
        ascii_class = { r"[[:alpha:]]", false },
        negated_ascii_class = { r"[[:^alpha:]]", true },
        two_classes = { "[^a][^b]", false },
        alternation = { "[^a]|b", false },
        unparsed = { "[^a", false },
    )]
    fn a_class_text_is_read_for_surrogates_by_its_parts(class: &str, expected: bool) {
        check!(class_has_surrogates(class) == expected, "{class:?}");
    }
}
