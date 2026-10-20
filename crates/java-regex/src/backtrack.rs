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
//! above the frames of the block they are in, so none of them allocates. A
//! look-behind tries only the starts that its body can match from, as the
//! window of Java's `Behind` does, so one that fails costs as much as the
//! longest text its body matches, not the text before it.

use {
    crate::{look_behind::JavaWindow, translate::BackrefFold},
    fancy_regex::{Assertion, Expr, LookAround},
    regex::Regex,
    std::{iter, ops::Range, slice::Iter},
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
    /// here and starts within the lengths its body can match, or with
    /// `negated` it does not.
    Look {
        block: usize,
        behind: Option<Lengths>,
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

/// One char that a regex, compiled as `\A(?:class)\z`, matches.
///
/// The answer for each ASCII char is worked out when the class is compiled,
/// by the same regex, so that one under a case flag reads as it does. Only a
/// char past ASCII runs the regex.
#[derive(Debug)]
struct Class {
    /// Bit `c` is whether the regex matches the ASCII char `c`.
    ascii: u128,
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
        Some(Self { ascii, regex })
    }

    fn matches(&self, c: char) -> bool {
        if c.is_ascii() {
            self.ascii >> u32::from(c) & 1 == 1
        } else {
            self.regex.is_match(c.encode_utf8(&mut [0; 4]))
        }
    }
}

/// The least and the most UTF-16 units that a look-behind's body can match,
/// with `max` `None` when there is no most. Java's `Behind` tries the starts
/// between its `rmin` and `rmax`. These are bounds of what the body matches
/// on the machine instead, so no start outside them can match.
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
    /// whether a quantifier binds to each of its `\R`s alone, in order. `None`
    /// for a pattern the machine does not know a node of.
    pub(super) fn new(
        translated: &str,
        groups: usize,
        folds: &[BackrefFold],
        linebreak_atoms: &[bool],
        group_repeats: &[bool],
        windows: &[JavaWindow],
    ) -> Option<Self> {
        let _ = windows;
        let tree = Expr::parse_tree(translated).ok()?;
        let mut compiler = Compiler {
            blocks: vec![Vec::new()],
            slot_count: 2 * groups,
            next_group: 1,
            folds: folds.iter(),
            linebreak_atoms: linebreak_atoms.iter(),
            group_repeats,
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
                let block = self.nested_block(child)?;
                let (behind, negated) = match kind {
                    LookAround::LookAhead => (false, false),
                    LookAround::LookAheadNeg => (false, true),
                    LookAround::LookBehind => (true, false),
                    LookAround::LookBehindNeg => (true, true),
                };
                let behind = behind.then(|| Lengths::of(child));
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
                    .next_char(pos)
                    .filter(|actual| actual == expected)
                    .map(|actual| (pc + 1, pos + actual.len_utf8())),
                Inst::Class(class) => self
                    .next_char(pos)
                    .filter(|&actual| class.matches(actual))
                    .map(|actual| (pc + 1, pos + actual.len_utf8())),
                Inst::Any { newline } => self
                    .next_char(pos)
                    .filter(|&actual| *newline || actual != '\n')
                    .map(|actual| (pc + 1, pos + actual.len_utf8())),
                &Inst::Linebreak { give_back } => {
                    if self.input[pos..].starts_with("\r\n") {
                        if give_back {
                            self.stack.push(Frame::Alternative {
                                pc: pc + 1,
                                pos: pos + 1,
                            });
                        }
                        Some((pc + 1, pos + 2))
                    } else {
                        self.next_char(pos)
                            .filter(|actual| {
                                matches!(
                                    actual,
                                    '\n' | '\x0B'
                                        | '\x0C'
                                        | '\r'
                                        | '\u{85}'
                                        | '\u{2028}'
                                        | '\u{2029}'
                                )
                            })
                            .map(|actual| (pc + 1, pos + actual.len_utf8()))
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
                &Inst::Backref { group, fold } => {
                    match (slots[2 * group].zip(slots[2 * group + 1]), fold) {
                        (None, _) => None,
                        (Some((start, stop)), None) => self.input[pos..]
                            .starts_with(&self.input[start..stop])
                            .then_some((pc + 1, pos + stop - start)),
                        (Some((start, stop)), Some(fold)) => {
                            folded_backref(self.input, pos, start..stop, fold)
                                .map(|after| (pc + 1, after))
                        }
                    }
                }
                Inst::Assert(check) => self.holds(check, pos).then_some((pc + 1, pos)),
                &Inst::Look {
                    block,
                    behind,
                    negated,
                } => {
                    let nested = self.stack.len();
                    let found = match behind {
                        Some(lengths) => self.behind(block, pos, lengths, slots)?,
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
    /// the nearest start first, as Java's `Behind` does. Only a start that is
    /// within `lengths` of `pos`, in UTF-16 units, can match.
    fn behind(
        &mut self,
        block: usize,
        pos: usize,
        lengths: Lengths,
        slots: &mut [Option<usize>],
    ) -> Result<bool, SearchError> {
        let before = self.input[..pos]
            .char_indices()
            .rev()
            .scan(0, |units, (start, c)| {
                *units += c.len_utf16();
                Some((start, *units))
            });
        let starts = iter::once((pos, 0))
            .chain(before)
            .skip_while(|&(_, units)| units < lengths.min)
            .take_while(|&(_, units)| lengths.max.is_none_or(|max| units <= max));
        for (start, _) in starts {
            self.spend()?;
            if self.run(block, start, Some(pos), slots)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
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

    fn next_char(&self, pos: usize) -> Option<char> { self.input[pos..].chars().next() }

    fn holds(&self, check: &Check, pos: usize) -> bool {
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
    use {super::*, assert2::check};

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
}
