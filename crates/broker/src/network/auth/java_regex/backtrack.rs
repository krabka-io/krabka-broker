//! A backtracking matcher for a pattern that has a backreference under a case
//! flag, which compares the text of the group as Java does.
//!
//! The rewrite writes such a reference as `(?i:\1)`, and `fancy_regex`
//! compares the text it refers to by its own fold, which is not Java's
//! `CIBackRef` (`java_fold` in `client_metrics::config` lists where they
//! differ). Nothing in `fancy_regex` can be told to compare another way. So
//! [`Program`] takes the tree that `fancy_regex` parses from the rewritten
//! pattern and runs it on a backtracking machine of its own. Every node but
//! the reference matches as `fancy_regex` matches it, and the reference
//! compares as JDK 17's `CIBackRef.match` does, by
//! [`BackrefFold::chars_match`]. A tree with a node the machine does not
//! know is not compiled, and the pattern keeps `fancy_regex`'s answer. The
//! rewrite of a pattern that `Pattern.compile` takes writes none.
//!
//! The machine keeps its alternatives on a stack of its own, so a long input
//! cannot overflow the stack of the thread, and it gives up after as many
//! backtracking steps as `fancy_regex` takes by default.

use {
    crate::client_metrics::config::BackrefFold,
    fancy_regex::{Assertion, Expr, LookAround},
    regex::Regex,
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
    /// JDK 17's `CIBackRef` compares one more pair of chars, past both texts,
    /// for each supplementary char in the text it refers to, and throws
    /// `StringIndexOutOfBoundsException` when that pair is past the end of
    /// the input.
    #[error(
        "a case-insensitive backreference read past the end of the input, where JDK 17 throws \
         StringIndexOutOfBoundsException"
    )]
    PastEnd,
}

/// A pattern compiled for the machine.
#[derive(Debug)]
pub(super) struct Program {
    /// The instructions of the pattern, then those of each look-around and
    /// atomic group. Each block ends in [`Inst::Match`].
    blocks: Vec<Vec<Inst>>,
    /// Two slots for each group, group 0 first, then two for each repeat.
    slot_count: usize,
    /// The groups, group 0 included.
    groups: usize,
}

/// One instruction of the machine. A jump target is an index into the block
/// the instruction is in.
#[derive(Debug)]
enum Inst {
    /// This char.
    Char(char),
    /// One char that this regex, compiled as `\A(?:class)\z`, matches.
    Class(Regex),
    /// One char, other than `\n` unless `newline`.
    Any {
        newline: bool,
    },
    /// `\R`: `\r\n`, or else one line terminator, with no backtracking from
    /// `\r\n` to `\r`. Under Unicode, U+0085, U+2028 and U+2029 are
    /// terminators too.
    Linebreak {
        unicode: bool,
    },
    /// Go on at `first`, and at `second` when that fails.
    Split {
        first: usize,
        second: usize,
    },
    Jump(usize),
    /// Record the position in the slot.
    Save(usize),
    /// The text that `group` matched: as it is, or under `fold`.
    Backref {
        group: usize,
        fold: Option<BackrefFold>,
    },
    Assert(Check),
    /// The block matches here, or with `behind` it matches text that ends
    /// here, or with `negated` it does not.
    Look {
        block: usize,
        behind: bool,
        negated: bool,
    },
    /// The first way the block matches here, which nothing backtracks into.
    Atomic(usize),
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
    /// The end of an iteration that started at the position in `start`. An
    /// iteration that matched nothing leaves the repeat, as in Java's `Loop`.
    RepeatEnd {
        count: usize,
        start: usize,
        head: usize,
        exit: usize,
    },
    Match,
}

/// A zero-width test, with the meaning `fancy_regex` gives it.
#[derive(Debug)]
enum Check {
    StartText,
    EndText,
    /// `\G`: where the search started.
    SearchStart,
    /// `\Z`: the end, or only `\n`s after here.
    EndTextBeforeNewlines,
    /// Under `(?m)`: the start, or after `\n`.
    StartLine,
    /// Under `(?m)`: the end, or before `\n`.
    EndLine,
    /// `\b`, or `\B` when `negated`, under `(?U)`: between a Unicode word
    /// char, which `word` matches, and one that is not.
    WordBoundary {
        word: Regex,
        negated: bool,
    },
}

impl Program {
    /// Compiles the pattern that `translated` is, in `fancy_regex` syntax, with
    /// `groups` groups, group 0 included, and `folds` the fold of each of its
    /// backreferences under a case flag in order. `None` for a pattern the
    /// machine does not know a node of.
    pub(super) fn new(translated: &str, groups: usize, folds: &[BackrefFold]) -> Option<Self> {
        let tree = Expr::parse_tree(translated).ok()?;
        let mut compiler = Compiler {
            blocks: vec![Vec::new()],
            slot_count: 2 * groups,
            next_group: 1,
            folds: folds.iter(),
        };
        let mut main = Vec::new();
        compiler.compile(&tree.expr, &mut main)?;
        main.push(Inst::Match);
        compiler.blocks[0] = main;
        // Every group and every fold the rewrite wrote is in the tree.
        if compiler.next_group != groups || compiler.folds.next().is_some() {
            return None;
        }
        Some(Self {
            blocks: compiler.blocks,
            slot_count: compiler.slot_count,
            groups,
        })
    }

    /// `Matcher.matches()`: whether some way of matching from the start spans
    /// the whole input.
    pub(super) fn matches(&self, input: &str) -> Result<bool, SearchError> {
        let mut slots = vec![None; self.slot_count];
        let mut machine = Machine::new(self, input, 0);
        Ok(machine.run(0, 0, Some(input.len()), &mut slots)?.is_some())
    }

    /// `Matcher.find(from)`: the first match that starts at `from` or after
    /// it, as the span of each group, group 0 the whole match.
    pub(super) fn find(
        &self,
        input: &str,
        from: usize,
    ) -> Result<Option<Vec<Option<Range<usize>>>>, SearchError> {
        let mut machine = Machine::new(self, input, from);
        let starts = input[from..]
            .char_indices()
            .map(|(offset, _)| from + offset)
            .chain([input.len()]);
        for start in starts {
            let mut slots = vec![None; self.slot_count];
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
}

impl Compiler<'_> {
    /// A new slot, after those of the groups.
    fn slot(&mut self) -> usize {
        self.slot_count += 1;
        self.slot_count - 1
    }

    /// Compiles `expr` into a block of its own, and gives its index.
    fn block(&mut self, expr: &Expr) -> Option<usize> {
        let mut code = Vec::new();
        self.compile(expr, &mut code)?;
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
            Expr::GeneralNewline { unicode } => code.push(Inst::Linebreak { unicode: *unicode }),
            Expr::ContinueFromPreviousMatchEnd => code.push(Inst::Assert(Check::SearchStart)),
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
                code.push(Inst::Save(2 * group));
                self.compile(child, code)?;
                code.push(Inst::Save(2 * group + 1));
            }
            Expr::LookAround(child, kind) => {
                let block = self.block(child)?;
                let (behind, negated) = match kind {
                    LookAround::LookAhead => (false, false),
                    LookAround::LookAheadNeg => (false, true),
                    LookAround::LookBehind => (true, false),
                    LookAround::LookBehindNeg => (true, true),
                };
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
                code.push(Inst::Class(Regex::new(&format!("\\A(?:{inner})\\z")).ok()?));
            }
            Expr::Backref { group, casei } => {
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
                let block = self.block(child)?;
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
        self.compile(child, code)?;
        let exit = code.len() + 1;
        code.push(Inst::RepeatEnd {
            count,
            start,
            head,
            exit,
        });
        if let Inst::Repeat { exit: target, .. } = &mut code[head] {
            *target = exit;
        }
        Some(())
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
            word: Regex::new(r"\A\w\z").ok()?,
            negated: matches!(assertion, Assertion::NotWordBoundary),
        },
        _ => return None,
    })
}

/// An entry of the machine's backtracking stack.
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
    /// Where the search started, which `\G` matches.
    search_start: usize,
    /// The backtracking steps left.
    budget: usize,
}

impl<'a> Machine<'a> {
    fn new(program: &'a Program, input: &'a str, search_start: usize) -> Self {
        Self {
            program,
            input,
            search_start,
            budget: BACKTRACK_LIMIT,
        }
    }

    /// Runs `block` at `at`, and gives the position the first way of matching
    /// ends at, with the slots it set. With `end`, a way of matching that ends
    /// elsewhere is not one. On no match the slots are as they were.
    fn run(
        &mut self,
        block: usize,
        at: usize,
        end: Option<usize>,
        slots: &mut [Option<usize>],
    ) -> Result<Option<usize>, SearchError> {
        let program = self.program;
        let code = &program.blocks[block];
        let mut stack = Vec::new();
        let (mut pc, mut pos) = (0, at);
        loop {
            let next = match &code[pc] {
                Inst::Char(expected) => self
                    .next_char(pos)
                    .filter(|actual| actual == expected)
                    .map(|actual| (pc + 1, pos + actual.len_utf8())),
                Inst::Class(class) => self
                    .next_char(pos)
                    .filter(|actual| class.is_match(actual.encode_utf8(&mut [0; 4])))
                    .map(|actual| (pc + 1, pos + actual.len_utf8())),
                Inst::Any { newline } => self
                    .next_char(pos)
                    .filter(|&actual| *newline || actual != '\n')
                    .map(|actual| (pc + 1, pos + actual.len_utf8())),
                &Inst::Linebreak { unicode } => {
                    let terminator = |c: char| {
                        matches!(c, '\n' | '\x0B' | '\x0C' | '\r')
                            || unicode && matches!(c, '\u{85}' | '\u{2028}' | '\u{2029}')
                    };
                    if self.input[pos..].starts_with("\r\n") {
                        Some((pc + 1, pos + 2))
                    } else {
                        self.next_char(pos)
                            .filter(|&actual| terminator(actual))
                            .map(|actual| (pc + 1, pos + actual.len_utf8()))
                    }
                }
                &Inst::Split { first, second } => {
                    stack.push(Frame::Alternative { pc: second, pos });
                    Some((first, pos))
                }
                &Inst::Jump(target) => Some((target, pos)),
                &Inst::Save(slot) => {
                    set(&mut stack, slots, slot, Some(pos));
                    Some((pc + 1, pos))
                }
                &Inst::Backref { group, fold } => {
                    match (slots[2 * group].zip(slots[2 * group + 1]), fold) {
                        (None, _) => None,
                        (Some((start, stop)), None) => self.input[pos..]
                            .starts_with(&self.input[start..stop])
                            .then_some((pc + 1, pos + stop - start)),
                        (Some((start, stop)), Some(fold)) => {
                            folded_backref(self.input, pos, start..stop, fold)?
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
                    let before = slots.to_vec();
                    let found = if behind {
                        self.behind(block, pos, slots)?
                    } else {
                        self.run(block, pos, None, slots)?.is_some()
                    };
                    if negated {
                        slots.copy_from_slice(&before);
                    } else {
                        keep(&mut stack, slots, &before);
                    }
                    (found != negated).then_some((pc + 1, pos))
                }
                &Inst::Atomic(block) => {
                    let before = slots.to_vec();
                    let after = self.run(block, pos, None, slots)?;
                    keep(&mut stack, slots, &before);
                    after.map(|after| (pc + 1, after))
                }
                &Inst::RepeatStart { count } => {
                    set(&mut stack, slots, count, Some(0));
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
                        stack.push(Frame::Alternative { pc: exit, pos });
                        Some((pc + 1, pos))
                    } else {
                        stack.push(Frame::Alternative { pc: pc + 1, pos });
                        Some((exit, pos))
                    }
                }
                &Inst::RepeatEnd {
                    count,
                    start,
                    head,
                    exit,
                } => {
                    let done = slots[count].unwrap_or(0) + 1;
                    set(&mut stack, slots, count, Some(done));
                    let target = if slots[start] == Some(pos) {
                        exit
                    } else {
                        head
                    };
                    Some((target, pos))
                }
                Inst::Match if end.is_none_or(|end| end == pos) => return Ok(Some(pos)),
                Inst::Match => None,
            };
            (pc, pos) = match next {
                Some(next) => next,
                None => match self.backtrack(&mut stack, slots)? {
                    Some(next) => next,
                    None => return Ok(None),
                },
            };
        }
    }

    /// Pops the stack, undoing the slots, to the next alternative, or `None`
    /// when there is none left.
    fn backtrack(
        &mut self,
        stack: &mut Vec<Frame>,
        slots: &mut [Option<usize>],
    ) -> Result<Option<(usize, usize)>, SearchError> {
        while let Some(frame) = stack.pop() {
            match frame {
                Frame::Undo { slot, value } => slots[slot] = value,
                Frame::Alternative { pc, pos } => {
                    self.spend()?;
                    return Ok(Some((pc, pos)));
                }
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
    /// the nearest start first, as Java's `Behind` does.
    fn behind(
        &mut self,
        block: usize,
        pos: usize,
        slots: &mut [Option<usize>],
    ) -> Result<bool, SearchError> {
        let starts = [pos].into_iter().chain(
            self.input[..pos]
                .char_indices()
                .rev()
                .map(|(start, _)| start),
        );
        for start in starts {
            self.spend()?;
            if self.run(block, start, Some(pos), slots)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn next_char(&self, pos: usize) -> Option<char> { self.input[pos..].chars().next() }

    fn holds(&self, check: &Check, pos: usize) -> bool {
        let (before, after) = self.input.split_at(pos);
        match check {
            Check::StartText => before.is_empty(),
            Check::EndText => after.is_empty(),
            Check::SearchStart => pos == self.search_start,
            Check::EndTextBeforeNewlines => after.bytes().all(|byte| byte == b'\n'),
            Check::StartLine => before.is_empty() || before.ends_with('\n'),
            Check::EndLine => after.is_empty() || after.starts_with('\n'),
            Check::WordBoundary { word, negated } => {
                let is_word =
                    |c: Option<char>| c.is_some_and(|c| word.is_match(c.encode_utf8(&mut [0; 4])));
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

/// Records on the stack the old value of each slot that a nested run changed,
/// so that backtracking past it undoes the change.
fn keep(stack: &mut Vec<Frame>, slots: &[Option<usize>], before: &[Option<usize>]) {
    for (slot, (&now, &value)) in slots.iter().zip(before).enumerate() {
        if now != value {
            stack.push(Frame::Undo { slot, value });
        }
    }
}

/// JDK 17's `CIBackRef.match` at `at`, for the text in `group`: the position
/// after the text it matches, or `None`.
///
/// Java counts the text in UTF-16 units. It needs that many units of input
/// left, and goes on after that many. It compares one code point of each side
/// for each unit, so for each supplementary char in the group it compares one
/// more pair, past both texts, and throws when that pair is past the input.
/// JDK 21 counts the code points instead. This follows JDK 17, as the rest of
/// the rewrite does.
fn folded_backref(
    input: &str,
    at: usize,
    group: Range<usize>,
    fold: BackrefFold,
) -> Result<Option<usize>, SearchError> {
    let units: usize = input[group.clone()].chars().map(char::len_utf16).sum();
    // `i + groupSize > matcher.to`: not enough input left. Where the units end
    // inside a surrogate pair, the code points of the two sides fall out of
    // step before that, one supplementary where the other is not, and no fold
    // takes one for the other, so the comparison below fails first.
    let mut left = units;
    let mut after = at;
    for c in input[at..].chars() {
        if left == 0 {
            break;
        }
        left = left.saturating_sub(c.len_utf16());
        after += c.len_utf8();
    }
    if left > 0 {
        return Ok(None);
    }
    let (mut x, mut j) = (at, group.start);
    for _ in 0..units {
        let (Some(c1), Some(c2)) = (input[x..].chars().next(), input[j..].chars().next()) else {
            return Err(SearchError::PastEnd);
        };
        if !fold.chars_match(c2, c1) {
            return Ok(None);
        }
        x += c1.len_utf8();
        j += c2.len_utf8();
    }
    Ok(Some(after))
}
