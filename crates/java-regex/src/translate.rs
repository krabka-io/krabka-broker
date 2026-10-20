//! The rewrite of a `java.util.regex` pattern into `fancy_regex` syntax that
//! reads as Java reads it, [`translate_java`].

mod java_case;
mod java_fold;
mod java_property;

pub(crate) use self::java_fold::BackrefFold;
use self::java_fold::{Consumed, Escape};

/// A pattern that [`translate_java`] rewrote.
#[derive(Debug)]
pub(crate) struct JavaTranslation {
    /// The pattern in `fancy_regex` syntax.
    pub(crate) pattern: String,
    /// How each backreference that `pattern` writes as `(?i:\1)` compares in
    /// Java, in the order they appear in it.
    pub(crate) backref_folds: Vec<BackrefFold>,
    /// For each `\R` that `pattern` writes, in order, whether a quantifier
    /// follows it in the Java pattern. Java binds that quantifier to the `\R`
    /// alone, and the one after `(?:\R)` to the group, which it matches
    /// another way. `fancy_regex` parses the two the same.
    pub(crate) linebreak_atoms: Vec<bool>,
    /// For each capturing group by number, group 0 first, whether a `*`, `+`
    /// or counted quantifier that is not possessive follows its `)` in the
    /// Java pattern. Java repeats such a group with a `GroupCurly` that sets
    /// the group itself. `(?:(x))*` has the tree of `(x)*` in `fancy_regex`,
    /// and Java repeats it with a `GroupCurly` that does not.
    pub(crate) group_repeats: Vec<bool>,
    /// For each look-behind that `pattern` writes, in order, whether Java
    /// counts its window in code points (`BehindS`), as it does where the
    /// Java pattern has a supplementary char from the look-behind's `(` to its
    /// end, or in UTF-16 units (`Behind`). A look-behind the rewrite writes
    /// for `^`, `$` or `\b` counts in units.
    pub(crate) look_behind_code_points: Vec<bool>,
}

/// Rewrite a `java.util.regex.Pattern` into `fancy_regex` syntax, or `None`
/// for a group opener Java refuses, a `)` that closes no group, a class range
/// Java refuses (`[a-\w]`), a `{` outside a class that does not open the
/// bounds of a quantifier (`a{x}`), a `*`, `+` or `?` that repeats nothing
/// (`(?i)*`), or a `\N{name}`, which the rewrite cannot read.
///
/// `fancy_regex` accepts Python and Oniguruma forms Java refuses, such as
/// `(?P<name>x)`, `(?P=name)`, `(?'name'x)` and `(?~x)`, so every `(?` outside
/// a character class must be one of Java's openers: `(?:`, `(?=`, `(?!`,
/// `(?>`, `(?<=`, `(?<!`, `(?<name>` with an ASCII-letter-then-alphanumeric
/// name, or inline flags from `idmsuxU-`. Java's `\Q...\E` quoting, which
/// `fancy_regex` lacks, becomes escaped literals before anything else is read,
/// as Java does it. Bounds where Java has no atom to repeat, at the start of
/// a sequence or after a quantifier, as in `{2}`, `x|{2}` and `a*{2}`, repeat
/// the empty atom, which matches the empty string. `fancy_regex` reads them as
/// text, or as a quantifier of the quantified atom before, so the rewrite
/// drops them, with the `?` or `+` that makes them lazy or possessive.
///
/// Eight things read differently by default, and the rewrite gives the Java
/// reading, following the flags in force at each place (`(?s)`, `(?d)`,
/// `(?m)`, `(?U)`, `(?i)`, `(?u)` and `(?x)`, alone or scoped to a group):
///
/// - `\w`, `\d`, `\s`, `\b` and their negations are ASCII, where
///   `fancy_regex`'s are Unicode, until `(?U)` (`UNICODE_CHARACTER_CLASS`)
///   asks for the Unicode ones.
/// - The POSIX classes `\p{Alpha}`, `\p{Alnum}`, `\p{Punct}` and the rest are
///   ASCII too, where `fancy_regex`'s are Unicode, and under `(?U)` they are
///   the Unicode properties that Java gives them.
/// - `.` does not match `\n`, `\r`, U+0085, U+2028 or U+2029, where
///   `fancy_regex`'s stops at `\n` only, until `(?s)` (`DOTALL`) lets it match
///   any, or `(?d)` (`UNIX_LINES`) leaves `\n` the only line terminator.
/// - `$` and `\Z` also match before a line terminator that ends the input,
///   and under `(?m)` (`MULTILINE`) `$` and `^` match at every line
///   terminator, where `fancy_regex`'s know `\n` only and its `\Z` skips any
///   number of them. The rewrite writes them as look-arounds ([`dollar`] and
///   [`caret`]).
/// - `\h`, `\v` and their negations are Java's horizontal and vertical
///   whitespace, where `fancy_regex` reads other chars ([`java_space`]).
/// - `(?i)` (`CASE_INSENSITIVE`) folds ASCII case only, where `fancy_regex`'s
///   `i` folds Unicode case, until `(?u)` (`UNICODE_CASE`) or `(?U)` asks for
///   Unicode folding, and Java's Unicode folding is not `fancy_regex`'s. The
///   rewrite never turns the fancy `i` on. It writes the folding out: both
///   cases of each ASCII letter under an ASCII fold, and under a Unicode fold
///   the code points that Java accepts for a letter, a class member and a class
///   range, in a class and out of one ([`java_case`]). A backreference is
///   written `(?i:\1)`, which `fancy_regex` compares differently from Java
///   for some letters ([`java_fold`]), so the translation also gives the
///   [`BackrefFold`] of each one, for a matcher that compares as Java does.
/// - Under `(?x)` (`COMMENTS`) the rewrite drops the whitespace and the `#`
///   comments Java ignores, so that the text of a comment is never read as
///   pattern.
/// - A class with `&&` is the set Java's `Pattern.clazz` builds, where
///   `fancy_regex` reads an empty side of `&&` as the empty set. An empty
///   right side takes the operand before the `&&`, an empty left side
///   leaves the right one, `&&` binds to the right, and the single chars
///   below U+0100 of one class are one set, which a char after the
///   intersection still joins ([`ClassMember`]). So `[a-z&&]` is `[a-z]`,
///   `[&&a]` is `[a]`, and `[a&&&b]` is `[a&b]`.
pub(crate) fn translate_java(pattern: &str) -> Option<JavaTranslation> {
    let chars = java_fold::remove_qe_quoting(&pattern.chars().collect::<Vec<char>>());
    let mut translator = Translator {
        chars: &chars,
        out: String::with_capacity(pattern.len()),
        scope: Scope::default(),
        outer: Vec::new(),
        groups: Vec::new(),
        classes: Vec::new(),
        class_start: false,
        run: 0,
        prior_run: 0,
        at: 0,
        backref_folds: Vec::new(),
        linebreak_atoms: Vec::new(),
        group_names: Vec::new(),
        group_repeats: vec![false],
        quantified: Quantified::Start,
    };
    while translator.at < chars.len() {
        translator.step()?;
    }
    Some(JavaTranslation {
        pattern: translator.out,
        backref_folds: translator.backref_folds,
        linebreak_atoms: translator.linebreak_atoms,
        group_repeats: translator.group_repeats,
        look_behind_code_points: Vec::new(),
    })
}

/// The state of one [`translate_java`] pass over a pattern.
struct Translator<'a> {
    chars: &'a [char],
    out: String,
    /// The Java flags in force here.
    scope: Scope,
    /// The flags of the groups around here, the innermost last.
    outer: Vec<Scope>,
    /// The number of each group in `outer`, or `None` for one that does not
    /// capture.
    groups: Vec<Option<usize>>,
    /// The character classes open here, the innermost last.
    classes: Vec<OpenClass>,
    /// The next char is the first in a class, where a `]` is a member.
    class_start: bool,
    /// How many literal chars, counting the one just read, Java has read as
    /// one run since the last token that is not one.
    run: usize,
    /// What `run` was before the token now being read.
    prior_run: usize,
    /// The index of the next char to read.
    at: usize,
    /// The fold of each backreference under a case flag written so far.
    backref_folds: Vec<BackrefFold>,
    /// See [`JavaTranslation::linebreak_atoms`].
    linebreak_atoms: Vec<bool>,
    /// The names of the named groups opened so far.
    group_names: Vec<String>,
    /// See [`JavaTranslation::group_repeats`].
    group_repeats: Vec<bool>,
    /// What the token before this one leaves a quantifier here.
    quantified: Quantified,
}

/// What the token before leaves a quantifier. Java's `closure` reads one
/// quantifier after an atom and then one `?` or `+` that makes it lazy or
/// possessive, and a `*`, `+` or `?` after that is `Dangling meta character`.
/// A `{` after it, or where a sequence starts, goes to Java's `atom`, which
/// reads no char before it, and `closure` repeats that empty atom.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Quantified {
    /// The token before was an atom, or a token in a class.
    No,
    /// A quantifier, which a `?` or `+` may still make lazy or possessive.
    Open,
    /// A quantifier and its `?` or `+`.
    Closed,
    /// No atom precedes: the start of the pattern, a group opener, a `|`,
    /// inline flags, or a repeat of the empty atom and its `?` or `+`.
    Start,
    /// A repeat of the empty atom, which a `?` or `+` may still make lazy or
    /// possessive.
    Empty,
}

impl Translator<'_> {
    fn in_class(&self) -> bool { !self.classes.is_empty() }

    /// Copies `len` chars as they are.
    fn copy(&mut self, len: usize) {
        self.out.extend(&self.chars[self.at..self.at + len]);
        self.at += len;
    }

    /// Reads and writes the next token: a char, an escape, a group opener, or a
    /// run of `(?x)` whitespace and comment.
    fn step(&mut self) -> Option<()> {
        let c = self.chars[self.at];
        let first_in_class = std::mem::take(&mut self.class_start);
        let ignored = self.ignored_len(self.at);
        if ignored > 0 {
            self.class_start = first_in_class;
            self.at += ignored;
            return Some(());
        }
        self.prior_run = std::mem::take(&mut self.run);
        let quantified = std::mem::replace(&mut self.quantified, Quantified::No);
        if !self.in_class() {
            match (quantified, c) {
                (Quantified::No, '*' | '+' | '?') => self.quantified = Quantified::Open,
                (Quantified::Open, '?' | '+') => self.quantified = Quantified::Closed,
                // A repeat of the empty atom matches the empty string, greedy,
                // lazy or possessive, so neither it nor its `?` or `+` is
                // written.
                (Quantified::Empty, '?' | '+') => {
                    self.quantified = Quantified::Start;
                    self.at += 1;
                    return Some(());
                }
                (_, '*' | '+' | '?') => return None,
                (
                    Quantified::Open | Quantified::Closed | Quantified::Start | Quantified::Empty,
                    '{',
                ) => {
                    self.at += self.bounds_len(self.at)?;
                    self.quantified = Quantified::Empty;
                    return Some(());
                }
                _ => {}
            }
        }
        // A member of the innermost class: not one that opens or closes a
        // class, nor a `&`, which [`Self::ampersand`] reads.
        let closes = c == ']' && !first_in_class;
        let member = (self.in_class() && !closes && !matches!(c, '[' | '&'))
            .then_some((self.out.len(), self.at));
        match c {
            '\\' => self.escape()?,
            '(' if !self.in_class() => self.group()?,
            // Every `(` that opens a group pushed its flags, so an empty stack
            // is a `)` with no group: `Unmatched closing ')'` in Java.
            ')' if !self.in_class() => {
                self.scope = self.outer.pop()?;
                let number = self.groups.pop().flatten();
                self.copy(1);
                if let Some(number) = number {
                    self.group_repeats[number] = self.group_curly_at(self.at);
                }
            }
            '|' if !self.in_class() => {
                self.copy(1);
                self.quantified = Quantified::Start;
            }
            '[' => self.open_class(),
            // First in a class, a `]` is a member, and may start a range.
            ']' if self.in_class() && !first_in_class => self.close_class()?,
            '.' if !self.in_class() && !self.scope.dot.dotall && !self.scope.dot.unix_lines => {
                self.out.push_str(JAVA_DOT);
                self.at += 1;
            }
            '^' if !self.in_class() && self.scope.dot.multiline => {
                self.push_anchor(caret(self.scope.dot), 1);
            }
            '$' if !self.in_class() => self.push_anchor(dollar(self.scope.dot), 1),
            '{' if !self.in_class() => self.braces()?,
            '&' if self.in_class() => self.ampersand()?,
            _ => self.atom(u32::from(c), 1)?,
        }
        if let Some((out_at, at)) = member {
            self.class_member(out_at, at);
        }
        Some(())
    }

    /// The chars of `(?x)` whitespace and comment at `at`: none unless the
    /// `x` flag is on.
    fn ignored_len(&self, at: usize) -> usize {
        if self.scope.comments {
            java_fold::ignorable_len(&self.chars[at..], self.scope.dot.unix_lines)
        } else {
            0
        }
    }

    /// The escape at `at`.
    fn escape(&mut self) -> Option<()> {
        let in_class = self.in_class();
        let Consumed { value: escape, len } = java_fold::read_escape(self.chars, self.at, in_class);
        let kind = self.chars.get(self.at + 1).copied();
        // Java's `escape` refuses a boundary, an anchor, `\R`, `\X` and a
        // reference in a class.
        if in_class
            && matches!(
                kind,
                Some('b' | 'B' | 'A' | 'G' | 'z' | 'Z' | 'R' | 'X' | 'k')
            )
        {
            return None;
        }
        // Java reads `\k<name>` against the named groups opened before it,
        // which `fancy_regex` would also take a group number for.
        if kind == Some('k') {
            if escape != Escape::BackRef {
                return None;
            }
            let name: String = self.chars[self.at + 3..self.at + len - 1].iter().collect();
            if !self.group_names.contains(&name) {
                return None;
            }
        }
        match escape {
            // `fancy_regex` reads `\N{name}` as `\N` and the text of the name,
            // and the rewrite has no table of Unicode names to write the char.
            Escape::Named => return None,
            // Java compares the text of the group case-insensitively, which
            // `fancy_regex` can do only for a whole reference. See
            // [`java_fold`] for what that leaves different.
            Escape::BackRef if self.scope.case.ignore => {
                self.backref_folds.push(if self.scope.case.unicode {
                    BackrefFold::Unicode
                } else {
                    BackrefFold::Ascii
                });
                self.out.push_str("(?i:");
                self.copy(len);
                self.out.push(')');
            }
            Escape::Char(code) => self.atom(code, len)?,
            // `Unknown character property name`, where `fancy_regex` would
            // read the name another way.
            Escape::Property { name, .. }
                if !java_property::java_knows(&name, self.scope.unicode_classes) =>
            {
                return None;
            }
            Escape::Property { name, negated } => {
                match java_fold::property_class(&name, negated, in_class, self.scope) {
                    Some(class) => {
                        self.out.push_str(&class);
                        self.at += len;
                    }
                    None => self.copy(len),
                }
            }
            // `\Z` is the `$` of no `(?m)`.
            Escape::Other if !in_class && self.chars.get(self.at + 1) == Some(&'Z') => {
                let dot = Dot {
                    multiline: false,
                    ..self.scope.dot
                };
                self.push_anchor(dollar(dot), len);
            }
            // Java reads `\v` as the char `\x0B` where it starts a range.
            Escape::Other
                if in_class
                    && self.chars.get(self.at + 1) == Some(&'v')
                    && self.chars.get(self.at + len) == Some(&'-') =>
            {
                self.atom(0x0B, len)?;
            }
            Escape::Other
                if self
                    .chars
                    .get(self.at + 1)
                    .is_some_and(|&next| java_space(next, in_class).is_some()) =>
            {
                let next = self.chars[self.at + 1];
                self.out.push_str(java_space(next, in_class)?);
                self.at += len;
            }
            // `\R`, whose quantifier [`JavaTranslation::linebreak_atoms`]
            // records.
            Escape::Other if !in_class && self.chars.get(self.at + 1) == Some(&'R') => {
                self.linebreak_atoms.push(self.quantifier_at(self.at + len));
                self.copy(len);
            }
            Escape::BackRef | Escape::Other => {
                let ascii = self
                    .chars
                    .get(self.at + 1)
                    .and_then(|&next| ascii_shorthand(next, in_class));
                match ascii {
                    Some(ascii) if !self.scope.unicode_classes => {
                        self.out.push_str(ascii);
                        self.at += len;
                    }
                    _ => self.copy(len),
                }
            }
        }
        Some(())
    }

    /// One char, `len` chars long in the pattern and standing for `code`, and
    /// in a class the range it starts. Under an ASCII fold a letter is both of
    /// its cases, and a range takes the other case of the letters in it. Under
    /// a Unicode fold each is the set of code points that Java's predicate
    /// accepts. `None` for a range that Java refuses.
    fn atom(&mut self, code: u32, len: usize) -> Option<()> {
        let (chars, start, in_class) = (self.chars, self.at, self.in_class());
        let fold = self.scope.ascii_fold();
        let unicode_fold = self.scope.unicode_fold();
        let after = start + len;
        let range = if in_class {
            self.range_end(after)
        } else {
            RangeEnd::Single
        };
        match range {
            RangeEnd::Illegal => return None,
            RangeEnd::To {
                end,
                code: end_code,
                len: end_len,
            } => {
                if unicode_fold {
                    // Java's `Illegal character range`, which the fold would
                    // otherwise hide in the explicit set.
                    if end_code < code {
                        return None;
                    }
                    java_case::push_members(&mut self.out, &java_case::range(code, end_code));
                } else {
                    let (first, last) = (&chars[start..after], &chars[end..end + end_len]);
                    java_fold::push_member(&mut self.out, first, code, true);
                    self.out.push('-');
                    java_fold::push_member(&mut self.out, last, end_code, true);
                    if fold {
                        java_fold::push_other_case_ranges(&mut self.out, code, end_code);
                    }
                }
                self.at = end + end_len;
            }
            RangeEnd::Single => {
                let source = &chars[start..after];
                if !in_class && !is_meta(source) {
                    self.run = self.prior_run + 1;
                }
                if fold {
                    java_fold::push_folded(&mut self.out, source, code, in_class);
                } else if let Some(cases) = unicode_fold.then(|| self.cases(code, after)).flatten()
                {
                    if in_class {
                        java_case::push_members(&mut self.out, &cases);
                    } else {
                        java_case::push_class(&mut self.out, &cases);
                    }
                } else {
                    java_fold::push_member(&mut self.out, source, code, in_class);
                }
                self.at = after;
            }
        }
        Some(())
    }

    /// The code points that the char `code`, which ends before `after`,
    /// matches under a Unicode fold, or `None` where it has no case and
    /// matches only itself.
    fn cases(&self, code: u32, after: usize) -> Option<java_case::Ranges> {
        if self.in_class() {
            java_case::member(code)
        } else {
            java_case::literal(code, || self.in_literal_run(after))
        }
    }

    /// Writes `anchor`, a zero-width test, for the token of `len` chars here.
    /// `fancy_regex` takes no quantifier after a look-around, so where one
    /// follows, the anchor is written as an alternation whose other branch
    /// never matches. Java reads a quantified anchor as the anchor when the
    /// quantifier's minimum is one or more, and as nothing when it is zero,
    /// and so does that alternation.
    fn push_anchor(&mut self, anchor: &str, len: usize) {
        if self.quantifier_at(self.at + len) {
            self.out.push_str("(?:");
            self.out.push_str(anchor);
            self.out.push_str("|(?!))");
        } else {
            self.out.push_str(anchor);
        }
        self.at += len;
    }

    /// Whether a quantifier starts at `at`, past any `(?x)` whitespace and
    /// comment, as Java's `closure` peeks for one.
    fn quantifier_at(&self, at: usize) -> bool {
        let at = at + self.ignored_len(at);
        matches!(self.chars.get(at), Some('*' | '+' | '?' | '{'))
    }

    /// Whether Java reads the literal char that ends before `after` in a run
    /// of two or more, as one slice, and not on its own. A run is the literal
    /// chars up to the next token that is not one, and Java takes the last
    /// char off it when a quantifier follows, as the quantifier binds to that
    /// char alone. `run` counts the chars of the run up to this one.
    fn in_literal_run(&self, after: usize) -> bool {
        let (chars, mut at, mut following) = (self.chars, after, 0);
        loop {
            at += self.ignored_len(at);
            match chars.get(at) {
                Some('\\') => match java_fold::read_escape(chars, at, false) {
                    Consumed {
                        value: Escape::Char(_),
                        len,
                    } => at += len,
                    _ => break,
                },
                Some(&c) if !is_meta(&[c]) && !matches!(c, '(' | ')' | '[') => at += 1,
                _ => break,
            }
            following += 1;
        }
        let quantified = matches!(chars.get(at), Some('*' | '+' | '?' | '{'));
        let unwound = quantified && following == 0;
        !unwound && self.run + following - usize::from(quantified) >= 2
    }

    /// A `&` in a class, which is the intersection operator when another `&`
    /// follows. Java reads `&&` before any member, so the second `&` is never
    /// the start of a range, as in `[a&&-1]`.
    fn ampersand(&mut self) -> Option<()> {
        let second = self.at + 1 + self.ignored_len(self.at + 1);
        let (out_at, at) = (self.out.len(), self.at);
        if self.chars.get(second) == Some(&'&') {
            self.push_class_member(out_at, ClassMember::Intersect);
            self.out.push_str("&&");
            self.at = second + 1;
        } else {
            self.atom(u32::from('&'), 1)?;
            self.class_member(out_at, at);
        }
        Some(())
    }

    /// A `{` after an atom, which starts the bounds of its quantifier. They
    /// are copied whole, so that the digits in them are not read as literal
    /// chars of a run.
    fn braces(&mut self) -> Option<()> {
        let len = self.bounds_len(self.at)?;
        self.copy(len);
        self.quantified = Quantified::Open;
        Some(())
    }

    /// The chars of the bounds of a quantifier at `at`, `{n}`, `{n,}` or
    /// `{n,m}`. `None` for any other `{` outside a class, Java's `Illegal
    /// repetition`, and for a bound past `i32::MAX` or a maximum below the
    /// minimum, Java's `Illegal repetition range`.
    fn bounds_len(&self, at: usize) -> Option<usize> {
        let rest = &self.chars[at..];
        let end = rest.iter().position(|&c| c == '}')?;
        let bounds = &rest[1..end];
        if !bounds.first().is_some_and(char::is_ascii_digit)
            || !bounds.iter().all(|&c| c.is_ascii_digit() || c == ',')
        {
            return None;
        }
        let text: String = bounds.iter().collect();
        let count = |digits: &str| digits.parse::<i32>().ok();
        let legal = match text.split_once(',') {
            Some((min, "")) => count(min).is_some(),
            Some((min, max)) => count(min)
                .zip(count(max))
                .is_some_and(|(min, max)| min <= max),
            None => count(&text).is_some(),
        };
        legal.then_some(end + 1)
    }

    /// Whether the quantifier at `at`, past any `(?x)` whitespace and
    /// comment, repeats the group that closes before it with Java's
    /// `GroupCurly`: a `*`, `+` or counted quantifier, greedy or lazy. A
    /// possessive one is a `Curly`, and `closure` reads a `?` as a `Ques`.
    fn group_curly_at(&self, at: usize) -> bool {
        let at = at + self.ignored_len(at);
        let len = match self.chars.get(at) {
            Some('*' | '+') => Some(1),
            Some('{') => self.bounds_len(at),
            _ => None,
        };
        len.is_some_and(|len| {
            let modifier = at + len;
            self.chars.get(modifier + self.ignored_len(modifier)) != Some(&'+')
        })
    }

    /// Opens a group with the flags in force here, and gives it the next
    /// group number when it `captures`.
    fn open_group(&mut self, captures: bool) {
        self.outer.push(self.scope);
        let number = captures.then(|| {
            self.group_repeats.push(false);
            self.group_repeats.len() - 1
        });
        self.groups.push(number);
        self.quantified = Quantified::Start;
    }

    /// How the class member that ends before `after` goes on. Java reads a `-`
    /// after a member and before a char that is not `]` or `[` as the range of
    /// the two. It looks at the char right after the `-`, without skipping
    /// `(?x)` white space, so `(?x)[a- ]` is a range from `a` to the `]` past
    /// the space, and an illegal one.
    fn range_end(&self, after: usize) -> RangeEnd {
        let dash = after + self.ignored_len(after);
        if self.chars.get(dash) != Some(&'-') {
            return RangeEnd::Single;
        }
        if matches!(self.chars.get(dash + 1), None | Some(']' | '[')) {
            return RangeEnd::Single;
        }
        let end = dash + 1 + self.ignored_len(dash + 1);
        match java_fold::read_member(self.chars, end) {
            Some(Consumed { value: code, len }) => RangeEnd::To { end, code, len },
            // The end is an escape for more than one char, or none Java
            // reads: `[a-\w]` is `Illegal character range`, and `[a-\p{L}]`
            // is `Illegal/unsupported escape sequence`.
            None => RangeEnd::Illegal,
        }
    }

    /// A `[`, which opens a class, or a class in a class.
    fn open_class(&mut self) {
        let out_at = self.out.len();
        self.push_class_member(out_at, ClassMember::Nested);
        self.class_start = true;
        self.copy(1);
        if self.chars.get(self.at) == Some(&'^') {
            self.copy(1);
        }
        self.classes.push(OpenClass {
            members_at: self.out.len(),
            members: Vec::new(),
        });
    }

    /// A `]` that closes the innermost class. A class with `&&` is written
    /// again, as the set that Java builds from its members ([`java_class`]).
    /// `None` where Java refuses the class: `Bad class syntax` for `[&&]`, and
    /// `Bad intersection syntax` for an empty side after a single char.
    fn close_class(&mut self) -> Option<()> {
        let class = self.classes.pop()?;
        if class
            .members
            .iter()
            .any(|&(_, member)| member == ClassMember::Intersect)
        {
            let text = self.out.split_off(class.members_at);
            let mut members = Vec::with_capacity(class.members.len());
            for (index, &(at, member)) in class.members.iter().enumerate() {
                let end = class
                    .members
                    .get(index + 1)
                    .map_or(text.len(), |&(next, _)| next - class.members_at);
                members.push((member, &text[at - class.members_at..end]));
            }
            let mut bit_sets = Vec::new();
            let mut next = 0;
            let set = java_class(&members, &mut next, &mut bit_sets)?;
            set.write(&mut self.out, &bit_sets);
        }
        self.out.push(']');
        self.at += 1;
        Some(())
    }

    /// Records a member of the innermost class, if one is open, which starts
    /// at `out_at` in the output.
    fn push_class_member(&mut self, out_at: usize, member: ClassMember) {
        if let Some(class) = self.classes.last_mut() {
            class.members.push((out_at, member));
        }
    }

    /// Records the member of the innermost class just read from `at`, written
    /// from `out_at`. Java keeps a single char below U+0100 in the class's
    /// `BitClass` (`bitsOrSingle`), except under a Unicode fold the chars
    /// whose other case is above U+00FF, or that another char folds to.
    fn class_member(&mut self, out_at: usize, at: usize) {
        let single = java_fold::read_member(self.chars, at)
            .filter(|member| at + member.len == self.at)
            .map(|member| member.value);
        let unicode_fold = self.scope.unicode_fold();
        let bits = single.is_some_and(|code| {
            code < 0x100
                && !(unicode_fold
                    && matches!(
                        code,
                        0xFF | 0xB5 | 0x49 | 0x69 | 0x53 | 0x73 | 0x4B | 0x6B | 0xC5 | 0xE5
                    ))
        });
        let ampersand = self.chars[at] == '&';
        self.push_class_member(
            out_at,
            if bits {
                ClassMember::Bits { ampersand }
            } else {
                ClassMember::Set { ampersand }
            },
        );
    }

    /// A `(`: a group, a lookaround, or inline flags.
    fn group(&mut self) -> Option<()> {
        let chars = self.chars;
        if chars.get(self.at + 1) != Some(&'?') {
            self.open_group(true);
            self.copy(1);
            return Some(());
        }
        let rest = &chars[self.at + 2..];
        let (opener, captures) = match rest.first() {
            Some(':' | '=' | '!' | '>') => (3, false),
            Some('<') => match rest.get(1) {
                Some('=' | '!') => (4, false),
                Some(first) if first.is_ascii_alphabetic() => {
                    let name = rest[2..].iter().position(|c| !c.is_ascii_alphanumeric())?;
                    if rest[2 + name] != '>' {
                        return None;
                    }
                    // `Named capturing group <name> is already defined`.
                    let text: String = rest[1..2 + name].iter().collect();
                    if self.group_names.contains(&text) {
                        return None;
                    }
                    self.group_names.push(text);
                    (name + 5, true)
                }
                _ => return None,
            },
            Some(_) => return self.flag_group(rest),
            None => return None,
        };
        self.open_group(captures);
        self.copy(opener);
        Some(())
    }

    /// Inline flags, `rest` starting just after their `(?`.
    fn flag_group(&mut self, rest: &[char]) -> Option<()> {
        let group = java_flag_group(rest)?;
        // `(?flags:x)` is a group the flags last to the end of, `(?flags)`
        // lasts to the end of the group it is in. Java's `group0` reads
        // `(?flags)` as no node, so what follows starts as a sequence does: a
        // `*`, `+` or `?` is dangling, and a `{` repeats the empty atom, not
        // the atom before the flags.
        if group.scoped {
            self.open_group(false);
        } else {
            self.quantified = Quantified::Start;
        }
        for &FlagChange { flag, on } in &group.changes {
            self.scope.set(flag, on);
        }
        self.out.push_str(&group.translation());
        self.at += 2 + group.len;
        Some(())
    }
}

/// What a `-` after a class member makes of the member, as
/// [`Translator::range_end`] reads it.
enum RangeEnd {
    /// No range: there is no `-`, or it is a member of its own.
    Single,
    /// A range that ends with the char of `len` chars at `end`, which stands
    /// for `code`.
    To { end: usize, code: u32, len: usize },
    /// A range Java refuses.
    Illegal,
}

/// The Java flags that change what [`translate_java`] writes for a construct.
#[derive(Debug, Clone, Copy, Default)]
struct Scope {
    /// `s` and `d`: what `.` matches.
    dot: Dot,
    /// `U` (`UNICODE_CHARACTER_CLASS`): `\w`, `\d`, `\s` and `\b` are Unicode.
    unicode_classes: bool,
    /// `i` and `u`: how letters match.
    case: Case,
    /// `x` (`COMMENTS`): whitespace and `#` comments are ignored.
    comments: bool,
}

/// The [`Scope`] flags for `.` and the line terminators.
#[derive(Debug, Clone, Copy, Default)]
struct Dot {
    /// `s` (`DOTALL`): `.` matches a line terminator.
    dotall: bool,
    /// `d` (`UNIX_LINES`): `\n` is the only line terminator.
    unix_lines: bool,
    /// `m` (`MULTILINE`): `^` and `$` match at the line terminators too.
    multiline: bool,
}

/// The [`Scope`] flags for the case of letters.
#[derive(Debug, Clone, Copy, Default)]
struct Case {
    /// `i` (`CASE_INSENSITIVE`): letters match in both cases.
    ignore: bool,
    /// `u` (`UNICODE_CASE`), which `U` turns on too: `i` folds Unicode case,
    /// and not only ASCII.
    unicode: bool,
}

impl Scope {
    fn set(&mut self, flag: char, on: bool) {
        match flag {
            's' => self.dot.dotall = on,
            'd' => self.dot.unix_lines = on,
            'm' => self.dot.multiline = on,
            'i' => self.case.ignore = on,
            'u' => self.case.unicode = on,
            'x' => self.comments = on,
            // As in `Pattern.addFlag` and `Pattern.subFlag`, `U` takes
            // `UNICODE_CASE` with it, in both directions.
            'U' => {
                self.unicode_classes = on;
                self.case.unicode = on;
            }
            _ => {}
        }
    }

    /// `i` without `u`: Java folds ASCII case only, which `fancy_regex` cannot
    /// do, so the rewrite writes both cases and leaves the fancy `i` off.
    fn ascii_fold(self) -> bool { self.case.ignore && !self.case.unicode }

    /// `i` with `u`: Java folds Unicode case, by a relation other than the
    /// simple case folding of the fancy `i`, so the rewrite writes the code
    /// points that Java accepts and leaves the fancy `i` off.
    fn unicode_fold(self) -> bool { self.case.ignore && self.case.unicode }
}

/// Whether `source`, the pattern text of one token that [`Translator::atom`]
/// reads, is a char that Java does not take into a run of literal chars: a
/// quantifier, an anchor, `.` or `|`.
fn is_meta(source: &[char]) -> bool {
    matches!(source, ['*' | '+' | '?' | '{' | '$' | '.' | '^' | '|'])
}

/// What Java's `.` matches by default: anything but the line terminators
/// `\n`, `\r`, U+0085, U+2028 and U+2029 (`Pattern.Dot`, without `DOTALL`
/// or `UNIX_LINES`).
const JAVA_DOT: &str = "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]";

/// Java's `$` (`Pattern.Dollar`, or `UnixDollar` under `(?d)`), which also
/// stands for `\Z` with `multiline` off, written as a look-ahead.
/// `fancy_regex`'s `$` matches at the end only, and under `(?m)` before `\n`
/// only.
///
/// Without `(?m)` it matches at the end, or before a line terminator that
/// ends the input: `\r\n`, `\n` after anything but `\r`, `\r`, U+0085,
/// U+2028 or U+2029. With `(?m)` it matches before any line terminator, and
/// never between `\r` and `\n`. Under `(?d)` the only line terminator is
/// `\n`.
fn dollar(dot: Dot) -> &'static str {
    match (dot.multiline, dot.unix_lines) {
        (false, false) => "(?:(?=(?:\\r\\n|(?<!\\r)\\n|[\\r\\x{85}\\x{2028}\\x{2029}])?\\z))",
        (true, false) => "(?:(?=\\z|(?<!\\r)\\n|[\\r\\x{85}\\x{2028}\\x{2029}]))",
        (false, true) => "(?:(?=\\n?\\z))",
        (true, true) => "(?:(?=\\n|\\z))",
    }
}

/// Java's `^` under `(?m)` (`Pattern.Caret`, or `UnixCaret` under `(?d)`),
/// written as look-arounds. It matches at the start, or after a line
/// terminator, but never between `\r` and `\n`, and never at the end of the
/// input, even after a line terminator. Without `(?m)`, `^` is the start of
/// the input in both, and is written as it is.
fn caret(dot: Dot) -> &'static str {
    if dot.unix_lines {
        "(?:(?!\\z)(?:\\A|(?<=\\n)))"
    } else {
        "(?:(?!\\z)(?:\\A|(?<=[\\n\\x{85}\\x{2028}\\x{2029}])|(?<=\\r)(?!\\n)))"
    }
}

/// Java's `\h`, `\H`, `\v` and `\V` (`HorizWS` and `VertWS`), which
/// `fancy_regex` reads otherwise, and which `(?U)` does not change. `escape`
/// is the character after the backslash. Inside a character class the
/// positive forms are the bare members, and the negations are a nested
/// negated class. `None` for anything else.
fn java_space(escape: char, in_class: bool) -> Option<&'static str> {
    Some(match (escape, in_class) {
        ('h', false) => {
            "[\\t\\x20\\xA0\\x{1680}\\x{180E}\\x{2000}-\\x{200A}\\x{202F}\\x{205F}\\x{3000}]"
        }
        ('h', true) => {
            "\\t\\x20\\xA0\\x{1680}\\x{180E}\\x{2000}-\\x{200A}\\x{202F}\\x{205F}\\x{3000}"
        }
        ('H', _) => {
            "[^\\t\\x20\\xA0\\x{1680}\\x{180E}\\x{2000}-\\x{200A}\\x{202F}\\x{205F}\\x{3000}]"
        }
        ('v', false) => "[\\n-\\r\\x{85}\\x{2028}\\x{2029}]",
        ('v', true) => "\\n-\\r\\x{85}\\x{2028}\\x{2029}",
        ('V', _) => "[^\\n-\\r\\x{85}\\x{2028}\\x{2029}]",
        _ => return None,
    })
}

/// A character class that [`Translator`] has open.
struct OpenClass {
    /// Where its members start in the output, after the `[` or `[^`.
    members_at: usize,
    /// Each member written so far, and where it starts in the output.
    members: Vec<(usize, ClassMember)>,
}

/// What a member of a class is to Java's `Pattern.clazz`, which builds the
/// set of a class with `&&` from them ([`java_class`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClassMember {
    /// A single char that goes in the class's `BitClass`. Java keeps one
    /// `BitClass` for the whole class, so every such char joins the same set,
    /// one after an intersection too. `ampersand` for a literal `&`, which
    /// ends the right side of a `&&`.
    Bits { ampersand: bool },
    /// Any other member: a char that Java matches by its own predicate, a
    /// range, a shorthand or a property.
    Set { ampersand: bool },
    /// A class in the class.
    Nested,
    /// `&&`.
    Intersect,
}

/// A set that [`java_class`] builds, as Java's `CharPredicate` tree.
#[derive(Clone)]
enum ClassSet<'a> {
    /// The `BitClass` of a class, at an index into the bit sets, as it is when
    /// the class closes.
    Bits(usize),
    /// One member, as the rewrite wrote it.
    Member(&'a str),
    Union(Box<Self>, Box<Self>),
    And(Box<Self>, Box<Self>),
}

impl ClassSet<'_> {
    fn union(left: Option<Self>, right: Self) -> Self {
        match left {
            Some(left) => Self::Union(Box::new(left), Box::new(right)),
            None => right,
        }
    }

    /// Writes the set as a class in `fancy_regex` syntax.
    fn write(&self, out: &mut String, bit_sets: &[String]) {
        out.push('[');
        match self {
            Self::Bits(index) => out.push_str(&bit_sets[*index]),
            Self::Member(text) => push_class_text(out, text),
            Self::Union(left, right) => {
                left.write(out, bit_sets);
                right.write(out, bit_sets);
            }
            Self::And(left, right) => {
                left.write(out, bit_sets);
                out.push_str("&&");
                right.write(out, bit_sets);
            }
        }
        out.push(']');
    }
}

/// Writes the text of a class member where another member's text, or the `[`
/// of a class, may now be next to it. A `&` or `~` alone would make an
/// operator of `fancy_regex`'s with a neighbour, and a leading `^` a negation.
fn push_class_text(out: &mut String, text: &str) {
    if text.starts_with('^') || matches!(text, "&" | "~") {
        out.push('\\');
    }
    out.push_str(text);
}

/// The set of a class, or of the right side of a `&&` in it, from the members
/// at `next` on, as Java's `Pattern.clazz` builds it. `None` for a class that
/// Java refuses.
///
/// `clazz` keeps `prev`, the set so far, `curr`, the last member that is not
/// a single char in the `BitClass`, and the `BitClass`, which it adds to
/// `prev` before a `&&` and at the `]`. The side after `&&` is read up to the
/// `]` or the next `&`, as a class of its own, so `&&` binds to the right.
/// When it is empty, `curr` takes its place, and when the side before is,
/// the side after is the set. The `BitClass` that an intersection took is
/// the same one the chars after it go in, so they join it there too.
fn java_class<'a>(
    members: &[(ClassMember, &'a str)],
    next: &mut usize,
    bit_sets: &mut Vec<String>,
) -> Option<ClassSet<'a>> {
    let bits = bit_sets.len();
    bit_sets.push(String::new());
    let (mut prev, mut curr, mut has_bits) = (None, None, false);
    while let Some(&(member, text)) = members.get(*next) {
        match member {
            ClassMember::Bits { .. } => {
                push_class_text(&mut bit_sets[bits], text);
                curr = None;
                has_bits = true;
                *next += 1;
            }
            ClassMember::Set { .. } | ClassMember::Nested => {
                curr = Some(ClassSet::Member(text));
                prev = Some(ClassSet::union(prev, ClassSet::Member(text)));
                *next += 1;
            }
            ClassMember::Intersect => {
                *next += 1;
                let mut right = None;
                while let Some(&(member, text)) = members.get(*next) {
                    right = Some(match member {
                        ClassMember::Intersect
                        | ClassMember::Bits { ampersand: true }
                        | ClassMember::Set { ampersand: true } => break,
                        ClassMember::Nested => {
                            *next += 1;
                            ClassSet::union(right, ClassSet::Member(text))
                        }
                        _ => ClassSet::union(right, java_class(members, next, bit_sets)?),
                    });
                }
                if std::mem::take(&mut has_bits) {
                    match prev {
                        None => {
                            prev = Some(ClassSet::Bits(bits));
                            curr = Some(ClassSet::Bits(bits));
                        }
                        Some(set) => prev = Some(ClassSet::union(Some(set), ClassSet::Bits(bits))),
                    }
                }
                if right.is_some() {
                    curr.clone_from(&right);
                }
                prev = Some(match prev {
                    None => right?,
                    Some(set) => ClassSet::And(Box::new(set), Box::new(curr.clone()?)),
                });
            }
        }
    }
    match (prev, has_bits) {
        (None, false) => None,
        (None, true) => Some(ClassSet::Bits(bits)),
        (Some(set), false) => Some(set),
        (Some(set), true) => Some(ClassSet::union(Some(set), ClassSet::Bits(bits))),
    }
}

/// The ASCII form of a Java shorthand class, or word boundary, which is what
/// Java reads without `(?U)`: `\w` is `[a-zA-Z_0-9]`, `\d` is `[0-9]`, `\s` is
/// `[ \t\n\x0B\f\r]`, and `\b` is the edge of a run of `\w` (as of JDK 19,
/// where it stopped reading Unicode letters and digits).
///
/// `escape` is the character after the backslash. Inside a character class
/// the positive forms are the bare members, and the negations are a nested
/// negated class. `None` for anything else, and for a boundary in a class.
fn ascii_shorthand(escape: char, in_class: bool) -> Option<&'static str> {
    Some(match (escape, in_class) {
        ('w', false) => "[0-9A-Za-z_]",
        ('w', true) => "0-9A-Za-z_",
        ('W', _) => "[^0-9A-Za-z_]",
        ('d', false) => "[0-9]",
        ('d', true) => "0-9",
        ('D', _) => "[^0-9]",
        ('s', false) => "[ \\t\\n\\x0B\\x0C\\r]",
        ('s', true) => " \\t\\n\\x0B\\x0C\\r",
        ('S', _) => "[^ \\t\\n\\x0B\\x0C\\r]",
        ('b', false) => "(?:(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|(?<![0-9A-Za-z_])(?=[0-9A-Za-z_]))",
        ('B', false) => "(?:(?<=[0-9A-Za-z_])(?=[0-9A-Za-z_])|(?<![0-9A-Za-z_])(?![0-9A-Za-z_]))",
        _ => return None,
    })
}

/// Translate a Java inline flag group, `rest` starting just after its `(?`,
/// into the flags that `fancy_regex` shares and the flags [`Scope`] follows.
/// Returns `None` where `Pattern.compile` answers "Unknown inline modifier".
///
/// Java's `Pattern.addFlag` takes `idmsuxcU`, then optionally one `-` and
/// the same letters to clear, then `)` or `:`; an empty group such as `(?)`
/// or `(?-:x)` is valid. `fancy_regex` refuses empty groups and gives `U` a
/// different meaning, so only the flags it shares keep their letter:
///
/// - `s` (`DOTALL`) passes through, and [`Scope`] follows it.
/// - `m` (`MULTILINE`) is dropped from the output, and read by
///   [`translate_java`] to translate `^` and `$`, which it writes out.
/// - `x` (`COMMENTS`) is read by [`Scope`] and dropped from the output: the
///   rewrite removes the whitespace and the comments itself. `fancy_regex`'s
///   `x` would also hide the space and the `#` that the rewrite writes after
///   the flag's scope has ended, as `fancy_regex` does not end the flags set
///   inside a capturing group at its `)`.
/// - `i` (`CASE_INSENSITIVE`) and `u` (`UNICODE_CASE`) are read by
///   [`Scope`] and dropped from the output: `fancy_regex`'s `i` folds Unicode
///   case by another relation than Java's, with or without `u`, so
///   [`Translator`] writes the folding out. See [`java_fold`].
/// - `d` (`UNIX_LINES`) is dropped from the output, and read by
///   [`translate_java`] to translate `.`.
/// - `U` (`UNICODE_CHARACTER_CLASS`) is dropped from the output, and read by
///   [`translate_java`] to keep `\w`, `\d`, `\s` and `\b` Unicode where it
///   otherwise makes them ASCII, and to fold Unicode case, which Java's `U`
///   turns on. Passing it through would swap greediness in `fancy_regex`.
/// - `c` (`CANON_EQ`) is dropped: canonical-equivalence matching has no
///   `fancy_regex` form, and it changes nothing for text already in one
///   normalization form.
fn java_flag_group(rest: &[char]) -> Option<FlagGroup> {
    let mut on = String::new();
    let mut off = String::new();
    let mut changes = Vec::new();
    let mut clearing = false;
    for (n, &c) in rest.iter().enumerate() {
        match c {
            's' => {
                if clearing {
                    off.push(c);
                } else {
                    on.push(c);
                }
                changes.push(FlagChange {
                    flag: c,
                    on: !clearing,
                });
            }
            'i' | 'u' | 'd' | 'm' | 'x' | 'U' => changes.push(FlagChange {
                flag: c,
                on: !clearing,
            }),
            'c' => {}
            '-' if !clearing => clearing = true,
            ')' | ':' => {
                return Some(FlagGroup {
                    on,
                    off,
                    len: n + 1,
                    scoped: c == ':',
                    changes,
                });
            }
            _ => return None,
        }
    }
    None
}

/// A Java inline flag group, as [`java_flag_group`] reads it.
struct FlagGroup {
    /// The flags `fancy_regex` shares that it turns on.
    on: String,
    /// The flags `fancy_regex` shares that it turns off.
    off: String,
    /// The chars of the group after its `(?`, through the closing `)` or `:`.
    len: usize,
    /// Whether it ends in `:`, a group that the flags last to the end of.
    scoped: bool,
    /// The flags [`Scope`] follows that it sets (`true`) or clears (`false`),
    /// in the order Java applies them.
    changes: Vec<FlagChange>,
}

/// A flag that a Java inline flag group sets or clears.
#[derive(Clone, Copy)]
struct FlagChange {
    flag: char,
    /// Whether the group turns the flag on.
    on: bool,
}

impl FlagGroup {
    /// The `fancy_regex` text for the group. It is empty for a `(?flags)`
    /// group that has no flag of `fancy_regex`'s to write, which
    /// `fancy_regex` refuses.
    fn translation(&self) -> String {
        let (on, off) = (&self.on, &self.off);
        let end = if self.scoped { ':' } else { ')' };
        match (on.is_empty() && off.is_empty(), self.scoped) {
            (true, false) => String::new(),
            (true, true) => "(?:".to_string(),
            (false, _) if off.is_empty() => format!("(?{on}{end}"),
            (false, _) => format!("(?{on}-{off}{end}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::translate_java, crate::JavaPattern, assert2::check};

    /// One whole-input match: whether `source` matches all of `input`.
    struct MatchCase<'a> {
        source: &'a str,
        input: &'a str,
        expected: bool,
    }

    /// A [`MatchCase`] of `source` against `input`.
    const fn row<'a>(source: &'a str, input: &'a str, expected: bool) -> MatchCase<'a> {
        MatchCase {
            source,
            input,
            expected,
        }
    }

    /// Bounds where Java has no atom, at the start of a sequence, after inline
    /// flags or after a quantifier, repeat the empty atom, and match the empty
    /// string. Bounds after an atom repeat it. Each row is `Pattern.matches`
    /// on JDK 25.
    #[test]
    fn bounds_with_no_atom_before_them_repeat_the_empty_atom() {
        let cases = [
            row("{2}", "", true),
            row("{2}", "{2}", false),
            row("x|{2}", "", true),
            row("x|{2}{3}", "", true),
            row("a|{2}b", "b", true),
            row("a|{2}b", "{2}b", false),
            row("({2})", "", true),
            row("(?:{2})", "", true),
            row("(?<n>{2})", "", true),
            row("(?={2})", "", true),
            row("(?>{2})a", "a", true),
            row("(?i:{2})", "", true),
            row("{2}{3}", "", true),
            row("{2}+", "", true),
            row("{2}?", "", true),
            row("(?i){2}", "", true),
            row("(?s){2}", "", true),
            row("a(?s){2}", "a", true),
            row("a(?i){2}b", "ab", true),
            row("a(?i){2}b", "aB", true),
            row("(?x) {2}", "", true),
            row("(?iu){2}a", "A", true),
            row("(?ix) {2}a", "A", true),
            row("(?ix) {2}a", "", false),
            row("a{2}{3}", "aa", true),
            row("x{2}{3}", "xx", true),
            row("a{2}{3}+", "", false),
            row("a{2}{3}b{2}", "aabb", true),
            row("(a){2}{3}", "aa", true),
            row("(a)*{2}", "aaa", true),
            row("a*{2}", "aaa", true),
            row("a*{2}", "", true),
            row("a*{2}b", "aab", true),
            row("a*{2}?b", "aab", true),
            row("a*{2}+b", "aab", true),
            row("a*?{2}b", "aab", true),
            row("a*+{2}b", "aab", true),
            row("a*{2}{3}", "aa", true),
            row("a*{2}{3}?b", "ab", true),
            row("a+{2}", "a", true),
            row("a?{2}", "aa", false),
            row("a{1,2}{2}", "aaa", false),
            row("a*{0}", "a", true),
            row("(?x)a* {2} ?b", "aab", true),
            row("(?x)ab * {2} c", "abbc", true),
            row("\\R*{2}", "\r\n\n", true),
            row("\\R*{2}", "\n", true),
            // After an atom, the bounds are its quantifier.
            row("^{2}", "", true),
            row("a {2}", "", false),
            row("ab{2}", "abb", true),
            row("ab{2}", "abab", false),
            row("ab*{2}c", "abbc", true),
            row("ab*{2}c", "ac", true),
            row("ab*?{2}c", "abbc", true),
            row("ab*+{2}c", "abbc", true),
            row("abc{2}{3}", "abcc", true),
            row("\\R{2}", "\n\n", true),
        ];
        for MatchCase {
            source,
            input,
            expected,
        } in cases
        {
            let matched = JavaPattern::compile(source).map(|pattern| pattern.matches(input).ok());
            check!(
                matched == Ok(Some(expected)),
                "{source:?} against {input:?}"
            );
        }
    }

    /// A `*`, `+` or `?` with nothing to repeat is Java's `Dangling meta
    /// character`, a `{` that opens no bounds `Illegal repetition`, and bounds
    /// past `i32::MAX` or out of order `Illegal repetition range`, whether an
    /// atom precedes them or not. Each row is refused by `Pattern.compile` on
    /// JDK 25.
    #[yare::parameterized(
        star_after_empty_repeat = { "a*{2}*" },
        second_lazy_after_empty_repeat = { "a*{2}??" },
        star_after_empty_repeat_past_whitespace = { "(?x)a* {2} *" },
        star_after_leading_repeat = { "{2}*" },
        second_lazy_after_leading_repeat = { "{2}??" },
        star_at_start = { "*" },
        star_after_bar = { "x|*" },
        star_after_open = { "(*)" },
        star_after_emptied_flags = { "(?i)*" },
        plus_after_emptied_flags = { "(?i)+" },
        question_after_emptied_flags = { "(?i)?" },
        star_after_flags = { "(?s)*" },
        letter_bounds_after_bounds = { "a{2}{x}" },
        no_minimum_after_star = { "a*{,2}" },
        reversed_range_after_star = { "a*{2,1}" },
        huge_bound_after_star = { "a*{99999999999}" },
    )]
    fn a_quantifier_java_refuses_is_refused(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source:?}");
    }

    /// Whether a quantifier that Java reads as a `GroupCurly` follows each
    /// capturing group, group 0 first.
    #[yare::parameterized(
        star = { "()*", &[false, true] },
        inside_noncapturing_group = { "(?:())*", &[false, false] },
        possessive = { "()*+", &[false, false] },
        optional = { "()?", &[false, false] },
        lazy = { "(a)*?", &[false, true] },
        counted_named = { "(?<n>a){2}", &[false, true] },
        nested = { "((a))+", &[false, true, false] },
        second = { "(a)(b)*", &[false, false, true] },
        past_whitespace = { "(?x)(a) *", &[false, true] },
        inside_scoped_flags = { "(?i:(a))*", &[false, false] },
        unquantified = { "(a)", &[false, false] },
    )]
    fn group_repeats_marks_the_groups_a_group_curly_repeats(source: &str, expected: &[bool]) {
        let group_repeats = translate_java(source).map(|translation| translation.group_repeats);
        check!(group_repeats.as_deref() == Some(expected), "{source:?}");
    }
}
