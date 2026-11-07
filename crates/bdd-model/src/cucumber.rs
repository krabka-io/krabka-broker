//! Cucumber expressions, expanded into literal rstest-bdd step patterns.
//!
//! A step attribute such as `#[krabka_bdd::given("a/an topic with {int} partition(s)")]`
//! takes a [Cucumber expression]. rstest-bdd matches literal patterns with
//! named placeholders, so [`expand`] rewrites the expression into every
//! pattern it stands for:
//!
//! - `{int}` becomes `{name:i64}`, `{float}` becomes `{name:f64}`, `{string}`
//!   becomes `{name:string}` (rstest-bdd strips the quotes), and `{word}` and
//!   `{}` become `{name}`. `name` is the step function's next non-fixture
//!   parameter. An integer or float parameter type replaces the default hint,
//!   so `{int}` bound to a `u32` becomes `{name:u32}`.
//! - Optional text `item(s)` stands for `item` and `items`.
//! - Alternation `a/an` stands for `a` and `an`. It spans the whitespace-free
//!   word it sits in, and an alternative may hold optional text.
//! - rstest-bdd placeholders `{name}` and `{name:hint}` pass through. An
//!   untyped one whose parameter is an integer or float type gets that type as
//!   its hint.
//! - `\` escapes the next character, so `\(`, `\/` and `\{` are literal.
//!
//! The expansions multiply, and more than [`MAX_EXPANSIONS`] patterns is an
//! error: split such an expression into several.
//!
//! [Cucumber expression]: https://github.com/cucumber/cucumber-expressions

use std::{iter::Peekable, str::Chars};

/// The most patterns one expression may expand to.
pub const MAX_EXPANSIONS: usize = 16;

/// One non-fixture parameter of a step function, in declaration order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParamInfo {
    /// The parameter's name as declared, such as `count` or `_count`.
    pub name: String,
    /// The parameter's type as source text, such as `u32` or `& str`.
    pub type_name: String,
}

impl ParamInfo {
    /// A parameter named `name` of type `type_name`.
    #[must_use]
    pub fn new(name: &str, type_name: &str) -> Self {
        Self {
            name: name.to_owned(),
            type_name: type_name.to_owned(),
        }
    }

    /// The placeholder name the parameter binds: its name without one leading
    /// underscore, as rstest-bdd matches it.
    #[must_use]
    pub fn placeholder_name(&self) -> &str { self.name.strip_prefix('_').unwrap_or(&self.name) }

    /// The type without whitespace.
    fn compact_type(&self) -> String { self.type_name.split_whitespace().collect() }
}

/// The integer types rstest-bdd has a hint for.
const INTEGER_TYPES: [&str; 12] = [
    "u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize",
];

/// The float types rstest-bdd has a hint for.
const FLOAT_TYPES: [&str; 2] = ["f32", "f64"];

/// The integer hint for `param`'s type, if it is an integer type.
fn integer_hint(param: &ParamInfo) -> Option<String> {
    let ty = param.compact_type();
    INTEGER_TYPES.contains(&ty.as_str()).then_some(ty)
}

/// The float hint for `param`'s type, if it is a float type.
fn float_hint(param: &ParamInfo) -> Option<String> {
    let ty = param.compact_type();
    FLOAT_TYPES.contains(&ty.as_str()).then_some(ty)
}

/// The numeric hint for `param`'s type, if it is an integer or float type.
fn numeric_hint(param: &ParamInfo) -> Option<String> {
    integer_hint(param).or_else(|| float_hint(param))
}

/// `{name}` or `{name:hint}`.
fn placeholder(name: &str, hint: Option<&str>) -> String {
    hint.map_or_else(
        || format!("{{{name}}}"),
        |hint| format!("{{{name}:{hint}}}"),
    )
}

/// One lexed piece of an expression.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Piece {
    /// Literal non-whitespace text, already escaped for rstest-bdd.
    Text(String),
    /// A run of whitespace.
    Space(String),
    /// Optional text, already escaped for rstest-bdd.
    Optional(String),
    /// The `/` between two alternatives.
    Slash,
    /// A placeholder, rendered as rstest-bdd writes it.
    Param(String),
}

/// `character` as rstest-bdd literal text: a brace or backslash is escaped.
fn literal(character: char) -> String {
    if matches!(character, '{' | '}' | '\\') {
        format!("\\{character}")
    } else {
        character.to_string()
    }
}

/// Lexes `expression`, binding each placeholder to its parameter.
struct Lexer<'a> {
    expression: &'a str,
    characters: Peekable<Chars<'a>>,
    params: &'a [ParamInfo],
    /// How many placeholders have been read so far.
    position: usize,
    pieces: Vec<Piece>,
}

impl<'a> Lexer<'a> {
    fn new(expression: &'a str, params: &'a [ParamInfo]) -> Self {
        Self {
            expression,
            characters: expression.chars().peekable(),
            params,
            position: 0,
            pieces: Vec::new(),
        }
    }

    fn run(mut self) -> Result<Vec<Piece>, String> {
        while let Some(character) = self.characters.next() {
            let piece = match character {
                '\\' => Piece::Text(self.escaped()),
                '{' if self.characters.next_if_eq(&'{').is_some() => Piece::Text(literal('{')),
                '{' => self.placeholder()?,
                '}' if self.characters.next_if_eq(&'}').is_some() => Piece::Text(literal('}')),
                '}' => {
                    return Err(format!(
                        "`{}` has an unmatched `}}`: escape a literal brace as `\\}}`",
                        self.expression
                    ));
                }
                '(' => self.optional()?,
                '/' => Piece::Slash,
                space if space.is_whitespace() => Piece::Space(space.to_string()),
                other => Piece::Text(other.to_string()),
            };
            self.push(piece);
        }
        Ok(self.pieces)
    }

    /// Appends `piece`, joining it to a previous piece of the same kind.
    fn push(&mut self, piece: Piece) {
        match (self.pieces.last_mut(), piece) {
            (Some(Piece::Text(text)), Piece::Text(more))
            | (Some(Piece::Space(text)), Piece::Space(more)) => text.push_str(&more),
            (_, piece) => self.pieces.push(piece),
        }
    }

    /// The character after a `\`, as literal text. A trailing `\` is itself.
    fn escaped(&mut self) -> String { literal(self.characters.next().unwrap_or('\\')) }

    /// The optional text after a `(`, up to its `)`.
    fn optional(&mut self) -> Result<Piece, String> {
        let mut text = String::new();
        loop {
            match self.characters.next() {
                None => {
                    return Err(format!(
                        "`{}` has an unclosed `(`: escape a literal parenthesis as `\\(`",
                        self.expression
                    ));
                }
                Some(')') => break,
                Some('\\') => text.push_str(&self.escaped()),
                Some(nested @ ('(' | '{' | '/')) => {
                    return Err(format!(
                        "`{}` has `{nested}` inside optional text: optional text is literal; \
                         escape it as `\\{nested}`",
                        self.expression
                    ));
                }
                Some(other) => text.push_str(&literal(other)),
            }
        }
        if text.is_empty() {
            return Err(format!(
                "`{}` has empty optional text `()`: escape literal parentheses as `\\(\\)`",
                self.expression
            ));
        }
        Ok(Piece::Optional(text))
    }

    /// The placeholder after a `{`, up to its `}`, bound to its parameter.
    fn placeholder(&mut self) -> Result<Piece, String> {
        let mut content = String::new();
        loop {
            match self.characters.next() {
                None => {
                    return Err(format!(
                        "`{}` has an unclosed `{{`: escape a literal brace as `\\{{`",
                        self.expression
                    ));
                }
                Some('}') => break,
                Some(other) => content.push(other),
            }
        }
        let position = self.position;
        self.position += 1;
        let rendered = match content.as_str() {
            "" | "word" => {
                let param = self.positional(&content, position)?;
                placeholder(param.placeholder_name(), numeric_hint(param).as_deref())
            }
            "int" => {
                let param = self.positional(&content, position)?;
                let hint = integer_hint(param).unwrap_or_else(|| "i64".to_owned());
                placeholder(param.placeholder_name(), Some(&hint))
            }
            "float" => {
                let param = self.positional(&content, position)?;
                let hint = float_hint(param).unwrap_or_else(|| "f64".to_owned());
                placeholder(param.placeholder_name(), Some(&hint))
            }
            "string" => {
                let param = self.positional(&content, position)?;
                placeholder(param.placeholder_name(), Some("string"))
            }
            named => self.named(named)?,
        };
        Ok(Piece::Param(rendered))
    }

    /// The parameter a Cucumber placeholder at `position` binds.
    fn positional(&self, content: &str, position: usize) -> Result<&'a ParamInfo, String> {
        self.params.get(position).ok_or_else(|| {
            format!(
                "`{}`: `{{{content}}}` is placeholder {} but the step function has {} \
                 non-fixture parameter(s) to bind it to",
                self.expression,
                position + 1,
                self.params.len()
            )
        })
    }

    /// An rstest-bdd placeholder `name` or `name:hint`, which keeps its name.
    fn named(&self, content: &str) -> Result<String, String> {
        let (name, hint) = content
            .split_once(':')
            .map_or((content, None), |(name, hint)| (name, Some(hint)));
        let mut characters = name.chars();
        let valid = characters
            .next()
            .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
            && characters.all(|rest| rest.is_ascii_alphanumeric() || rest == '_')
            && hint.is_none_or(|hint| !hint.is_empty());
        if !valid {
            return Err(format!(
                "`{}`: `{{{content}}}` is not a parameter: write `{{int}}`, `{{float}}`, \
                 `{{string}}`, `{{word}}`, `{{}}` or `{{name}}`, and escape a literal brace as \
                 `\\{{`",
                self.expression
            ));
        }
        let hint = hint.map(str::to_owned).or_else(|| {
            self.params
                .iter()
                .find(|param| param.placeholder_name() == name)
                .and_then(numeric_hint)
        });
        Ok(placeholder(name, hint.as_deref()))
    }
}

/// The error for an expression with more than [`MAX_EXPANSIONS`] patterns.
fn too_many(expression: &str) -> String {
    format!(
        "`{expression}` expands to more than {MAX_EXPANSIONS} patterns: split it into several \
         expressions"
    )
}

/// Every text `pieces` stands for, where `pieces` hold no space or slash.
///
/// Fails as soon as the texts would pass [`MAX_EXPANSIONS`], before they are
/// built: each optional group doubles them.
fn expand_run(expression: &str, pieces: &[Piece]) -> Result<Vec<String>, String> {
    let mut texts = vec![String::new()];
    for piece in pieces {
        match piece {
            Piece::Text(text) | Piece::Param(text) | Piece::Space(text) => {
                for existing in &mut texts {
                    existing.push_str(text);
                }
            }
            Piece::Optional(text) => {
                if texts.len().saturating_mul(2) > MAX_EXPANSIONS {
                    return Err(too_many(expression));
                }
                texts = texts
                    .into_iter()
                    .flat_map(|existing| {
                        let with = format!("{existing}{text}");
                        [existing, with]
                    })
                    .collect();
            }
            Piece::Slash => {}
        }
    }
    Ok(texts)
}

/// Every text one whitespace-free word stands for, alternatives in order.
fn expand_word(expression: &str, word: &[Piece]) -> Result<Vec<String>, String> {
    if !word.contains(&Piece::Slash) {
        return expand_run(expression, word);
    }
    let mut texts: Vec<String> = Vec::new();
    for alternative in word.split(|piece| *piece == Piece::Slash) {
        if alternative.is_empty() {
            return Err(format!(
                "`{expression}` has an empty alternative: escape a literal slash as `\\/`"
            ));
        }
        if alternative
            .iter()
            .any(|piece| matches!(piece, Piece::Param(_)))
        {
            return Err(format!(
                "`{expression}` has a parameter inside an alternation: alternatives are \
                 literal text"
            ));
        }
        for text in expand_run(expression, alternative)? {
            if !texts.contains(&text) {
                texts.push(text);
            }
        }
        if texts.len() > MAX_EXPANSIONS {
            return Err(too_many(expression));
        }
    }
    Ok(texts)
}

/// Expands the Cucumber `expression` into the literal rstest-bdd patterns it
/// stands for.
///
/// `params` are the step function's non-fixture parameters in declaration
/// order: the `n`th placeholder of the expression binds the `n`th of them.
///
/// # Errors
///
/// A message naming the expression when it is malformed, when a Cucumber
/// placeholder has no parameter left to bind, or when it expands to more than
/// [`MAX_EXPANSIONS`] patterns.
pub fn expand(expression: &str, params: &[ParamInfo]) -> Result<Vec<String>, String> {
    let pieces = Lexer::new(expression, params).run()?;
    let mut patterns = vec![String::new()];
    for (is_space, group) in chunk_words(&pieces) {
        let choices = if is_space {
            expand_run(expression, group)?
        } else {
            expand_word(expression, group)?
        };
        if patterns.len().saturating_mul(choices.len()) > MAX_EXPANSIONS {
            return Err(too_many(expression));
        }
        patterns = patterns
            .iter()
            .flat_map(|prefix| {
                choices
                    .iter()
                    .map(move |choice| format!("{prefix}{choice}"))
            })
            .collect();
    }
    Ok(patterns)
}

/// `pieces` split into alternating whitespace and word groups, each flagged
/// with whether it is whitespace.
fn chunk_words(pieces: &[Piece]) -> Vec<(bool, &[Piece])> {
    pieces
        .chunk_by(|left, right| matches!(left, Piece::Space(_)) == matches!(right, Piece::Space(_)))
        .map(|group| {
            (
                group
                    .first()
                    .is_some_and(|piece| matches!(piece, Piece::Space(_))),
                group,
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::{ParamInfo, expand},
        assert2::assert,
        rstest::rstest,
    };

    /// `text` with `<` and `>` for `{` and `}`, which keeps Clippy from reading the
    /// test patterns as format strings.
    fn braced(text: &str) -> String { text.replace('<', "{").replace('>', "}") }

    fn params(declared: &[(&str, &str)]) -> Vec<ParamInfo> {
        declared
            .iter()
            .map(|(name, type_name)| ParamInfo::new(name, type_name))
            .collect()
    }

    #[rstest]
    #[case::plain_text("the broker starts", &[], &["the broker starts"])]
    #[case::int("<int> brokers", &[("count", "i64")], &["<count:i64> brokers"])]
    #[case::int_takes_the_integer_type("<int> brokers", &[("count", "u32")], &["<count:u32> brokers"])]
    #[case::int_defaults_to_i64("<int> brokers", &[("count", "Count")], &["<count:i64> brokers"])]
    #[case::float("ratio <float>", &[("ratio", "Ratio")], &["ratio <ratio:f64>"])]
    #[case::float_takes_f32("ratio <float>", &[("ratio", "f32")], &["ratio <ratio:f32>"])]
    #[case::string("topic <string>", &[("name", "String")], &["topic <name:string>"])]
    #[case::word("policy <word>", &[("policy", "Policy")], &["policy <policy>"])]
    #[case::anonymous("policy <>", &[("policy", "Policy")], &["policy <policy>"])]
    #[case::anonymous_numeric("<> brokers", &[("count", "u16")], &["<count:u16> brokers"])]
    #[case::leading_underscore("<int> brokers", &[("_count", "u8")], &["<count:u8> brokers"])]
    #[case::positional(
        "<word> has <int> partitions",
        &[("topic", "String"), ("partitions", "u32")],
        &["<topic> has <partitions:u32> partitions"]
    )]
    #[case::named_passes_through("<topic> exists", &[("topic", "String")], &["<topic> exists"])]
    #[case::named_hint_passes_through("<n:u8> exist", &[("n", "u64")], &["<n:u8> exist"])]
    #[case::named_gets_numeric_hint("<n> exist", &[("n", "u64")], &["<n:u64> exist"])]
    #[case::named_is_looked_up_by_name(
        "<b> and <a>",
        &[("a", "u8"), ("b", "String")],
        &["<b> and <a:u8>"]
    )]
    #[case::named_then_anonymous(
        "<topic> has <int> partitions",
        &[("topic", "String"), ("partitions", "u32")],
        &["<topic> has <partitions:u32> partitions"]
    )]
    #[case::optional("<int> item(s)", &[("n", "u32")], &["<n:u32> item", "<n:u32> items"])]
    #[case::alternation("a/an topic", &[], &["a topic", "an topic"])]
    #[case::alternation_mid_sentence(
        "it is in my belly/stomach now",
        &[],
        &["it is in my belly now", "it is in my stomach now"]
    )]
    #[case::alternation_with_optional(
        "the color/colour(s)",
        &[],
        &["the color", "the colour", "the colours"]
    )]
    #[case::combinations(
        "a/an topic(s)",
        &[],
        &["a topic", "a topics", "an topic", "an topics"]
    )]
    #[case::escaped_parenthesis("the \\(default\\) policy", &[], &["the (default) policy"])]
    #[case::escaped_slash("compact\\/delete", &[], &["compact/delete"])]
    #[case::escaped_brace("a \\<literal\\> brace", &[], &["a \\<literal\\> brace"])]
    #[case::doubled_brace("a <<literal>> brace", &[], &["a \\<literal\\> brace"])]
    #[case::escaped_backslash("a \\\\ b", &[], &["a \\\\ b"])]
    #[case::trailing_backslash("a \\", &[], &["a \\\\"])]
    #[case::stray_close_paren("a ) b", &[], &["a ) b"])]
    #[case::whitespace_kept("a  b\tc", &[], &["a  b\tc"])]
    #[case::sixteen(
        "a/b c/d e/f g/h",
        &[],
        &[
            "a c e g", "a c e h", "a c f g", "a c f h", "a d e g", "a d e h", "a d f g",
            "a d f h", "b c e g", "b c e h", "b c f g", "b c f h", "b d e g", "b d e h",
            "b d f g", "b d f h",
        ]
    )]
    fn expands_expressions(
        #[case] expression: &str,
        #[case] declared: &[(&str, &str)],
        #[case] expected: &[&str],
    ) {
        let expanded = expand(&braced(expression), &params(declared));
        let expected: Vec<String> = expected.iter().map(|text| braced(text)).collect();
        assert!(expanded == Ok(expected));
    }

    #[rstest]
    #[case::no_parameter_left(
        "<int> and <int>",
        &[("a", "u8")],
        "`<int> and <int>`: `<int>` is placeholder 2 but the step function has 1 non-fixture \
         parameter(s) to bind it to"
    )]
    #[case::no_parameters(
        "<string>",
        &[],
        "`<string>`: `<string>` is placeholder 1 but the step function has 0 non-fixture \
         parameter(s) to bind it to"
    )]
    #[case::unclosed_brace(
        "a <int",
        &[("a", "u8")],
        "`a <int` has an unclosed `<`: escape a literal brace as `\\<`"
    )]
    #[case::unmatched_brace("a > b", &[], "`a > b` has an unmatched `>`: escape a literal brace as `\\>`")]
    #[case::bad_placeholder(
        "a <1> b",
        &[("a", "u8")],
        "`a <1> b`: `<1>` is not a parameter: write `<int>`, `<float>`, `<string>`, `<word>`, \
         `<>` or `<name>`, and escape a literal brace as `\\<`"
    )]
    #[case::empty_hint(
        "a <n:> b",
        &[("n", "u8")],
        "`a <n:> b`: `<n:>` is not a parameter: write `<int>`, `<float>`, `<string>`, `<word>`, \
         `<>` or `<name>`, and escape a literal brace as `\\<`"
    )]
    #[case::unclosed_optional(
        "item(s",
        &[],
        "`item(s` has an unclosed `(`: escape a literal parenthesis as `\\(`"
    )]
    #[case::empty_optional(
        "item()",
        &[],
        "`item()` has empty optional text `()`: escape literal parentheses as `\\(\\)`"
    )]
    #[case::nested_optional(
        "item((s))",
        &[],
        "`item((s))` has `(` inside optional text: optional text is literal; escape it as `\\(`"
    )]
    #[case::placeholder_in_optional(
        "item(<int>)",
        &[],
        "`item(<int>)` has `<` inside optional text: optional text is literal; escape it as `\\<`"
    )]
    #[case::empty_alternative(
        "a/ topic",
        &[],
        "`a/ topic` has an empty alternative: escape a literal slash as `\\/`"
    )]
    #[case::parameter_in_alternation(
        "<int>/none",
        &[("n", "u8")],
        "`<int>/none` has a parameter inside an alternation: alternatives are literal text"
    )]
    #[case::too_many(
        "a/b c/d e/f g/h i(s)",
        &[],
        "`a/b c/d e/f g/h i(s)` expands to more than 16 patterns: split it into several \
         expressions"
    )]
    #[case::too_many_optional_groups_in_one_word(
        "item(s)(s)(s)(s)(s)",
        &[],
        "`item(s)(s)(s)(s)(s)` expands to more than 16 patterns: split it into several \
         expressions"
    )]
    #[case::too_many_alternatives_in_one_word(
        "a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q",
        &[],
        "`a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q` expands to more than 16 patterns: split it into \
         several expressions"
    )]
    fn rejects_malformed_expressions(
        #[case] expression: &str,
        #[case] declared: &[(&str, &str)],
        #[case] message: &str,
    ) {
        assert!(expand(&braced(expression), &params(declared)) == Err(braced(message)));
    }

    /// Thirty optional groups would be a billion texts; the limit stops the
    /// expansion before it builds them.
    #[test]
    fn many_optional_groups_fail_before_they_expand() {
        let expression = format!("item{}", "(s)".repeat(30));
        assert!(expand(&expression, &[]) == Err(super::too_many(&expression)));
    }
}
