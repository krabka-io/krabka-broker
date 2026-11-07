#![cfg(test)]
//! The scenarios under `tests/features`: the three Kafka configs that take a
//! Java regex, `ssl.principal.mapping.rules`, the Kerberos `auth_to_local`
//! rules and the KIP-714 `CLIENT_METRICS` match patterns, read as JDK 25
//! reads them.

use {
    assert2::assert,
    krabka_bdd::Given,
    krabka_java_regex::{JavaPattern, MatchError, PatternError},
    rstest_bdd_macros::{given, then, when},
};

/// Either Given fills the pattern: an SSL principal mapping rule, or a client
/// metrics subscription.
const GIVEN_PATTERN: &str = "Given an SSL principal mapping rule with pattern <pattern>";
const GIVEN_SUBSTITUTION: &str = "Given a Kerberos substitution <substitution>";
const WHEN_TESTED: &str = "When the rule is tested against the distinguished name <name>";
const WHEN_REWRITTEN: &str = "When the substitution rewrites the name <name>";
const WHEN_WRITTEN: &str = "When an operator writes the client metrics match pattern <pattern>";

/// Whether a pattern matched, in the words of the actor that tested it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum Verdict {
    #[step_param("matches", "selects")]
    Matches,
    #[step_param("does not match", "does not select")]
    DoesNotMatch,
}

impl Verdict {
    fn matched(self) -> bool { self == Self::Matches }
}

/// What `Pattern.compile` makes of a pattern an operator writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
enum Compilation {
    #[step_param("accepted")]
    Accepted,
    #[step_param("refused as invalid", "refused as an invalid pattern")]
    Refused,
}

impl Compilation {
    fn of<T>(result: &Result<T, PatternError>) -> Self {
        if result.is_ok() {
            Self::Accepted
        } else {
            Self::Refused
        }
    }
}

/// The `s/pattern/replacement/[g]` part of an `auth_to_local` rule, which
/// Kafka's `KerberosRule` applies with `replaceFirst`, or with `replaceAll`
/// under the `g` flag.
struct Substitution {
    from: JavaPattern,
    to: String,
    global: bool,
}

impl Substitution {
    /// Reads `s/from/to/` or `s/from/to/g`, as Kafka's rule grammar does: no
    /// slash inside either part.
    fn parse(rule: &str) -> Result<Self, PatternError> {
        let parts = rule.strip_prefix("s/").and_then(|rest| {
            let (from, rest) = rest.split_once('/')?;
            let (to, flags) = rest.split_once('/')?;
            Some((from, to, flags))
        });
        let Some((from, to, flags)) = parts else {
            panic!("{rule} is not a substitution of the form s/pattern/replacement/[g]");
        };
        Ok(Self {
            from: JavaPattern::compile(from)?,
            to: to.to_owned(),
            global: flags == "g",
        })
    }

    fn rewrite(&self, name: &str) -> Result<String, MatchError> {
        self.from.replace(name, &self.to, self.global)
    }
}

/// An input as a feature writes it: an invisible char as its Java escape,
/// `\n`, `\r` or `\uXXXX`. Patterns are not decoded, because Java's pattern
/// syntax reads those escapes itself.
fn java_input(text: &str) -> String {
    let mut decoded = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(character) = chars.next() {
        if character != '\\' {
            decoded.push(character);
            continue;
        }
        match chars.next() {
            Some('n') => decoded.push('\n'),
            Some('r') => decoded.push('\r'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                let code = u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32);
                let Some(code) = code else {
                    panic!("\\u{hex} in {text} is not a char");
                };
                decoded.push(code);
            }
            other => panic!("{text} has an escape this notation does not define: {other:?}"),
        }
    }
    decoded
}

struct World {
    pattern: Given<JavaPattern>,
    substitution: Given<Substitution>,
    tested: Given<Result<bool, MatchError>>,
    rewritten: Given<Result<String, MatchError>>,
    compiled: Given<Result<(), PatternError>>,
}

#[rstest::fixture]
fn world() -> World {
    World {
        pattern: Given::new("pattern", GIVEN_PATTERN),
        substitution: Given::new("substitution", GIVEN_SUBSTITUTION),
        tested: Given::new("tested", WHEN_TESTED),
        rewritten: Given::new("rewritten", WHEN_REWRITTEN),
        compiled: Given::new("compiled", WHEN_WRITTEN),
    }
}

fn compiled(pattern: &str) -> JavaPattern {
    JavaPattern::compile(pattern)
        .unwrap_or_else(|error| panic!("{pattern} does not compile: {error}"))
}

#[given("an SSL principal mapping rule with pattern {pattern}")]
fn ssl_rule(world: &mut World, pattern: String) { world.pattern.set(compiled(&pattern)); }

#[given("a client metrics subscription that matches client ids with pattern {pattern}")]
fn client_metrics_subscription(world: &mut World, pattern: String) {
    world.pattern.set(compiled(&pattern));
}

#[given("a Kerberos substitution {substitution}")]
fn kerberos_substitution(world: &mut World, substitution: String) {
    let parsed = Substitution::parse(&substitution)
        .unwrap_or_else(|error| panic!("{substitution} does not compile: {error}"));
    world.substitution.set(parsed);
}

#[when("the rule is tested against the distinguished name {name}")]
fn test_distinguished_name(world: &mut World, name: String) {
    let tested = world.pattern.get().matches(&java_input(&name));
    world.tested.set(tested);
}

#[when("the subscription is tested against the client id {id}")]
fn test_client_id(world: &mut World, id: String) {
    let tested = world.pattern.get().matches(&java_input(&id));
    world.tested.set(tested);
}

#[when("the substitution rewrites the name {name}")]
fn rewrite_name(world: &mut World, name: String) {
    let rewritten = world.substitution.get().rewrite(&java_input(&name));
    world.rewritten.set(rewritten);
}

#[when("the substitution tries to rewrite the name {name}")]
fn try_rewrite_name(world: &mut World, name: String) { rewrite_name(world, name); }

#[when("an operator writes the client metrics match pattern {pattern}")]
fn write_match_pattern(world: &mut World, pattern: String) {
    world.compiled.set(JavaPattern::compile(&pattern).map(drop));
}

#[when("an operator writes the Kerberos substitution {substitution}")]
fn write_substitution(world: &mut World, substitution: String) {
    world
        .compiled
        .set(Substitution::parse(&substitution).map(drop));
}

#[then("the rule {verdict}")]
fn rule_verdict(world: &World, verdict: Verdict) {
    assert!(*world.tested.get() == Ok(verdict.matched()));
}

#[then("the subscription {verdict} the client")]
fn subscription_verdict(world: &World, verdict: Verdict) {
    assert!(*world.tested.get() == Ok(verdict.matched()));
}

#[then("the test fails at the backtrack limit")]
fn backtrack_limit(world: &World) {
    assert!(let Err(_) = world.tested.get());
}

#[then("the rewritten name is {name}")]
fn rewritten_name(world: &World, name: String) {
    assert!(*world.rewritten.get() == Ok(name));
}

#[then("the rewrite is refused")]
fn rewrite_refused(world: &World) {
    assert!(let Err(_) = world.rewritten.get());
}

#[then("the pattern is {compilation}")]
fn pattern_compilation(world: &World, compilation: Compilation) {
    let compiled = world.compiled.get();
    assert!(Compilation::of(compiled) == compilation, "{compiled:?}");
}

#[then("the substitution is {compilation}")]
fn substitution_compilation(world: &World, compilation: Compilation) {
    pattern_compilation(world, compilation);
}

krabka_bdd::scenarios!("tests/features/whole_input_match", fixtures = [world: World]);
krabka_bdd::scenarios!("tests/features/replacement", fixtures = [world: World]);
krabka_bdd::scenarios!("tests/features/empty_matches", fixtures = [world: World]);
krabka_bdd::scenarios!("tests/features/character_classes", fixtures = [world: World]);
krabka_bdd::scenarios!("tests/features/line_terminators", fixtures = [world: World]);
krabka_bdd::scenarios!("tests/features/look_behind", fixtures = [world: World]);
krabka_bdd::scenarios!("tests/features/pattern_compilation", fixtures = [world: World]);
