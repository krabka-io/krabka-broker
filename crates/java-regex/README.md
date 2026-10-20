# krabka-java-regex

[![Crates.io](https://img.shields.io/crates/v/krabka-java-regex.svg)](https://crates.io/crates/krabka-java-regex)
[![Docs.rs](https://docs.rs/krabka-java-regex/badge.svg)](https://docs.rs/krabka-java-regex)

Java's `java.util.regex` in Rust: `Pattern.compile`, `Matcher.matches()`, `String.replaceAll` and `String.replaceFirst`, answered as JDK 25 answers them.

Part of [Krabka](https://github.com/robot-head/crabka), a Rust implementation of Apache Kafka.

## Overview

Three Apache Kafka configs take a Java regular expression: `ssl.principal.mapping.rules` (KIP-371), the Kerberos `auth_to_local` rules, and the `match` patterns of a KIP-714 `CLIENT_METRICS` subscription. A rule that matches differently from Java maps a principal to another name, or selects other clients, so `krabka-broker` needs Java's answer exactly. This crate gives it. It rewrites the Java pattern for [`fancy-regex`](https://crates.io/crates/fancy-regex), and runs the constructs `fancy-regex` reads otherwise on a backtracking matcher of its own.

## Features

- `JavaPattern::matches` is `Matcher.matches()`: some way of matching spans the whole input, through backtracking and lazy quantifiers.
- `JavaPattern::replace` is `replaceAll` or `replaceFirst`. Its matches are `Matcher.find`'s, empty matches and `\G` included, and its replacement syntax is `appendReplacement`'s: `$n`, `${name}` and `\x`.
- Java's reading of the syntax:
  - the ASCII `\w`, `\d`, `\s`, `\b` and POSIX classes, and their Unicode forms under `(?U)`
  - `.`, `$`, `^` and `\Z` with Java's line terminators, under `(?s)`, `(?m)` and `(?d)`
  - `\h`, `\v` and `\R`
  - ASCII case folding under `(?i)`, and Java's own Unicode case folding under `(?iu)`
  - class intersection with `&&`
  - `\Q…\E` and `(?x)` comments
- Java's matcher, where `fancy-regex` differs:
  - backreferences under a case flag
  - `\R` giving back the `\n` of `\r\n`
  - groups that keep their text after a look-around, an atomic group, a possessive quantifier or an iteration of a repeated group
  - empty iterations of a repeated group: kept by Java's `Loop`, and skipped by its `GroupCurly` past the least count
  - `\G` after an empty match
- `JavaPattern::compile` refuses what `Pattern.compile` refuses: duplicate group names, a dangling quantifier, a backwards repeat range, an unknown property name, a look-behind with no obvious maximum length, and the rest. It also refuses the few constructs it cannot read as Java does, such as Unicode blocks and `\X`, a look-behind too large to build, and a look-behind whose maximum length overflows Java's `int` and so tries fewer starts, rather than match them another way. Where such a look-behind still tries every start, an input longer than its maximum length is an error.
- A search takes at most 1,000,000 backtracking steps. One past the limit is an error, not a no match, so a mapping rule cannot fall through to the next rule when Java would have applied it. The backtracking matcher keeps its own stack, so a long input cannot overflow the thread's.

## Usage

```rust
use krabka_java_regex::JavaPattern;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rule = JavaPattern::compile("CN=([^,]+),OU=(?i)ops.*")?;
    // `Matcher.matches()`: the whole input, so `OU=OPS-EU` matches.
    if rule.matches("CN=alice,OU=OPS-EU")? {
        // `String.replaceAll`: prints `alice`.
        println!("{}", rule.replace("CN=alice,OU=OPS-EU", "$1", true)?);
    }
    Ok(())
}
```

## Documentation

- [API Documentation](https://docs.rs/krabka-java-regex)

## License

Apache-2.0. Derivative work of [Apache Kafka](https://kafka.apache.org); see [NOTICE](../../NOTICE).
