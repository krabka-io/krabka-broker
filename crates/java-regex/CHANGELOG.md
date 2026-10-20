# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-java-regex`.

## [Unreleased]

### Fixed

- Bounds where Java has no atom to repeat, as in `a*{2}`, `a{2}{3}`, `{2}`
  and `(?i){2}`, repeat the empty atom as in Java, instead of the atom
  before them or the text of the braces. `(?i){2}` is no longer refused.
- A quantified group that Java repeats with its `GroupCurly` takes no empty
  iteration past its least count, so `()*\1` no longer matches the empty
  string. The groups inside it keep their text when the match backtracks
  out of the repeat, as in Java: `(?:((a))*y|a)\2` matches `aa`.
- A look-behind too large for the `regex` crate to build, such as
  `(?<=x{1,1000000000})`, is refused in milliseconds. `fancy-regex` built it
  for minutes.
- A look-behind whose maximum length overflows Java's `int`, such as
  `(?<=a*b*)`, is refused where Java then tries fewer starts than the pattern
  allows, instead of matching where Java does not.
