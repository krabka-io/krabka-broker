//! What `#[derive(RefinedNewtype)]` accepts, refuses, parses and prints.

use assert2::assert;
use krabka_macros::RefinedNewtype;
use refined_type::rule::{GreaterU32, MinMaxU32};

/// The rule's own error, a `u32` getter named `into_value`, and a free parse
/// function.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(MinMaxU32<0, 100>), parse_fn = parse_percent)]
struct Percent(u32);

/// A labelled `String` error, a renamed getter, and every optional impl.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(
    rule(GreaterU32<0>),
    string_error,
    label = "miss limit",
    getter = get,
    default = 3,
    from_str,
    display
)]
struct MissLimit(u32);

/// A `String` error without a label.
#[derive(Clone, Copy, Debug, Eq, PartialEq, RefinedNewtype)]
#[refined(rule(GreaterU32<0>), string_error, from_str)]
struct Attempts(u32);

#[test]
fn new_accepts_what_the_rule_accepts_and_returns_it_unchanged() {
    assert!(Percent::new(100).ok().map(Percent::into_value) == Some(100));
    assert!(MissLimit::new(7).map(MissLimit::get) == Ok(7));
    assert!(Attempts::new(1).map(|attempts| attempts.0) == Ok(1));
}

#[test]
fn new_refuses_with_the_rules_error_its_label_or_neither() {
    let rule_error = MinMaxU32::<0, 100>::new(101).unwrap_err();
    let rule_text = GreaterU32::<0>::new(0).unwrap_err().to_string();

    assert!(Percent::new(101).unwrap_err().to_string() == rule_error.to_string());
    assert!(MissLimit::new(0) == Err(format!("miss limit: {rule_text}")));
    assert!(Attempts::new(0) == Err(rule_text));
}

#[test]
fn default_display_and_from_str() {
    assert!(MissLimit::default().get() == 3);
    assert!(MissLimit::default().to_string() == "3");
    assert!(format!("{:>3}", MissLimit::default()) == "  3");
    assert!("9".parse::<MissLimit>() == Ok(MissLimit(9)));
    assert!("9".parse::<Attempts>() == Ok(Attempts(9)));
}

#[test]
fn text_is_refused_by_the_integer_parse_then_by_the_rule() {
    let rule_text = GreaterU32::<0>::new(0).unwrap_err().to_string();
    let not_a_number = "x".parse::<u32>().unwrap_err().to_string();

    for (text, expected) in [
        ("50", Ok(Percent(50))),
        ("x", Err(not_a_number.clone())),
        (
            "101",
            Err(MinMaxU32::<0, 100>::new(101).unwrap_err().to_string()),
        ),
    ] {
        assert!(parse_percent(text) == expected);
    }
    assert!("x".parse::<MissLimit>() == Err(not_a_number));
    assert!("0".parse::<MissLimit>() == Err(format!("miss limit: {rule_text}")));
    assert!("0".parse::<Attempts>() == Err(rule_text));
}
