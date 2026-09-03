//! KIP-371 `ssl.principal.mapping.rules`: the Subject DN of an mTLS peer
//! certificate turned into the principal name ACLs are written against.
//!
//! Without a rule the principal is the whole RFC 2253 DN, so every ACL entry
//! and every `super_users` line has to pin `CN=alice,OU=x,O=y` verbatim and a
//! certificate reissue that reorders an RDN invalidates them. A rule list
//! rewrites the DN into a short name once, at accept time, so the rest of the
//! broker only ever sees `alice`.
//!
//! The grammar is Kafka's, and it is *not* the Kerberos `auth_to_local`
//! grammar that [`super::gssapi`] leans on: an entry is either the literal
//! `DEFAULT`, which passes the DN through, or `RULE:pattern/replacement/[L|U]`,
//! where `pattern` has to match the whole DN, `replacement` may reference
//! capture groups as `$1`, and the trailing `L` or `U` lowercases or
//! uppercases the result. The first rule that matches wins.

use regex::Regex;

/// Why a rule spec is not a rule.
#[derive(Debug, thiserror::Error)]
pub enum SslPrincipalRuleError {
    /// The spec is neither `DEFAULT` nor a well-formed `RULE:` entry.
    #[error("expected `DEFAULT` or `RULE:pattern/replacement/[L|U]`")]
    Syntax,
    /// The `pattern` between the first two slashes is not a regular
    /// expression.
    #[error("invalid pattern: {0}")]
    Pattern(#[from] regex::Error),
    /// The text after the second slash is not one of `L`, `U` or nothing.
    #[error("unknown case flag `{0}`, expected `L`, `U` or nothing")]
    CaseFlag(String),
}

/// What a rule does to the case of its result.
#[derive(Debug, Clone, Copy)]
enum Case {
    Preserve,
    Lower,
    Upper,
}

/// One parsed entry of `ssl.principal.mapping.rules`.
#[derive(Debug, Clone)]
enum Rule {
    /// `DEFAULT`: the Subject DN is the principal name.
    Default,
    /// `RULE:pattern/replacement/[L|U]`.
    Mapping {
        pattern: Regex,
        replacement: String,
        case: Case,
    },
}

impl Rule {
    /// The principal `distinguished_name` maps to, or `None` when this rule
    /// does not match and the next one should be tried.
    fn apply(&self, distinguished_name: &str) -> Option<String> {
        match self {
            Self::Default => Some(distinguished_name.to_owned()),
            Self::Mapping {
                pattern,
                replacement,
                case,
            } => {
                // Kafka checks `Matcher.matches()`, a whole-input match, before
                // it rewrites, so a pattern that only matches part of the DN
                // falls through to the next rule.
                let found = pattern.find(distinguished_name)?;
                if found.start() != 0 || found.end() != distinguished_name.len() {
                    return None;
                }
                let mapped = pattern
                    .replace_all(distinguished_name, replacement.as_str())
                    .into_owned();
                Some(match case {
                    Case::Preserve => mapped,
                    Case::Lower => mapped.to_lowercase(),
                    Case::Upper => mapped.to_uppercase(),
                })
            }
        }
    }

    /// Parses one spec, the way Kafka's `SslPrincipalMapper.Rule` does.
    fn parse(spec: &str) -> Result<Self, SslPrincipalRuleError> {
        if spec == "DEFAULT" {
            return Ok(Self::Default);
        }
        let body = spec
            .strip_prefix("RULE:")
            .ok_or(SslPrincipalRuleError::Syntax)?;
        let (pattern, replacement, flag) = split_rule(body).ok_or(SslPrincipalRuleError::Syntax)?;
        let case = match flag {
            "" => Case::Preserve,
            "L" => Case::Lower,
            "U" => Case::Upper,
            other => return Err(SslPrincipalRuleError::CaseFlag(other.to_owned())),
        };
        Ok(Self::Mapping {
            // Kafka's grammar escapes a literal slash as `\/`, which Java's
            // regex engine accepts and the `regex` crate rejects, so the
            // escape is undone here rather than passed through.
            pattern: Regex::new(&pattern.replace("\\/", "/"))?,
            replacement: replacement.replace("\\/", "/"),
            case,
        })
    }
}

/// Splits a `RULE:` body into `pattern`, `replacement` and the case flag at
/// its first two unescaped slashes. `None` when there are fewer than two.
fn split_rule(body: &str) -> Option<(&str, &str, &str)> {
    let mut slashes = [0_usize; 2];
    let mut seen = 0_usize;
    let mut escaped = false;
    for (index, character) in body.char_indices() {
        if escaped {
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '/' && seen < 2 {
            slashes[seen] = index;
            seen += 1;
        }
    }
    (seen == 2).then(|| {
        (
            &body[..slashes[0]],
            &body[slashes[0] + 1..slashes[1]],
            &body[slashes[1] + 1..],
        )
    })
}

/// The rule list of one listener's `ssl.principal.mapping.rules`, applied in
/// order with the first match winning.
///
/// The default is Kafka's default value for the property, the single rule
/// `DEFAULT`, which maps every DN to itself.
#[derive(Debug, Clone)]
pub struct SslPrincipalMapper {
    rules: Vec<Rule>,
}

impl Default for SslPrincipalMapper {
    fn default() -> Self {
        Self {
            rules: vec![Rule::Default],
        }
    }
}

impl SslPrincipalMapper {
    /// Parses each spec in order, rejecting the whole list on the first one
    /// that is not a rule.
    ///
    /// # Errors
    ///
    /// [`SslPrincipalRuleError`] when a spec is neither `DEFAULT` nor a
    /// well-formed `RULE:pattern/replacement/[L|U]`.
    pub fn parse<S: AsRef<str>>(specs: &[S]) -> Result<Self, SslPrincipalRuleError> {
        Ok(Self {
            rules: specs
                .iter()
                .map(|spec| Rule::parse(spec.as_ref()))
                .collect::<Result<_, _>>()?,
        })
    }

    /// The principal name for a peer certificate's Subject DN: the first
    /// matching rule's result, or `None` when no rule matches.
    ///
    /// Kafka's `SslPrincipalMapper.getName` throws `NoMatchingRule` in that
    /// case rather than falling back to the DN, and the DN pass-through is the
    /// `DEFAULT` rule's job. An operator whose rule list is exhaustive by
    /// design therefore gets a rejected connection, not a peer authenticated
    /// under its full DN.
    #[must_use]
    pub fn apply(&self, distinguished_name: &str) -> Option<String> {
        self.rules
            .iter()
            .find_map(|rule| rule.apply(distinguished_name))
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// Kafka's own documented `ssl.principal.mapping.rules` examples, each
    /// against a DN it matches and one it does not.
    #[test]
    fn kafka_documented_rules_map_the_subject_dn() {
        // (rules, distinguished name, principal)
        let cases = [
            (
                &["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"][..],
                "CN=serviceuser,OU=ServiceUsers,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                Some("serviceuser"),
            ),
            // No rule matches and there is no DEFAULT tail, so the mapping is
            // refused rather than falling back to the DN.
            (
                &["RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/"][..],
                "CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                None,
            ),
            (
                &["RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L"][..],
                "CN=adminUser,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                Some("adminuser@admin"),
            ),
            (
                &["RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U"][..],
                "cn=User,OU=Admin,O=Unknown,L=Unknown,ST=Unknown,C=Unknown",
                Some("USER"),
            ),
            (
                &["DEFAULT"][..],
                "CN=alice,OU=x,O=y",
                Some("CN=alice,OU=x,O=y"),
            ),
            // First match wins: the specific rule shadows the DEFAULT tail.
            (
                &["RULE:^CN=(.*?),.*$/$1/", "DEFAULT"][..],
                "CN=alice,OU=integration,O=krabka",
                Some("alice"),
            ),
            // The DEFAULT tail is what passes an unmatched DN through.
            (
                &["RULE:^CN=(.*?),.*$/$1/", "DEFAULT"][..],
                "OU=integration,O=krabka",
                Some("OU=integration,O=krabka"),
            ),
            // An explicitly empty list has no rule to match, so it maps
            // nothing.
            (&[][..], "CN=alice,OU=x,O=y", None),
        ];
        for (specs, distinguished_name, expected) in cases {
            let mapper =
                SslPrincipalMapper::parse(specs).unwrap_or_else(|_| panic!("{specs:?} parses"));
            check!(
                mapper.apply(distinguished_name).as_deref() == expected,
                "{specs:?} against {distinguished_name}"
            );
        }
    }

    /// The configured default is Kafka's `["DEFAULT"]`, so a listener with no
    /// rules of its own still authenticates a peer under its Subject DN.
    #[test]
    fn the_default_mapper_passes_the_subject_dn_through() {
        assert!(
            SslPrincipalMapper::default().apply("CN=alice,OU=x,O=y")
                == Some("CN=alice,OU=x,O=y".to_owned())
        );
    }

    /// A pattern may carry an escaped slash, which the DN it matches carries
    /// literally.
    #[test]
    fn an_escaped_slash_is_part_of_the_pattern() {
        let mapper =
            SslPrincipalMapper::parse(&["RULE:^CN=(.*?)\\/svc$/$1/"]).expect("escaped rule parses");
        assert!(mapper.apply("CN=alice/svc") == Some("alice".to_owned()));
    }

    #[test]
    fn malformed_specs_are_rejected() {
        for spec in [
            "NOT_A_RULE:::",
            "RULE:^CN=(.*?)$",
            "RULE:^CN=(.*?)$/$1",
            "RULE:^CN=(.*?)$/$1/X",
            "RULE:^CN=([a-z$/$1/",
            "default",
        ] {
            check!(SslPrincipalMapper::parse(&[spec]).is_err(), "{spec}");
        }
    }
}
