//! `sasl.kerberos.principal.to.local.rules` (`auth_to_local`): a Kerberos
//! principal, `service/host@REALM`, turned into the short name ACLs are
//! written against.
//!
//! The grammar and the rule semantics are Kafka's `KerberosShortNamer` and
//! `KerberosRule`, not Hadoop's or MIT's: an entry is the literal `DEFAULT`,
//! or `RULE:[n:format](match)s/from/to/g/L` where each part after the
//! bracket is optional. `DEFAULT` takes the first component of any principal
//! whose realm is the default realm, `(match)` has to match the whole
//! formatted name, a result that still holds a `/` or an `@` is an error
//! rather than a fall-through, and a trailing `L` or `U` changes the case of
//! the result.

use krabka_java_regex::JavaPattern;

/// Why a rule is not a rule, or why a principal did not map.
#[derive(Debug, thiserror::Error)]
pub enum KerberosNameError {
    /// The spec is neither `DEFAULT` nor a well-formed `RULE:` entry, or one
    /// of its regular expressions does not compile.
    #[error("invalid auth_to_local rule {0:?}")]
    Rule(String),
    /// The principal has more than two components, or a realm that holds a
    /// `/`.
    #[error("malformed Kerberos name {0:?}")]
    Malformed(String),
    /// No rule produced a name.
    #[error("no auth_to_local rule matched {0}")]
    NoMatch(String),
    /// A rule produced a name that still holds a `/` or an `@`.
    #[error("non-simple name {0} after an auth_to_local rule")]
    NonSimple(String),
    /// A rule's format or replacement refers to something that does not
    /// exist.
    #[error("bad auth_to_local rule: {0}")]
    Format(String),
}

/// What a rule does to the case of its result.
#[derive(Debug, Clone, Copy)]
enum Case {
    Preserve,
    Lower,
    Upper,
}

/// The `s/from/to/[g]` part of a rule.
#[derive(Debug, Clone)]
struct Substitution {
    from: JavaPattern,
    to: String,
    global: bool,
}

/// A `RULE:[n:format](match)s/from/to/[g][/][L|U]` entry.
#[derive(Debug, Clone)]
pub struct Translation {
    components: usize,
    format: String,
    guard: Option<JavaPattern>,
    substitution: Option<Substitution>,
    case: Case,
}

/// One parsed entry of `sasl.kerberos.principal.to.local.rules`.
#[derive(Debug, Clone)]
pub enum KerberosRule {
    /// `DEFAULT`: the first component of a principal in the default realm.
    Default,
    /// `RULE:[n:format](match)s/from/to/[g][/][L|U]`.
    Translate(Translation),
}

impl KerberosRule {
    /// Parses one spec, the way `KerberosShortNamer.parseRules` does: the
    /// whole spec has to be consumed.
    ///
    /// # Errors
    ///
    /// [`KerberosNameError::Rule`] when the spec is not a rule.
    pub fn parse(spec: &str) -> Result<Self, KerberosNameError> {
        let spec = spec.trim();
        if spec == "DEFAULT" {
            return Ok(Self::Default);
        }
        let invalid = || KerberosNameError::Rule(spec.to_owned());
        let rest = spec.strip_prefix("RULE:[").ok_or_else(invalid)?;
        let (count, rest) = rest.split_once(':').ok_or_else(invalid)?;
        // `Integer.parseInt(group(5))`, where the group is `\d*`.
        if count.is_empty() || !count.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(invalid());
        }
        let components = count.parse::<i32>().map_err(|_| invalid())?;
        let (format, mut rest) = rest.split_once(']').ok_or_else(invalid)?;

        let mut guard = None;
        if let Some(after) = rest.strip_prefix('(')
            && let Some((pattern, after)) = after.split_once(')')
        {
            guard = Some(JavaPattern::compile(pattern).map_err(|_| invalid())?);
            rest = after;
        }

        let mut substitution = None;
        if let Some(after) = rest.strip_prefix("s/")
            && let Some((from, after)) = after.split_once('/')
            && let Some((to, after)) = after.split_once('/')
        {
            let global = after.starts_with('g');
            substitution = Some(Substitution {
                from: JavaPattern::compile(from).map_err(|_| invalid())?,
                to: to.to_owned(),
                global,
            });
            rest = after.strip_prefix('g').unwrap_or(after);
        }

        rest = rest.strip_prefix('/').unwrap_or(rest);
        let case = match rest.chars().next() {
            Some('L') => Case::Lower,
            Some('U') => Case::Upper,
            _ => Case::Preserve,
        };
        if let Some(stripped) = rest.strip_prefix(['L', 'U']) {
            rest = stripped;
        }
        if !rest.is_empty() {
            return Err(invalid());
        }
        Ok(Self::Translate(Translation {
            components: usize::try_from(components).map_err(|_| invalid())?,
            format: format.to_owned(),
            guard,
            substitution,
            case,
        }))
    }

    /// `KerberosRule.apply`: the short name `params` maps to, `None` when this
    /// rule does not apply, or an error where Kafka throws. `params` is the
    /// realm followed by the components.
    fn apply(
        &self,
        params: &[&str],
        default_realm: &str,
    ) -> Result<Option<String>, KerberosNameError> {
        let (result, case) = match self {
            Self::Default => (
                match params {
                    [realm, first, ..] if *realm == default_realm => Some((*first).to_owned()),
                    _ => None,
                },
                Case::Preserve,
            ),
            Self::Translate(translation) => (translation.apply(params)?, translation.case),
        };
        let Some(result) = result else {
            return Ok(None);
        };
        if result.contains(['/', '@']) {
            return Err(KerberosNameError::NonSimple(result));
        }
        Ok(Some(match case {
            Case::Preserve => result,
            Case::Lower => result.to_lowercase(),
            Case::Upper => result.to_uppercase(),
        }))
    }
}

impl Translation {
    fn apply(&self, params: &[&str]) -> Result<Option<String>, KerberosNameError> {
        if params.len() - 1 != self.components {
            return Ok(None);
        }
        let base = replace_parameters(&self.format, params)?;
        if let Some(guard) = &self.guard
            && !guard
                .matches(&base)
                .map_err(|error| KerberosNameError::Format(error.to_string()))?
        {
            return Ok(None);
        }
        let Some(substitution) = &self.substitution else {
            return Ok(Some(base));
        };
        substitution
            .from
            .replace(&base, &substitution.to, substitution.global)
            .map(Some)
            .map_err(|error| KerberosNameError::Format(error.to_string()))
    }
}

/// `KerberosRule.replaceParameters`: `$0` is the realm and `$1` onwards the
/// components. A `$` with no number, or with one past the last component, is
/// an error.
fn replace_parameters(format: &str, params: &[&str]) -> Result<String, KerberosNameError> {
    let mut out = String::with_capacity(format.len());
    let mut chars = format.chars().peekable();
    while let Some(character) = chars.next() {
        if character != '$' {
            out.push(character);
            continue;
        }
        let mut digits = String::new();
        while let Some(digit) = chars.next_if(char::is_ascii_digit) {
            digits.push(digit);
        }
        let param = digits
            .parse::<usize>()
            .ok()
            .and_then(|index| params.get(index))
            .ok_or_else(|| {
                KerberosNameError::Format(format!(
                    "index {digits:?} from {format} is outside of the valid range 0 to {}",
                    params.len() - 1
                ))
            })?;
        out.push_str(param);
    }
    Ok(out)
}

/// `KerberosName.parse` followed by `KerberosShortNamer.shortName`: the short
/// name `principal` maps to under `rules`, where `default_realm` is the realm
/// `DEFAULT` accepts.
///
/// A principal with no `@` is already a short name and maps to itself. An
/// empty `default_realm` is Kafka's answer when the realm cannot be found,
/// and `DEFAULT` then matches nothing.
///
/// # Errors
///
/// [`KerberosNameError`] when the principal is malformed, no rule produces a
/// name, or a rule refuses the principal.
pub fn short_name(
    rules: &[KerberosRule],
    principal: &str,
    default_realm: &str,
) -> Result<String, KerberosNameError> {
    let Some((head, realm)) = principal.split_once('@') else {
        return Ok(principal.to_owned());
    };
    // `([^/@]*)(/([^/@]*))?@([^/@]*)`
    let (service, host) = match head.split_once('/') {
        Some((service, host)) => (service, Some(host)),
        None => (head, None),
    };
    if [service, host.unwrap_or_default(), realm]
        .iter()
        .any(|part| part.contains(['/', '@']))
    {
        return Err(KerberosNameError::Malformed(principal.to_owned()));
    }
    let mut params = vec![realm, service];
    params.extend(host);
    for rule in rules {
        if let Some(name) = rule.apply(&params, default_realm)? {
            return Ok(name);
        }
    }
    Err(KerberosNameError::NoMatch(principal.to_owned()))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest_bdd::{Slot, datatable::Rows},
    };

    fn map(specs: &[&str], principal: &str, default_realm: &str) -> Option<String> {
        let rules: Vec<_> = specs
            .iter()
            .map(|spec| KerberosRule::parse(spec).unwrap_or_else(|_| panic!("{spec} parses")))
            .collect();
        short_name(&rules, principal, default_realm).ok()
    }

    #[derive(Default)]
    struct KerberosPrincipalScenario {
        rules: Vec<String>,
        principal: String,
        realm: String,
        short_name: Slot<Option<String>>,
    }

    #[derive(rstest_bdd_macros::DataTableRow)]
    struct LocalNameRule {
        rule: String,
    }

    #[rstest::fixture]
    fn kerberos_principal() -> KerberosPrincipalScenario {
        // Keep kerberos principal inputs and observations fresh for each scenario.
        KerberosPrincipalScenario::default()
    }

    #[rstest_bdd_macros::given("the Kerberos default realm is {realm}")]
    fn kerberos_default_realm(kerberos_principal: &mut KerberosPrincipalScenario, realm: String) {
        kerberos_principal.realm = realm;
    }

    #[rstest_bdd_macros::given("the ordered Kerberos short-name rules are:")]
    fn kerberos_mapping_rules(
        kerberos_principal: &mut KerberosPrincipalScenario,
        #[datatable] rows: Rows<LocalNameRule>,
    ) {
        kerberos_principal.rules = rows.into_iter().map(|row| row.rule).collect();
    }

    #[rstest_bdd_macros::given("the authenticated Kerberos principal is {principal}")]
    fn authenticated_kerberos_principal(
        kerberos_principal: &mut KerberosPrincipalScenario,
        principal: String,
    ) {
        kerberos_principal.principal = principal;
    }

    #[rstest_bdd_macros::when("the broker derives the Kerberos principal's ACL short name")]
    fn map_kerberos_principal(kerberos_principal: &mut KerberosPrincipalScenario) {
        let rules: Vec<_> = kerberos_principal
            .rules
            .iter()
            .map(String::as_str)
            .collect();
        kerberos_principal.short_name.set(map(
            &rules,
            &kerberos_principal.principal,
            &kerberos_principal.realm,
        ));
    }

    #[rstest_bdd_macros::then("the Kerberos ACL short name is {expected}")]
    fn kerberos_short_name(kerberos_principal: &KerberosPrincipalScenario, expected: String) {
        check!(kerberos_principal.short_name.get() == Some(Some(expected)));
    }

    #[rstest_bdd_macros::then("the Kerberos principal cannot be mapped to an ACL short name")]
    fn unmapped_kerberos_principal(kerberos_principal: &KerberosPrincipalScenario) {
        check!(kerberos_principal.short_name.get() == Some(None));
    }

    rstest_bdd_macros::scenarios!(
        "tests/features/migrated/src/network/auth/kerberos_name.feature",
        fixtures = [kerberos_principal: KerberosPrincipalScenario]
    );

    /// Kafka does not know the default realm on a host with no krb5 config,
    /// and `DEFAULT` then maps nothing.
    #[test]
    fn an_unknown_default_realm_maps_nothing_under_default() {
        check!(map(&["DEFAULT"], "alice@ANY.REALM", "") == None);
        check!(
            map(
                &["RULE:[1:$1@$0](.*@ANY.REALM)s/@.*//"],
                "alice@ANY.REALM",
                ""
            ) == Some("alice".to_owned())
        );
    }

    #[yare::parameterized(
        lowercase_default = { "default" },
        suffixed_default = { "DEFAULT2" },
        unclosed_rule = { "RULE:[1:$1" },
        empty_field = { "RULE:[:$1]" },
        nonnumeric_field = { "RULE:[x:$1]" },
        overflowing_field = { "RULE:[99999999999:$1]" },
        unclosed_match = { "RULE:[1:$1](abc" },
        missing_substitution_target = { "RULE:[1:$1]s/a/b" },
        incomplete_substitution = { "RULE:[1:$1]s/a" },
        unsupported_flag = { "RULE:[1:$1]/X" },
        invalid_flag = { "RULE:[1:$1]/Lx" },
        nested_match = { "RULE:[1:$1](a(b))" },
        unmatched_character_class = { "RULE:[1:$1](a[)" },
        escaped_slash_splits_rule = { "RULE:[1:$1]s/^(.*)\\/(.*)$/$2/" },
        non_rule = { "NOT_A_RULE:::" },
    )]
    fn malformed_rules_are_rejected(spec: &str) {
        check!(KerberosRule::parse(spec).is_err(), "{spec}");
    }

    #[yare::parameterized(
        default = { "DEFAULT" },
        minimal_rule = { "RULE:[1:$1]" },
        match_and_substitute = { "RULE:[1:$1](.*)s/a/b//L" },
        global_uppercase_substitution = { "RULE:[1:$1](.*)s/a/b/gU" },
        second_field_and_global_lowercase = { "RULE:[2:$1@$2]s/@.*//g/L" },
        lowercase_flag = { "RULE:[1:$1]/L" },
        padded_default = { "  DEFAULT " },
    )]
    fn well_formed_rules_are_accepted(spec: &str) {
        check!(KerberosRule::parse(spec).is_ok(), "{spec}");
    }
}
