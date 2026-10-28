//! Reading the ACL set that `kafka-acls --list` printed.
//!
//! The shorthand flags are the reason this module exists. `--producer`,
//! `--consumer`, `--idempotent` and the rest are not requests the broker ever
//! sees: `AclCommand` expands each one, client-side, into several
//! `AclBinding`s across several resource types, and sends those. What a suite
//! can check is therefore not the flag but its consequence -- the set that
//! comes back from `--list` afterwards -- and a set is only comparable once it
//! is parsed out of the tool's two-level rendering.
//!
//! That rendering nests: a "Current ACLs for resource ResourcePattern(…):"
//! header, then one indented `(principal=…, host=…, operation=…,
//! permissionType=…)` line per entry under it. [`parse_acls`] flattens the two
//! levels into one binding per entry, which is the form the two sides are
//! compared in and the form a case can write an expectation in.

use std::collections::BTreeSet;

/// One ACL binding, flattened out of the resource it was printed under.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct AclBinding {
    /// `TOPIC`, `GROUP`, `CLUSTER`, `TRANSACTIONAL_ID`, `DELEGATION_TOKEN`…
    pub(crate) resource_type: String,
    pub(crate) resource_name: String,
    /// `LITERAL` or `PREFIXED`.
    pub(crate) pattern_type: String,
    pub(crate) principal: String,
    pub(crate) host: String,
    pub(crate) operation: String,
    /// `ALLOW` or `DENY`.
    pub(crate) permission: String,
}

/// The resource a `ResourcePattern(` line names, which the entries under it
/// belong to.
#[derive(Clone)]
struct ResourcePattern {
    resource_type: String,
    resource_name: String,
    pattern_type: String,
}

/// Parse every binding a `--list` printed, as a set.
///
/// A set rather than a list: the tool walks the authorizer's own iteration
/// order, which neither broker promises and which differs between the two
/// sides for the same bindings. What the shorthands are about is *which*
/// bindings exist, and that is exactly what a set states.
pub fn parse_acls(stdout: &str) -> BTreeSet<AclBinding> {
    let mut bindings = BTreeSet::new();
    let mut resource: Option<ResourcePattern> = None;
    for line in stdout.lines() {
        if line.contains("ResourcePattern(") {
            resource = Some(ResourcePattern {
                resource_type: field(line, "resourceType=").unwrap_or_default(),
                resource_name: field(line, "name=").unwrap_or_default(),
                pattern_type: field(line, "patternType=").unwrap_or_default(),
            });
        } else if line.contains("principal=") {
            let Some(ResourcePattern {
                resource_type,
                resource_name,
                pattern_type,
            }) = resource.clone()
            else {
                continue;
            };
            bindings.insert(AclBinding {
                resource_type,
                resource_name,
                pattern_type,
                principal: field(line, "principal=").unwrap_or_default(),
                host: field(line, "host=").unwrap_or_default(),
                operation: field(line, "operation=").unwrap_or_default(),
                permission: field(line, "permissionType=").unwrap_or_default(),
            });
        }
    }
    bindings
}

/// The value of one `key=value` field of a rendered Java object.
///
/// The fields are separated by `, ` and the object is closed by `)`, so a
/// value ends at whichever of the two comes first. `name=` needs the second
/// terminator as often as the first, because it is the last field of an
/// `AccessControlEntry`-shaped render and the first of a `ResourcePattern`.
fn field(line: &str, key: &str) -> Option<String> {
    let (_, rest) = line.split_once(key)?;
    // The value ends at the first `, ` or `)`, whichever comes first.
    let value = rest.split(", ").next()?.split(')').next()?;
    Some(value.trim().to_owned())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest::fixture,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    struct AclListing {
        stdout: String,
        parsed: Vec<BTreeSet<AclBinding>>,
    }

    #[fixture]
    fn listing() -> AclListing {
        // Start this scenario with fresh inputs and observations.

        AclListing {
            stdout: String::new(),
            parsed: Vec::new(),
        }
    }

    #[given("the ACL tool printed")]
    fn tool_listing(listing: &mut AclListing, docstring: String) { listing.stdout = docstring; }

    #[when("the ACL listing is read")]
    fn read_listing(listing: &mut AclListing) {
        listing.parsed = vec![parse_acls(&listing.stdout)];
    }

    #[when("the ACL listing is read in its original and reversed resource order")]
    fn read_both_orders(listing: &mut AclListing) {
        let mut resources = listing.stdout.split("\n\n").collect::<Vec<_>>();
        resources.reverse();
        listing.parsed = vec![
            parse_acls(&listing.stdout),
            parse_acls(&resources.join("\n\n")),
        ];
    }

    #[then("every listing contains exactly these bindings")]
    fn expected_bindings(listing: &AclListing, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission,
                ]: [String; 7] = row.try_into().expect("seven ACL binding fields");
                AclBinding {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission,
                }
            })
            .collect::<BTreeSet<_>>();
        check!(!listing.parsed.is_empty());
        for actual in &listing.parsed {
            check!(*actual == expected);
        }
    }

    #[then("the listing contains no ACL bindings")]
    fn empty_listing(listing: &AclListing) {
        check!(!listing.parsed.is_empty());
        for bindings in &listing.parsed {
            check!(bindings.is_empty());
        }
    }

    scenarios!("tests/features/migrated/tests/jvm_acceptance_sasl/acl_output.feature", fixtures = [listing: AclListing]);
}
