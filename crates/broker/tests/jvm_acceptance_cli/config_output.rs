//! Reading what `kafka-configs --describe` printed, for each of the entity
//! types it prints differently.
//!
//! `ConfigCommand` has two describe paths and they do not look alike. A
//! resource-backed entity -- `topics`, `brokers`, `groups`, `client-metrics`
//! -- goes through `DescribeConfigs` and prints a header line followed by one
//! indented `name=value sensitive=… synonyms={…}` line per key. A quota-backed
//! entity -- `clients`, `users`, `ips`, and the `users`+`clients` tuple --
//! goes through `DescribeClientQuotas` and prints a single
//! `Quota configs for <entity> are <k=v, k=v>` line.
//!
//! Both are reduced here to the same `BTreeMap<String, String>`, so one case
//! can state the same rule -- what was altered comes back -- over every entity
//! type in a table, and the oracle differential compares maps rather than two
//! renderings of them.

use std::collections::BTreeMap;

/// The configuration one `--describe` reported, whichever path printed it.
pub type Configs = BTreeMap<String, String>;

/// Parse a `kafka-configs --describe` stdout into the keys it reported.
///
/// Both shapes are read in one pass because a case does not know, and should
/// not have to know, which path its entity type took: that is `ConfigCommand`'s
/// private business, and a krabka change that moved an entity from one path to
/// the other would be a divergence this suite must catch rather than a reason
/// to fail parsing.
pub fn parse_describe(stdout: &str) -> Configs {
    let mut configs = Configs::new();
    for line in stdout.lines() {
        let line = line.trim();
        if let Some(entries) = line.split_once(" are ").filter(|_| is_quota_line(line)) {
            for entry in entries.1.split(',') {
                if let Some((key, value)) = entry.trim().split_once('=') {
                    configs.insert(key.trim().to_owned(), value.trim().to_owned());
                }
            }
        } else if let Some((assignment, _)) = line.split_once(" sensitive=")
            && let Some((key, value)) = assignment.split_once('=')
        {
            configs.insert(key.trim().to_owned(), value.trim().to_owned());
        }
    }
    configs
}

/// Whether a line is the quota path's one-line summary.
fn is_quota_line(line: &str) -> bool { line.starts_with("Quota configs for ") }

/// The entity a quota line named, as the tool spelled it.
///
/// The tuple case is the reason this exists: `--entity-type users
/// --entity-name u --entity-type clients --entity-name c` is one quota entity
/// with two components, and the tool renders it as
/// `user-principal 'u', client-id 'c'`. A case that only compared the values
/// would pass on a broker that quietly dropped the client half and answered
/// for the user alone.
pub fn quota_entities(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter(|line| is_quota_line(line))
        .filter_map(|line| {
            let rest = line.strip_prefix("Quota configs for ")?;
            let (entity, _) = rest.split_once(" are ")?;
            Some(entity.trim().to_owned())
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    struct ConfigOutput {
        stdout: String,
        configs: Slot<Configs>,
        entities: Vec<String>,
    }

    #[fixture]
    fn output() -> ConfigOutput {
        // Start this scenario with fresh inputs and observations.

        ConfigOutput {
            stdout: String::new(),
            configs: Slot::new(),
            entities: Vec::new(),
        }
    }

    #[given("the configuration tool printed")]
    fn tool_stdout(output: &mut ConfigOutput, docstring: String) { output.stdout = docstring; }

    #[when("the described configuration and quota entities are read")]
    fn read_output(output: &mut ConfigOutput) {
        output.configs.set(parse_describe(&output.stdout));
        output.entities = quota_entities(&output.stdout);
    }

    #[then("the described configuration contains exactly")]
    fn described_values(output: &ConfigOutput, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let columns: [String; 2] = row.try_into().expect("a key and a value");
                columns.into()
            })
            .collect::<Configs>();
        check!(
            output.configs.get() == Some(expected),
            "stdout: {:?}",
            output.stdout
        );
    }

    #[then("the quota entity retains both user u and client c")]
    fn tuple_entity(output: &ConfigOutput) {
        check!(output.entities == vec!["user-principal 'u', client-id 'c'".to_owned()]);
    }

    #[then("no configuration values or quota entities are invented")]
    fn no_values(output: &ConfigOutput) {
        check!(
            output
                .configs
                .get()
                .expect("configuration output was read")
                .is_empty(),
            "parsed keys out of {:?}",
            output.stdout
        );
        check!(output.entities.is_empty());
    }

    scenarios!("tests/features/migrated/tests/jvm_acceptance_cli/config_output.feature", fixtures = [output: ConfigOutput]);
}
