//! What a lifecycle run keeps beside the load: the release images, the CAs of
//! its certificate rotations, the broker configuration it set, and the
//! checkpoint it takes after each transition.
//!
//! A checkpoint reads, while the load keeps running:
//!
//! - the image every broker's container runs, by `docker inspect`;
//! - the finalized feature levels, by `ApiVersions`, and from them whether a
//!   rollback to the source is still supported
//!   ([`krabka_bench_driver::soak::lifecycle::rollback`]);
//! - every path from the earliest offset at `read_uncommitted`, against the
//!   records the path's producer had been acknowledged when the read began;
//! - every path's topic configuration, against what the run created it with,
//!   and every rotated broker's dynamic listener keys, against what the
//!   rotation set.
//!
//! Committed offsets are checked across every transition as across every
//! fault, by the executor.

use {
    crate::{
        host::{docker_try, now_ms},
        plan,
        topology::Nodes,
        transitions::CertAuthority,
    },
    krabka_bench_driver::soak::{
        client::{
            load::{LoadContext, producer_name},
            readback::read_back,
        },
        contract::{Contract, Isolation},
        lifecycle::{Checkpoint, CheckpointRead, LevelRange, Lifecycle, Side, rollback},
    },
    krabka_protocol::owned::{
        api_versions_request::ApiVersionsRequest,
        api_versions_response::ApiVersionsResponse,
        describe_configs_request::{DescribeConfigsRequest, DescribeConfigsResource},
        describe_configs_response::DescribeConfigsResponse,
    },
    std::{collections::BTreeMap, sync::Mutex},
};

/// Kafka's `ConfigResource.Type.TOPIC`.
const TOPIC_RESOURCE: i8 = 2;

/// Kafka's `ConfigResource.Type.BROKER`.
const BROKER_RESOURCE: i8 = 4;

/// The lifecycle state of one run.
pub(crate) struct LifecycleRun {
    pub(crate) lifecycle: Lifecycle,
    /// The image reference each side runs from.
    images: BTreeMap<Side, String>,
    pub(crate) authority: Mutex<CertAuthority>,
    /// The broker configuration the run set, by node.
    broker_configs: Mutex<BTreeMap<i32, BTreeMap<String, String>>>,
}

impl LifecycleRun {
    pub(crate) fn new(lifecycle: Lifecycle, target_image: String) -> Self {
        let images = BTreeMap::from([
            (Side::Source, source_image(&lifecycle)),
            (Side::Target, target_image),
        ]);
        Self {
            lifecycle,
            images,
            authority: Mutex::new(CertAuthority::default()),
            broker_configs: Mutex::new(BTreeMap::new()),
        }
    }

    /// The image reference `side` runs from.
    pub(crate) fn image(&self, side: Side) -> &str {
        self.images.get(&side).map_or("", String::as_str)
    }

    /// Each side's image as the daemon identifies it, by side name: what a
    /// checkpoint's running images are compared with.
    pub(crate) fn release_ids(&self) -> Result<BTreeMap<String, String>, String> {
        self.images
            .iter()
            .map(|(side, image)| Ok((side.to_string(), image_id(image)?)))
            .collect()
    }

    /// Record that node `node` was set `configs`.
    pub(crate) fn expect_broker_configs(&self, node: i32, configs: Vec<(String, String)>) {
        if let Ok(mut expected) = self.broker_configs.lock() {
            expected.entry(node).or_default().extend(configs);
        }
    }

    fn expected_broker_configs(&self) -> BTreeMap<i32, BTreeMap<String, String>> {
        self.broker_configs
            .lock()
            .map(|expected| expected.clone())
            .unwrap_or_default()
    }
}

/// The image the source runs from: `KRABKA_LIFECYCLE_SOURCE_IMAGE` where the
/// runner loaded the release under a tag, as the container suites do, else
/// the contract's digest reference, which the daemon pulls.
pub(crate) fn source_image(lifecycle: &Lifecycle) -> String {
    std::env::var("KRABKA_LIFECYCLE_SOURCE_IMAGE")
        .ok()
        .filter(|image| !image.is_empty())
        .unwrap_or_else(|| lifecycle.source.image.clone())
}

/// Pull `image` unless the daemon has it, and return the id it has for it.
pub(crate) fn image_id(image: &str) -> Result<String, String> {
    let inspect = || docker_try(&["image", "inspect", "--format", "{{.Id}}", image]);
    inspect().or_else(|_| {
        docker_try(&["pull", "--quiet", image])?;
        inspect()
    })
}

/// The id of the image `container` runs, or why it cannot be read.
fn running_image(container: &str) -> String {
    docker_try(&["inspect", "--format", "{{.Image}}", container])
        .unwrap_or_else(|error| format!("<not running: {error}>"))
}

/// The `ApiVersions` answer of the first node that gives one.
async fn api_versions(nodes: &Nodes) -> Result<ApiVersionsResponse, String> {
    let mut last = "no node".to_owned();
    for id in nodes.ids() {
        // From v3 the broker refuses an empty or malformed client software
        // name or version with INVALID_REQUEST, as Kafka does.
        let request = ApiVersionsRequest {
            client_software_name: "krabka-soak-contract".to_owned(),
            client_software_version: env!("CARGO_PKG_VERSION").to_owned(),
            ..Default::default()
        };
        match nodes.send(id, request).await {
            Ok(response) if response.error_code == 0 => return Ok(response),
            Ok(response) => last = format!("node {id}: error {}", response.error_code),
            Err(error) => last = format!("node {id}: {error}"),
        }
    }
    Err(format!("no node answered ApiVersions: {last}"))
}

/// The feature ranges the cluster advertises, by feature.
pub(crate) async fn supported_features(
    nodes: &Nodes,
) -> Result<BTreeMap<String, LevelRange>, String> {
    Ok(api_versions(nodes)
        .await?
        .supported_features
        .into_iter()
        .map(|feature| {
            (feature.name, LevelRange {
                min: feature.min_version,
                max: feature.max_version,
            })
        })
        .collect())
}

/// The finalized feature levels, by feature.
async fn finalized_features(nodes: &Nodes) -> Result<BTreeMap<String, i16>, String> {
    Ok(api_versions(nodes)
        .await?
        .finalized_features
        .into_iter()
        .map(|feature| (feature.name, feature.max_version_level))
        .collect())
}

/// One `DescribeConfigs` of `resource` from any node.
async fn describe(
    nodes: &Nodes,
    resource_type: i8,
    resource: &str,
    keys: Vec<String>,
) -> Result<BTreeMap<String, Option<String>>, String> {
    let request = DescribeConfigsRequest {
        resources: vec![DescribeConfigsResource {
            resource_type,
            resource_name: resource.to_owned(),
            configuration_keys: Some(keys),
            ..Default::default()
        }],
        ..Default::default()
    };
    // A broker resource is described by the broker it names.
    let order: Vec<i32> = match resource.parse::<i32>() {
        Ok(id) if resource_type == BROKER_RESOURCE => vec![id],
        _ => nodes.ids(),
    };
    let mut last = String::new();
    for id in order {
        match nodes.send(id, request.clone()).await {
            Ok(response) => return described(&response, resource),
            Err(error) => last = error,
        }
    }
    Err(format!("DescribeConfigs of {resource}: {last}"))
}

fn described(
    response: &DescribeConfigsResponse,
    resource: &str,
) -> Result<BTreeMap<String, Option<String>>, String> {
    let result = response
        .results
        .iter()
        .find(|result| result.resource_name == resource)
        .ok_or_else(|| format!("DescribeConfigs answered nothing for {resource}"))?;
    if result.error_code != 0 {
        return Err(format!(
            "DescribeConfigs of {resource}: error {} {}",
            result.error_code,
            result.error_message.as_deref().unwrap_or("")
        ));
    }
    Ok(result
        .configs
        .iter()
        .map(|config| (config.name.clone(), config.value.clone()))
        .collect())
}

/// Every entry of `expected` that `found` does not hold, one line each.
pub(crate) fn config_mismatches(
    resource: &str,
    expected: &BTreeMap<String, String>,
    found: &BTreeMap<String, Option<String>>,
) -> Vec<String> {
    expected
        .iter()
        .filter_map(|(name, value)| match found.get(name) {
            Some(Some(found)) if found == value => None,
            Some(found) => Some(format!("{resource} {name} is {found:?}, not {value:?}")),
            None => Some(format!(
                "{resource} {name} is not described; it was set to {value:?}"
            )),
        })
        .collect()
}

/// The configuration a path's topic was created with that `DescribeConfigs`
/// reports. A krabka-internal key is set at creation but never described.
fn expected_topic_configs(contract: &Contract, path: &str) -> BTreeMap<String, String> {
    contract
        .path(path)
        .map(|path| plan::topic_configs(path, contract.cluster.min_insync_replicas))
        .unwrap_or_default()
        .into_iter()
        .filter(|(name, _)| !name.starts_with("internal."))
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
}

/// The cluster as it stands after transition `fault`.
pub(crate) async fn checkpoint(
    fault: &str,
    run: &LifecycleRun,
    nodes: &Nodes,
    ctx: &LoadContext,
) -> Checkpoint {
    let contract = &ctx.contract;
    let images = nodes
        .ids()
        .into_iter()
        .filter_map(|id| {
            nodes
                .node(id)
                .map(|node| (id, running_image(&node.container)))
        })
        .collect();
    let mut mismatches = Vec::new();
    let finalized = finalized_features(nodes).await.unwrap_or_else(|error| {
        mismatches.push(format!("feature levels unreadable: {error}"));
        BTreeMap::new()
    });
    let mut records = Vec::new();
    for path in &contract.paths {
        let acknowledged = ctx.acknowledged(&path.name);
        let reader = read_back(
            &ctx.endpoint,
            path,
            Isolation::ReadUncommitted,
            &producer_name(&path.name),
        )
        .await;
        records.push(CheckpointRead {
            path: path.name.clone(),
            semantics: path.semantics,
            key_space: path.key_space,
            acknowledged,
            reader,
        });
        let expected = expected_topic_configs(contract, &path.name);
        match describe(
            nodes,
            TOPIC_RESOURCE,
            &path.name,
            expected.keys().cloned().collect(),
        )
        .await
        {
            Ok(found) => mismatches.extend(config_mismatches(&path.name, &expected, &found)),
            Err(error) => mismatches.push(error),
        }
    }
    for (node, expected) in run.expected_broker_configs() {
        let resource = node.to_string();
        match describe(
            nodes,
            BROKER_RESOURCE,
            &resource,
            expected.keys().cloned().collect(),
        )
        .await
        {
            Ok(found) => mismatches.extend(config_mismatches(
                &format!("broker {resource}"),
                &expected,
                &found,
            )),
            Err(error) => mismatches.push(error),
        }
    }
    Checkpoint {
        fault: fault.to_owned(),
        unix_ms: now_ms(),
        images,
        rollback: rollback(&run.lifecycle.source, &finalized),
        finalized,
        records,
        config_mismatches: mismatches,
    }
}

#[cfg(test)]
mod tests {
    use {super::config_mismatches, assert2::check, std::collections::BTreeMap};

    #[yare::parameterized(
        held = { Some(Some("2")), None },
        changed = { Some(Some("1")), Some("t min.insync.replicas is Some(\"1\"), not \"2\"") },
        hidden = { Some(None), Some("t min.insync.replicas is None, not \"2\"") },
        missing = { None, Some("t min.insync.replicas is not described; it was set to \"2\"") },
    )]
    fn a_configuration_holds_what_was_set(found: Option<Option<&str>>, mismatch: Option<&str>) {
        let expected = BTreeMap::from([("min.insync.replicas".to_owned(), "2".to_owned())]);
        let found: BTreeMap<String, Option<String>> = found
            .map(|value| ("min.insync.replicas".to_owned(), value.map(str::to_owned)))
            .into_iter()
            .collect();
        check!(
            config_mismatches("t", &expected, &found)
                == mismatch.map(str::to_owned).into_iter().collect::<Vec<_>>()
        );
    }
}
