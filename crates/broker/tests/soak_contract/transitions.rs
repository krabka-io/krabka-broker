//! The lifecycle transitions that are requests to the running cluster rather
//! than Docker operations: a certificate rotation and a reassignment.
//!
//! A **certificate rotation** gives one broker's `TLS` listener a certificate
//! from a CA it has never served, with the DN and the SAN its old certificate
//! had, as Kafka's `CertificateEntries.ensureCompatible` requires of a dynamic
//! keystore. The new files are written over the static `tls_config` files in
//! place, so a restart or a rollback to a release without dynamic stores
//! serves them too, and to the PEM keystore and truststore the listener's
//! dynamic `listener.name.tls.ssl.keystore.*` and `.ssl.truststore.*` keys
//! then name. The truststore holds every CA of the run, so a client that
//! trusts either the old or the new CA keeps connecting through the swap.
//! The rotation is served once a handshake that trusts only the new CA
//! completes.
//!
//! A **reassignment** drains the target broker of a path, as an operator
//! drains a broker before it is retired: every replica of the path that the
//! target holds moves, with `AlterPartitionReassignments`, to a broker that
//! holds no replica of that partition, in the same position of the replica
//! list. Where the target leads, leadership moves too. The new replica copies
//! the partition from its start under the load. The reassignment is complete
//! when `ListPartitionReassignments` reports nothing in flight for the path and
//! every partition has the planned replicas and a full ISR.

use {
    crate::{
        cluster::{CONTAINER_TLS_DIR, TLS_LISTENER, TlsFiles},
        faults::{Cleanup, FaultGuard, Injected},
        lifecycle_run::LifecycleRun,
        topology::{Node, Nodes, PartitionState, isr_complete},
    },
    krabka_bench_driver::soak::contract::FaultKind,
    krabka_protocol::owned::{
        alter_partition_reassignments_request::{
            AlterPartitionReassignmentsRequest, ReassignablePartition, ReassignableTopic,
        },
        incremental_alter_configs_request::{
            AlterConfigsResource, AlterableConfig, IncrementalAlterConfigsRequest,
        },
        list_partition_reassignments_request::{
            ListPartitionReassignmentsRequest, ListPartitionReassignmentsTopics,
        },
    },
    rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, Issuer, KeyPair,
        KeyUsagePurpose,
    },
    rustls::{
        ClientConfig, RootCertStore,
        crypto::ring,
        pki_types::{CertificateDer, ServerName, pem::PemObject as _},
    },
    std::{collections::BTreeMap, sync::Arc, time::Duration},
    tokio::net::TcpStream,
    tokio_rustls::TlsConnector,
};

/// The subject every node's certificate carries, before and after a rotation.
const LEAF_COMMON_NAME: &str = "krabka-lifecycle-broker";

/// The name every node is reached by, and so the SAN of its certificate.
const SERVER_NAME: &str = "host.docker.internal";

/// Kafka's `ConfigResource.Type.BROKER`.
const BROKER_RESOURCE: i8 = 4;

/// Kafka's `AlterConfigOp.OpType.SET`.
const SET: i8 = 0;

/// How long one TLS handshake may take before the check counts it a failure.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// The CAs of a run, and the issuance of each node's certificate.
#[derive(Debug, Default)]
pub(crate) struct CertAuthority {
    /// Every CA the run has issued from, oldest first, PEM.
    cas: Vec<String>,
}

impl CertAuthority {
    /// A new CA, and a certificate for every node issued by it, with a
    /// truststore of every CA so far.
    pub(crate) fn issue(&mut self) -> Result<TlsFiles, String> {
        let generation = self.cas.len();
        let ca_key = KeyPair::generate().map_err(|error| error.to_string())?;
        let mut ca =
            CertificateParams::new(Vec::<String>::new()).map_err(|error| error.to_string())?;
        let mut ca_name = DistinguishedName::new();
        ca_name.push(
            DnType::CommonName,
            format!("krabka-lifecycle-ca-{generation}"),
        );
        ca.distinguished_name = ca_name;
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let ca_cert = ca.self_signed(&ca_key).map_err(|error| error.to_string())?;
        let issuer = Issuer::new(ca, ca_key);

        let leaf_key = KeyPair::generate().map_err(|error| error.to_string())?;
        let mut leaf = CertificateParams::new(vec![SERVER_NAME.to_owned()])
            .map_err(|error| error.to_string())?;
        let mut leaf_name = DistinguishedName::new();
        leaf_name.push(DnType::CommonName, LEAF_COMMON_NAME);
        leaf.distinguished_name = leaf_name;
        let leaf_cert = leaf
            .signed_by(&leaf_key, &issuer)
            .map_err(|error| error.to_string())?;

        self.cas.push(ca_cert.pem());
        Ok(TlsFiles {
            cert: leaf_cert.pem(),
            key: leaf_key.serialize_pem(),
            truststore: self.cas.concat(),
        })
    }

    /// The newest CA, PEM: the one a rotated listener's chain must verify
    /// against.
    pub(crate) fn newest(&self) -> Option<String> { self.cas.last().cloned() }
}

/// What a lifecycle check waits to observe.
#[derive(Debug, Clone)]
pub(crate) enum Expect {
    /// The node's TLS listener, at this host address, serves a chain that
    /// verifies against this CA alone.
    Certificate { address: String, ca: String },
    /// Every partition of `topic` has the replicas `plan` gives it, and no
    /// reassignment of the topic is in flight.
    Reassigned {
        topic: String,
        plan: BTreeMap<i32, Vec<i32>>,
    },
}

/// The dynamic listener keys a rotation sets, by name and value.
pub(crate) fn rotation_configs() -> Vec<(String, String)> {
    let prefix = format!("listener.name.{}.ssl", TLS_LISTENER.to_ascii_lowercase());
    vec![
        (format!("{prefix}.keystore.type"), "PEM".to_owned()),
        (
            format!("{prefix}.keystore.location"),
            format!("{CONTAINER_TLS_DIR}/keystore.pem"),
        ),
        (format!("{prefix}.truststore.type"), "PEM".to_owned()),
        (
            format!("{prefix}.truststore.location"),
            format!("{CONTAINER_TLS_DIR}/truststore.pem"),
        ),
    ]
}

/// The replicas each partition of `topic` moves to so that `target` holds
/// none, from the replicas `states` reports and the cluster's `nodes`: the
/// target's replica is replaced, in place, by the lowest node that holds no
/// replica of the partition. A partition the target does not hold, or one no
/// other node can take, is left out.
pub(crate) fn reassignment_plan(
    states: &[PartitionState],
    topic: &str,
    target: i32,
    nodes: &[i32],
) -> BTreeMap<i32, Vec<i32>> {
    states
        .iter()
        .filter(|state| state.topic == topic)
        .filter_map(|state| {
            let at = state
                .replicas
                .iter()
                .position(|replica| *replica == target)?;
            let spare = nodes
                .iter()
                .copied()
                .filter(|node| !state.replicas.contains(node))
                .min()?;
            let mut replicas = state.replicas.clone();
            replicas[at] = spare;
            Some((state.partition, replicas))
        })
        .collect()
}

/// Whether `states` shows every partition of `plan` on its planned replicas,
/// in the planned order, or which partition is not yet. The order is part of
/// the plan: the first replica is the preferred leader, and the drained
/// broker's replacement takes its position.
pub(crate) fn on_planned_replicas(
    states: &[PartitionState],
    topic: &str,
    plan: &BTreeMap<i32, Vec<i32>>,
) -> Result<(), String> {
    for (partition, planned) in plan {
        let have = states
            .iter()
            .find(|state| state.topic == topic && state.partition == *partition)
            .map(|state| state.replicas.as_slice())
            .unwrap_or_default();
        if have != planned.as_slice() {
            return Err(format!(
                "{topic}-{partition} is on {have:?}, not {planned:?}"
            ));
        }
    }
    Ok(())
}

/// Inject a rotation or a reassignment on `node`.
pub(crate) async fn inject(
    kind: &FaultKind,
    node: &Node,
    nodes: &Nodes,
    run: &LifecycleRun,
) -> Result<(Injected, Expect), String> {
    match kind {
        FaultKind::CertificateRotation => rotate(node, nodes, run).await,
        FaultKind::Reassignment { path } => reassign(node, nodes, path).await,
        other => Err(format!(
            "a {} is not a request to the cluster",
            other.name()
        )),
    }
}

async fn rotate(
    node: &Node,
    nodes: &Nodes,
    run: &LifecycleRun,
) -> Result<(Injected, Expect), String> {
    let address = node
        .tls
        .clone()
        .ok_or_else(|| format!("{} has no TLS listener", node.container))?;
    let (files, ca) = {
        let mut authority = run
            .authority
            .lock()
            .map_err(|_| "the certificate authority is poisoned".to_owned())?;
        let files = authority.issue()?;
        (files, authority.newest().unwrap_or_default())
    };
    files
        .write(&node.root)
        .map_err(|error| format!("write the rotated files: {error}"))?;
    let configs = rotation_configs();
    let request = IncrementalAlterConfigsRequest {
        resources: vec![AlterConfigsResource {
            resource_type: BROKER_RESOURCE,
            resource_name: node.id.to_string(),
            configs: configs
                .iter()
                .map(|(name, value)| AlterableConfig {
                    name: name.clone(),
                    config_operation: SET,
                    value: Some(value.clone()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        validate_only: false,
        ..Default::default()
    };
    // A broker config is altered on the broker it names, which validates the
    // new stores before the change commits.
    let response = nodes.send(node.id, request).await?;
    if let Some(refused) = response
        .responses
        .iter()
        .find(|resource| resource.error_code != 0)
    {
        return Err(format!(
            "IncrementalAlterConfigs refused the rotation: {} {}",
            refused.error_code,
            refused.error_message.as_deref().unwrap_or("")
        ));
    }
    run.expect_broker_configs(node.id, configs);
    Ok((
        Injected {
            guard: FaultGuard::new(Cleanup::Nothing {
                what: "a rotation is not undone".to_owned(),
            }),
            detail: format!(
                "rewrote {CONTAINER_TLS_DIR} with a certificate from a new CA and set the {TLS_LISTENER} listener's dynamic PEM keystore and truststore"
            ),
        },
        Expect::Certificate { address, ca },
    ))
}

async fn reassign(node: &Node, nodes: &Nodes, path: &str) -> Result<(Injected, Expect), String> {
    let (answered, states) = nodes
        .metadata_from_any(None)
        .await
        .ok_or_else(|| "no node answers Metadata".to_owned())?;
    let plan = reassignment_plan(&states, path, node.id, &nodes.ids());
    if plan.is_empty() {
        return Err(format!(
            "node {} holds no partition of {path} that another node can take",
            node.id
        ));
    }
    let request = AlterPartitionReassignmentsRequest {
        timeout_ms: 30_000,
        topics: vec![ReassignableTopic {
            name: path.to_owned(),
            partitions: plan
                .iter()
                .map(|(partition, replicas)| ReassignablePartition {
                    partition_index: *partition,
                    replicas: Some(replicas.clone()),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = nodes.send(answered, request).await?;
    let refused: Vec<String> = response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .filter(|partition| partition.error_code != 0)
        .map(|partition| {
            format!(
                "{path}-{}: {} {}",
                partition.partition_index,
                partition.error_code,
                partition.error_message.as_deref().unwrap_or("")
            )
        })
        .collect();
    if response.error_code != 0 || !refused.is_empty() {
        return Err(format!(
            "AlterPartitionReassignments refused: {} {}; {refused:?}",
            response.error_code,
            response.error_message.as_deref().unwrap_or("")
        ));
    }
    Ok((
        Injected {
            guard: FaultGuard::new(Cleanup::Nothing {
                what: "a reassignment is not undone".to_owned(),
            }),
            detail: format!("drained node {} of {path}: {plan:?}", node.id),
        },
        Expect::Reassigned {
            topic: path.to_owned(),
            plan,
        },
    ))
}

/// Whether the cluster shows what `expect` waits for, or what it shows instead.
pub(crate) async fn observed(nodes: &Nodes, expect: &Expect) -> Result<(), String> {
    match expect {
        Expect::Certificate { address, ca } => handshake(address, ca).await,
        Expect::Reassigned { topic, plan } => {
            let (answered, states) = nodes
                .metadata_from_any(None)
                .await
                .ok_or_else(|| "no node answers Metadata".to_owned())?;
            on_planned_replicas(&states, topic, plan)?;
            let partitions = states.iter().filter(|state| state.topic == *topic).count();
            let partitions = i32::try_from(partitions).unwrap_or(i32::MAX);
            if !isr_complete(&states, &[(topic.clone(), partitions)]) {
                return Err(format!(
                    "node {answered} reports an incomplete ISR for {topic}"
                ));
            }
            let listed = nodes
                .send(answered, ListPartitionReassignmentsRequest {
                    timeout_ms: 30_000,
                    topics: Some(vec![ListPartitionReassignmentsTopics {
                        name: topic.clone(),
                        partition_indexes: plan.keys().copied().collect(),
                        ..Default::default()
                    }]),
                    ..Default::default()
                })
                .await?;
            let ongoing: Vec<i32> = listed
                .topics
                .iter()
                .flat_map(|topic| {
                    topic
                        .partitions
                        .iter()
                        .map(|partition| partition.partition_index)
                })
                .collect();
            if ongoing.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "{topic} partitions {ongoing:?} are still reassigning"
                ))
            }
        }
    }
}

/// Complete a TLS handshake with `address` trusting `ca` alone.
async fn handshake(address: &str, ca: &str) -> Result<(), String> {
    let mut roots = RootCertStore::empty();
    let der = CertificateDer::from_pem_slice(ca.as_bytes()).map_err(|error| error.to_string())?;
    roots.add(der).map_err(|error| error.to_string())?;
    // The provider is named rather than installed: this process links more
    // than one, so rustls has no default to pick.
    let config = ClientConfig::builder_with_provider(Arc::new(ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|error| error.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    let name = ServerName::try_from(SERVER_NAME).map_err(|error| error.to_string())?;
    let attempt = async {
        let tcp = TcpStream::connect(address)
            .await
            .map_err(|error| format!("connect {address}: {error}"))?;
        TlsConnector::from(Arc::new(config))
            .connect(name, tcp)
            .await
            .map(drop)
            .map_err(|error| format!("handshake with {address}: {error}"))
    };
    tokio::time::timeout(HANDSHAKE_TIMEOUT, attempt)
        .await
        .map_err(|_| format!("no handshake with {address} within {HANDSHAKE_TIMEOUT:?}"))?
}

#[cfg(test)]
mod tests {
    use {
        super::{CertAuthority, on_planned_replicas, reassignment_plan},
        crate::topology::PartitionState,
        assert2::check,
        std::collections::BTreeMap,
    };

    fn state(partition: i32, leader: i32, replicas: &[i32]) -> PartitionState {
        PartitionState {
            topic: "t".to_owned(),
            partition,
            leader,
            replicas: replicas.to_vec(),
            isr: replicas.to_vec(),
        }
    }

    #[yare::parameterized(
        follower_moved_in_place = { state(0, 1, &[1, 4, 3]), Some(vec![1, 2, 3]) },
        leader_moved_in_place = { state(0, 4, &[4, 2, 3]), Some(vec![1, 2, 3]) },
        not_held = { state(0, 1, &[1, 2, 3]), None },
        nowhere_to_go = { state(0, 1, &[1, 2, 3, 4]), None },
    )]
    fn a_reassignment_drains_the_target_onto_the_spare_node(
        before: PartitionState,
        after: Option<Vec<i32>>,
    ) {
        let plan = reassignment_plan(&[before], "t", 4, &[1, 2, 3, 4]);
        check!(
            plan == after
                .into_iter()
                .map(|replicas| (0, replicas))
                .collect::<BTreeMap<_, _>>()
        );
    }

    #[yare::parameterized(
        same_order = { &[1, 2, 4], true },
        another_order = { &[4, 1, 2], false },
        not_yet = { &[1, 2, 3], false },
    )]
    fn a_partition_is_on_its_plan_in_the_planned_order(replicas: &[i32], done: bool) {
        let plan = BTreeMap::from([(0, vec![1, 2, 4])]);
        check!(on_planned_replicas(&[state(0, 1, replicas)], "t", &plan).is_ok() == done);
    }

    #[test]
    fn each_issue_comes_from_a_new_ca_and_trusts_every_earlier_one() {
        let mut authority = CertAuthority::default();
        let first = authority.issue().unwrap();
        let second = authority.issue().unwrap();
        check!(first.truststore.matches("BEGIN CERTIFICATE").count() == 1);
        check!(second.truststore.matches("BEGIN CERTIFICATE").count() == 2);
        check!(second.truststore.starts_with(&first.truststore));
        check!(first.cert != second.cert);
        check!(
            authority
                .newest()
                .is_some_and(|ca| second.truststore.ends_with(&ca))
        );
    }
}
