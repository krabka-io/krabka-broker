//! `ElectLeaders` (`api_key` 43, KIP-460).
//!
//! Operator-triggered leader election. PREFERRED type moves leadership
//! back to `replicas[0]` after operator intervention. UNCLEAN type
//! elects outside the ISR when every ISR member is dead.
//!
//! Authorization: `Alter` on `Cluster("kafka-cluster")`. On Deny the
//! whole request answers `CLUSTER_AUTHORIZATION_FAILED (31)` at the top level
//! (v1+) and on every partition row it named.
//!
//! # KFC-9: an unclean election needs two people
//!
//! An unclean election elects a replica that does not hold every committed
//! record, so it is one of the transitions the break-glass two-person rule
//! gates. The request gains no field for it. KIP-460 defines the shape that
//! `kafka-leader-election.sh` sends and there is nowhere in it to name a
//! proposal, so an operator gets the approval out of band through
//! `krabka-guard` and the broker looks it up in its own metadata image.
//!
//! **Preferred election is not gated.** It elects a replica that is already in
//! the ISR, it loses nothing, and gating it would stop routine operation on
//! every cluster that turns the rule on.
//!
//! A refused partition answers `POLICY_VIOLATION` (44) on its own row, with the
//! refusal text in `error_message`. The gate is active only when
//! `[break_glass]` names an approver set, so a stock cluster elects exactly as
//! it does today.

use bytes::Bytes;
use krabka_protocol::owned::{
    elect_leaders_request::ElectLeadersRequest,
    elect_leaders_response::{ElectLeadersResponse, ReplicaElectionResult},
};

use self::{
    batch::ElectionBatch,
    env::ElectionEnv,
    partition::elect_one,
    response::{encode_response, whole_request_error},
    targets::resolve_targets,
};
use crate::{
    broker::Broker,
    codes,
    elr::ElrPublisher,
    handlers::{RequestContext, cluster_alter_denied},
    leader_election::ElectionType,
};

mod batch;
mod env;
mod partition;
mod recovery;
mod response;
mod targets;
mod unclean_gate;

#[cfg(test)]
mod tests;

const WIRE_ELECTION_PREFERRED: i8 = 0;
const WIRE_ELECTION_UNCLEAN: i8 = 1;

/// The row message of an authorization refusal. Kafka's `AuthHelper` writes
/// "Request <request> needs ALTER permission.", where `<request>` is the JVM
/// `toString` of the channel request; krabka names the API in its place.
const CLUSTER_ALTER_DENIED_MESSAGE: &str = "Request ElectLeaders needs ALTER permission.";

#[tracing::instrument(
    name = "handle_elect_leaders",
    level = "info",
    skip_all,
    fields(api = "ElectLeaders"),
    err
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: ElectLeadersRequest,
    ctx: &RequestContext<'_>,
    api_version: i16,
) -> Result<Bytes, crate::error::BrokerError> {
    let image = broker.controller.current_image();
    let denied = cluster_alter_denied(broker.config.authorizer.as_ref(), &image, ctx);
    let election = match admit(&req, denied) {
        Ok(election) => election,
        Err(refusal) => return encode_response(&refusal, api_version),
    };

    // Resolve target partition set:
    //   topic_partitions = None      → every partition in the image
    //   Some([{topic, []}])          → no partition of that topic
    //   Some([{topic, [p, q, ...]}]) → exact set
    let targets = resolve_targets(&image, &req);

    // Which brokers an election may elect. Read from replicated state rather
    // than this node's heartbeat registry, because `controllerId` rotates and
    // an `AdminClient` sends the election to whichever broker it last named.
    let alive = crate::handlers::offline_replicas::live_brokers(broker, &image).await;

    // Run the algorithm per target; accumulate new records to submit
    // and per-partition results to ship back.
    let env = ElectionEnv {
        broker,
        image: &image,
        ctx,
        alive: &alive,
        // Witness nodes never lead a partition. Build the set once for the
        // whole request, not once per target partition.
        witnesses: &crate::config_keys::witness_node_ids(&image),
        election,
    };
    // KIP-460: a request that named no topics asked about every partition, and
    // Kafka answers such a request only with the partitions it acted on --
    // `ReplicationControlManager.electLeaders` drops every
    // `ELECTION_NOT_NEEDED` row when `topicPartitions` is null, because "we do
    // not return partitions which already have the desired leader". It still
    // answers a row for every topic, with an empty partition list when nothing
    // in it moved. Without the drop, `kafka-leader-election
    // --all-topic-partitions` prints a "valid replica already elected" line for
    // every partition in the cluster, internal topics included, where Kafka
    // prints nothing.
    let elect_all_partitions = req.topic_partitions.is_none();
    let mut by_topic: Vec<ReplicaElectionResult> = Vec::with_capacity(targets.len());
    let mut batch = ElectionBatch::default();
    for (topic, partitions) in targets {
        let mut partition_result = Vec::with_capacity(partitions.len());
        for p in partitions {
            let row = elect_one(&env, &mut batch, &topic, p).await;
            if elect_all_partitions && row.error_code == codes::ELECTION_NOT_NEEDED {
                continue;
            }
            partition_result.push(row);
        }
        by_topic.push(ReplicaElectionResult {
            topic,
            partition_result,
            ..Default::default()
        });
    }

    // KIP-966: an election decides which replicas are still known to hold
    // every committed record, so the ELR state rides the batch the election
    // records go out in.
    ElrPublisher::new(&image).extend(&mut batch.records);

    // Submit accumulated records. On failure, mark every queued OK row
    // with COORDINATOR_NOT_AVAILABLE.
    let mut submit_failure = None;
    if !batch.records.is_empty() {
        let failure = match batch.require_audit(broker, ctx).await {
            Ok(()) => broker
                .controller
                .submit_change(std::mem::take(&mut batch.records))
                .await
                .err()
                .map(|error| {
                    (
                        crate::handlers::submit_failure_code(
                            &error,
                            codes::COORDINATOR_NOT_AVAILABLE,
                        ),
                        format!("submit failed: {error}"),
                    )
                }),
            Err(error) => Some((
                codes::POLICY_VIOLATION,
                format!("privileged action refused: {error}"),
            )),
        };
        if let Some((code, failure)) = failure {
            tracing::warn!(error = %failure, "elect-leaders submit refused or failed");
            for topic in &mut by_topic {
                for r in &mut topic.partition_result {
                    if r.error_code == 0 {
                        r.error_code = code;
                        r.error_message = Some(failure.clone());
                    }
                }
            }
            submit_failure = Some(failure);
        }
    }
    // KFC-9: audit the approvals this append spent, now that its outcome is
    // known. An `applied` event for a transition that never committed would be
    // a false record of a data-losing election.
    batch.audit_applied(broker, ctx, submit_failure.as_deref());

    // The gated transitions above audit themselves as `PrivilegedAction`. An
    // ordinary preferred election spends no approval and would otherwise leave
    // no record, so every partition that actually moved is audited here.
    crate::handlers::audit_admin_success(
        broker.audit_log.as_ref(),
        ctx,
        "ElectLeaders",
        by_topic
            .iter()
            .flat_map(|topic| {
                topic
                    .partition_result
                    .iter()
                    .filter(|row| row.error_code == codes::NONE)
                    .map(move |row| {
                        crate::handlers::audit_resource(
                            "Partition",
                            format!("{}-{}", topic.topic, row.partition_id),
                        )
                    })
            })
            .collect(),
    );

    let resp = ElectLeadersResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        replica_election_results: by_topic,
        ..Default::default()
    };
    encode_response(&resp, api_version)
}

/// Admit a request, or answer the refusal of the whole of it.
///
/// Kafka's `ControllerApis.handleElectLeaders` authorizes `Alter` on the
/// cluster first and throws `ClusterAuthorizationException`; the controller
/// then reads the election type and throws `InvalidRequestException` for one
/// it does not know. Either exception becomes
/// `ElectLeadersRequest.getErrorResponse`, which [`whole_request_error`]
/// mirrors.
fn admit(req: &ElectLeadersRequest, denied: bool) -> Result<ElectionType, ElectLeadersResponse> {
    if denied {
        return Err(whole_request_error(
            req,
            codes::CLUSTER_AUTHORIZATION_FAILED,
            CLUSTER_ALTER_DENIED_MESSAGE,
        ));
    }
    match req.election_type {
        WIRE_ELECTION_PREFERRED => Ok(ElectionType::Preferred),
        WIRE_ELECTION_UNCLEAN => Ok(ElectionType::Unclean),
        unknown => Err(whole_request_error(
            req,
            codes::INVALID_REQUEST,
            &format!("Unknown election type {unknown}"),
        )),
    }
}
