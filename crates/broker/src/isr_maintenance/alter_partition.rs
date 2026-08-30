//! Delivery of an ISR proposal to the controller quorum: it picks the
//! candidate targets from the metadata image, sends the request over a
//! short-lived client, and classifies the response. Kept apart from the scan
//! loop because every step here is about reaching a controller, not about
//! deciding what to propose.

use std::sync::Arc;

use krabka_protocol::owned::alter_partition_request::AlterPartitionRequest;
use krabka_raft::NodeId;
use tracing::{debug, warn};

use super::request_builder::build_alter_partition_request;

#[tracing::instrument(
    name = "isr_send_alter_partition",
    level = "info",
    skip_all,
    fields(topic = %topic, partition, leader_epoch, new_isr_len = new_isr.len()),
    err,
)]
pub(super) async fn send_alter_partition(
    controller: &Arc<dyn crate::metadata_source::MetadataSource>,
    broker_id: i32,
    topic: &str,
    partition: i32,
    new_isr: Vec<NodeId>,
    leader_epoch: i32,
    client_resource_policy: (
        krabka_client_core::ConnectionDispatchQueueCapacity,
        krabka_client_core::ClientFrameMax,
    ),
) -> Result<(), String> {
    let image = controller.current_image();
    let leader_id = *controller.watch_leader().borrow();
    let targets = alter_partition_targets(&image, leader_id);
    if targets.is_empty() {
        return match leader_id {
            Some(_) => Err("controller leader not in image".into()),
            None => Err("no controller leader".into()),
        };
    }

    let req =
        build_alter_partition_request(&image, broker_id, topic, partition, &new_isr, leader_epoch);
    let mut last_err = String::new();
    for (target_id, addr) in targets {
        match send_alter_partition_to(
            broker_id,
            &addr,
            req.clone(),
            client_resource_policy.0,
            client_resource_policy.1,
        )
        .await
        {
            Ok(()) => {
                debug!(
                    topic = topic,
                    partition = partition,
                    new_isr_len = new_isr.len(),
                    controller_target = target_id.0,
                    "AlterPartition proposed"
                );
                return Ok(());
            }
            Err(AlterPartitionSendError::NotController) => {
                last_err = format!("target {target_id} is not controller");
            }
            Err(AlterPartitionSendError::Rejected {
                global_err,
                part_err,
            }) => {
                warn!(
                    topic = topic,
                    partition = partition,
                    new_isr_len = new_isr.len(),
                    controller_target = target_id.0,
                    global_error_code = global_err,
                    partition_error_code = part_err,
                    "AlterPartition rejected by controller"
                );
                return Err(format!(
                    "AlterPartition rejected: global={global_err} partition={part_err}"
                ));
            }
            Err(AlterPartitionSendError::Transport(e)) => {
                last_err = format!("target {target_id} ({addr}): {e}");
            }
        }
    }
    Err(last_err)
}

fn alter_partition_targets(
    image: &krabka_metadata::MetadataImage,
    leader_id: Option<NodeId>,
) -> Vec<(NodeId, String)> {
    let mut out = Vec::new();
    if let Some(id) = leader_id
        && let Some(b) = image.broker(id)
    {
        out.push((id, format!("{}:{}", b.host, b.port)));
    }
    let mut others: Vec<(NodeId, String)> = image
        .brokers()
        .filter(|b| Some(b.node_id) != leader_id)
        .map(|b| (b.node_id, format!("{}:{}", b.host, b.port)))
        .collect();
    others.sort_by_key(|(id, _)| *id);
    out.extend(others);
    out
}

#[derive(Debug, PartialEq, Eq)]
enum AlterPartitionSendError {
    NotController,
    Rejected { global_err: i16, part_err: i16 },
    Transport(String),
}

async fn send_alter_partition_to(
    broker_id: i32,
    addr: &str,
    req: AlterPartitionRequest,
    dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity,
    frame_max: krabka_client_core::ClientFrameMax,
) -> Result<(), AlterPartitionSendError> {
    let client = krabka_client_core::Client::builder()
        .bootstrap(addr.to_string())
        .client_id(format!("krabka-broker-{broker_id}-isr"))
        .dispatch_queue_capacity(dispatch_queue_capacity.get())
        .frame_max(frame_max.size())
        .build()
        .await
        .map_err(|e| AlterPartitionSendError::Transport(format!("connect: {e}")))?;

    let resp = client
        .send(req)
        .await
        .map_err(|e| AlterPartitionSendError::Transport(format!("send: {e}")))?;
    let global_err = resp.error_code;
    let part_err = resp
        .topics
        .first()
        .and_then(|t| t.partitions.first())
        .map_or(0, |p| p.error_code);
    classify_alter_partition_response(global_err, part_err)
}

fn classify_alter_partition_response(
    global_err: i16,
    part_err: i16,
) -> Result<(), AlterPartitionSendError> {
    if is_not_controller_response(global_err, part_err) {
        return Err(AlterPartitionSendError::NotController);
    }
    if global_err != 0 || part_err != 0 {
        return Err(AlterPartitionSendError::Rejected {
            global_err,
            part_err,
        });
    }
    Ok(())
}

fn is_not_controller_response(global_err: i16, part_err: i16) -> bool {
    global_err == crate::codes::NOT_CONTROLLER || part_err == crate::codes::NOT_CONTROLLER
}

#[cfg(test)]
mod tests {
    use krabka_metadata::MetadataImage;

    use super::*;
    use crate::isr_maintenance::test_support::{TestMetadataSource, reg};

    #[tokio::test]
    async fn send_alter_partition_errors_without_controller_target() {
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = Arc::new(
            TestMetadataSource::new(MetadataImage::new(uuid::Uuid::nil()), None),
        );

        let err = send_alter_partition(
            &controller,
            1,
            "orders",
            0,
            vec![NodeId(1)],
            3,
            (
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
                krabka_client_core::ClientFrameMax::default(),
            ),
        )
        .await
        .expect_err("missing controller leader should reject the send");

        assert2::assert!((err) == ("no controller leader"));
    }

    #[tokio::test]
    async fn send_alter_partition_to_reports_transport_error_for_closed_port() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        let err = send_alter_partition_to(
            1,
            &addr.to_string(),
            AlterPartitionRequest::default(),
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            krabka_client_core::ClientFrameMax::default(),
        )
        .await
        .expect_err("closed local port should fail as transport");

        assert2::assert!(matches!(err, AlterPartitionSendError::Transport(_)));
    }

    #[test]
    fn alter_partition_targets_try_hint_first_then_remaining_brokers() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&reg(NodeId(2)));
        image.apply(&reg(NodeId(0)));
        image.apply(&reg(NodeId(1)));

        let targets = alter_partition_targets(&image, Some(NodeId(2)));

        assert2::assert!(
            (targets)
                == (vec![
                    (NodeId(2), "b2:9092".to_string()),
                    (NodeId(0), "b0:9092".to_string()),
                    (NodeId(1), "b1:9092".to_string()),
                ])
        );
    }

    #[test]
    fn alter_partition_targets_fall_back_when_hint_missing() {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&reg(NodeId(1)));
        image.apply(&reg(NodeId(0)));

        let targets = alter_partition_targets(&image, Some(NodeId(9)));

        assert2::assert!(
            (targets)
                == (vec![
                    (NodeId(0), "b0:9092".to_string()),
                    (NodeId(1), "b1:9092".to_string())
                ])
        );
    }

    #[test]
    fn not_controller_classification_covers_global_and_partition_codes() {
        let cases = [
            (crate::codes::NOT_CONTROLLER, 0, true),
            (0, crate::codes::NOT_CONTROLLER, true),
            (0, 0, false),
            (crate::codes::UNKNOWN_SERVER_ERROR, 0, false),
        ];
        for (global_err, part_err, want) in cases {
            assert2::assert!(
                (is_not_controller_response(global_err, part_err)) == (want),
                "global_err={global_err} part_err={part_err}"
            );
        }
    }

    #[test]
    fn alter_partition_response_classifies_all_error_surfaces() {
        let cases = [
            (0, 0, Ok(())),
            (
                crate::codes::NOT_CONTROLLER,
                0,
                Err(AlterPartitionSendError::NotController),
            ),
            (
                0,
                crate::codes::NOT_CONTROLLER,
                Err(AlterPartitionSendError::NotController),
            ),
            (
                crate::codes::UNKNOWN_SERVER_ERROR,
                0,
                Err(AlterPartitionSendError::Rejected {
                    global_err: crate::codes::UNKNOWN_SERVER_ERROR,
                    part_err: 0,
                }),
            ),
            (
                0,
                crate::codes::UNKNOWN_SERVER_ERROR,
                Err(AlterPartitionSendError::Rejected {
                    global_err: 0,
                    part_err: crate::codes::UNKNOWN_SERVER_ERROR,
                }),
            ),
            (
                crate::codes::UNKNOWN_SERVER_ERROR,
                crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                Err(AlterPartitionSendError::Rejected {
                    global_err: crate::codes::UNKNOWN_SERVER_ERROR,
                    part_err: crate::codes::UNKNOWN_TOPIC_OR_PARTITION,
                }),
            ),
        ];
        for (global_err, part_err, want) in cases {
            assert2::assert!(
                (classify_alter_partition_response(global_err, part_err)) == (want),
                "global_err={global_err} part_err={part_err}"
            );
        }
    }
}
