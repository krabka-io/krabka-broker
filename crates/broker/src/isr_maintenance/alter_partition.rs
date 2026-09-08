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
    outbound_client: &crate::network::client::InterBrokerClient,
    listener_protocol: krabka_security::ListenerProtocol,
    server_name: &str,
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
        let Some((host, port)) = crate::host_port::parse_host_port(&addr) else {
            last_err = format!("invalid target address {addr}");
            continue;
        };
        match send_alter_partition_to(
            broker_id,
            &host,
            port,
            req.clone(),
            outbound_client,
            listener_protocol,
            server_name,
        )
        .await
        {
            Ok(()) => {
                debug!(controller_target = target_id.0, "AlterPartition proposed");
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
                    controller_target = target_id.0,
                    global_error_code = global_err,
                    partition_error_code = part_err,
                    "AlterPartition rejected by controller"
                );
                return Err(format!(
                    "AlterPartition rejected: global={global_err} partition={part_err}"
                ));
            }
            Err(AlterPartitionSendError::Transport(error)) => {
                last_err = format!("target {target_id} ({addr}): {error}");
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
        && let Some(broker) = image.broker(id)
    {
        out.push((id, format!("{}:{}", broker.host, broker.port)));
    }
    let mut others: Vec<_> = image
        .brokers()
        .filter(|broker| Some(broker.node_id) != leader_id)
        .map(|broker| (broker.node_id, format!("{}:{}", broker.host, broker.port)))
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
    host: &str,
    port: u16,
    req: AlterPartitionRequest,
    outbound_client: &crate::network::client::InterBrokerClient,
    listener_protocol: krabka_security::ListenerProtocol,
    server_name: &str,
) -> Result<(), AlterPartitionSendError> {
    let client = outbound_client
        .connect_as_connection(
            host,
            port,
            listener_protocol,
            server_name,
            krabka_client_core::ConnectionOptions {
                client_id: format!("krabka-broker-{broker_id}-isr"),
                ..krabka_client_core::ConnectionOptions::default()
            },
        )
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
    use crate::isr_maintenance::test_support::fake_source;

    fn plaintext_client() -> Arc<crate::network::client::InterBrokerClient> {
        Arc::new(crate::network::client::InterBrokerClient::new(None, None))
    }

    #[tokio::test]
    async fn send_alter_partition_errors_without_controller_target() {
        let controller: Arc<dyn crate::metadata_source::MetadataSource> =
            Arc::new(fake_source(MetadataImage::new(uuid::Uuid::nil()), None));

        let err = send_alter_partition(
            &controller,
            1,
            "orders",
            0,
            vec![NodeId(1)],
            3,
            &plaintext_client(),
            krabka_security::ListenerProtocol::Plaintext,
            "localhost",
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

        let client = plaintext_client();
        let err = send_alter_partition_to(
            1,
            &addr.ip().to_string(),
            addr.port(),
            AlterPartitionRequest::default(),
            &client,
            krabka_security::ListenerProtocol::Plaintext,
            "localhost",
        )
        .await
        .expect_err("closed local port should fail as transport");

        assert2::assert!(matches!(err, AlterPartitionSendError::Transport(_)));
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
