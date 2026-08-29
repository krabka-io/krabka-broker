//! Behaviour tests for the real peer sender: what it puts on the wire for a
//! KIP-595 RPC, what it remembers about a peer it reached through a bootstrap
//! address, and when it reports a candidate as speaking `kraft.version`.
//!
//! The fake peer is a real `TcpListener` that parses the request header itself,
//! so the assertions are about bytes rather than about a mock.

use bytes::BufMut;
use krabka_ids::ApiVersion;
use krabka_protocol::{
    Encode,
    // The generated `ApiVersion` message struct is aliased so the
    // `krabka_ids::ApiVersion` newtype keeps the bare name this module's
    // header assertions use.
    owned::api_versions_response::{ApiVersion as WireApiVersion, ApiVersionsResponse},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;
use crate::{kraft::transport::api_key, network::dialer::PlaintextDialer};

fn voter_set_with_controller(id: NodeId, host: &str, port: u16) -> VoterSet {
    VoterSet::from_voters([krabka_metadata::Voter {
        id,
        directory_id: uuid::Uuid::nil(),
        endpoints: vec![krabka_metadata::VoterEndpoint {
            name: "CONTROLLER".into(),
            host: host.into(),
            port,
        }],
        kraft_version: krabka_metadata::KRaftVersionRange::default(),
    }])
}

fn api_versions_response_v0() -> Vec<u8> {
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![
            WireApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            },
            WireApiVersion {
                api_key: api_key::VOTE,
                min_version: 0,
                max_version: 2,
                ..Default::default()
            },
        ],
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    resp.encode(&mut buf, 0).unwrap();
    buf.to_vec()
}

#[test]
fn bootstrap_servers_remain_available_without_a_voter_set() {
    let sender = RealPeerSender::new(
        VoterSet::default(),
        &["controller.example:9093".into()],
        "raft-client".into(),
        Arc::new(PlaintextDialer),
        krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        krabka_client_core::ClientFrameMax::default(),
    );
    let bootstrap = sender.discovery_peers();
    assert2::assert!(bootstrap.len() == 1);
    sender.remember_peer(bootstrap[0], NodeId(7));
    assert2::assert!(
        sender
            .aliases
            .read()
            .expect("alias lock")
            .get(&NodeId(7))
            .is_some_and(|address| address == "controller.example:9093")
    );
}

async fn read_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.unwrap();
    frame
}

async fn write_response_frame(
    stream: &mut tokio::net::TcpStream,
    correlation_id: i32,
    tagged_fields: bool,
    body: &[u8],
) {
    let mut frame = bytes::BytesMut::new();
    frame.put_i32(correlation_id);
    if tagged_fields {
        frame.put_u8(0);
    }
    frame.put_slice(body);

    let mut out = Vec::with_capacity(frame.len() + 4);
    out.extend_from_slice(&(u32::try_from(frame.len()).unwrap()).to_be_bytes());
    out.extend_from_slice(&frame);
    stream.write_all(&out).await.unwrap();
    stream.flush().await.unwrap();
}

fn parse_request_header(frame: &[u8]) -> (ApiKey, ApiVersion, i32, String, &[u8]) {
    assert2::assert!(frame.len() >= 10);
    let api_key = ApiKey(i16::from_be_bytes([frame[0], frame[1]]));
    let version = ApiVersion(i16::from_be_bytes([frame[2], frame[3]]));
    let correlation_id = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
    let client_len = i16::from_be_bytes([frame[8], frame[9]]);
    assert2::assert!(client_len >= 0);
    let client_start = 10;
    let client_end = client_start + usize::try_from(client_len).unwrap();
    let client_id = std::str::from_utf8(&frame[client_start..client_end])
        .unwrap()
        .to_string();
    let body_start = if frame.get(client_end) == Some(&0) {
        client_end + 1
    } else {
        client_end
    };
    (
        api_key,
        version,
        correlation_id,
        client_id,
        &frame[body_start..],
    )
}

/// The candidate's `kraft.version` support, read off its `ApiVersions`.
///
/// A candidate is admitted only when it advertises the feature *and* the
/// cluster's finalized version falls inside the range it advertises. Both
/// halves are silent alone: matching any feature name would read some
/// other feature's range, and accepting a version on either side of the
/// range would admit a voter that cannot speak the protocol in force.
#[tokio::test]
async fn probe_reports_kraft_support_only_within_the_advertised_range() {
    /// An `ApiVersions` response advertising one supported feature.
    fn response_with_feature(name: &str, min: i16, max: i16) -> Vec<u8> {
        let response = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![WireApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 4,
                ..Default::default()
            }],
            supported_features: vec![
                krabka_protocol::owned::api_versions_response::SupportedFeatureKey {
                    name: name.to_owned(),
                    min_version: min,
                    max_version: max,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut buf = bytes::BytesMut::new();
        response.encode(&mut buf, 3).expect("encode api versions");
        buf.to_vec()
    }

    const KRAFT: &str = krabka_metadata::metadata_version::KRAFT_VERSION_FEATURE;
    // (what it is, feature advertised, range, finalized version, supported?)
    let cases: [(&str, &str, i16, i16, u16, bool); 5] = [
        (
            "the version at the bottom of the range",
            KRAFT,
            1,
            3,
            1,
            true,
        ),
        ("the version inside the range", KRAFT, 1, 3, 2, true),
        ("the version at the top of the range", KRAFT, 1, 3, 3, true),
        ("a version above the range", KRAFT, 1, 3, 4, false),
        (
            "some other feature entirely",
            "metadata.version",
            1,
            3,
            2,
            false,
        ),
    ];

    for (what, feature, min, max, finalized, supported) in cases {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let body = response_with_feature(feature, min, max);

        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // The dialer's own handshake first, then the probe's request.
            let handshake = read_frame(&mut stream).await;
            let (_, _, corr, _, _) = parse_request_header(&handshake);
            write_response_frame(&mut stream, corr, false, &api_versions_response_v0()).await;

            let probe = read_frame(&mut stream).await;
            let (key, _, corr, client_id, _) = parse_request_header(&probe);
            assert2::assert!(key == ApiKey(18));
            assert2::assert!(client_id == "krabka-voter-probe");
            write_response_frame(&mut stream, corr, false, &body).await;
        });

        let voters = voter_set_with_controller(NodeId(2), &addr.ip().to_string(), addr.port());
        let sender = RealPeerSender::new(
            voters,
            &[],
            "raft-client".into(),
            Arc::new(PlaintextDialer),
            krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap(),
            krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32)).unwrap(),
        );
        let got = PeerSender::probe_kraft_version(&sender, &addr.to_string(), finalized)
            .await
            .expect("probe");
        assert2::check!(got == supported, "{what}");
        server.await.expect("fake peer");
    }
}

#[tokio::test]
async fn real_peer_sender_sends_expected_api_version_client_id_and_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (observed_tx, mut observed_rx) = tokio::sync::mpsc::channel(1);

    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();

        let api_versions = read_frame(&mut stream).await;
        let (key, _version, corr, client_id, _body) = parse_request_header(&api_versions);
        assert2::assert!((key, client_id.as_str()) == (ApiKey(18), "raft-client"));
        write_response_frame(&mut stream, corr, false, &api_versions_response_v0()).await;

        let request = read_frame(&mut stream).await;
        let (key, version, corr, client_id, body) = parse_request_header(&request);
        observed_tx
            .send((key, version, client_id, bytes::Bytes::copy_from_slice(body)))
            .await
            .unwrap();
        write_response_frame(&mut stream, corr, true, b"raft-response").await;
    });

    let voters = voter_set_with_controller(NodeId(2), &addr.ip().to_string(), addr.port());
    let sender = RealPeerSender::new(
        voters,
        &[],
        "raft-client".into(),
        Arc::new(PlaintextDialer),
        krabka_client_core::ConnectionDispatchQueueCapacity::new(7).unwrap(),
        krabka_client_core::ClientFrameMax::try_from(krabka_units::kibibytes(32)).unwrap(),
    );
    assert2::assert!(sender.dispatch_queue_capacity.get() == 7);
    assert2::assert!(sender.frame_max.size() == krabka_units::kibibytes(32));
    let response = sender
        .send(NodeId(2), api_key::VOTE, Bytes::from_static(b"vote-body"))
        .await
        .expect("send");

    assert2::assert!(response == Bytes::from_static(b"raft-response"));
    let observed = tokio::time::timeout(std::time::Duration::from_secs(5), observed_rx.recv())
        .await
        .expect("server observed request")
        .expect("server sent request details");
    assert2::assert!(
        observed
            == (
                ApiKey(api_key::VOTE),
                ApiVersion(2),
                "raft-client".to_string(),
                Bytes::from_static(b"vote-body"),
            )
    );

    server.await.unwrap();
}
