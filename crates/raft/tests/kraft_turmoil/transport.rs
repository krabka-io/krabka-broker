//! Test transport for encoded KIP-595 bodies over Turmoil TCP. The small envelope carries the API key and body length; it is not the production Kafka connection protocol.

use {
    bytes::Bytes,
    krabka_client_core::ClientError,
    krabka_raft::{
        RaftError,
        kraft::{
            KraftController, NodeId, PeerSender,
            transport::{Inbound, PeerApi},
        },
    },
    std::{io, time::Duration},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::oneshot,
        task::JoinSet,
        time,
    },
    turmoil::net::{TcpListener, TcpStream},
};

pub const PORT: u16 = 9093;

pub fn hostname(node: NodeId) -> String { format!("voter-{}", node.0) }

pub struct SimulatedPeers;

#[async_trait::async_trait]
impl PeerSender for SimulatedPeers {
    async fn send(&self, peer: NodeId, api_key: i16, body: Bytes) -> Result<Bytes, RaftError> {
        // Bound both a black-holed connect and a response lost during a partition.
        match time::timeout(Duration::from_millis(250), exchange(peer, api_key, body)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(_)) | Err(_) => Err(ClientError::Disconnected.into()),
        }
    }
}

async fn exchange(peer: NodeId, api_key: i16, body: Bytes) -> io::Result<Bytes> {
    let mut stream = TcpStream::connect((hostname(peer), PORT)).await?;
    stream.write_i16(api_key).await?;
    write_body(&mut stream, &body).await?;
    read_body(&mut stream).await
}

async fn write_body(stream: &mut TcpStream, body: &[u8]) -> io::Result<()> {
    stream
        .write_u32(u32::try_from(body.len()).expect("test frame fits in u32"))
        .await?;
    stream.write_all(body).await
}

async fn read_body(stream: &mut TcpStream) -> io::Result<Bytes> {
    let len = usize::try_from(stream.read_u32().await?).expect("frame length fits in usize");
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await?;
    Ok(body.into())
}

async fn respond(mut stream: TcpStream, controller: KraftController) -> io::Result<()> {
    let key = stream.read_i16().await?;
    let api = PeerApi::from_api_key(key).expect("engine sends a KIP-595 peer API");
    let body = read_body(&mut stream).await?;
    let (reply, response) = oneshot::channel();
    controller
        .deliver(Inbound::new(api, body, api.version(), reply))
        .await
        .map_err(io::Error::other)?;
    let body = response.await.map_err(io::Error::other)?;
    write_body(&mut stream, &body).await
}

pub async fn serve(listener: TcpListener, controller: KraftController) -> turmoil::Result {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                connections.spawn(respond(stream, controller.clone()));
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                // Disconnects are expected under hardship. Task panics fail the host.
                if let Err(error) = completed.expect("connection task exists")? {
                    tracing::debug!(%error, "simulated peer connection closed");
                }
            }
        }
    }
}
