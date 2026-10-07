//! Wire and socket fixtures shared by network unit tests.

use bytes::{BufMut, BytesMut};
use tokio::net::{TcpListener, TcpStream};

/// Build request headers independently of the parser under test. Arbitrary
/// tagged bytes let malformed-header cases exercise the same path.
pub(crate) fn request_frame(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&[u8]>,
    tagged: Option<&[u8]>,
    body: &[u8],
) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    match client_id {
        Some(id) => {
            buf.put_i16(i16::try_from(id.len()).expect("client id length"));
            buf.put_slice(id);
        }
        None => buf.put_i16(-1),
    }
    if let Some(tagged) = tagged {
        buf.put_slice(tagged);
    }
    buf.put_slice(body);
    buf
}

pub(crate) async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback listener");
    let addr = listener.local_addr().expect("listener addr");
    let client_task = tokio::spawn(TcpStream::connect(addr));
    let (server, _) = listener.accept().await.expect("accept loopback client");
    let client = client_task
        .await
        .expect("connect task")
        .expect("connect loopback client");
    (server, client)
}

/// Kernels clamp and may double requested sizes; compare the configured
/// buffers to their deliberately smaller baselines rather than host-specific
/// values. Both accepted and outbound tuning must also enable `TCP_NODELAY`.
pub(crate) fn check_socket_tuning(socket: &TcpStream, tune: impl FnOnce(&TcpStream)) {
    let sock = socket2::SockRef::from(socket);
    socket.set_nodelay(false).expect("clear TCP_NODELAY");
    sock.set_send_buffer_size(4096).expect("shrink send buffer");
    sock.set_recv_buffer_size(8192).expect("shrink recv buffer");
    let send_before = sock.send_buffer_size().expect("read baseline send buffer");
    let recv_before = sock.recv_buffer_size().expect("read baseline recv buffer");
    tune(socket);
    assert2::assert!(socket.nodelay().expect("read TCP_NODELAY"));
    let send_after = sock.send_buffer_size().expect("read send buffer");
    let recv_after = sock.recv_buffer_size().expect("read recv buffer");
    assert2::assert!(send_after > send_before);
    assert2::assert!(recv_after > recv_before);
    assert2::assert!(recv_after > send_after);
}
