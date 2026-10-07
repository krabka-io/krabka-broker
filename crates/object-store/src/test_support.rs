use tokio::io::AsyncReadExt as _;

pub(crate) async fn read_request(socket: &mut tokio::net::TcpStream) -> String {
    let mut request = Vec::new();
    while !request.ends_with(b"\r\n\r\n") {
        let mut chunk = [0; 1024];
        let read = socket.read(&mut chunk).await.unwrap();
        assert2::assert!(read > 0, "client closed before completing HTTP headers");
        request.extend_from_slice(&chunk[..read]);
    }
    String::from_utf8(request).unwrap()
}
