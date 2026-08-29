//! Drains an ordered [`WriteOp`] plan to a connection stream.
//!
//! Inline ops go out with `write_all`. A file-backed op goes through the
//! kernel `sendfile(2)` when the stream allows it, and through a buffered
//! `pread` and `write_all` fallback that produces identical wire bytes when it
//! does not.

use tokio::io::{AsyncWrite, AsyncWriteExt};

use super::{WriteOp, sink::SendfileSink};
use crate::error::BrokerError;

crate::sendfile_cfg! {
    use bytes::BytesMut;

    use super::sendfile::sendfile_region;
}

/// Drain a fetch write-plan to `stream`, write each op in order, then flush.
///
/// The caller MUST have flushed any pending `Framed` codec output first, so
/// the bytes do not interleave with the codec's write buffer. Inline ops use
/// `write_all`. File ops use `sendfile` when the stream is a Linux plaintext
/// `TcpStream`, and a buffered `pread` + `write_all` fallback otherwise.
pub async fn write_fetch_plan<S>(stream: &mut S, ops: Vec<WriteOp>) -> Result<(), BrokerError>
where
    S: AsyncWrite + SendfileSink + Unpin,
{
    let mut ops = ops.into_iter();
    let first = ops.next().ok_or_else(|| {
        BrokerError::Io(std::io::Error::other(
            "fetch handler produced an empty write plan",
        ))
    })?;
    for op in std::iter::once(first).chain(ops) {
        match op {
            WriteOp::Inline(b) => {
                stream.write_all(&b).await.map_err(BrokerError::Io)?;
            }
            #[cfg(any(
                target_os = "linux",
                target_os = "macos",
                target_os = "ios",
                target_os = "tvos",
                target_os = "watchos",
                target_os = "freebsd",
                target_os = "dragonfly",
            ))]
            WriteOp::File(region) => {
                drain_file_region(stream, &region).await?;
            }
        }
    }
    stream.flush().await.map_err(BrokerError::Io)?;
    Ok(())
}

crate::sendfile_cfg! {
    /// Positioned, full read of a `FileRegion` into `dst`, in a loop over
    /// short reads. `dst` must be exactly `region.len` bytes. This is the TLS
    /// and non-sendfile fallback for `WriteOp::File`. `read_at` (`FileExt`) is
    /// portable across every SENDFILE-alias unix.
    fn read_region_exact(
        region: &krabka_protocol::records::FileRegion,
        dst: &mut [u8],
    ) -> Result<(), BrokerError> {
        use std::os::unix::fs::FileExt;
        debug_assert_eq!(dst.len(), region.len);
        let mut filled = 0usize;
        let mut offset = region.offset;
        while filled < dst.len() {
            match region.file.read_at(&mut dst[filled..], offset) {
                Ok(0) => {
                    return Err(BrokerError::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "FileRegion read hit EOF before len bytes",
                    )));
                }
                Ok(n) => {
                    filled += n;
                    offset += n as u64;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(BrokerError::Io(e)),
            }
        }
        Ok(())
    }

    /// Drain one `FileRegion` to the socket. This method uses the kernel
    /// `sendfile(2)` when the stream is a plaintext `TcpStream` on a
    /// SENDFILE-alias platform. On TLS it falls back to a buffered `pread` +
    /// `write_all` that produces identical wire bytes.
    async fn drain_file_region<S>(
        stream: &mut S,
        region: &krabka_protocol::records::FileRegion,
    ) -> Result<(), BrokerError>
    where
        S: AsyncWrite + SendfileSink + Unpin,
    {
        if stream.tcp_for_sendfile().is_some() {
            // Re-borrow immutably for the readiness loop. `writable()`/`try_io()`
            // take `&self`, so this never conflicts with the (released) `&mut`.
            let tcp = stream
                .tcp_for_sendfile()
                .expect("checked Some on the line above");
            sendfile_region(tcp, region).await
        } else {
            // TLS fallback: pread the region into a buffer and write it.
            let mut buf = BytesMut::zeroed(region.len);
            read_region_exact(region, &mut buf)?;
            stream.write_all(&buf).await.map_err(BrokerError::Io)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn writer_rejects_an_empty_fetch_plan() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let connect = tokio::spawn(tokio::net::TcpStream::connect(
            listener.local_addr().unwrap(),
        ));
        let (mut server, _) = listener.accept().await.unwrap();
        let client = connect.await.unwrap().unwrap();

        let error = write_fetch_plan(&mut server, Vec::new())
            .await
            .expect_err("empty plans cannot form a Kafka response frame");

        assert!(error.to_string().contains("empty write plan"));
        drop(client);
    }

    // ─── Increment D/E (cross-platform sendfile) tests ────────────────────
    // Compiled on every SENDFILE-alias platform (Linux + Apple + FreeBSD/
    // DragonFly). The loopback-TCP roundtrip exercises the real readiness +
    // partial-write loop. The kTLS roundtrip below stays Linux-only (kTLS is a
    // Linux-only dependency).
    #[cfg(any(
        target_os = "linux",
        target_os = "macos",
        target_os = "ios",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "freebsd",
        target_os = "dragonfly",
    ))]
    mod sendfile_tests {
        use bytes::Bytes;

        use super::*;
        use crate::network::fetch_writer::{resolve_records_sendfile, test_support::file_payload};

        /// End-to-end `sendfile` over a real loopback TCP socket: the bytes
        /// the client reads must equal the file region. The test drives the
        /// real readiness and partial-write loop in `write_fetch_plan`.
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn sendfile_roundtrip_over_tcp_is_byte_exact() {
            use tokio::{
                io::AsyncReadExt,
                net::{TcpListener, TcpStream},
            };

            // A payload comfortably larger than a typical socket buffer so the
            // sendfile loop must iterate across several partial writes.
            let mut records = Vec::new();
            for i in 0..4000u32 {
                records.extend_from_slice(&i.to_le_bytes());
            }
            let records = Bytes::from(records);
            let (_tf, payload) = file_payload(&records);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let expected = records.clone();
            let client = tokio::spawn(async move {
                let mut stream = TcpStream::connect(addr).await.unwrap();
                let mut got = vec![0u8; expected.len()];
                stream.read_exact(&mut got).await.unwrap();
                assert_eq!(got, &expected[..], "sendfile'd bytes must match file");
            });

            let (mut server, _) = listener.accept().await.unwrap();
            // Shrink the send buffer to force partial sendfile writes.
            {
                use socket2::SockRef;
                let sr = SockRef::from(&server);
                let _ = sr.set_send_buffer_size(8 * 1024);
            }
            let ops = resolve_records_sendfile(&payload).unwrap();
            assert!(ops.iter().any(|o| matches!(o, WriteOp::File(_))));
            write_fetch_plan(&mut server, ops).await.unwrap();
            drop(server); // EOF for the client's read_exact tail
            client.await.unwrap();
        }

        /// Increment F end-to-end: `sendfile(2)` a `FileRegions` payload onto a
        /// `ktls::KtlsStream` (kernel-offloaded TLS) and assert the bytes a
        /// rustls TLS *client* decrypts are byte-identical to the file region.
        /// This proves that the kTLS path is wire-compatible: the kernel
        /// encrypts the same plaintext the userspace rustls path would have,
        /// so the client sees the same plaintext after decryption.
        ///
        /// The test skips, and does not fail, when the host kernel has no
        /// kTLS support, that is, when the `tls` module is not loaded or
        /// `CONFIG_TLS` is absent. The startup probe gates this exact
        /// condition in production, so a skip here mirrors a run of the
        /// fallback path.
        ///
        /// Linux-only: `ktls` is a Linux-only dependency, so this test is not
        /// compiled on the Apple/BSD members of the SENDFILE alias.
        #[cfg(target_os = "linux")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
        async fn ktls_sendfile_over_tls_is_byte_exact() {
            use std::sync::Arc;

            use tokio::{
                io::{AsyncReadExt, AsyncWriteExt},
                net::{TcpListener, TcpStream},
            };

            // The request the client sends before reading the response —
            // mirrors the real broker flow (client sends ApiVersions/Fetch, the
            // broker reads it via `Framed`, then writes the fetch response). It
            // also exercises the kTLS RX path on the server side.
            const REQ: &[u8] = b"fetch-request";

            let _ = rustls::crypto::ring::default_provider().install_default();

            // Records large enough to span several TLS records + partial writes.
            let mut records = Vec::new();
            for i in 0..8000u32 {
                records.extend_from_slice(&i.to_le_bytes());
            }
            let records = Bytes::from(records);
            let (_tf, payload) = file_payload(&records);

            // Throwaway self-signed cert (localhost SAN).
            let key = rcgen::KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
            let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
            let cert = params.self_signed(&key).unwrap();
            let cert_der = rustls::pki_types::CertificateDer::from(cert.der().to_vec());
            let key_der = rustls::pki_types::PrivateKeyDer::try_from(key.serialize_der()).unwrap();

            let mut server_cfg =
                rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                    .with_no_client_auth()
                    .with_single_cert(vec![cert_der.clone()], key_der)
                    .unwrap();
            server_cfg.enable_secret_extraction = true;
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_cfg));

            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert_der).unwrap();
            let client_cfg =
                rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                    .with_root_certificates(roots)
                    .with_no_client_auth();
            let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let expected = records.clone();
            let client = tokio::spawn(async move {
                let tcp = TcpStream::connect(addr).await.unwrap();
                let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
                let mut tls = connector.connect(name, tcp).await.unwrap();
                tls.write_all(REQ).await.unwrap();
                tls.flush().await.unwrap();
                let mut got = vec![0u8; expected.len()];
                tls.read_exact(&mut got).await.unwrap();
                got
            });

            let (tcp, _) = listener.accept().await.unwrap();
            {
                use socket2::SockRef;
                let sr = SockRef::from(&tcp);
                let _ = sr.set_send_buffer_size(16 * 1024);
            }
            let tls = acceptor.accept(ktls::CorkStream::new(tcp)).await.unwrap();
            let mut ktls_stream = match ktls::config_ktls_server(tls).await {
                Ok(s) => s,
                Err(e) => {
                    // Kernel lacks kTLS support — the production startup probe
                    // would have returned false and the fallback path runs.
                    eprintln!(
                        "skipping ktls_sendfile_over_tls_is_byte_exact: kTLS unsupported on this host: {e}"
                    );
                    client.abort();
                    return;
                }
            };

            // Read the client's request through the KtlsStream first (kTLS RX
            // + the ktls crate's drained-bytes replay). The real broker always
            // reads the Fetch request before writing the response.
            let mut req = vec![0u8; REQ.len()];
            ktls_stream.read_exact(&mut req).await.unwrap();
            assert_eq!(req, REQ, "kTLS RX must deliver the request bytes intact");

            // The KtlsStream must report itself sendfile-capable, and the
            // resolver must emit a File op (true zero-copy over TLS).
            assert!(SendfileSink::is_sendfile_capable(&ktls_stream));
            let ops = resolve_records_sendfile(&payload).unwrap();
            assert!(ops.iter().any(|o| matches!(o, WriteOp::File(_))));

            // sendfile the file region onto the kTLS socket — the kernel
            // encrypts it into TLS records on the way out.
            write_fetch_plan(&mut ktls_stream, ops).await.unwrap();
            ktls_stream.flush().await.unwrap();
            drop(ktls_stream); // sends close_notify; EOF for the client tail

            let got = client.await.unwrap();
            assert_eq!(
                got,
                &records[..],
                "client-decrypted kTLS bytes must equal the file region (wire byte-exact)"
            );
        }
    }
}
