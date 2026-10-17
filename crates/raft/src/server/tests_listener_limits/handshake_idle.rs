//! `connections.max.idle.ms` covers the TLS and SASL handshake as it covers a
//! request: Kafka's `Selector` refreshes a channel's idle clock on every read
//! and write, authenticating or not, so the window bounds the silence of a
//! handshake, not its length.

use {
    super::*,
    crate::HandshakeStream,
    tokio::{io::DuplexStream, time::Instant},
};

/// The bytes each side of the stand-in handshake sends.
const HANDSHAKE_BYTES: usize = 8;

/// Reads the peer's handshake bytes, answers with as many, and hands the
/// stream back, as a TLS or SASL exchange hands the listener its stream.
async fn stand_in_handshake(
    mut stream: HandshakeStream<DuplexStream>,
) -> std::io::Result<HandshakeStream<DuplexStream>> {
    let mut request = [0u8; HANDSHAKE_BYTES];
    stream.read_exact(&mut request).await?;
    stream.write_all(&[0xab; HANDSHAKE_BYTES]).await?;
    stream.flush().await?;
    Ok(stream)
}

/// A peer that sends `sent` handshake bytes and reads `received` answer bytes,
/// one byte six milliseconds after the last. Over a 10 ms window, a whole
/// handshake takes about a tenth of a second. The pipe holds one byte each
/// way, so the listener's answer moves only as fast as the peer reads it.
///
/// A peer that finishes is served, and its stream outlives a silence longer
/// than the window once the request loop has it. A peer that stops is closed
/// one window after the last byte either side moved.
#[yare::parameterized(
    trickled_to_the_end = { HANDSHAKE_BYTES, HANDSHAKE_BYTES, true },
    stalls_while_sending = { HANDSHAKE_BYTES / 2, 0, false },
    stalls_while_reading_the_answer = { HANDSHAKE_BYTES, 3, false },
)]
#[test_macro(tokio::test(start_paused = true))]
async fn the_handshake_idle_window_restarts_on_every_byte(
    sent: usize,
    received: usize,
    completes: bool,
) {
    let window = Duration::from_millis(10);
    let gap = Duration::from_millis(6);
    let (mut client, server) = tokio::io::duplex(1);
    let mut upgrade = tokio::spawn(upgrade_within_idle_window(
        server,
        Some(window),
        stand_in_handshake,
    ));

    let mut last_byte = Instant::now();
    for _ in 0..sent {
        tokio::time::sleep(gap).await;
        if client.write_all(&[0x5a]).await.is_err() {
            break;
        }
        last_byte = Instant::now();
    }
    let mut answer = Vec::new();
    for _ in 0..received {
        tokio::time::sleep(gap).await;
        let mut byte = [0u8];
        if client.read_exact(&mut byte).await.is_err() {
            break;
        }
        answer.push(byte[0]);
        last_byte = Instant::now();
    }

    let upgraded = tokio::time::timeout(Duration::from_secs(5), &mut upgrade)
        .await
        .expect("the handshake ends")
        .expect("the handshake task does not panic");
    check!(answer == vec![0xab; received]);
    if completes {
        let mut stream = upgraded
            .expect("the handshake is not closed as idle")
            .expect("the handshake succeeds");
        tokio::time::sleep(window * 3).await;
        client.write_all(&[0x01]).await.unwrap();
        let mut byte = [0u8];
        stream.read_exact(&mut byte).await.unwrap();
        check!(byte == [0x01]);
    } else {
        check!(
            upgraded.is_none(),
            "the stalled handshake is closed as idle"
        );
        check!(last_byte.elapsed() == window);
    }
}
