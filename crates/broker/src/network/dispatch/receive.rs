//! Reserve request bytes after the size prefix and before reading the body.

use {
    super::RequestByteBudget,
    crate::time_util::sleep_until_opt,
    bytes::{Bytes, BytesMut},
    std::{io, time::Duration},
    tokio::{
        io::{AsyncRead, AsyncReadExt as _},
        sync::OwnedSemaphorePermit,
    },
};

pub(super) struct RequestFrame {
    pub(super) bytes: Bytes,
    pub(super) byte_permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug)]
pub(super) enum ReceiveError {
    Idle,
    Io(io::Error),
}

impl From<io::Error> for ReceiveError {
    fn from(error: io::Error) -> Self { Self::Io(error) }
}

impl RequestByteBudget {
    async fn acquire(&self, frame_bytes: usize) -> io::Result<OwnedSemaphorePermit> {
        let wanted = frame_bytes.max(1);
        if wanted > self.total {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame exceeds queued.max.request.bytes",
            ));
        }
        let wanted = u32::try_from(wanted).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "request size exceeds the int32 frame limit",
            )
        })?;
        self.permits
            .clone()
            .acquire_many_owned(wanted)
            .await
            .map_err(|_| io::Error::other("request-byte budget is closed"))
    }
}

/// An idle peer holds no capacity, and a waiting size prefix holds no body.
/// Broker byte-budget waits do not consume the client's idle window.
///
/// The idle window measures silence, as Kafka's `Selector` does by refreshing
/// a channel's idle clock on every read it serves: each read that delivers a
/// byte starts a fresh window, so a frame that arrives slowly but steadily is
/// read however long it takes in all.
pub(super) async fn receive_request<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_request_bytes: usize,
    budget: Option<&RequestByteBudget>,
    idle: Option<Duration>,
) -> Result<Option<RequestFrame>, ReceiveError> {
    let Some(size) = read_size(stream, max_request_bytes, idle).await? else {
        return Ok(None);
    };
    let byte_permit = match budget {
        Some(budget) => Some(budget.acquire(size).await?),
        None => None,
    };
    // Allocate only after the advertised body has reserved its whole budget.
    let bytes = read_body(stream, size, idle).await?;
    Ok(Some(RequestFrame { bytes, byte_permit }))
}

/// One read, given up once `idle` passes without it delivering anything.
async fn read_within_idle<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut [u8],
    idle: Option<Duration>,
) -> Result<usize, ReceiveError> {
    let deadline = idle.map(|window| tokio::time::Instant::now() + window);
    tokio::select! {
        biased;
        () = sleep_until_opt(deadline) => Err(ReceiveError::Idle),
        result = stream.read(buf) => result.map_err(ReceiveError::Io),
    }
}

/// Fills `buf`, with a fresh idle window for each read.
async fn fill_within_idle<S: AsyncRead + Unpin>(
    stream: &mut S,
    buf: &mut [u8],
    idle: Option<Duration>,
) -> Result<(), ReceiveError> {
    let mut filled = 0;
    while filled < buf.len() {
        match read_within_idle(stream, &mut buf[filled..], idle).await? {
            0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
            read => filled += read,
        }
    }
    Ok(())
}

async fn read_size<S: AsyncRead + Unpin>(
    stream: &mut S,
    maximum: usize,
    idle: Option<Duration>,
) -> Result<Option<usize>, ReceiveError> {
    let mut prefix = [0_u8; 4];
    let read = read_within_idle(stream, &mut prefix, idle).await?;
    if read == 0 {
        return Ok(None);
    }
    fill_within_idle(stream, &mut prefix[read..], idle).await?;
    let size = usize::try_from(i32::from_be_bytes(prefix))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "negative Kafka frame size"))?;
    if size > maximum {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame exceeds configured maximum size",
        )
        .into());
    }
    Ok(Some(size))
}

async fn read_body<S: AsyncRead + Unpin>(
    stream: &mut S,
    size: usize,
    idle: Option<Duration>,
) -> Result<Bytes, ReceiveError> {
    let mut body = BytesMut::with_capacity(size);
    body.resize(size, 0);
    fill_within_idle(stream, &mut body, idle).await?;
    Ok(body.freeze())
}

#[cfg(test)]
mod tests;
