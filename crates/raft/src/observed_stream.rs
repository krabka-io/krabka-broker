//! A stream that passes every byte through and reports each poll's outcome,
//! the plumbing that the listeners' idle windows share.

use {
    std::{
        io::{self, IoSlice},
        pin::Pin,
        task::{Context, Poll},
    },
    tokio::io::{AsyncRead, AsyncWrite, ReadBuf},
};

/// What an [`ObservedStream`] reports to.
///
/// Each method sees one poll of the wrapped stream and, for a write, flush or
/// shutdown, returns what the caller gets. The defaults pass every outcome on
/// unchanged.
pub trait StreamObserver {
    /// A read poll moved `filled` bytes into the caller's buffer.
    fn on_read(&mut self, _filled: usize) {}

    /// A write or vectored write polled `result`.
    fn on_write(
        &mut self,
        _cx: &mut Context<'_>,
        result: Poll<io::Result<usize>>,
    ) -> Poll<io::Result<usize>> {
        result
    }

    /// A flush or shutdown polled `result`.
    fn on_flush(
        &mut self,
        _cx: &mut Context<'_>,
        result: Poll<io::Result<()>>,
    ) -> Poll<io::Result<()>> {
        result
    }
}

/// A stream that reads and writes through `inner` and shows `observer` every
/// poll.
pub struct ObservedStream<S, O> {
    inner: S,
    observer: O,
}

impl<S, O> ObservedStream<S, O> {
    /// Wraps `inner`, reporting to `observer`.
    pub fn new(inner: S, observer: O) -> Self { Self { inner, observer } }

    /// The wrapped stream, for what a caller reads off the socket itself,
    /// such as the peer address.
    pub fn get_ref(&self) -> &S { &self.inner }

    /// What the stream reports to.
    pub fn observer(&self) -> &O { &self.observer }
}

impl<S: AsyncRead + Unpin, O: StreamObserver + Unpin> AsyncRead for ObservedStream<S, O> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        let result = Pin::new(&mut this.inner).poll_read(cx, buf);
        this.observer.on_read(buf.filled().len() - before);
        result
    }
}

impl<S: AsyncWrite + Unpin, O: StreamObserver + Unpin> AsyncWrite for ObservedStream<S, O> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.observer.on_write(cx, result)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_write_vectored(cx, bufs);
        this.observer.on_write(cx, result)
    }

    fn is_write_vectored(&self) -> bool { self.inner.is_write_vectored() }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_flush(cx);
        this.observer.on_flush(cx, result)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let result = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.observer.on_flush(cx, result)
    }
}
