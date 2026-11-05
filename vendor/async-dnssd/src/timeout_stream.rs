use futures_core::Stream;
use std::{
	future::Future,
	pin::Pin,
	task::{Context, Poll},
	time::Duration,
};

/// `Stream` extension to simplify building
/// [`TimeoutStream`](struct.TimeoutStream.html)
pub trait StreamTimeoutExt: Stream + Sized {
	/// Create new [`TimeoutStream`](struct.TimeoutStream.html)
	fn timeout(self, duration: Duration) -> TimeoutStream<Self>;
}

impl<S: Stream> StreamTimeoutExt for S {
	fn timeout(self, duration: Duration) -> TimeoutStream<Self> {
		TimeoutStream::new(self, duration)
	}
}

/// Add a timeout to a stream; each time an item is received the timer
/// is reset
///
/// If the timeout triggers the stream ends (without an error).
#[must_use = "streams do nothing unless polled"]
#[pin_project::pin_project]
pub struct TimeoutStream<S> {
	#[pin]
	stream: S,
	duration: Duration,
	#[pin]
	timeout: tokio::time::Sleep,
}

impl<S: Stream> TimeoutStream<S> {
	fn stream(self: Pin<&mut Self>) -> Pin<&mut S> {
		self.project().stream
	}

	fn timeout(self: Pin<&mut Self>) -> Pin<&mut tokio::time::Sleep> {
		self.project().timeout
	}

	/// Create new `TimeoutStream`.
	///
	/// Also see [`StreamTimeoutExt::timeout`](trait.StreamTimeoutExt.html#method.timeout).
	pub fn new(stream: S, duration: Duration) -> Self {
		Self {
			stream,
			duration,
			timeout: tokio::time::sleep(duration),
		}
	}
}

impl<S: Stream> TimeoutStream<S> {
	fn reset_timer(self: Pin<&mut Self>) {
		let next = tokio::time::Instant::now() + self.duration;
		self.timeout().reset(next);
	}
}

impl<S: futures_core::TryStream> Stream for TimeoutStream<S> {
	type Item = Result<S::Ok, S::Error>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		match self.as_mut().stream().try_poll_next(cx) {
			Poll::Ready(None) => Poll::Ready(None), // end of stream
			Poll::Ready(Some(Ok(item))) => {
				// not end of stream: reset timeout
				self.reset_timer();
				Poll::Ready(Some(Ok(item)))
			},
			Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
			Poll::Pending => {
				// check timeout
				match self.timeout().poll(cx) {
					// timed out?
					Poll::Ready(()) => {
						// not an error
						Poll::Ready(None)
					},
					// still time left
					Poll::Pending => Poll::Pending,
				}
			},
		}
	}
}
