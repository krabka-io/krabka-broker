use futures_channel::mpsc;
use futures_util::StreamExt;
use std::{
	io,
	os::raw::c_void,
	pin::Pin,
	task::{
		Context,
		Poll,
	},
};

use crate::{
	error::Error,
	inner::EventedService,
};

type CallbackContext<T> = mpsc::UnboundedSender<io::Result<T>>;

#[must_use = "streams do nothing unless polled"]
pub(crate) struct ServiceStream<S: EventedService, T> {
	service: S,
	_sender: Box<CallbackContext<T>>,
	receiver: mpsc::UnboundedReceiver<io::Result<T>>,
}

impl<S: EventedService, T> ServiceStream<S, T> {
	crate::future::callback_handler!(|sender: &mut CallbackContext<T>, error_code, f| {
		sender
			.unbounded_send(crate::future::callback_result(error_code, f))
			.expect("receiver must still be alive");
	});

	pub(crate) fn new<F>(f: F) -> io::Result<Self>
	where
		F: FnOnce(*mut c_void) -> Result<S, Error>,
	{
		let (sender, receiver) = mpsc::unbounded::<io::Result<T>>();
		let mut sender = Box::new(sender);

		let service = f(crate::future::box_raw(&mut sender))?;

		Ok(Self {
			service,
			_sender: sender,
			receiver,
		})
	}
}

impl<S: EventedService, T> futures_core::Stream for ServiceStream<S, T> {
	type Item = io::Result<T>;

	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
		self.service.poll_service(cx)?;
		self.receiver.poll_next_unpin(cx)
	}
}
