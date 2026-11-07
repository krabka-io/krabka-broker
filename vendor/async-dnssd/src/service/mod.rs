mod callback_support {
	pub(super) use crate::{
		cstr,
		ffi,
		inner,
		interface::Interface,
	};
	pub(super) use std::{
		io,
		os::raw::{
			c_char,
			c_void,
		},
	};
}
type ServiceCallback<T> = crate::stream::ServiceStream<crate::inner::OwnedService, T>;

// All callback streams share the same structurally pinned forwarding implementation.
macro_rules! callback_stream {
    ($(#[$attr:meta])* $name:ident, $item:ty) => {
        $(#[$attr])*
        #[must_use = "streams do nothing unless polled"]
        #[pin_project::pin_project]
        pub struct $name {
            #[pin]
            stream: crate::fused_err_stream::FusedErrorStream<CallbackStream>,
        }
        impl futures_core::Stream for $name {
            type Item = std::io::Result<$item>;
            fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
                -> std::task::Poll<Option<Self::Item>> {
                self.project().stream.poll_next(cx)
            }
        }
    };
}

pub use self::{
	browse::*,
	connection::*,
	enumerate_domains::*,
	query_record::*,
	records::Record,
	register::*,
	resolve::*,
	resolve_host::*,
};

mod browse;
mod connection;
mod enumerate_domains;
mod query_record;
mod records;
mod register;
mod resolve;
mod resolve_host;

use crate::dns_consts::{
	Class,
	Type,
};
use std::os::raw::c_char;

/// Purge record from cache
///
/// See [`DNSServiceReconfirmRecord`](https://developer.apple.com/documentation/dnssd/1804726-dnsservicereconfirmrecord).
#[doc(alias = "DNSServiceReconfirmRecord")]
pub fn reconfirm_record(
	interface: crate::interface::Interface,
	fullname: &str,
	rr_type: Type,
	rr_class: Class,
	rdata: &[u8],
) -> ::std::io::Result<()> {
	crate::init();

	let fullname = crate::cstr::CStr::from(&fullname)?;
	crate::inner::reconfirm_record(
		0, // no flags
		interface.into_raw(),
		&fullname,
		rr_type,
		rr_class,
		rdata,
	);

	Ok(())
}

/// Full name consiting of (up to) three parts
pub struct FullName<'a> {
	/// (unescaped) service name (becomes single label in full name)
	pub service: Option<&'a str>,
	/// registration type (valid names don't need escaping)
	pub reg_type: &'a str,
	/// (escaped) domain name (most names don't need escaping)
	pub domain: &'a str,
}

impl<'a> FullName<'a> {
	/// Escape and concatenate all three parts to a full name
	///
	/// See [`DNSServiceConstructFullName`](https://developer.apple.com/documentation/dnssd/1804753-dnsserviceconstructfullname)
	#[doc(alias = "DNSServiceConstructFullName")]
	pub fn construct(&self) -> ::std::io::Result<String> {
		use std::io;

		let service = crate::cstr::NullableCStr::from(&self.service)?;
		let reg_type = crate::cstr::CStr::from(&self.reg_type)?;
		let domain = crate::cstr::CStr::from(&self.domain)?;

		const SIZE: usize = crate::ffi::MAX_DOMAIN_NAME;
		let mut buf: Vec<u8> = Vec::with_capacity(SIZE);
		let result = unsafe {
			crate::ffi::DNSServiceConstructFullName(
				buf.as_mut_ptr() as *mut c_char,
				service.as_ptr(),
				reg_type.as_ptr(),
				domain.as_ptr(),
			)
		};

		if result != 0 {
			// manual only mentions a single possible error (kDNSServiceErr_BadParam), so we use a static io::Error here for now
			// TODO: convert to our normal `Error` type?
			return Err(io::Error::new(io::ErrorKind::InvalidInput, "BadParam"));
		}

		// ensure NUL termination (MAX_DOMAIN_NAME includes space for trailing NUL, so content must fit)
		buf.spare_capacity_mut()[SIZE - 1].write(0);
		unsafe {
			buf.set_len(libc::strlen(buf.as_ptr() as *const libc::c_char));
		};

		String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))
	}
}
