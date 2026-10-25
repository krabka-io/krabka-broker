//! Renders an [`IpAddr`] the way the JDK's `InetAddress.getHostAddress()`
//! renders it, because that is the text Kafka compares ACL hosts against.
//!
//! Rust's [`std::fmt::Display`] impl for [`Ipv6Addr`] and [`IpAddr`] favors
//! the RFC 5952 compressed form (`::1`), and keeps an IPv4-mapped IPv6 peer
//! (`::ffff:10.0.0.5`) as an IPv6 address. The JDK does neither:
//!
//! - `Inet6Address.getHostAddress()` writes all eight 16-bit groups in lower
//!   hex, colon-separated, with no leading zeros dropped from the group count
//!   and no `::` zero-compression, e.g. loopback is `0:0:0:0:0:0:0:1`.
//! - The socket layer hands the JDK an `Inet4Address` for an IPv4-mapped IPv6
//!   peer, so `getHostAddress()` on that peer is the dotted-quad form, e.g.
//!   `10.0.0.5`, not `::ffff:10.0.0.5`.
//!
//! [`jdk_host_address`] reproduces both: [`IpAddr::to_canonical`] performs the
//! IPv4-mapped unwrap, then a genuine IPv6 address is rendered group by group.
//! [`jdk_host_address_is`] compares that text against an ACL host without
//! building it, for the per-request authorization path.

use std::{
    fmt::{self, Write},
    net::IpAddr,
};

/// Formats `ip` the way `InetAddress.getHostAddress()` would, so it can be
/// compared against an ACL host string written by Kafka tooling.
#[must_use]
pub fn jdk_host_address(ip: IpAddr) -> String {
    let mut out = String::with_capacity(39);
    let _ = write_jdk_host_address(ip, &mut out);
    out
}

/// True when [`jdk_host_address`] of `ip` equals `text`, decided without
/// allocating: the rendering is streamed into a comparator that consumes
/// `text` piece by piece.
pub(crate) fn jdk_host_address_is(ip: IpAddr, text: &str) -> bool {
    let mut rest = PrefixMatcher { rest: text };
    write_jdk_host_address(ip, &mut rest).is_ok() && rest.rest.is_empty()
}

/// IPv4 in dotted-quad form; a genuine IPv6 address as eight lower-hex groups,
/// colon-separated, with no zero-compression -- the form
/// `Inet6Address.getHostAddress()` uses.
fn write_jdk_host_address(ip: IpAddr, out: &mut impl Write) -> fmt::Result {
    match ip.to_canonical() {
        IpAddr::V4(v4) => write!(out, "{v4}"),
        IpAddr::V6(v6) => {
            for (i, segment) in v6.segments().iter().enumerate() {
                if i > 0 {
                    out.write_char(':')?;
                }
                write!(out, "{segment:x}")?;
            }
            Ok(())
        }
    }
}

/// A [`Write`] sink that fails as soon as the written text stops being a
/// prefix of `rest`, and otherwise leaves the unmatched remainder in `rest`.
struct PrefixMatcher<'a> {
    rest: &'a str,
}

impl Write for PrefixMatcher<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        self.rest = self.rest.strip_prefix(s).ok_or(fmt::Error)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{jdk_host_address, jdk_host_address_is},
        proptest::prelude::*,
        std::net::{IpAddr, Ipv4Addr, Ipv6Addr},
    };

    #[yare::parameterized(
        ipv4_localhost = { IpAddr::V4(Ipv4Addr::LOCALHOST), "127.0.0.1" },
        ipv4_other = { IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), "10.0.0.5" },
        ipv6_localhost_expanded = { IpAddr::V6(Ipv6Addr::LOCALHOST), "0:0:0:0:0:0:0:1" },
        ipv6_unspecified_expanded = { IpAddr::V6(Ipv6Addr::UNSPECIFIED), "0:0:0:0:0:0:0:0" },
        ipv6_groups_expanded = {
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 5)),
            "2001:db8:0:0:0:0:0:5",
        },
        ipv4_mapped_ipv6 = {
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 0x0005)),
            "10.0.0.5",
        },
    )]
    fn matches_jdk_text_form(ip: IpAddr, expected: &str) {
        assert2::assert!(jdk_host_address(ip) == expected);
        assert2::assert!(jdk_host_address_is(ip, expected));
    }

    /// The streaming comparison rejects a text that is a strict prefix of the
    /// rendering, one that extends it, and Rust's compressed IPv6 form.
    #[yare::parameterized(
        truncated = { IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), "10.0.0." },
        extended = { IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)), "10.0.0.50" },
        compressed_ipv6 = { IpAddr::V6(Ipv6Addr::LOCALHOST), "::1" },
        mapped_ipv6_text = {
            IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0xffff, 0x0a00, 0x0005)),
            "::ffff:10.0.0.5",
        },
        empty = { IpAddr::V4(Ipv4Addr::LOCALHOST), "" },
        wildcard = { IpAddr::V4(Ipv4Addr::LOCALHOST), "*" },
    )]
    fn rejects_other_text(ip: IpAddr, text: &str) {
        assert2::assert!(!jdk_host_address_is(ip, text));
    }

    proptest! {
        /// The streaming comparison agrees with comparing against the built
        /// string, for the rendering itself and for any other text.
        #[test]
        fn streaming_comparison_agrees_with_rendering(
            ip in any::<IpAddr>(),
            other in "[0-9a-f:.]{0,40}",
        ) {
            let rendered = jdk_host_address(ip);
            assert2::assert!(jdk_host_address_is(ip, &rendered));
            assert2::assert!(jdk_host_address_is(ip, &other) == (other == rendered));
        }
    }
}
