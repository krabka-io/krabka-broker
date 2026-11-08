//! The platforms with a usable file-to-socket `sendfile(2)`, which items on the
//! zero-copy fetch path are gated on.

use moxy::token::TokenStream;

pub fn platform_cfg() -> TokenStream {
    moxy::template! {
        #[cfg(any(target_os = "linux", target_os = "macos", target_os = "ios",
            target_os = "tvos", target_os = "watchos", target_os = "freebsd", target_os = "dragonfly"))]
    }
}

/// Apply the shared file-region platform condition without rebuilding the caller's tokens.
pub fn sendfile_platform(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut output: proc_macro::TokenStream = platform_cfg().into();
    output.extend(input);
    output
}
