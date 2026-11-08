//! Shared command-line entry points and independent parser/filesystem fixtures.

use {
    crate::{
        fixtures::{name, named_arguments},
        meta::{ItemBody, argument_array, item_body},
    },
    moxy::{
        ast::ParseError,
        token::{Span, Spanner, TokenStream, TokenTree},
    },
};

pub fn flag_metadata_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(arg: &::clap::Arg) -> (&str, String, Vec<String>) {
            let long = arg.get_long().unwrap_or_default();
            let env = arg.get_env().map(|env| env.to_string_lossy().into_owned()).unwrap_or_default();
            let defaults = arg.get_default_values().iter().map(|value| value.to_string_lossy().into_owned()).collect();
            (long, env, defaults)
        }
    })
}

pub fn bind_retry_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, [timeout]) = named_arguments(input)?;
    Ok(moxy::template! {
        pub(super) async fn {{ name }}(addr: ::std::net::SocketAddr) -> ::tokio::net::TcpListener {
            let deadline = ::tokio::time::Instant::now() + {{ timeout }};
            loop {
                match ::tokio::net::TcpListener::bind(addr).await {
                    Ok(listener) => return listener,
                    Err(err) if ::tokio::time::Instant::now() < deadline => {
                        let _ = err;
                        ::tokio::time::sleep(::std::time::Duration::from_millis(10)).await;
                    }
                    Err(err) => panic!("listener address {addr} was not released: {err}"),
                }
            }
        }
    })
}

pub fn frame_max_parser(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(value: &str) -> Result<::krabka_units::ByteSize, String> {
            let value = ::krabka_units::parse::positive_byte_size(value)
                .map_err(|error| error.to_string())?;
            ::krabka_client_core::ClientFrameMax::try_from(value)
                .map(::krabka_client_core::ClientFrameMax::size)
        }
    })
}

pub fn dispatch_capacity_parser(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(value: &str) -> Result<usize, String> {
            let value = value.parse::<usize>().map_err(|error| error.to_string())?;
            ::krabka_client_core::ConnectionDispatchQueueCapacity::new(value)
                .map(::krabka_client_core::ConnectionDispatchQueueCapacity::get)
        }
    })
}

fn no_options(meta: TokenStream) -> Result<(), ParseError> {
    meta.into_iter().next().map_or(Ok(()), |token| {
        Err(ParseError::new(
            token.span(),
            "this attribute takes no arguments",
        ))
    })
}

pub fn argv_entrypoint(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    no_options(meta)?;
    let ItemBody { tokens, body_index } =
        item_body(item, "argv_entrypoint", TokenTree::is_keyword_fn)?;
    let header_error = || ParseError::new(Span::call_site(), "expected an entry point function");
    let function = tokens
        .iter()
        .position(TokenTree::is_keyword_fn)
        .ok_or_else(header_error)?;
    let (Some(prefix), Some(declaration), Some(body)) = (
        tokens.get(..function),
        tokens.get(function + 1..body_index),
        tokens.get(body_index),
    ) else {
        return Err(header_error());
    };
    let [
        TokenTree::Ident(name),
        TokenTree::Group(arguments),
        arrow_start,
        arrow_end,
        TokenTree::Ident(output),
    ] = declaration
    else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected an entry point declared as fn name(...) -> i32",
        ));
    };
    if !arguments.delim.is_paren()
        || !arrow_start.is_punct_minus()
        || !arrow_end.is_punct_gt()
        || output != "i32"
    {
        return Err(ParseError::new(
            name.span(),
            "expected an entry point declared as fn name(...) -> i32",
        ));
    }
    let prefix: TokenStream = prefix.to_vec().into();
    let extra = &arguments.tokens;
    Ok(moxy::template! {
        {{ prefix }} fn {{ name }}<I, T>(argv: I, {{ extra }}) -> i32
        where
            I: IntoIterator<Item = T>,
            T: Into<::std::ffi::OsString> + Clone,
        {{ body }}
    })
}

pub fn duration_cases(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    let assertion = duration_parse_assertion();
    Ok(moxy::template! {
        fn {{ name }}(cases: &[(&str, Option<i64>)]) {
            for &(raw, expected) in cases {
                {{ assertion }}
            }
        }
    })
}

fn duration_parse_assertion() -> TokenStream {
    moxy::template! {
        ::assert2::check!(parse_time(raw).ok().map(::krabka_units::Time::millis_i64) == expected, "{raw}");
    }
}

pub fn directory_tree(input: TokenStream) -> Result<TokenStream, ParseError> {
    let (name, [element, docs, project]) = named_arguments(input)?;
    let docs = docs.into_iter().collect::<Vec<_>>();
    let [TokenTree::Group(docs)] = docs.as_slice() else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected a braced documentation block",
        ));
    };
    if !docs.delim.is_brace() {
        return Err(ParseError::new(
            docs.span(),
            "expected a braced documentation block",
        ));
    }
    let docs = &docs.tokens;
    let project: Vec<_> = project.into_iter().collect();
    let project: TokenStream = match project.as_slice() {
        [TokenTree::Group(group)] if group.delim.is_paren() => group.tokens.clone(),
        _ => project.into(),
    };
    Ok(moxy::template! {
        {{ docs }}
        fn {{ name }}(dir: &::std::path::Path) -> Vec<{{ element }}> {
            let mut files = Vec::new();
            let mut pending = vec![dir.to_path_buf()];
            while let Some(next) = pending.pop() {
                for entry in ::std::fs::read_dir(&next).expect("list") {
                    let path = entry.expect("entry").path();
                    if path.is_dir() {
                        pending.push(path);
                    } else {
                        files.push(({{ project }})(dir, &path));
                    }
                }
            }
            files.sort();
            files
        }
    })
}

fn documentation(input: TokenStream) -> Result<TokenStream, ParseError> {
    let tokens: Vec<_> = input.into_iter().collect();
    match tokens.as_slice() {
        [TokenTree::Group(group)] if group.delim.is_brace() => Ok(group.tokens.clone()),
        _ => Err(ParseError::new(
            Span::call_site(),
            "expected a braced documentation block",
        )),
    }
}

/// `run_from_args`, which turns `argv` into a `Cli` with `parse`, an expression
/// of type `Result<Cli, clap::Error>`, and hands the result to the caller's
/// `run`.
///
/// A parse error prints clap's message and returns the caller's `EXIT_USAGE`,
/// so the code is the tool's own choice rather than clap's `2`. clap reports
/// `--help` and `--version` as errors too, with exit code `0`, and those print
/// to stdout and return `0`.
fn cli_entrypoint(input: TokenStream, parse: &TokenStream) -> Result<TokenStream, ParseError> {
    let docs = documentation(input)?;
    Ok(moxy::template! {
        {{ docs }}
        pub async fn run_from_args<I, T>(argv: I) -> i32
        where
            I: IntoIterator<Item = T>,
            T: Into<::std::ffi::OsString> + Clone,
        {
            match {{ parse }} {
                Ok(cli) => run(cli).await,
                Err(error) => {
                    // A stream that refuses clap's message leaves the exit
                    // code as the whole report.
                    _ = error.print();
                    if error.exit_code() == 0 { 0 } else { EXIT_USAGE }
                }
            }
        }
    })
}

pub fn parsed_cli_entrypoint(input: TokenStream) -> Result<TokenStream, ParseError> {
    cli_entrypoint(input, &moxy::template! { Cli::try_parse_from(argv) })
}

pub fn bootstrap_cli_entrypoint(input: TokenStream) -> Result<TokenStream, ParseError> {
    cli_entrypoint(input, &moxy::template! {
        ::krabka_cli_connection::try_parse_with_env::<Cli, _, _>(argv, |name| ::std::env::var(name).ok())
    })
}

pub fn duration_parser_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [docs, cases] = argument_array(input)?;
    let docs = documentation(docs)?;
    let cases: Vec<_> = cases.into_iter().collect();
    let [TokenTree::Group(cases)] = cases.as_slice() else {
        return Err(ParseError::new(
            Span::call_site(),
            "expected duration case list",
        ));
    };
    if !cases.delim.is_bracket() {
        return Err(ParseError::new(cases.span(), "expected duration case list"));
    }
    let cases = cases.tokens.clone();
    let assertion = duration_parse_assertion();
    Ok(moxy::template! {
        {{ docs }}
        #[yare::parameterized(
            milliseconds = { "500ms", Some(500) },
            seconds = { "30s", Some(30_000) },
            {{ cases }}
        )]
        fn a_time_argument_takes_any_unit(raw: &str, expected: Option<i64>) {
            {{ assertion }}
        }
    })
}

pub fn bootstrap_parser_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [refused, accepted] = argument_array(input)?;
    Ok(moxy::template! {
        /// `--bootstrap-server` is the one flag every subcommand needs, so the
        /// parser refuses a command line without it rather than defaulting to a
        /// guess about where the cluster is.
        #[test]
        fn a_command_line_without_a_bootstrap_server_is_refused() {
            ::assert2::assert!(Cli::try_parse_from({{ refused }}).is_err());
            ::assert2::assert!(Cli::try_parse_from({{ accepted }}).is_ok());
        }
    })
}

pub fn snapshot_node_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        fn {{ name }}() -> ::tempfile::TempDir {
            let log_dir = ::tempfile::tempdir().expect("log dir");
            let rlmm = log_dir.path().join("remote-log-metadata");
            ::std::fs::create_dir_all(&rlmm).expect("create the rlmm dir");
            ::std::fs::write(rlmm.join("snapshot"), b"rlmm snapshot bytes").expect("write the rlmm snapshot");
            log_dir
        }
    })
}

pub fn format_directories_fixture(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        fn {{ name }}() -> (::tempfile::TempDir, ::std::path::PathBuf, ::std::path::PathBuf) {
            let dir = ::tempfile::tempdir().expect("tempdir");
            let metadata = dir.path().join("meta");
            let data = dir.path().join("data");
            (dir, metadata, data)
        }
    })
}
