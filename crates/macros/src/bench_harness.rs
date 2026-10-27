//! The hand-written `criterion` harness every benchmark suite shares.
//!
//! The suites write their group out by hand rather than with
//! `criterion_group!`: a fallible target has to report its error and stop the
//! run, and a recording suite reports the first failure only after every
//! benchmark has measured. Both shapes are generated here, so the suites keep
//! only their target lists.

use {
    crate::meta::{no_arguments, nonempty_paths},
    moxy::{
        ast::ParseError,
        token::{LitStr, Spanner, ToTokenStream, TokenStream},
    },
};

/// `benches()` running each target in order, and the `criterion_main!` that
/// calls it.
///
/// The input is the comma-separated target paths. A target returns `()` or a
/// `Result` whose error displays; the first error is printed under the
/// target's name and exits the process with status 1. Each target runs under
/// the name `criterion_group!` would give it, its path, when `CODSPEED_ENV` is
/// set.
pub fn main(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let targets = nonempty_paths(input, "expected benchmark target paths")?;
    let named: Vec<_> = targets
        .iter()
        .map(|target| {
            let name: String = target
                .to_token_stream()
                .to_string()
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect();
            (LitStr::new(&name, target.span()), target)
        })
        .collect();
    Ok(moxy::template! {
        /// What a benchmark target returned: nothing, or a result.
        trait BenchOutcome {
            /// The error the target stopped with, if any.
            fn failure(self) -> ::std::option::Option<::std::string::String>;
        }

        impl BenchOutcome for () {
            fn failure(self) -> ::std::option::Option<::std::string::String> {
                ::std::option::Option::None
            }
        }

        impl<E: ::std::fmt::Display> BenchOutcome for ::std::result::Result<(), E> {
            fn failure(self) -> ::std::option::Option<::std::string::String> {
                self.err().map(|error| error.to_string())
            }
        }

        fn benches() {
            let mut criterion = ::criterion::Criterion::default().configure_from_args();
            @for (name, target) in &named {
                if ::std::env::var("CODSPEED_ENV").is_ok() {
                    criterion.set_current_file(::criterion::abs_file!());
                    criterion.set_macro_group(::std::format!("benches::{}", {{ name }}));
                }
                if let ::std::option::Option::Some(error) =
                    BenchOutcome::failure({{ target }}(&mut criterion))
                {
                    ::std::eprintln!("{}: {error}", {{ name }});
                    ::std::process::exit(1);
                }
            }
            ::std::mem::drop(criterion);
        }

        ::criterion::criterion_main!(benches);
    })
}

/// The recording harness of a suite whose benchmarks measure on after one of
/// them fails: `BenchResult`, `FirstError`, `run` and `report_failures`.
///
/// `run` keeps the first error of a group and lets the later groups measure;
/// `report_failures`, the suite's last target, exits with status 1 when one was
/// kept.
pub fn failures(input: &TokenStream) -> Result<TokenStream, ParseError> {
    no_arguments(input.clone(), "bench_failures")?;
    Ok(moxy::template! {
        /// What a benchmark's setup or measured work returns.
        type BenchResult<T = ()> = ::std::result::Result<T, ::std::boxed::Box<dyn ::std::error::Error>>;

        /// The first error a measured closure hit. A closure that criterion
        /// times cannot return an error, so it records one here and the
        /// benchmark function returns it once the measurement is over.
        #[derive(Default)]
        struct FirstError(::std::option::Option<::std::boxed::Box<dyn ::std::error::Error>>);

        impl FirstError {
            /// The value of `result`, or `None` after recording its error.
            fn record<T, E: ::std::convert::Into<::std::boxed::Box<dyn ::std::error::Error>>>(
                &mut self,
                result: ::std::result::Result<T, E>,
            ) -> ::std::option::Option<T> {
                match result {
                    ::std::result::Result::Ok(value) => ::std::option::Option::Some(value),
                    ::std::result::Result::Err(error) => {
                        self.0.get_or_insert_with(|| error.into());
                        ::std::option::Option::None
                    }
                }
            }

            /// The recorded error, if any.
            fn into_result(self) -> BenchResult {
                self.0.map_or(::std::result::Result::Ok(()), ::std::result::Result::Err)
            }
        }

        /// The error the benchmarks stopped with, which `report_failures` reports.
        static FAILURE: ::std::sync::Mutex<::std::option::Option<::std::string::String>> =
            ::std::sync::Mutex::new(::std::option::Option::None);

        /// Run the benchmarks of one group, keeping the error they stopped with.
        fn run(
            c: &mut ::criterion::Criterion,
            name: &str,
            bench: impl ::std::ops::FnOnce(
                &mut ::criterion::BenchmarkGroup<'_, ::criterion::measurement::WallTime>,
            ) -> BenchResult,
        ) {
            let mut group = c.benchmark_group(name);
            if let ::std::result::Result::Err(error) = bench(&mut group) {
                FAILURE
                    .lock()
                    .unwrap_or_else(::std::sync::PoisonError::into_inner)
                    .get_or_insert_with(|| ::std::format!("{name}: {error}"));
            }
            group.finish();
        }

        /// Exit with a failure when a benchmark function stopped on an error.
        /// It runs last, after every benchmark that could still measure.
        fn report_failures(_c: &mut ::criterion::Criterion) {
            let failure = FAILURE
                .lock()
                .unwrap_or_else(::std::sync::PoisonError::into_inner)
                .take();
            if let ::std::option::Option::Some(failure) = failure {
                ::std::eprintln!("benchmark failed: {failure}");
                ::std::process::exit(1);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    fn compact(tokens: &TokenStream) -> String {
        tokens
            .to_string()
            .chars()
            .filter(|c| !c.is_whitespace())
            .collect()
    }

    #[yare::parameterized(
        empty = { "" },
        bad_separator = { "a; b" },
        not_a_path = { "42" },
    )]
    fn main_needs_a_target_list(input: &str) {
        assert2::assert!(main(&input.parse().unwrap()).is_err(), "{input}");
    }

    #[test]
    fn each_target_runs_under_its_path_and_its_failure_stops_the_run() {
        let generated = compact(&main(&"a, m::b".parse().unwrap()).unwrap());
        assert!(generated.contains("\"m::b\""));
        assert!(generated.contains("BenchOutcome::failure(m::b(&mutcriterion))"));
        assert!(generated.matches("::std::process::exit(1)").count() == 2);
        assert!(generated.contains("criterion_main!(benches)"));
    }

    #[test]
    fn failures_takes_no_arguments() {
        assert!(failures(&"x".parse().unwrap()).is_err());
        assert!(compact(&failures(&TokenStream::new()).unwrap()).contains("fnreport_failures"));
    }
}
