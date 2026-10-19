//! Library argument parsing must return control to its embedding process.

use std::process::Command;

#[yare::parameterized(
    unknown_flag = { "--unknown-format-flag", 2 },
    help = { "--help", 0 },
    version = { "--version", 0 },
)]
fn library_argv_returns_instead_of_exiting_the_embedding_process(flag: &str, expected: i32) {
    const CHILD: &str = "KRABKA_FORMAT_ARGV_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            assert2::assert!(krabka_format::run_from_args(["format", flag]).await == expected);
            assert2::assert!(
                krabka_format::run_from_args_with_records(["format", flag], Vec::new()).await
                    == expected
            );
        });
        println!("formatter returned to caller");
        return;
    }
    let current = std::thread::current();
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            current.name().expect("test thread name"),
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    assert2::assert!(output.status.success(), "{output:?}");
    assert2::assert!(
        String::from_utf8_lossy(&output.stdout).contains("formatter returned to caller")
    );
}

/// A caller that formats as one step of its own command gets the progress lines
/// in its own sink, so its stdout stays its own.
#[tokio::test]
async fn progress_lines_go_to_the_callers_sink() {
    const CLUSTER_ID: &str = "8a2f4e1c-0000-4000-8000-00000000c1d0";
    let log_dir = tempfile::tempdir().expect("log dir");
    let dir = log_dir.path().display().to_string();
    let mut progress = Vec::new();
    let code = krabka_format::run_from_args_with_records_to(
        [
            "krabka-format",
            "--log-dir",
            &dir,
            "--cluster-id",
            CLUSTER_ID,
            "--node-id",
            "1",
            "--standalone",
            "--controller-listener",
            "127.0.0.1:9093",
        ],
        Vec::new(),
        &mut progress,
    )
    .await;
    assert2::assert!(code == 0);
    let progress = String::from_utf8(progress).expect("utf-8 progress");
    assert2::assert!(
        progress
            == format!(
                "Formatting dynamic metadata voter directory {dir} with metadata.version \
                 4.3-IV0.\nFormatted 1 log directory with cluster-id ii9OHAAAQACAAAAAAADB0A (6 \
                 seed record(s))\n"
            )
    );
}
