//! Assertions over completed stock Kafka client output.
use {assert2::assert, std::process::Output};

pub fn assert_successful_jvm_output(output: &Output) {
    assert!(
        output.status.success(),
        "JVM command failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn assert_numbered_console_values(output: &Output, prefix: &str, count: usize) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    for index in 0..count {
        let expected = format!("{prefix}{index}");
        assert!(
            stdout.contains(&expected),
            "consumer missing {expected}: {stdout:?}"
        );
    }
}
