//! The flag combinations clap cannot express, checked after parsing.
//!
//! A backend sub-flag such as `--archive-s3-region` belongs to the backend its
//! `--archive-*` flag selected, and a bound belongs to a partition of a topic
//! `--topic` selects; neither is a constraint clap's `ArgGroup` can state, so
//! both are checked here and reported as `RestoreError::InvalidArgument`.

use super::{PartitionRef, RestoreArgs};
use crate::error::RestoreError;

impl RestoreArgs {
    /// Reject a sub-flag of a backend that was not selected.
    fn validate_backend_flags(&self) -> Result<(), RestoreError> {
        let archive = &self.archive;
        let s3 = [
            ("--archive-s3-region", archive.s3_region.is_some()),
            ("--archive-s3-endpoint", archive.s3_endpoint.is_some()),
            (
                "--archive-s3-access-key-id",
                archive.s3_access_key_id.is_some(),
            ),
            (
                "--archive-s3-secret-access-key",
                archive.s3_secret_access_key.is_some(),
            ),
            ("--archive-s3-allow-http", archive.s3_allow_http),
        ];
        let gcs = [
            (
                "--archive-gcs-service-account-path",
                archive.gcs_service_account_path.is_some(),
            ),
            ("--archive-gcs-endpoint", archive.gcs_endpoint.is_some()),
            ("--archive-gcs-allow-http", archive.gcs_allow_http),
        ];
        for (flags, selected, needs) in [
            (&s3[..], archive.s3_bucket.is_some(), "--archive-s3-bucket"),
            (
                &gcs[..],
                archive.gcs_bucket.is_some(),
                "--archive-gcs-bucket",
            ),
        ] {
            if selected {
                continue;
            }
            if let Some((flag, _)) = flags.iter().find(|(_, given)| *given) {
                return Err(RestoreError::InvalidArgument(format!(
                    "{flag} needs {needs}"
                )));
            }
        }
        Ok(())
    }

    /// Check the flag combinations clap cannot express.
    ///
    /// # Errors
    ///
    /// Returns [`RestoreError::InvalidArgument`] when a backend sub-flag names
    /// a backend that `--archive-*` did not select, when one partition carries
    /// two `--to-offset` bounds, or when a bound names a topic that `--topic`
    /// excludes. Each means the operator wrote a flag that can never apply.
    pub fn validate(&self) -> Result<(), RestoreError> {
        self.validate_backend_flags()?;

        let mut bounded: Vec<&PartitionRef> = Vec::with_capacity(self.to_offset.len());
        for bound in &self.to_offset {
            if bounded.contains(&&bound.partition) {
                return Err(RestoreError::InvalidArgument(format!(
                    "--to-offset names {} more than once",
                    bound.partition
                )));
            }
            bounded.push(&bound.partition);
        }

        for partition in self
            .to_offset
            .iter()
            .map(|bound| &bound.partition)
            .chain(self.exclude_offset.iter().map(|range| &range.partition))
        {
            if !self.selects_topic(&partition.topic) {
                return Err(RestoreError::InvalidArgument(format!(
                    "a bound names {partition}, which --topic does not select"
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use clap::Parser as _;

    use crate::args::test_support::args_from;

    #[test]
    fn backend_sub_flags_require_their_bucket() {
        for stray in [
            vec!["--archive-s3-region", "eu-west-1"],
            vec!["--archive-s3-endpoint", "http://minio:9000"],
            vec!["--archive-s3-access-key-id", "key"],
            vec!["--archive-s3-secret-access-key", "secret"],
            vec!["--archive-s3-allow-http"],
            vec!["--archive-gcs-service-account-path", "/etc/sa.json"],
            vec!["--archive-gcs-endpoint", "http://fake-gcs:4443"],
            vec!["--archive-gcs-allow-http"],
        ] {
            // The archive source in `args_from` is `--archive-local`, so every one
            // of these names a backend that was not selected.
            let args = args_from(&stray).expect("args");
            check!(args.validate().is_err(), "{stray:?}");
        }
    }

    #[test]
    fn backend_sub_flags_are_accepted_with_their_bucket() {
        let args = crate::Cli::try_parse_from([
            "krabka-restore",
            "--log-dir",
            "/target",
            "--archive-s3-bucket",
            "backups",
            "--archive-s3-region",
            "eu-west-1",
            "--archive-s3-allow-http",
        ])
        .expect("args")
        .args;
        check!(args.validate().is_ok());
    }

    #[test]
    fn validate_rejects_two_bounds_on_one_partition() {
        let args =
            args_from(&["--to-offset", "orders:0=10", "--to-offset", "orders:0=20"]).expect("args");
        check!(args.validate().is_err());
    }

    #[test]
    fn validate_accepts_bounds_on_distinct_partitions() {
        let args =
            args_from(&["--to-offset", "orders:0=10", "--to-offset", "orders:1=20"]).expect("args");
        check!(args.validate().is_ok());
    }

    #[test]
    fn validate_rejects_a_bound_on_an_unselected_topic() {
        let args = args_from(&["--topic", "orders", "--to-offset", "payments:0=10"]).expect("args");
        check!(args.validate().is_err());

        let args =
            args_from(&["--topic", "orders", "--exclude-offset", "payments:0=1..2"]).expect("args");
        check!(args.validate().is_err());
    }

    #[test]
    fn validate_accepts_a_bound_on_a_selected_topic() {
        let args = args_from(&["--topic", "orders", "--to-offset", "orders:0=10"]).expect("args");
        check!(args.validate().is_ok());
    }
}
