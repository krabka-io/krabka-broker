//! Structured operation results and the command boundary's terminal reporting.

use {
    crate::{
        error::BackupError,
        manifest::{CAPTURE_ROOT, Manifest},
    },
    krabka_remote_storage::ChainHead,
    std::io::{self, Write},
};

/// An operation's outcome and the diagnostics produced before it completed.
///
/// Events remain available when the operation fails, including successful
/// artifact checks and group commits before the failure.
#[derive(Debug)]
#[must_use]
pub struct OperationReport<T> {
    /// The operation's result, with the original backup error on failure.
    pub result: Result<T, BackupError>,
    /// Diagnostics in operation order.
    pub events: Vec<BackupEvent>,
}

/// Result information that a caller can inspect without capturing stdout.
#[derive(Debug)]
pub enum BackupEvent {
    /// The diskless boundary was sealed with a WORM signature.
    DisklessSealed {
        /// The signed WORM chain head.
        head: ChainHead,
    },
    /// A capture whose manifest was successfully stored.
    Capture {
        /// The manifest that was stored with the capture.
        manifest: Manifest,
        /// Metadata checkpoint diagnostics in their original order.
        notes: Vec<String>,
        /// Artifacts that were absent from the capture source.
        missing: Vec<String>,
    },
    /// The archive holds no captures.
    EmptyList,
    /// The result of reading one manifest during a listing.
    Listed {
        /// The capture directory's name.
        id: String,
        /// The decoded manifest or its original read error.
        manifest: Result<Manifest, BackupError>,
    },
    /// An artifact passed its size and digest checks.
    ArtifactVerified {
        /// The artifact's object name.
        name: String,
        /// The artifact's recorded size.
        size_bytes: u64,
    },
    /// Every artifact in a capture passed verification.
    CaptureVerified {
        /// The capture's identifier.
        id: String,
        /// The number of verified artifacts.
        artifacts: usize,
    },
    /// One offset selected by a dry run, without a cluster commit.
    WouldCommit {
        /// The consumer group identifier.
        group: String,
        /// The topic name.
        topic: String,
        /// The partition index.
        partition: i32,
        /// The committed offset to restore.
        offset: i64,
    },
    /// A group's offset commit succeeded.
    OffsetsCommitted {
        /// The consumer group identifier.
        group: String,
        /// The number of offsets committed for the group.
        offsets: usize,
    },
}

pub(crate) fn render(events: &[BackupEvent], output: &mut impl Write) -> io::Result<()> {
    for event in events {
        match event {
            BackupEvent::DisklessSealed { head } => {
                writeln!(output, "  diskless-capture WORM head {head}")?;
            }
            BackupEvent::Capture {
                manifest,
                notes,
                missing,
            } => {
                writeln!(output, "capture {}", manifest.capture_id)?;
                for artifact in &manifest.artifacts {
                    writeln!(
                        output,
                        "  {} <- {} ({} bytes, sha256 {})",
                        artifact.name, artifact.source, artifact.size_bytes, artifact.sha256
                    )?;
                }
                for note in notes {
                    writeln!(output, "  NOTE: {note}")?;
                }
                for absent in missing {
                    writeln!(output, "  WARNING: {absent}")?;
                }
            }
            BackupEvent::EmptyList => writeln!(output, "no captures under {CAPTURE_ROOT}")?,
            BackupEvent::Listed { id, manifest } => match manifest {
                Ok(manifest) => {
                    let names: Vec<&str> = manifest
                        .artifacts
                        .iter()
                        .map(|artifact| artifact.name.as_str())
                        .collect();
                    writeln!(output, "{id}  {}", names.join(" "))?;
                }
                Err(error) => writeln!(output, "{id}  UNREADABLE: {error}")?,
            },
            BackupEvent::ArtifactVerified { name, size_bytes } => {
                writeln!(output, "  {name} ok ({size_bytes} bytes)")?;
            }
            BackupEvent::CaptureVerified { id, artifacts } => {
                writeln!(output, "capture {id} verified: {artifacts} artifacts")?;
            }
            BackupEvent::WouldCommit {
                group,
                topic,
                partition,
                offset,
            } => writeln!(
                output,
                "would commit {group}: {topic}-{partition} = {offset}"
            )?,
            BackupEvent::OffsetsCommitted { group, offsets } => {
                writeln!(output, "{group}: {offsets} offsets committed")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::{BackupEvent, render},
        crate::{
            error::BackupError,
            manifest::{Artifact, MANIFEST_VERSION, Manifest},
        },
        assert2::check,
        krabka_remote_storage::ChainHead,
    };

    fn manifest() -> Manifest {
        Manifest {
            version: MANIFEST_VERSION,
            capture_id: "capture-id".to_owned(),
            captured_at_ms: 0,
            log_dir: None,
            bootstrap_server: None,
            artifacts: vec![Artifact {
                name: "rlmm-snapshot".to_owned(),
                source: "/node/snapshot".to_owned(),
                size_bytes: 12,
                sha256: "digest".to_owned(),
            }],
        }
    }

    #[test]
    fn terminal_output_preserves_operation_order_and_text() {
        let events = [
            BackupEvent::DisklessSealed {
                head: ChainHead([0xab; 32]),
            },
            BackupEvent::Capture {
                manifest: manifest(),
                notes: vec!["checkpoint age".to_owned()],
                missing: vec!["no checkpoint".to_owned()],
            },
            BackupEvent::EmptyList,
            BackupEvent::Listed {
                id: "capture-id".to_owned(),
                manifest: Ok(manifest()),
            },
            BackupEvent::Listed {
                id: "broken-id".to_owned(),
                manifest: Err(BackupError::Integrity("bad manifest".to_owned())),
            },
            BackupEvent::ArtifactVerified {
                name: "rlmm-snapshot".to_owned(),
                size_bytes: 12,
            },
            BackupEvent::CaptureVerified {
                id: "capture-id".to_owned(),
                artifacts: 1,
            },
            BackupEvent::WouldCommit {
                group: "readers".to_owned(),
                topic: "orders".to_owned(),
                partition: 2,
                offset: 9,
            },
            BackupEvent::OffsetsCommitted {
                group: "readers".to_owned(),
                offsets: 1,
            },
        ];
        let mut output = Vec::new();
        render(&events, &mut output).expect("render the operation events");
        check!(
            String::from_utf8(output).unwrap()
                == concat!(
                    "  diskless-capture WORM head abababababababababababababababababababababababababababababababab\n",
                    "capture capture-id\n",
                    "  rlmm-snapshot <- /node/snapshot (12 bytes, sha256 digest)\n",
                    "  NOTE: checkpoint age\n",
                    "  WARNING: no checkpoint\n",
                    "no captures under restore-inputs\n",
                    "capture-id  rlmm-snapshot\n",
                    "broken-id  UNREADABLE: integrity failure: bad manifest\n",
                    "  rlmm-snapshot ok (12 bytes)\n",
                    "capture capture-id verified: 1 artifacts\n",
                    "would commit readers: orders-2 = 9\n",
                    "readers: 1 offsets committed\n",
                )
        );
    }

    #[test]
    fn an_empty_event_stream_writes_nothing() {
        let mut output = Vec::new();
        render(&[], &mut output).expect("render no events");
        check!(output.is_empty());
    }
}
