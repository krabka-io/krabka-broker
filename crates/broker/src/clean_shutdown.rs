//! The clean-shutdown proof a broker leaves behind, and reads back on its next
//! start.
//!
//! A broker that stops gracefully writes its current broker epoch to
//! `{log_dir}/clean_shutdown`. The next start reads that epoch and offers it
//! back at registration; the controller accepts the restart as clean only when
//! the epoch it still holds for that node is the one the file names. A broker
//! that died -- SIGKILL, a lost machine, a panic -- wrote no file, so it can
//! prove nothing and its restart is unclean.
//!
//! This is Kafka's `previousBrokerEpoch`.
//! `CleanShutdownFileHandler` in `kafka-storage-4.3.1.jar` writes
//! `.kafka_cleanshutdown` into each log dir on an orderly stop,
//! `LogManager.readBrokerEpochFromCleanShutdownFiles` reads it at startup, and
//! `ClusterControlManager.registerBroker` computes
//! `isCleanShutdown = storedBrokerEpoch == request.previousBrokerEpoch()`.
//! krabka keeps a plain decimal epoch rather than Kafka's one-field JSON
//! because nothing but this module reads the file; the *rule* is what has to
//! match, and it does.
//!
//! ## Absent means unclean
//!
//! Every read failure -- no file, an unreadable file, a file holding something
//! that is not an epoch -- answers [`UNPROVEN`], which no real broker epoch
//! can equal, so it never compares clean. Kafka takes the same default from
//! two directions at once: `previousBrokerEpoch` defaults to `-1` on the
//! wire, and the controller passes `cleanShutdownDetectionEnabled = false` for
//! any `BrokerRegistration` older than v3, which forces the comparison to
//! `false` outright. A controller that cannot prove clean assumes unclean.
//!
//! ## The proof is spent on use
//!
//! [`take`] deletes the file as it reads it. The proof covers exactly one
//! restart: a broker that starts, registers, and then dies has consumed it, so
//! the start after that one finds nothing and is unclean, which is the truth.
//! Kafka deletes the same file while `LogManager` loads the log dir.

use std::{
    io::{Read, Write},
    path::Path,
};

use krabka_metadata::{MetadataImage, NodeId};

const FILE_NAME: &str = "clean_shutdown";

/// The epoch a broker offers when it holds no clean-shutdown proof.
///
/// Broker epochs are commit offsets, so they are never negative and this
/// sentinel can never be mistaken for one. It is also the value Kafka's
/// `BrokerRegistrationRequest.previousBrokerEpoch` defaults to.
pub(crate) const UNPROVEN: i64 = -1;

/// Read the clean-shutdown proof from `{log_dir}/clean_shutdown` and delete
/// it, or return [`UNPROVEN`] when there is none to read.
pub(crate) fn take(log_dir: &Path) -> i64 {
    let path = log_dir.join(FILE_NAME);
    let mut epoch = UNPROVEN;
    if let Ok(mut file) = std::fs::File::open(&path) {
        let mut text = String::new();
        if file.read_to_string(&mut text).is_ok()
            && let Ok(parsed) = text.trim().parse::<i64>()
        {
            epoch = parsed;
        }
    }
    // Delete whatever was there, parsable or not: a proof this start could not
    // read is not one a later start should get to try again.
    if let Err(error) = std::fs::remove_file(&path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), %error, "failed to remove clean_shutdown");
    }
    epoch
}

/// Write `broker_epoch` to `{log_dir}/clean_shutdown` as this broker's proof
/// that it stopped on purpose.
///
/// A failure to write is logged and otherwise ignored: the broker is stopping
/// either way, and the consequence is that the next start is treated as
/// unclean, which is the safe direction.
pub(crate) fn write(log_dir: &Path, broker_epoch: i64) {
    let path = log_dir.join(FILE_NAME);
    match std::fs::File::create(&path) {
        Ok(mut file) => {
            if let Err(error) = file.write_all(broker_epoch.to_string().as_bytes()) {
                tracing::warn!(path = %path.display(), %error, "failed to write clean_shutdown");
            }
        }
        Err(error) => {
            tracing::warn!(path = %path.display(), %error, "failed to create clean_shutdown");
        }
    }
}

/// Whether a broker rejoining as `node_id` can prove it stopped gracefully
/// last time, by offering back the very epoch the cluster still holds for it.
///
/// A node the image has no registration for cannot prove anything, so it is
/// unclean. Kafka reaches that through a `-2` sentinel for the held epoch,
/// which no `previousBrokerEpoch` can equal either.
pub(crate) fn restart_was_clean(
    image: &MetadataImage,
    node_id: NodeId,
    previous_broker_epoch: i64,
) -> bool {
    image
        .broker_epoch(node_id)
        .is_some_and(|held| held == previous_broker_epoch)
}

#[cfg(test)]
mod tests;
