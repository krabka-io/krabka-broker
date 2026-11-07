//! The two spellings of a configured key prefix, and the live gate that picks
//! the one new tiered objects are written under.
//!
//! Tiered object keys are a persisted format. Every 1.x broker before
//! `krabka.version` 3 joins the prefix to a key exactly as the operator typed
//! it, so a prefix copied from a console with stray whitespace, such as
//! ` tier `, lands in the bucket as ` tier /orders-0-…`. Level 3 writes under
//! the prefix that [`normalize_prefix`] trims instead. The broker keeps
//! writing the old spelling until the operator finalizes that level, and a
//! reader looks under both spellings at every level, so a segment written on
//! either side of the finalization stays reachable.

use {
    krabka_object_store::normalize_prefix,
    std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// The spelling of the configured key prefix that new tiered objects take.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum KeyPrefixForm {
    /// The prefix exactly as configured, stray whitespace included. Every 1.x
    /// broker writes this spelling until `krabka.version` 3 is finalized.
    #[default]
    AsConfigured,
    /// The prefix with surrounding whitespace and `/` separators trimmed.
    /// A prefix that trims to nothing means no prefix. `krabka.version` 3
    /// writes this spelling.
    Normalized,
}

/// A shared, live choice of [`KeyPrefixForm`].
///
/// Clones share one switch, so the broker keeps a clone, installs another in
/// the backend with
/// [`S3RemoteStorage::with_key_prefix_gate`](crate::S3RemoteStorage::with_key_prefix_gate),
/// and flips it when the finalized `krabka.version` changes. A backend reads
/// the gate once per operation. The default is [`KeyPrefixForm::AsConfigured`].
#[derive(Clone, Debug, Default)]
pub struct KeyPrefixGate(Arc<AtomicBool>);

impl KeyPrefixGate {
    /// A gate that starts at `form`.
    #[must_use]
    pub fn new(form: KeyPrefixForm) -> Self {
        Self(Arc::new(AtomicBool::new(form == KeyPrefixForm::Normalized)))
    }

    /// Switches every clone of this gate to `form`.
    pub fn set(&self, form: KeyPrefixForm) {
        // The flag guards no other memory: each operation reads it once and
        // derives its keys from the value it read.
        self.0
            .store(form == KeyPrefixForm::Normalized, Ordering::Relaxed);
    }

    /// The form new objects are written under now.
    #[must_use]
    pub fn form(&self) -> KeyPrefixForm {
        if self.0.load(Ordering::Relaxed) {
            KeyPrefixForm::Normalized
        } else {
            KeyPrefixForm::AsConfigured
        }
    }
}

/// The configured prefix in both spellings, and the gate between them.
#[derive(Debug)]
pub(super) struct KeyPrefix {
    as_configured: Option<String>,
    normalized: Option<String>,
    pub(super) gate: KeyPrefixGate,
}

/// The roots one operation derives its keys from: the one new objects are
/// written under, then the other spelling.
pub(super) struct PrefixRoots<'a> {
    pub(super) write: Option<&'a str>,
    pub(super) other: Option<&'a str>,
}

impl KeyPrefix {
    pub(super) fn new(configured: Option<String>) -> Self {
        Self {
            normalized: normalize_prefix(configured.as_deref()),
            as_configured: configured,
            gate: KeyPrefixGate::default(),
        }
    }

    /// The prefix as configured, for the backend's `Debug` rendering.
    pub(super) fn configured(&self) -> Option<&str> { self.as_configured.as_deref() }

    /// Both roots, ordered by the gate's current form.
    pub(super) fn roots(&self) -> PrefixRoots<'_> {
        let (as_configured, normalized) =
            (self.as_configured.as_deref(), self.normalized.as_deref());
        match self.gate.form() {
            KeyPrefixForm::AsConfigured => PrefixRoots {
                write: as_configured,
                other: normalized,
            },
            KeyPrefixForm::Normalized => PrefixRoots {
                write: normalized,
                other: as_configured,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{KeyPrefixForm, KeyPrefixGate},
        assert2::check,
    };

    #[test]
    fn clones_share_one_gate() {
        let broker = KeyPrefixGate::default();
        let backend = broker.clone();
        check!(backend.form() == KeyPrefixForm::AsConfigured);

        broker.set(KeyPrefixForm::Normalized);
        check!(backend.form() == KeyPrefixForm::Normalized);

        broker.set(KeyPrefixForm::AsConfigured);
        check!(backend.form() == KeyPrefixForm::AsConfigured);
        check!(KeyPrefixGate::new(KeyPrefixForm::Normalized).form() == KeyPrefixForm::Normalized);
    }
}
