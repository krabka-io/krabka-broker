//! Why a record was rejected, in the two forms the rest of the broker needs.
//!
//! [`RejectReason`] is the one value the validator returns on a failure, and
//! it carries both a low-cardinality metric label and the message the producer
//! reads. It lives in its own module because those two renderings are the
//! whole of its job, and neither the cache nor the record check has to know
//! how they are worded.

/// Why a record failed validation.
///
/// Each variant is both a metric label, through [`RejectReason::label`], and
/// the KIP-467 `batch_index_error_message` the producer reads, through
/// [`std::fmt::Display`]. The two are deliberately different: the label has to
/// be low-cardinality for Prometheus, and the message has to name the id and
/// the subject so that a person can act on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    /// The field carries no Confluent frame.
    Unframed(String),
    /// The frame is well formed but the registry does not know the id.
    UnknownId(u32),
    /// The id resolves, but it is not registered under this topic's subject.
    WrongSubject { id: u32, subject: String },
    /// The id resolves and belongs here, but the body is not an instance of
    /// the schema. Only
    /// [`ValidationMode::Full`][crate::schema_validation::ValidationMode::Full]
    /// can produce this.
    BodyMismatch { id: u32, detail: String },
    /// The registry could not be reached or did not answer usefully, and
    /// `fail_open` is off.
    RegistryUnavailable(String),
}

impl RejectReason {
    /// Every value [`RejectReason::label`] can return.
    ///
    /// The `schema_validation_rejections` family keys on the topic paired with
    /// one of these, so evicting a deleted topic's series has to name each of
    /// them; there is no way to enumerate a `Family`'s live label sets.
    pub const LABELS: [&'static str; 5] = [
        "unframed",
        "unknown_id",
        "wrong_subject",
        "body_mismatch",
        "registry_unavailable",
    ];

    /// The metric label for this reason. Low cardinality by construction: it
    /// carries none of the ids or subjects the message does.
    #[must_use]
    pub fn label(&self) -> &'static str {
        match self {
            Self::Unframed(_) => "unframed",
            Self::UnknownId(_) => "unknown_id",
            Self::WrongSubject { .. } => "wrong_subject",
            Self::BodyMismatch { .. } => "body_mismatch",
            Self::RegistryUnavailable(_) => "registry_unavailable",
        }
    }
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unframed(detail) => {
                write!(f, "not a Confluent-framed payload: {detail}")
            }
            Self::UnknownId(id) => write!(f, "schema id {id} is not registered"),
            Self::WrongSubject { id, subject } => {
                write!(
                    f,
                    "schema id {id} is not registered under subject {subject}"
                )
            }
            Self::BodyMismatch { id, detail } => {
                write!(f, "body does not match schema id {id}: {detail}")
            }
            Self::RegistryUnavailable(detail) => {
                write!(f, "schema registry unavailable: {detail}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    #[test]
    fn every_reject_reason_has_a_label_and_a_message() {
        let cases = [
            (RejectReason::Unframed("bad magic".into()), "unframed"),
            (RejectReason::UnknownId(7), "unknown_id"),
            (
                RejectReason::WrongSubject {
                    id: 7,
                    subject: "orders-value".into(),
                },
                "wrong_subject",
            ),
            (
                RejectReason::BodyMismatch {
                    id: 7,
                    detail: "nope".into(),
                },
                "body_mismatch",
            ),
            (
                RejectReason::RegistryUnavailable("timeout".into()),
                "registry_unavailable",
            ),
        ];
        check!(cases.len() == RejectReason::LABELS.len());
        for (reason, label) in cases {
            check!(reason.label() == label);
            check!(!reason.to_string().is_empty(), "{label}");
            // Metric eviction names every label from `LABELS`, so a label a
            // variant can return but `LABELS` omits would leak a series.
            check!(RejectReason::LABELS.contains(&label), "{label}");
        }
    }
}
