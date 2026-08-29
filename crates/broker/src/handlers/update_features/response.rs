//! Construction of the `UpdateFeatures` response.
//!
//! One place builds every wire shape the handler can return: a per-feature
//! result row, a top-level failure with no rows, a request-wide failure that
//! overwrites the rows that had succeeded, and the final assembly that moves
//! the first row error up to the top level on v2 and later.

use krabka_protocol::owned::update_features_response::{
    UpdatableFeatureResult, UpdateFeaturesResponse,
};

use crate::codes;

pub(super) fn row(feature: String, error_code: i16, msg: &str) -> UpdatableFeatureResult {
    UpdatableFeatureResult {
        feature,
        error_code,
        error_message: (error_code != codes::NONE).then(|| msg.to_string()),
        ..Default::default()
    }
}

pub(super) fn top_level_error(code: i16, msg: &str, version: i16) -> UpdateFeaturesResponse {
    let _ = version;
    UpdateFeaturesResponse {
        error_code: code,
        error_message: Some(msg.to_string()),
        ..Default::default()
    }
}

/// Overwrites every `ok` row with a request-wide failure code, and sets the
/// top-level error as well.
pub(super) fn apply_request_wide(
    mut results: Vec<UpdatableFeatureResult>,
    code: i16,
    msg: &str,
    version: i16,
) -> UpdateFeaturesResponse {
    for r in results.iter_mut().filter(|r| r.error_code == codes::NONE) {
        r.error_code = code;
        r.error_message = Some(msg.to_string());
    }
    let mut resp = finalize(results, version);
    resp.error_code = code;
    resp.error_message = Some(msg.to_string());
    resp
}

/// Assembles the final response. On v2 the wire carries no `results` array, so
/// the top-level `error_code` must carry the first non-zero row code. The
/// client then still sees the failure.
pub(super) fn finalize(
    results: Vec<UpdatableFeatureResult>,
    version: i16,
) -> UpdateFeaturesResponse {
    let (top_code, top_msg) = if version >= 2 {
        results
            .iter()
            .find(|r| r.error_code != codes::NONE)
            .map_or((codes::NONE, None), |r| {
                (r.error_code, r.error_message.clone())
            })
    } else {
        (codes::NONE, None)
    };
    UpdateFeaturesResponse {
        error_code: top_code,
        error_message: top_msg,
        results,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers::update_features::test_support::VERSION;

    #[test]
    fn row_sets_message_only_on_error() {
        let ok = row("metadata.version".into(), codes::NONE, "x");
        assert!(ok.feature == "metadata.version");
        assert!(ok.error_message.is_none());

        let err = row(
            "metadata.version".into(),
            codes::INVALID_UPDATE_VERSION,
            "bad",
        );
        assert!(err.feature == "metadata.version");
        assert!(err.error_message.as_deref() == Some("bad"));
    }

    #[test]
    fn top_level_error_preserves_wire_shape() {
        let resp = top_level_error(codes::INVALID_REQUEST, "bad request", VERSION);

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_REQUEST,
            error_message: Some("bad request".to_string()),
            results: vec![],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn apply_request_wide_marks_only_successful_rows_and_sets_top_level() {
        let resp = apply_request_wide(
            vec![
                row("metadata.version".into(), codes::NONE, ""),
                row("eligible.feature".into(), codes::NONE, ""),
                row(
                    "not.a.feature".into(),
                    codes::INVALID_REQUEST,
                    "bad feature",
                ),
            ],
            codes::FEATURE_UPDATE_FAILED,
            "persist failed",
            VERSION,
        );

        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::FEATURE_UPDATE_FAILED,
            error_message: Some("persist failed".to_string()),
            results: vec![
                UpdatableFeatureResult {
                    feature: "metadata.version".to_string(),
                    error_code: codes::FEATURE_UPDATE_FAILED,
                    error_message: Some("persist failed".to_string()),
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: "eligible.feature".to_string(),
                    error_code: codes::FEATURE_UPDATE_FAILED,
                    error_message: Some("persist failed".to_string()),
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: "not.a.feature".to_string(),
                    error_code: codes::INVALID_REQUEST,
                    error_message: Some("bad feature".to_string()),
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn finalize_v2_promotes_first_error_to_top_level() {
        let results = vec![
            row("a".into(), codes::NONE, ""),
            row("b".into(), codes::INVALID_UPDATE_VERSION, "bad"),
        ];
        let resp = finalize(results, 2);
        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::INVALID_UPDATE_VERSION,
            error_message: Some("bad".to_string()),
            results: vec![
                UpdatableFeatureResult {
                    feature: "a".to_string(),
                    error_code: codes::NONE,
                    error_message: None,
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                },
                UpdatableFeatureResult {
                    feature: "b".to_string(),
                    error_code: codes::INVALID_UPDATE_VERSION,
                    error_message: Some("bad".to_string()),
                    unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
                },
            ],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }

    #[test]
    fn finalize_v1_keeps_top_level_none() {
        let results = vec![row("b".into(), codes::INVALID_UPDATE_VERSION, "bad")];
        let resp = finalize(results, 1);
        let expected = UpdateFeaturesResponse {
            throttle_time_ms: 0,
            error_code: codes::NONE,
            error_message: None,
            results: vec![UpdatableFeatureResult {
                feature: "b".to_string(),
                error_code: codes::INVALID_UPDATE_VERSION,
                error_message: Some("bad".to_string()),
                unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
            }],
            unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
        };
        assert!(resp == expected);
    }
}
