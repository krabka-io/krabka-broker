//! Fixed wire fields shared by independent ACL-filter fixtures.

use moxy::{
    ast::{ParseError, Path},
    token::TokenStream,
};

pub fn empty_match_acl_filter(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let target: Path = moxy::parse!({ input })?;
    Ok(moxy::template! {
        {{ target }} {
            resource_type_filter: crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC,
            resource_name_filter: Some(String::new()),
            pattern_type_filter: crate::handlers::acl_test_support::PATTERN_TYPE_MATCH,
            principal_filter: Some(String::new()),
            host_filter: None,
            operation: crate::handlers::acl_test_support::OPERATION_ANY,
            permission_type: crate::handlers::acl_test_support::PERMISSION_ANY,
            ..Default::default()
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::empty_match_acl_filter, crate::meta::compact};

    #[yare::parameterized(
        request = { "DescribeAclsRequest" },
        filter = { "DeleteAclsFilter" },
    )]
    fn generates_the_same_explicit_empty_axes_for_both_wire_shapes(target: &str) {
        let output = empty_match_acl_filter(&target.parse().unwrap()).unwrap();
        let rendered = compact(&output);
        assert2::assert!(rendered.starts_with(target));
        assert2::assert!(rendered.contains(
            "resource_type_filter:crate::handlers::acl_test_support::RESOURCE_TYPE_TOPIC"
        ));
        assert2::assert!(rendered.contains("resource_name_filter:Some(String::new())"));
        assert2::assert!(
            rendered.contains(
                "pattern_type_filter:crate::handlers::acl_test_support::PATTERN_TYPE_MATCH"
            )
        );
        assert2::assert!(rendered.contains("principal_filter:Some(String::new())"));
        assert2::assert!(rendered.contains("host_filter:None"));
        assert2::assert!(
            rendered.contains("operation:crate::handlers::acl_test_support::OPERATION_ANY")
        );
        assert2::assert!(
            rendered.contains("permission_type:crate::handlers::acl_test_support::PERMISSION_ANY")
        );
        assert2::assert!(rendered.contains("..Default::default()"));
    }

    #[yare::parameterized(
        empty = { "" },
        empty_group = { "{}" },
        trailing_item = { "Filter; second" },
        extra_argument = { "Filter, Other" },
    )]
    fn refuses_an_empty_target_or_extra_items(invalid: &str) {
        assert2::assert!(empty_match_acl_filter(&invalid.parse().unwrap()).is_err());
    }
}
