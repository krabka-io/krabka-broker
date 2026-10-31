//! Delegate a metadata-log wrapper's unchanged operations through `self.inner`.

use {
    crate::meta::{ItemBody, body_group, declares_fn, item_body, required_tokens},
    moxy::{
        ast::ParseError,
        token::{Span, TokenStream, TokenTree},
    },
};

/// Delegate a metadata-log wrapper's unchanged operations before `async_trait` expands it.
pub fn expand(root: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let ItemBody {
        mut tokens,
        body_index: body,
    } = item_body(item, "metadata_log_delegate", TokenTree::is_keyword_impl)?;
    let mut root_tokens: Vec<_> = root.into_iter().collect();
    // Optional transport operations require explicit selectors.
    let mut keyed = false;
    let mut range = false;
    loop {
        let selector = root_tokens.last().and_then(TokenTree::as_ident);
        match selector {
            Some(name) if name == "keyed" => keyed = true,
            Some(name) if name == "range" => range = true,
            _ => break,
        }
        root_tokens.pop();
        if root_tokens
            .last()
            .is_none_or(|token| !token.is_punct_comma())
        {
            return Err(ParseError::new(
                Span::call_site(),
                "expected crate path followed by comma-separated selectors",
            ));
        }
        root_tokens.pop();
    }
    let root = required_tokens(
        TokenStream::from(root_tokens),
        "metadata_log_delegate needs the metadata log crate path",
    )?;
    let group = body_group(&mut tokens, body)?;
    let candidates = [
        (
            "partition_count",
            moxy::template! { fn partition_count(&self) -> i32 { self.inner.partition_count() } },
        ),
        (
            "publish",
            moxy::template! { async fn publish(&self, partition: i32, event: ::bytes::Bytes) -> Result<i64, {{ root }}::error::MetadataLogError> { self.inner.publish(partition, event).await } },
        ),
        (
            "subscribe",
            moxy::template! { fn subscribe(&self, assignment: Vec<{{ root }}::log::PartitionStart>) -> ({{ root }}::log::MetadataEventStream, ::std::sync::Arc<dyn {{ root }}::log::AssignmentHandle>) { self.inner.subscribe(assignment) } },
        ),
        (
            "high_water_marks",
            moxy::template! { async fn high_water_marks(&self) -> Result<Vec<i64>, {{ root }}::error::MetadataLogError> { self.inner.high_water_marks().await } },
        ),
    ];
    let existing: Vec<_> = group.tokens.clone().into_iter().collect();
    let has_method = |method: &str| declares_fn(&existing, method);
    let mut methods = TokenStream::new();
    for (method, implementation) in candidates {
        if !has_method(method) {
            methods.extend(implementation);
        }
    }
    if keyed && !has_method("publish_keyed") {
        methods.extend(moxy::template! {
            async fn publish_keyed(&self, partition: i32, key: ::bytes::Bytes, event: Option<::bytes::Bytes>) -> Result<i64, {{ root }}::error::MetadataLogError> {
                self.inner.publish_keyed(partition, key, event).await
            }
        });
    }
    if range && !has_method("visit_range") {
        methods.extend(moxy::template! {
            async fn visit_range(
                &self,
                partition: i32,
                start: i64,
                end: i64,
                visit: &mut {{ root }}::log::RangeVisitor<'_>,
            ) -> Result<(), {{ root }}::error::MetadataLogError> {
                self.inner.visit_range(partition, start, end, visit).await
            }
        });
    }
    group.tokens = methods.into_iter().chain(group.tokens.clone()).collect();
    Ok(tokens.into())
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        crate::meta::compact,
        moxy::token::{TokenStream, TokenTree},
    };

    const ITEM: &str = "#[async_trait] impl MetadataEventLog for Wrapper { async fn high_water_marks(&self) -> Result<Vec<i64>, E> { Ok(vec![]) } }";

    /// Every function the expanded impl declares, in body order.
    fn declared_functions(output: &TokenStream) -> Vec<String> {
        let Some(TokenTree::Group(body)) = output.clone().into_iter().last() else {
            panic!("expanded impl has a body");
        };
        let tokens = Vec::from(body.tokens);
        tokens
            .windows(2)
            .filter(|pair| pair[0].is_keyword_fn())
            .filter_map(|pair| pair[1].as_ident().map(|name| compact(name)))
            .collect()
    }

    #[yare::parameterized(
        no_selectors = { "log", ITEM, &["partition_count", "publish", "subscribe", "high_water_marks"] },
        keyed = { "log, keyed", ITEM, &["partition_count", "publish", "subscribe", "publish_keyed", "high_water_marks"] },
        range = { "log, range", ITEM, &["partition_count", "publish", "subscribe", "visit_range", "high_water_marks"] },
        both_selectors = { "log, range, keyed", ITEM, &["partition_count", "publish", "subscribe", "publish_keyed", "visit_range", "high_water_marks"] },
        existing_publish = {
            "log",
            "impl MetadataEventLog for Wrapper { async fn publish(&self) {} }",
            &["partition_count", "subscribe", "high_water_marks", "publish"],
        },
        existing_keyed = {
            "log, keyed",
            "impl MetadataEventLog for Wrapper { async fn publish_keyed(&self) {} }",
            &["partition_count", "publish", "subscribe", "high_water_marks", "publish_keyed"],
        },
    )]
    fn delegates_missing_operations_ahead_of_the_wrapper_methods(
        root: &str,
        item: &str,
        expected: &[&str],
    ) {
        let output = expand(root.parse().unwrap(), item.parse().unwrap()).unwrap();
        assert2::assert!(declared_functions(&output) == expected, "{root}");
    }

    #[test]
    fn delegated_operations_name_the_metadata_log_crate() {
        let output = expand(
            "::nested::log".parse().unwrap(),
            "impl MetadataEventLog for Wrapper { fn partition_count(&self) -> i32 { 1 } fn subscribe() {} async fn high_water_marks() {} }".parse().unwrap(),
        )
        .unwrap();
        assert2::assert!(
            compact(&output)
                == "implMetadataEventLogforWrapper{asyncfnpublish(&self,partition:i32,event:::bytes::Bytes)->Result<i64,::nested::log::error::MetadataLogError>{self.inner.publish(partition,event).await}fnpartition_count(&self)->i32{1}fnsubscribe(){}asyncfnhigh_water_marks(){}}"
        );
    }

    #[yare::parameterized(
        missing_root = { "", ITEM },
        selector_only = { "keyed", ITEM },
        selector_without_comma = { "log keyed", ITEM },
        not_an_impl = { "log", "struct Wrapper {}" },
    )]
    fn rejects_missing_roots_unseparated_selectors_and_non_impls(root: &str, item: &str) {
        assert2::assert!(
            expand(root.parse().unwrap(), item.parse().unwrap()).is_err(),
            "{root} / {item}"
        );
    }
}
