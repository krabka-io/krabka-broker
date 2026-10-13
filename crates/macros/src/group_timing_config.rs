//! Per-group timing overrides for consumer and share membership configurations.

use {
    crate::meta::{Tokens, impl_block, named_fields},
    moxy::{
        ast::{Attributed, ItemStruct, ParseError},
        token::TokenStream,
    },
};

#[derive(moxy::FromMeta)]
struct TimingKeys {
    session: Tokens,
    heartbeat: Tokens,
    assignment: Tokens,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let keys = item.parse_meta::<TimingKeys>("group_timing")?.ok_or_else(|| {
        ParseError::new(
            item.ident.span(),
            "`GroupTimingConfig` needs `#[group_timing(session = KEY, heartbeat = KEY, assignment = KEY)]`",
        )
    })?;
    let fields = named_fields(&item, "GroupTimingConfig")?;
    for name in [
        "session_timeout",
        "heartbeat_interval",
        "assignment_interval",
        "min_session_timeout",
        "max_session_timeout",
        "min_heartbeat_interval",
        "max_heartbeat_interval",
    ] {
        if !fields.iter().any(|field| {
            field
                .ident
                .as_ref()
                .is_some_and(|ident| ident.text() == name)
        }) {
            return Err(ParseError::new(
                item.ident.span(),
                format!("`GroupTimingConfig` needs a `{name}` field"),
            ));
        }
    }
    let session = keys.session.0;
    let heartbeat = keys.heartbeat.0;
    let assignment = keys.assignment.0;
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        /// The defaults with no assignment interval, for tests that assign each
        /// membership change at once.
        #[cfg(test)]
        pub(crate) fn assigning_at_once() -> Self {
            Self { assignment_interval: ::std::time::Duration::ZERO, ..Self::default() }
        }

        /// Apply this protocol's stored group timings within the broker bounds.
        /// A group with no valid timing override borrows the broker defaults.
        #[must_use]
        pub(crate) fn for_group(
            &self,
            overrides: Option<&::std::collections::BTreeMap<String, String>>,
        ) -> ::std::borrow::Cow<'_, Self> {
            use crate::coordinator::unified::config::{
                group_millis, MIN_ASSIGNMENT_INTERVAL, MAX_ASSIGNMENT_INTERVAL,
            };
            let session = group_millis(
                overrides, {{ session }}, self.min_session_timeout, self.max_session_timeout,
            );
            let heartbeat = group_millis(
                overrides, {{ heartbeat }}, self.min_heartbeat_interval, self.max_heartbeat_interval,
            );
            let assignment = group_millis(
                overrides, {{ assignment }}, MIN_ASSIGNMENT_INTERVAL, MAX_ASSIGNMENT_INTERVAL,
            );
            if session.is_none() && heartbeat.is_none() && assignment.is_none() {
                ::std::borrow::Cow::Borrowed(self)
            } else {
                let mut config = self.clone();
                config.session_timeout = session.unwrap_or(config.session_timeout);
                config.heartbeat_interval = heartbeat.unwrap_or(config.heartbeat_interval);
                config.assignment_interval = assignment.unwrap_or(config.assignment_interval);
                ::std::borrow::Cow::Owned(config)
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn refuses_missing_keys_and_fields() {
        let without_keys: ItemStruct =
            moxy::parse!("struct Config { session_timeout: Duration }").unwrap();
        assert!(
            expand(without_keys)
                .unwrap_err()
                .to_string()
                .contains("#[group_timing")
        );
        let without_fields: ItemStruct = moxy::parse!(
            "#[group_timing(session = SESSION, heartbeat = HEARTBEAT, assignment = ASSIGNMENT)] struct Config {}"
        ).unwrap();
        assert!(
            expand(without_fields)
                .unwrap_err()
                .to_string()
                .contains("session_timeout")
        );
        let incomplete_keys: ItemStruct =
            moxy::parse!("#[group_timing(session = SESSION)] struct Config {}").unwrap();
        assert!(expand(incomplete_keys).is_err());
    }
}
