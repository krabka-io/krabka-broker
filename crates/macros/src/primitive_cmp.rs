//! `#[derive(PrimitiveCmp)]`: see the crate documentation.

use moxy::{
    ast::{ItemStruct, ParseError},
    token::TokenStream,
};

use crate::meta::inner_type;

/// Expands `#[derive(PrimitiveCmp)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let inner = inner_type(&item, "PrimitiveCmp")?;
    let ident = item.ident;
    Ok(moxy::template! {
        impl ::core::cmp::PartialEq<{{ inner }}> for {{ ident }} {
            #[inline]
            fn eq(&self, other: &{{ inner }}) -> bool {
                self.0 == *other
            }
        }

        impl ::core::cmp::PartialEq<{{ ident }}> for {{ inner }} {
            #[inline]
            fn eq(&self, other: &{{ ident }}) -> bool {
                *self == other.0
            }
        }

        impl ::core::cmp::PartialOrd<{{ inner }}> for {{ ident }} {
            #[inline]
            fn partial_cmp(&self, other: &{{ inner }}) -> ::core::option::Option<::core::cmp::Ordering> {
                ::core::cmp::PartialOrd::partial_cmp(&self.0, other)
            }
        }

        impl ::core::cmp::PartialOrd<{{ ident }}> for {{ inner }} {
            #[inline]
            fn partial_cmp(&self, other: &{{ ident }}) -> ::core::option::Option<::core::cmp::Ordering> {
                ::core::cmp::PartialOrd::partial_cmp(self, &other.0)
            }
        }
    })
}
