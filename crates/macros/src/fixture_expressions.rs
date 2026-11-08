//! Small expression contracts that retain caller-level returns and assertion policies.

use {
    crate::meta::argument_array,
    moxy::{
        ast::ParseError,
        token::{Ident, Span, TokenStream},
    },
};

pub fn guard_response(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let [client, request, err] = argument_array(input.clone())?;
    let client: Ident = moxy::parse!({ &client })?;
    let request: Ident = moxy::parse!({ &request })?;
    let err: Ident = moxy::parse!({ &err })?;
    let response = Ident::new("checked_response").with_span(Span::mixed_site());
    Ok(moxy::template! {
        {
            let {{ response }} = {{ client }}.send({{ request }}).await.map_err(Failure::from)?;
            if {{ response }}.error_code != 0 {
                return Ok(report_error({{ err }}, {{ response }}.error_code, {{ response }}.error_message.as_deref()));
            }
            {{ response }}
        }
    })
}

#[cfg(test)]
mod tests {
    use {super::guard_response, crate::meta::compact, assert2::assert};

    #[test]
    fn checked_send_keeps_transport_error_before_refusal_and_local_return() {
        let output = compact(&guard_response(&"client, request, err".parse().unwrap()).unwrap());
        assert!(
            output.find("map_err(Failure::from)?").unwrap() < output.find("error_code!=0").unwrap()
        );
        assert!(output.contains("returnOk(report_error(err,"));
        assert!(guard_response(&"client(), request, err".parse().unwrap()).is_err());
        assert!(guard_response(&"client, request".parse().unwrap()).is_err());
    }
}
