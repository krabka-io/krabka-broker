//! Broker response codes this command-line tool names specially.

pub const BARRIER_INJECTION_IN_PROGRESS: i16 = 1000;

#[cfg(test)]
mod tests {

    use krabka_broker::codes;

    #[test]
    fn cli_private_code_matches_the_broker_wire_contract() {
        assert2::assert!(
            super::BARRIER_INJECTION_IN_PROGRESS == codes::BARRIER_INJECTION_IN_PROGRESS
        );
    }
}
