//! Constants remain public while guard tables preserve declaration order, and
//! the generated lookup answers each declared Kafka row's name and message.
#![cfg(test)]

use assert2::assert;

mod codes {
    #[derive(Debug, PartialEq, Eq)]
    struct NamedCode {
        name: &'static str,
        code: i16,
        message: Option<&'static str>,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) struct KafkaError {
        pub(super) name: &'static str,
        pub(super) message: &'static str,
    }

    krabka_macros::wire_codes! {
        release RELEASE {
            /// A successful reply.
            NONE = 0 => "NONE";
            UNKNOWN = -1 => "Something went wrong.";
            MAX = 32767 => "The top.";
            MIN = -32768 => "The bottom.";
        }
        trunk TRUNK {
            TOPOLOGY = 135 => "The topology moved.";
            CONTROLLER = 136 => "No such controller.";
        }
        private PRIVATE {
            BUSY = 1000;
            REFUSED = 1013;
        }
    }

    pub(super) const fn lookup(code: i16) -> Option<KafkaError> { declared_kafka_error(code) }

    #[test]
    fn private_guard_tables_keep_every_name_value_and_message_in_declared_order() {
        use assert2::assert;

        let row = |name, code, message| NamedCode {
            name,
            code,
            message,
        };
        assert!(
            [RELEASE, TRUNK, PRIVATE]
                == [
                    &[
                        row("NONE", 0, Some("NONE")),
                        row("UNKNOWN", -1, Some("Something went wrong.")),
                        row("MAX", 32767, Some("The top.")),
                        row("MIN", -32768, Some("The bottom.")),
                    ][..],
                    &[
                        row("TOPOLOGY", 135, Some("The topology moved.")),
                        row("CONTROLLER", 136, Some("No such controller.")),
                    ][..],
                    &[row("BUSY", 1000, None), row("REFUSED", 1013, None)][..],
                ]
        );
    }
}

#[test]
fn generated_constants_are_public_i16_values() {
    let release: [i16; 4] = [codes::NONE, codes::UNKNOWN, codes::MAX, codes::MIN];
    assert!(release == [0, -1, i16::MAX, i16::MIN]);
    assert!(
        [
            codes::TOPOLOGY,
            codes::CONTROLLER,
            codes::BUSY,
            codes::REFUSED
        ] == [135, 136, 1000, 1013]
    );
}

#[yare::parameterized(
    release = { codes::UNKNOWN, Some(("UNKNOWN", "Something went wrong.")) },
    none = { codes::NONE, Some(("NONE", "NONE")) },
    trunk = { codes::CONTROLLER, Some(("CONTROLLER", "No such controller.")) },
    private = { codes::BUSY, None },
    undeclared = { 7, None },
)]
fn the_lookup_answers_declared_kafka_rows_only(code: i16, want: Option<(&str, &str)>) {
    let want = want.map(|(name, message)| codes::KafkaError { name, message });
    assert2::assert!(codes::lookup(code) == want);
}
