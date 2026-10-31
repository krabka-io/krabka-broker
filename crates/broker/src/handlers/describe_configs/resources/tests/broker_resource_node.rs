//! Which node a `BROKER` resource name asks about, and the message a name that
//! is not this node is refused with.

use {super::super::broker_resource_node, assert2::check, krabka_metadata::NodeId};

#[test]
fn a_broker_resource_name_is_empty_or_this_node() {
    let serving = NodeId(7);
    for (name, want) in [
        ("", Ok(None)),
        ("7", Ok(Some(NodeId(7)))),
        (
            "abc",
            Err("Broker id must be an integer, but it is: abc".to_owned()),
        ),
        (
            "8",
            Err("Unexpected broker id, expected 7 or empty string, but received 8".to_owned()),
        ),
        (
            "-1",
            Err("Unexpected broker id, expected 7 or empty string, but received -1".to_owned()),
        ),
        (
            "4294967296",
            Err("Broker id must be an integer, but it is: 4294967296".to_owned()),
        ),
    ] {
        check!(broker_resource_node(name, serving) == want, "name {name:?}");
    }
}
