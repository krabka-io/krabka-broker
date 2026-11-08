//! Native reassignment evaluation is an executable contract.
#![cfg(test)]

use {
    assert2::assert,
    std::{cell::RefCell, num::Wrapping},
};

#[derive(Default)]
struct Assignment {
    replicas: Vec<i32>,
    isr: Vec<i32>,
    adding: Vec<i32>,
    removing: Vec<i32>,
    leader: Wrapping<i32>,
    directories: Vec<u8>,
}
struct Builder<'a> {
    order: &'a RefCell<Vec<&'static str>>,
    fields: Assignment,
}
impl Builder<'_> {
    fn replicas(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("replicas");
        self.fields.replicas = value;
        self
    }

    fn isr(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("isr");
        self.fields.isr = value;
        self
    }

    fn adding(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("adding");
        self.fields.adding = value;
        self
    }

    fn removing(mut self, value: Vec<i32>) -> Self {
        self.order.borrow_mut().push("removing");
        self.fields.removing = value;
        self
    }

    fn leader(mut self, value: i32) -> Self {
        self.order.borrow_mut().push("leader");
        self.fields.leader = Wrapping(value);
        self
    }

    fn directories(mut self, value: Vec<u8>) -> Self {
        self.order.borrow_mut().push("directories");
        self.fields.directories = value;
        self
    }
}

#[test]
fn reassignment_sources_run_once_before_setters_and_keep_all_owned_fields() {
    let order = RefCell::new(Vec::new());
    let assignment = Assignment {
        replicas: vec![1, 2],
        isr: vec![1],
        adding: vec![3],
        removing: vec![2],
        leader: Wrapping(1),
        directories: vec![7, 8],
    };
    let result = krabka_broker_macros::reassignment_fields! {
        assignment { order.borrow_mut().push("assignment"); assignment }
        builder { order.borrow_mut().push("builder"); Builder { order: &order, fields: Assignment::default() } }
    };
    assert!(
        *order.borrow()
            == [
                "assignment",
                "builder",
                "replicas",
                "isr",
                "adding",
                "removing",
                "leader",
                "directories"
            ]
    );
    assert!(result.fields.replicas == [1, 2] && result.fields.isr == [1]);
    assert!(result.fields.adding == [3] && result.fields.removing == [2]);
    assert!(result.fields.leader == Wrapping(1) && result.fields.directories == [7, 8]);
}
