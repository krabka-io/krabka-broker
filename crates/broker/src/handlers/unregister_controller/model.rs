//! Admission/delete/re-registration interleavings through the real refusal
//! adapter, verified decision kernel and metadata-image record application.
//! No asynchronous submission, authorization, raft durability or restart IO.

use {
    super::{
        refusal,
        tests::{image_at, registration, voters},
    },
    crate::{codes, model_check::check_model},
    krabka_metadata::{MetadataImage, MetadataRecord, NodeId, UnregisterControllerRecord},
    stateright::{Model, Property},
};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct Attempt {
    voter: bool,
    feature: bool,
    registered: bool,
    code: i16,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct State {
    voter: bool,
    feature: bool,
    registered: bool,
    last: Option<Attempt>,
}

impl State {
    fn image(&self) -> MetadataImage {
        let mut image = image_at(if self.feature { 33 } else { 32 });
        image.apply(&voters(if self.voter { &[1, 7] } else { &[1] }));
        if !self.registered {
            image.apply(&remove_record());
        }
        image
    }
}

fn remove_record() -> MetadataRecord {
    MetadataRecord::V1UnregisterController(UnregisterControllerRecord { node_id: NodeId(7) })
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Action {
    Register,
    Voter(bool),
    Feature(bool),
    Remove,
}
struct UnregisterModel;

impl Model for UnregisterModel {
    type Action = Action;
    type State = State;

    fn init_states(&self) -> Vec<State> {
        vec![State {
            voter: true,
            feature: false,
            registered: true,
            last: None,
        }]
    }

    fn actions(&self, s: &State, a: &mut Vec<Action>) {
        if !s.registered {
            a.push(Action::Register);
        }
        a.extend([
            Action::Voter(!s.voter),
            Action::Feature(!s.feature),
            Action::Remove,
        ]);
    }

    fn next_state(&self, s: &State, a: Action) -> Option<State> {
        let mut next = s.clone();
        let mut image = s.image();
        match a {
            Action::Register => {
                image.apply(&registration(7));
                next.registered = image.controller(NodeId(7)).is_some();
            }
            Action::Voter(voter) => next.voter = voter,
            Action::Feature(feature) => next.feature = feature,
            Action::Remove => {
                let code = refusal(&image, 7).map_or(codes::NONE, |r| r.error_code);
                if code == codes::NONE {
                    image.apply(&remove_record());
                }
                next.registered = image.controller(NodeId(7)).is_some();
                next.last = Some(Attempt {
                    voter: s.voter,
                    feature: s.feature,
                    registered: s.registered,
                    code,
                });
            }
        }
        Some(next)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("voter_precedes_feature", |_, s: &State| {
                s.last
                    .as_ref()
                    .is_none_or(|a| !a.voter || a.code == codes::INVALID_REQUEST)
            }),
            Property::always("feature_precedes_registration", |_, s: &State| {
                s.last
                    .as_ref()
                    .is_none_or(|a| a.voter || a.feature || a.code == codes::UNSUPPORTED_VERSION)
            }),
            Property::always("append_requires_all_gates", |_, s: &State| {
                s.last.as_ref().is_none_or(|a| {
                    a.code != codes::NONE || (!a.voter && a.feature && a.registered)
                })
            }),
            Property::sometimes("successful_removal", |_, s: &State| {
                !s.registered && s.last.as_ref().is_some_and(|a| a.code == codes::NONE)
            }),
            Property::sometimes("repeat_remove_rejected", |_, s: &State| {
                s.last.as_ref().is_some_and(|a| {
                    !a.registered
                        && !a.voter
                        && a.feature
                        && a.code == codes::CONTROLLER_ID_NOT_REGISTERED
                })
            }),
            Property::sometimes("registration_after_removal", |_, s: &State| {
                s.registered && s.last.as_ref().is_some_and(|a| a.code == codes::NONE)
            }),
        ]
    }
}

#[test]
fn controller_unregistration_interleaves_metadata_changes_and_retries() {
    check_model(
        UnregisterModel,
        "controller_unregister",
        (20, 10000, 200),
        56,
    );
}
