#![cfg(test)]
//! `drivers = [..]` runs every scenario once per driver, and `@only_<driver>`
//! keeps a scenario to one of them: run against the in-process driver, the
//! wire-only scenario's Then would fail.

use {assert2::assert, krabka_bdd::Given};

/// What a step asks of the system under test, in domain terms.
trait TopicDriver {
    fn create(&mut self, name: &str);
    fn list(&mut self) -> Listing;
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Listing {
    topics: Vec<String>,
    over_wire: bool,
}

#[derive(Default)]
struct InProcess {
    topics: Vec<String>,
}

impl TopicDriver for InProcess {
    fn create(&mut self, name: &str) { self.topics.push(name.to_owned()); }

    fn list(&mut self) -> Listing {
        Listing {
            topics: self.topics.clone(),
            over_wire: false,
        }
    }
}

/// Stands in for a client speaking the Kafka protocol to a broker.
#[derive(Default)]
struct Wire {
    topics: Vec<String>,
}

impl TopicDriver for Wire {
    fn create(&mut self, name: &str) { self.topics.push(name.to_owned()); }

    fn list(&mut self) -> Listing {
        Listing {
            topics: self.topics.clone(),
            over_wire: true,
        }
    }
}

#[rstest::fixture]
fn in_process() -> Box<dyn TopicDriver> {
    // The in-process driver: calls into the broker's own types.
    Box::new(InProcess::default())
}

#[rstest::fixture]
fn wire() -> Box<dyn TopicDriver> {
    // The wire driver: a client speaking the Kafka protocol.
    Box::new(Wire::default())
}

struct World {
    listing: Given<Listing>,
}

#[rstest::fixture]
fn world() -> World {
    World {
        listing: Given::new("listing", "When the topics are listed"),
    }
}

#[krabka_bdd::given("a topic named {name} has been created")]
fn topic_created(driver: &mut Box<dyn TopicDriver>, name: String) { driver.create(&name); }

#[krabka_bdd::when("the topics are listed")]
fn list_topics(driver: &mut Box<dyn TopicDriver>, world: &mut World) {
    world.listing.set(driver.list());
}

#[krabka_bdd::then("the listing holds {name}")]
fn listing_holds(world: &World, name: String) {
    assert!(world.listing.get().topics == vec![name]);
}

#[krabka_bdd::then("the listing came over the Kafka protocol")]
fn listing_over_wire(world: &World) {
    assert!(world.listing.get().over_wire);
}

krabka_bdd::scenarios!(
    "tests/features/polymorphic",
    fixtures = [world: World],
    driver = driver: Box<dyn TopicDriver>,
    drivers = [in_process, wire],
);
