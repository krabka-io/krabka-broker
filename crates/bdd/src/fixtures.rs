//! The fixtures a generated scenario test hands the runner.

use {
    crate::{runner::Outcome, spec::ScenarioSpec, world::World},
    rstest_bdd::StepContext,
    std::{
        any::Any,
        cell::RefCell,
        fmt::{self, Debug, Formatter},
        future::Future,
        mem,
        pin::Pin,
    },
};

/// A type-erased owned fixture value.
type Cell = RefCell<Box<dyn Any>>;

/// A future that borrows the fixture it runs on.
type LocalFuture<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

/// Inserts a cell into a step context under its concrete type.
type InsertFn = for<'a> fn(&mut StepContext<'a>, &'static str, &'a Cell);

/// Runs a World's `before` on the World stored in a cell.
type BeforeFn = for<'a> fn(&'a Cell, &'a ScenarioSpec) -> LocalFuture<'a>;

/// Runs a World's `after` on the World stored in a cell.
type AfterFn = for<'a> fn(&'a Cell, &'a ScenarioSpec, &'a Outcome) -> LocalFuture<'a>;

struct Entry {
    name: &'static str,
    cell: Cell,
    insert: InsertFn,
    world: Option<(BeforeFn, AfterFn)>,
}

/// The owned fixtures of one scenario run, by name.
///
/// Each fixture is inserted into the step context under its name, so a step
/// parameter of that name borrows it, shared or mutably. A step that returns
/// a value of a fixture's type replaces that fixture for the later steps.
#[derive(Default)]
pub struct Fixtures {
    entries: Vec<Entry>,
}

impl Debug for Fixtures {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_list()
            .entries(self.entries.iter().map(|entry| entry.name))
            .finish()
    }
}

fn insert<'a, T: Any>(ctx: &mut StepContext<'a>, name: &'static str, cell: &'a Cell) {
    ctx.insert_owned::<T>(name, cell);
}

/// Holds a World moved out of its cell while its `before` or `after` awaits,
/// and puts it back when dropped, also when the future panics.
struct TakenWorld<'a> {
    cell: &'a Cell,
    value: Box<dyn Any>,
}

impl<'a> TakenWorld<'a> {
    fn take(cell: &'a Cell) -> Self {
        let value = cell.replace(Box::new(()));
        Self { cell, value }
    }
}

impl Drop for TakenWorld<'_> {
    fn drop(&mut self) {
        let value = mem::replace(&mut self.value, Box::new(()));
        *self.cell.borrow_mut() = value;
    }
}

fn world_before<'a, T: World + Any>(cell: &'a Cell, scenario: &'a ScenarioSpec) -> LocalFuture<'a> {
    Box::pin(async move {
        let mut taken = TakenWorld::take(cell);
        if let Some(world) = taken.value.downcast_mut::<T>() {
            world.before(scenario).await;
        }
    })
}

fn world_after<'a, T: World + Any>(
    cell: &'a Cell,
    scenario: &'a ScenarioSpec,
    outcome: &'a Outcome,
) -> LocalFuture<'a> {
    Box::pin(async move {
        let mut taken = TakenWorld::take(cell);
        if let Some(world) = taken.value.downcast_mut::<T>() {
            world.after(scenario, outcome).await;
        }
    })
}

/// A World fixture, with the hooks the runner calls on it.
pub struct WorldRef<'a> {
    name: &'static str,
    cell: &'a Cell,
    before: BeforeFn,
    after: AfterFn,
}

impl<'a> WorldRef<'a> {
    /// The fixture's name.
    pub const fn name(&self) -> &'static str { self.name }

    /// Runs the World's `before`.
    pub fn before(&self, scenario: &'a ScenarioSpec) -> LocalFuture<'a> {
        (self.before)(self.cell, scenario)
    }

    /// Runs the World's `after`.
    pub fn after(&self, scenario: &'a ScenarioSpec, outcome: &'a Outcome) -> LocalFuture<'a> {
        (self.after)(self.cell, scenario, outcome)
    }
}

impl Fixtures {
    /// No fixtures.
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Adds the fixture `name`.
    #[must_use]
    pub fn with<T: Any>(mut self, name: &'static str, value: T) -> Self {
        self.entries.push(Entry {
            name,
            cell: StepContext::owned_cell(value),
            insert: insert::<T>,
            world: None,
        });
        self
    }

    /// Adds the fixture `name`, whose [`World::before`] and [`World::after`]
    /// run around the scenario.
    #[must_use]
    pub fn with_world<T: World + Any>(mut self, name: &'static str, value: T) -> Self {
        self.entries.push(Entry {
            name,
            cell: StepContext::owned_cell(value),
            insert: insert::<T>,
            world: Some((world_before::<T>, world_after::<T>)),
        });
        self
    }

    /// Inserts every fixture into `ctx`.
    pub(crate) fn insert_into<'a>(&'a self, ctx: &mut StepContext<'a>) {
        for entry in &self.entries {
            (entry.insert)(ctx, entry.name, &entry.cell);
        }
    }

    /// The World fixtures, in the order they were added.
    pub(crate) fn worlds(&self) -> Vec<WorldRef<'_>> {
        self.entries
            .iter()
            .filter_map(|entry| {
                entry.world.map(|(before, after)| WorldRef {
                    name: entry.name,
                    cell: &entry.cell,
                    before,
                    after,
                })
            })
            .collect()
    }
}
