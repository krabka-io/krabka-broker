//! The `commit!` test macro, which spells one `ProducerState::commit` call as
//! flat positional arguments.
//!
//! `macro_rules!` is in scope only after its definition, so the root declares
//! this module `#[macro_use]` ahead of every module whose tests expand the
//! macro.

macro_rules! commit {
    ($state:expr, $topic:expr, $partition:expr, $pid:expr, $epoch:expr,
     $base:expr, $delta:expr, $offset:expr, $timestamp:expr $(,)?) => {
        $state.commit(
            $topic,
            $partition,
            ($pid, $epoch),
            ($base, $delta),
            ($offset, $timestamp),
        )
    };
}
