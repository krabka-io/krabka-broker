//! The fixture that more than one of this module's unit-test modules needs:
//! the per-fetch soft byte cap every test `ObserverConfig` is built with.

use krabka_units::{ByteSize, mebibytes};

/// Per-fetch soft byte cap for every observer fixture: 1 MiB.
pub(super) const TEST_MAX_FETCH_BYTES: ByteSize = mebibytes(1);
