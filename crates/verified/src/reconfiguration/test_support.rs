//! Domain values used by direct and composed reconfiguration fixtures.

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct VoterCount(pub usize);

#[derive(derive_more::From, derive_more::Into, Clone, Copy)]
pub struct KraftFeatureLevel(pub u16);

#[derive(Clone, Copy)]
pub enum ControlRecordWrite {
    Emit,
    Skip,
}
