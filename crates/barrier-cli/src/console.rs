//! Where the tool's lines go.
//!
//! The binary writes to stdout and stderr. A test hands [`crate::run_with`] two
//! buffers instead, so it reads exactly the lines an operator would see without
//! spawning the binary.

use std::{fmt::Display, io::Write};

/// The two streams the tool writes: stdout for what succeeded, and stderr for
/// what did not.
pub struct Console<'a> {
    out: &'a mut (dyn Write + Send),
    err: &'a mut (dyn Write + Send),
}

impl<'a> Console<'a> {
    /// A console over the two given streams.
    pub fn new(out: &'a mut (dyn Write + Send), err: &'a mut (dyn Write + Send)) -> Self {
        Self { out, err }
    }

    /// Write one line to the success stream.
    ///
    /// A write that fails is dropped: an operator who closed the pipe has
    /// stopped reading, and the exit code still says what happened.
    pub(crate) fn out(&mut self, line: impl Display) { _ = writeln!(self.out, "{line}"); }

    /// Write one line to the failure stream, dropping a failed write the same
    /// way [`Console::out`] does.
    pub(crate) fn err(&mut self, line: impl Display) { _ = writeln!(self.err, "{line}"); }
}
