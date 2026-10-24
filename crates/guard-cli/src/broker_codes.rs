//! Broker response codes this command-line tool names specially.

pub const BREAK_GLASS_APPROVAL_REQUIRED: i16 = 1006;
pub const BREAK_GLASS_DUPLICATE_APPROVER: i16 = 1007;
pub const BREAK_GLASS_NOT_AN_APPROVER: i16 = 1008;
pub const OPERATOR_SIGNATURE_INVALID: i16 = 1009;
pub const OPERATOR_SIGNATURE_REQUIRED: i16 = 1010;
pub const FREEZE_SCOPE_INVALID: i16 = 1011;
pub const FREEZE_LIMIT_EXCEEDED: i16 = 1012;
