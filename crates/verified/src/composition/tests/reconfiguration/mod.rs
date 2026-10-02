use super::*;
use crate::reconfiguration::{
    CurrentVoterSet, ReconfigurationLeadership, TargetMembership, TargetVoter, VoterChangeKind,
    VoterChangeRequest, VoterReconfigurationPlan,
};

mod oracle;
use oracle::{check_overlap, current, leading, target};
mod sets;
