use super::{
    HomogeneousMemberQuota, SubscriberLoad, UniformQuotaSplit, homogeneous_member_quotas,
    select_least_loaded, uniform_quota_split,
};

fn quota(takes_extra: bool, retain: usize, fill: usize) -> HomogeneousMemberQuota {
    HomogeneousMemberQuota {
        takes_extra,
        retain,
        fill,
    }
}

struct QuotaRow {
    name: &'static str,
    minimum: usize,
    extras: usize,
    owned: &'static [usize],
    expected: Vec<HomogeneousMemberQuota>,
}

fn load(assigned: usize, assigned_at_topic_start: usize) -> SubscriberLoad {
    SubscriberLoad {
        assigned,
        assigned_at_topic_start,
    }
}

mod quota_split_is_kafkas_floor_and_remainder;
