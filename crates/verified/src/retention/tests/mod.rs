use assert2::check;

use super::*;

/// A local segment that is neither blocked nor expired.
const fn fresh(size: u64) -> LocalRetentionSegment {
    LocalRetentionSegment {
        blocked: false,
        expired: false,
        size,
    }
}

/// A local segment past `retention.ms` or below the log start.
const fn expired(size: u64) -> LocalRetentionSegment {
    LocalRetentionSegment {
        blocked: false,
        expired: true,
        size,
    }
}

/// A local segment no pass may delete.
const fn blocked(expired: bool, size: u64) -> LocalRetentionSegment {
    LocalRetentionSegment {
        blocked: true,
        expired,
        size,
    }
}

/// A finished remote segment with the given axes.
const fn remote(log_start_breached: bool, time_expired: bool, size: u64) -> RemoteRetentionSegment {
    RemoteRetentionSegment {
        log_start_breached,
        time_expired,
        size,
    }
}

mod barrier_cut_expiry_is_exact_and_fails_closed_at_extremes;

mod delete_target_rejects_offset_exhaustion;

mod remote_charge;
