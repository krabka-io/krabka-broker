use {
    super::*,
    rstest::fixture,
    rstest_bdd::datatable::Rows,
    rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    std::vec,
};

struct IsrController {
    watermark_facts: Option<HighWatermarkFacts>,
    replicas: Vec<HwmReplica>,
    watermark: Option<i64>,
    current_leader_epoch: i32,
    current_partition_epoch: i32,
    alteration: Option<AlterPartitionFacts>,
    admission: Option<IsrAdmission>,
}

#[fixture]
fn controller() -> IsrController {
    // Start this scenario with fresh inputs and observations.

    IsrController {
        watermark_facts: None,
        replicas: Vec::new(),
        watermark: None,
        current_leader_epoch: 0,
        current_partition_epoch: 0,
        alteration: None,
        admission: None,
    }
}

#[given(
    "a leader at high watermark {current} and log end {leader_end} with ISR size {isr_size} and minimum ISR {min_isr}"
)]
fn leader_progress(
    controller: &mut IsrController,
    current: i64,
    leader_end: i64,
    isr_size: usize,
    min_isr: usize,
) {
    controller.watermark_facts = Some(HighWatermarkFacts {
        current,
        leader_log_end: leader_end,
        isr_size,
        effective_min_isr: min_isr,
    });
}

#[derive(DataTableRow)]
struct WatermarkReplica {
    log_end: i64,
    in_isr: bool,
    recently_caught_up: bool,
    fenced: bool,
}

#[given("its assigned followers have this replication state:")]
fn assigned_followers(controller: &mut IsrController, #[datatable] rows: Rows<WatermarkReplica>) {
    controller.replicas = rows
        .into_iter()
        .map(|row| HwmReplica {
            log_end: row.log_end,
            in_isr: row.in_isr,
            caught_up_within_lag: row.recently_caught_up,
            eligibility: IsrEligibilityFacts {
                fenced: row.fenced,
                ..ELIGIBLE
            },
        })
        .collect();
}

#[when("the leader advances its committed high watermark")]
fn commit_watermark(controller: &mut IsrController) {
    controller.watermark = Some(leader_high_watermark(
        controller
            .watermark_facts
            .expect("leader progress was given"),
        &controller.replicas,
    ));
}

#[then("the committed high watermark is {watermark}")]
fn committed_watermark(controller: &IsrController, watermark: i64) {
    assert2::check!(controller.watermark == Some(watermark));
}

#[given("the controller holds leader epoch {leader_epoch} and partition epoch {partition_epoch}")]
fn controller_epochs(controller: &mut IsrController, leader_epoch: i32, partition_epoch: i32) {
    controller.current_leader_epoch = leader_epoch;
    controller.current_partition_epoch = partition_epoch;
}

#[given(
    "an AlterPartition proposal at leader epoch {leader_epoch}, partition epoch {partition_epoch}, leader identity {is_leader}, ISR {proposed}, recovery valid {recovery}, and replicas eligible {eligible}"
)]
fn alteration_proposal(
    controller: &mut IsrController,
    leader_epoch: i32,
    partition_epoch: i32,
    is_leader: bool,
    proposed: String,
    recovery: bool,
    eligible: bool,
) {
    let proposed_isr = match proposed.as_str() {
        "valid" => ProposedIsr::Valid,
        "invalid" => ProposedIsr::Invalid,
        "without-leader" => ProposedIsr::WithoutLeader,
        _ => panic!("unknown proposed ISR shape: {proposed}"),
    };
    controller.alteration = Some(AlterPartitionFacts {
        request_leader_epoch: leader_epoch,
        current_leader_epoch: controller.current_leader_epoch,
        request_partition_epoch: partition_epoch,
        current_partition_epoch: controller.current_partition_epoch,
        requester_is_leader: is_leader,
        proposed_isr,
        recovery_state_valid: recovery,
        replicas_eligible: eligible,
    });
}

#[when("the controller validates the AlterPartition proposal")]
fn validate_proposal(controller: &mut IsrController) {
    controller.admission = Some(isr_admission(
        controller
            .alteration
            .expect("an AlterPartition proposal was given"),
    ));
}

#[then("AlterPartition admission returns {outcome}")]
fn admitted_proposal(controller: &IsrController, outcome: String) {
    let expected = match outcome.as_str() {
        "NONE" => IsrAdmission::Admit,
        "NOT_CONTROLLER" => IsrAdmission::NotController,
        "FENCED_LEADER_EPOCH" => IsrAdmission::FencedLeaderEpoch,
        "INVALID_REQUEST" => IsrAdmission::InvalidRequest,
        "INVALID_UPDATE_VERSION" => IsrAdmission::InvalidUpdateVersion,
        "INELIGIBLE_REPLICA" => IsrAdmission::IneligibleReplica,
        _ => panic!("unknown AlterPartition response: {outcome}"),
    };
    assert2::check!(controller.admission == Some(expected));
}

scenarios!("tests/features/migrated/src/isr/tests/leader_high_watermark_follows_kafka.feature", fixtures = [controller: IsrController]);
