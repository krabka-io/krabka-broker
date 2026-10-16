//! Render a record and its verdict as the Markdown that is published.
//!
//! The page states the contract it applies to before any number, and it
//! names the two exact builds rather than "krabka" and "Kafka": a result here
//! is a measured limit of one contract on one host, not a broker ranking.

use {
    crate::{
        budget::{BudgetVerdict, Rejection, Status, metric_value},
        contract::{Contract, Metric, Mode},
        record::{PerformanceRecord, Side, TrialOutcome},
    },
    std::fmt::Write as _,
};

fn value(metric: Metric, value: f64) -> String {
    match metric {
        Metric::RecordsPerSecond => format!("{value:.0}"),
        Metric::PeakRssMib => format!("{value:.1}"),
        Metric::LatencyMsP50
        | Metric::LatencyMsP95
        | Metric::LatencyMsP99
        | Metric::CpuMicrosPerRecord => format!("{value:.3}"),
    }
}

fn percent(ratio: f64) -> String { format!("{:+.1}%", ratio * 100.0) }

fn status(status: Status, gate_performance: bool) -> &'static str {
    match status {
        Status::WithinBudget => "within budget",
        Status::Improved => "improved",
        Status::NoisyNotMaterial => "noisy, not material",
        Status::MaterialRegression if gate_performance => "**material regression**",
        Status::MaterialRegression => "material difference (not gating)",
        Status::ExcessVariance => "**excess variance**",
        Status::NotBudgeted => "recorded",
    }
}

fn rejection(rejection: &Rejection) -> String {
    match rejection {
        Rejection::CorrectnessFailure {
            cell,
            round,
            side,
            reason,
        } => format!(
            "correctness failure in `{cell}` round {round} ({}): {reason}",
            side.name()
        ),
        Rejection::MissingTrial { cell, round, side } => {
            format!("missing trial: `{cell}` round {round} ({})", side.name())
        }
        Rejection::NotInterleaved { cell, round } => format!(
            "`{cell}` ran the same side first in rounds {} and {round}",
            round - 1
        ),
        Rejection::MaterialRegression {
            cell,
            metric,
            worse_change,
            allowed_change,
        } => format!(
            "`{cell}` {} is {} worse; the budget allows {}",
            metric.label(),
            percent(*worse_change),
            percent(*allowed_change)
        ),
        Rejection::ExcessVariance {
            cell,
            metric,
            spread,
            max_spread,
        } => format!(
            "`{cell}` {} spread is {}; the variance budget is {}",
            metric.label(),
            percent(*spread),
            percent(*max_spread)
        ),
    }
}

/// The published Markdown for one comparison.
#[must_use]
pub fn render_markdown(
    contract: &Contract,
    record: &PerformanceRecord,
    verdict: &BudgetVerdict,
) -> String {
    let mut out = String::new();
    let reference = &record.reference;
    let candidate = &record.candidate;
    let against = match record.mode {
        Mode::BeforeAfter => "an earlier krabka revision",
        Mode::PinnedKafka => "the pinned Apache Kafka image",
    };
    let _ = writeln!(out, "# Performance budget: `{}`\n", contract.name);
    let _ = writeln!(
        out,
        "These results compare one candidate build with {against} under the \
         `{}` contract on the host below. They are measured limits of that \
         contract, not a ranking of brokers and not a supported limit.\n",
        contract.name
    );
    let _ = writeln!(
        out,
        "**Verdict: {}.**\n",
        if verdict.passed { "pass" } else { "reject" }
    );
    if !verdict.rejections.is_empty() {
        for item in &verdict.rejections {
            let _ = writeln!(out, "- {}", rejection(item));
        }
        out.push('\n');
    }

    out.push_str("## Provenance\n\n| Side | Label | Revision | Image | Digest |\n| :--- | :--- | :--- | :--- | :--- |\n");
    for (side, subject) in [(Side::Reference, reference), (Side::Candidate, candidate)] {
        let _ = writeln!(
            out,
            "| {} | {} | `{}` | `{}` | `{}` |",
            side.name(),
            subject.label,
            subject.revision,
            subject.image,
            subject.image_digest
        );
    }
    let host = &record.host;
    let _ = writeln!(
        out,
        "\nClient: `{}` (`{}`). Contract digest: `{}`. Started {}.\n",
        record.client_image, record.client_image_digest, record.contract_sha256, record.started_at
    );
    let _ = writeln!(
        out,
        "Host: {}, {} logical CPUs, {} bytes of memory, {} bytes of free disk, \
         kernel {}.\n",
        host.cpu_model,
        host.logical_cpus,
        host.memory_bytes,
        host.disk_available_bytes,
        host.kernel
    );
    let cluster = &contract.cluster;
    let _ = writeln!(
        out,
        "Cluster: {} combined broker/controllers, replication factor {}, \
         `min.insync.replicas={}`, `acks=all`, idempotence, 65,536-byte batches, \
         5 ms linger. {} interleaved rounds; each round runs every cell once per side.\n",
        cluster.brokers, cluster.replication_factor, cluster.min_insync_replicas, contract.rounds
    );
    if !record.settings.is_empty() {
        out.push_str("Settings:");
        for (key, setting) in &record.settings {
            let _ = write!(out, " `{key}={setting}`");
        }
        out.push_str(".\n\n");
    }

    out.push_str("## Budgets\n\nDeclared before the run. A regression is material when the median gets worse by more than the materiality floor and by more than the multiplier times the larger side's relative median absolute deviation. A side whose spread exceeds the variance budget makes the cell inconclusive.\n\n| Metric | Materiality | Spread multiplier | Variance budget |\n| :--- | ---: | ---: | ---: |\n");
    for (metric, budget) in &contract.budgets {
        let _ = writeln!(
            out,
            "| {} | {:.1}% | {:.1} | {:.1}% |",
            metric.label(),
            budget.materiality_percent,
            budget.spread_multiplier,
            budget.max_spread_percent
        );
    }

    out.push_str("\n## Results\n\nMedians over the interleaved rounds. A change is relative to the reference median; positive is worse.\n\n| Cell | Metric | Reference | Candidate | Change | Spread (ref / cand) | Allowed | Status |\n| :--- | :--- | ---: | ---: | ---: | :--- | ---: | :--- |\n");
    for cell in &contract.cells {
        let Some(metrics) = verdict.cells.get(&cell.id) else {
            let _ = writeln!(
                out,
                "| `{}` | all | — | — | — | — | — | not evaluated |",
                cell.id
            );
            continue;
        };
        for (metric, judged) in metrics {
            let allowed = if judged.status == Status::NotBudgeted {
                "—".to_string()
            } else {
                percent(judged.allowed_change)
            };
            let _ = writeln!(
                out,
                "| `{}` | {} | {} | {} | {} | {:.1}% / {:.1}% | {} | {} |",
                cell.id,
                metric.label(),
                value(*metric, judged.reference_median),
                value(*metric, judged.candidate_median),
                percent(judged.worse_change),
                judged.reference_spread * 100.0,
                judged.candidate_spread * 100.0,
                allowed,
                status(judged.status, verdict.gate_performance)
            );
        }
    }

    out.push_str("\n## Raw trials\n\n| Order | Cell | Round | Side | records/s | p50 ms | p95 ms | p99 ms | CPU µs/record | peak RSS MiB | Outcome |\n| ---: | :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |\n");
    for trial in &record.trials {
        let _ = write!(
            out,
            "| {} | `{}` | {} | {} |",
            trial.order,
            trial.cell,
            trial.round,
            trial.side.name()
        );
        match &trial.outcome {
            TrialOutcome::Completed(measurement) => {
                for metric in Metric::ALL {
                    let _ = write!(
                        out,
                        " {} |",
                        value(metric, metric_value(metric, measurement))
                    );
                }
                let _ = writeln!(
                    out,
                    " sent {} / consumed {} / duplicates {} / errors {} |",
                    measurement.sent,
                    measurement.consumed,
                    measurement.duplicates,
                    measurement.errors
                );
            }
            TrialOutcome::Failed { reason } => {
                let _ = writeln!(out, " — | — | — | — | — | — | failed: {reason} |");
            }
        }
    }
    out
}
