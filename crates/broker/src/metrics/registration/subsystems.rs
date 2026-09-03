//! Registration of the families each optional broker subsystem owns: the
//! audit spool, the KIP-714 client-metrics export, the log cleaner, the
//! barrier coordinator, KFC-1 scheduled delivery, KFC-7 schema validation,
//! and the KFC-9 topic freeze and break-glass workflow.

use prometheus_client::registry::Registry;

use crate::metrics::BrokerMetrics;

impl BrokerMetrics {
    pub(super) fn register_group_5(&self, registry: &mut Registry) {
        registry.register(
            "audit_write_failures",
            "Cumulative audit records that failed to write to the audit topic",
            self.audit_write_failures.clone(),
        );

        registry.register(
            "audit_spool_depth",
            "Current count of audit records buffered in the durable spool",
            self.audit_spool_depth.clone(),
        );

        registry.register(
            "audit_spool_bytes",
            "Current bytes buffered in the durable audit spool",
            self.audit_spool_bytes.clone(),
        );

        registry.register(
            "audit_records_spooled",
            "Cumulative audit records diverted to the spool on topic-write failure",
            self.audit_records_spooled_total.clone(),
        );

        registry.register(
            "audit_records_replayed",
            "Cumulative audit records drained from the spool back to the topic",
            self.audit_records_replayed_total.clone(),
        );

        registry.register(
            "audit_records_dropped",
            "Cumulative audit records lost (channel-full or spool-full)",
            self.audit_records_dropped_total.clone(),
        );

        registry.register(
            "client_metrics_otlp_dropped",
            "Cumulative KIP-714 client-metric batches dropped before OTLP export",
            self.client_metrics_otlp_dropped_total.clone(),
        );
        registry.register(
            "client_metrics_otlp_failed",
            "Cumulative failed KIP-714 client-metric OTLP export attempts",
            self.client_metrics_otlp_failed_total.clone(),
        );

        registry.register(
            "log_cleaner_runs",
            "Cumulative count of completed log-compaction sweeps run by \
             this broker's cleaner (one per tick_all pass).",
            self.log_cleaner_runs_total.clone(),
        );

        registry.register(
            "log_compactions",
            "Per-partition cumulative count of compaction passes this \
             broker's cleaner completed successfully.",
            self.log_compactions_total.clone(),
        );
    }

    pub(super) fn register_group_6(&self, registry: &mut Registry) {
        registry.register(
            "barrier_epochs_started",
            "Per-barrier-group cumulative count of epochs the coordinator \
             started. Bumped when it writes the injection-start record that \
             freezes the target set, before the first marker append.",
            self.barrier_epochs_started_total.clone(),
        );

        registry.register(
            "barrier_epochs_committed",
            "Per-barrier-group cumulative count of epochs whose marker \
             reached every partition of the group. The coordinator published \
             a complete cut for each one.",
            self.barrier_epochs_committed_total.clone(),
        );

        registry.register(
            "barrier_epochs_published_partial",
            "Per-barrier-group cumulative count of epochs whose cut names at \
             least one partition that got no marker. The coordinator consumes \
             the epoch either way. Alert on rate(...) > 0 to catch a group \
             that no longer reaches all of its partitions.",
            self.barrier_epochs_published_partial_total.clone(),
        );

        registry.register(
            "barrier_injection_duration_seconds",
            "Per-barrier-group wall-clock seconds from the injection-start \
             record to the published cut. Graph histogram_quantile(0.99, \
             rate(..._bucket[5m])) against barrier_injection_timeout to see \
             how much headroom a group has.",
            self.barrier_injection_duration_seconds.clone(),
        );

        registry.register(
            "barrier_latest_epoch",
            "Per-barrier-group epoch of the newest cut this coordinator \
             published (gauge). A flat value beside a live \
             barrier_min_injection_interval says that injection stopped.",
            self.barrier_latest_epoch.clone(),
        );

        registry.register(
            "barrier_markers_written",
            "Per-topic cumulative count of barrier markers this broker \
             appended, across every group and every partition it leads.",
            self.barrier_markers_written_total.clone(),
        );

        registry.register(
            "barrier_groups_coordinated",
            "Number of barrier groups this broker coordinates (gauge). Zero \
             on a broker that leads no __barrier_state partition.",
            self.barrier_groups_coordinated.clone(),
        );

        registry.register(
            "delivery_watermark",
            "KFC-1 deliver-at-time watermark of each scheduled partition this \
             broker leads (gauge): the first offset that is not visible to a \
             consumer yet. A partition whose topic delivers immediately \
             reports no series.",
            self.delivery_watermark.clone(),
        );

        registry.register(
            "delivery_pending_records",
            "KFC-1 records of each scheduled partition that are durable but \
             not visible yet (gauge): the log end offset minus the delivery \
             watermark.",
            self.delivery_pending_records.clone(),
        );

        registry.register(
            "delivery_activation_lateness_seconds",
            "KFC-1 seconds from a batch's activation deadline to the moment \
             the broker first made it visible. The deadline is the record \
             timestamp plus the topic's declared clock bound, so this measures \
             the delay beyond that bound and a healthy broker sits at zero. A \
             rising tail says the bound is not honest, or that the scheduler \
             is starved of CPU.",
            self.delivery_activation_lateness_seconds.clone(),
        );

        registry.register(
            "delivery_scheduler_wakeups",
            "KFC-1 cumulative count of delivery-scheduler wakeups, whether a \
             deadline came due, a produce re-armed the task, or its idle \
             bound elapsed.",
            self.delivery_scheduler_wakeups_total.clone(),
        );

        registry.register(
            "schema_validation_rejections",
            "KFC-7 cumulative count of records rejected by schema \
             validation, per topic and reason. The reason is one of \
             unframed, unknown_id, wrong_subject, body_mismatch, and \
             registry_unavailable. The broker bumps it once per rejected \
             record. Read the split by reason during a rollout to see which \
             producer is at fault.",
            self.schema_validation_rejections.clone(),
        );

        registry.register(
            "schema_validation_cache_hits",
            "KFC-7 cumulative count of schema lookups the broker answered \
             from its local cache, with no call to the registry.",
            self.schema_validation_cache_hits.clone(),
        );

        registry.register(
            "schema_validation_cache_misses",
            "KFC-7 cumulative count of schema lookups that cost a registry \
             round trip on the produce path. The ratio against \
             schema_validation_cache_hits is what says whether this feature \
             costs anything at steady state.",
            self.schema_validation_cache_misses.clone(),
        );

        registry.register(
            "delivery_clock_uncertainty_seconds",
            "KFC-8 the clock bound this broker declares: the seconds KFC-1 \
             adds to a batch's timestamp before the batch activates. Compare \
             measured clock uncertainty against this series, so an alert \
             tracks the bound the broker relies on instead of a copy of it.",
            self.delivery_clock_uncertainty_seconds.clone(),
        );
    }

    pub(super) fn register_group_7(&self, registry: &mut Registry) {
        registry.register(
            "topic_freeze_rejections",
            "KFC-9 cumulative count of Produce partition rows the broker \
             refused because a freeze covers the topic, per topic. The gate \
             runs before the batch is parsed, so a refused row moves no log \
             end offset.",
            self.topic_freeze_rejections.clone(),
        );

        registry.register(
            "topic_freezes_active",
            "KFC-9 live entries in the freeze registry (gauge). One prefix \
             entry covers a whole namespace, so this counts entries and not \
             frozen topics. The freeze max_entries setting caps it.",
            self.topic_freezes_active.clone(),
        );

        registry.register(
            "break_glass_proposals",
            "KFC-9 break-glass proposals by state (gauge), where state is one \
             of pending, approved, expired, and consumed. A rise in pending \
             beside a flat approved is an incident where the second person \
             has not answered yet.",
            self.break_glass_proposals.clone(),
        );

        registry.register(
            "break_glass_refusals",
            "KFC-9 cumulative count of privileged transitions the broker \
             refused because no approved break-glass proposal covers them, \
             per action. A refusal is the expected answer when an operator \
             runs the tool before the approval lands.",
            self.break_glass_refusals.clone(),
        );

        registry.register(
            "break_glass_bypassed",
            "KFC-9 cumulative count of privileged transitions that ran \
             WITHOUT an approved break-glass proposal, per action. This is \
             the series to alert on: it counts data-losing unclean \
             recoveries that no second person approved, which the background \
             policy audit-only permits because that path has no caller to \
             refuse. Any non-zero rate needs an operator to read the audit \
             log for the partition it names.",
            self.break_glass_bypassed.clone(),
        );
    }

    pub(super) fn register_group_8(&self, registry: &mut Registry) {
        for (name, help, metric) in [
            (
                "diskless_wal_durable_watermark",
                "Quorum-durable offset for each diskless WAL shard led by this broker.",
                self.diskless_wal_durable_watermark.clone(),
            ),
            (
                "diskless_wal_index_projection_lag",
                "Durable offsets not yet represented by the committed diskless WAL object index.",
                self.diskless_wal_index_projection_lag.clone(),
            ),
            (
                "diskless_wal_trim_frontier",
                "Local log-start offset after trimming a diskless WAL shard.",
                self.diskless_wal_trim_frontier.clone(),
            ),
        ] {
            registry.register(name, help, metric);
        }
        registry.register(
            "diskless_wal_voter_lag",
            "Leader log-end offset minus each diskless WAL voter's durable offset.",
            self.diskless_wal_voter_lag.clone(),
        );
        registry.register(
            "diskless_wal_quorum_loss_events",
            "Leader-side diskless WAL acknowledgements that failed to form a quorum.",
            self.diskless_wal_quorum_loss_events_total.clone(),
        );
        registry.register(
            "diskless_wal_flush_attempts",
            "Non-empty diskless WAL objects submitted to object storage.",
            self.diskless_wal_flush_attempts_total.clone(),
        );
        registry.register(
            "diskless_wal_flush_bytes",
            "Bytes successfully written as diskless WAL objects.",
            self.diskless_wal_flush_bytes_total.clone(),
        );
        registry.register(
            "diskless_wal_flush_failures",
            "Diskless WAL object flushes that failed after an attempt began.",
            self.diskless_wal_flush_failures_total.clone(),
        );
        registry.register(
            "diskless_wal_expired_ranges",
            "Committed diskless WAL index ranges tombstoned by retention or DeleteRecords.",
            self.diskless_wal_expired_ranges_total.clone(),
        );
        registry.register(
            "diskless_wal_cold_read_hits",
            "Diskless WAL cold reads served from object storage.",
            self.diskless_wal_cold_read_hits_total.clone(),
        );
        registry.register(
            "diskless_wal_cold_read_misses",
            "Diskless WAL cold reads with no matching committed index entry.",
            self.diskless_wal_cold_read_misses_total.clone(),
        );
        registry.register(
            "diskless_wal_cold_read_errors",
            "Diskless WAL cold reads that failed while reading object storage.",
            self.diskless_wal_cold_read_errors_total.clone(),
        );
        registry.register(
            "worm_manifests_sealed",
            "WORM manifests sealed by the archive with a valid chain receipt.",
            self.worm_manifests_sealed_total.clone(),
        );
        registry.register(
            "worm_manifest_seal_failures",
            "Write-once segment copies that ended without a usable manifest.",
            self.worm_manifest_seal_failures_total.clone(),
        );
    }
}
