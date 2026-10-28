Feature: Sustained traffic preserves acknowledged data and bounds broker resources

  Scenario: a three broker cluster holds its resources over a soak
    Given three broker containers roll offsets and metadata every minute and run for KRABKA_SOAK_SECONDS or 180 seconds with no shorter run permitted
    And these RF3 topic configurations are applied to the soak cluster
      | topic          | partitions | key                        | value     |
      | soak-retention | 6          | cleanup.policy             | delete    |
      | soak-retention | 6          | internal.segment.bytes     | 65536     |
      | soak-retention | 6          | segment.ms                 | 5000      |
      | soak-retention | 6          | retention.ms               | 60000     |
      | soak-retention | 6          | retention.bytes            | 4194304   |
      | soak-compacted | 3          | cleanup.policy             | compact   |
      | soak-compacted | 3          | internal.segment.bytes     | 65536     |
      | soak-compacted | 3          | segment.ms                 | 5000      |
      | soak-compacted | 3          | min.cleanable.dirty.ratio   | 0.01      |
      | soak-compacted | 3          | min.compaction.lag.ms       | 0         |
      | soak-verify    | 1          | cleanup.policy             | delete    |
      | soak-verify    | 1          | segment.ms                 | 604800000 |
      | soak-verify    | 1          | retention.ms               | -1        |
      | soak-verify    | 1          | retention.bytes            | -1        |
    When 256-byte retention, 64-key compaction and verification producers run every 20ms,50ms,1s with continuous consumers while sixty resource samples are paced at least three seconds apart
    And a fresh consumer reads back the complete acknowledged verification ledger
    Then the verification ledger is nonempty and no acknowledged key is missing
    And the cleaner has no failed sweeps and cluster sweeps, compactions, rolls and retention deletions each reach at least 10
    And every broker's resource series stays within these ceilings and second-half drift bounds
      | series    | ceiling    | drift_fraction | floor    |
      | rss       | 1610612736 | 0.15           | 67108864 |
      | fds       | 4096       | 0.15           | 128      |
      | series    | 20000      | 0.10           | 200      |
      | log_dir   | 536870912  | 0.25           | 16777216 |
      | log_files | 100000     | 0.25           | 1000     |
