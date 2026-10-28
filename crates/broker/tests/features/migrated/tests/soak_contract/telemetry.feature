Feature: Soak telemetry retains units node identity and controller progress
  Scenario Outline: tests: a size reads in bytes
    Given Docker memory usage field is <text>
    When the telemetry parser converts the memory field to bytes
    Then the parsed memory byte reading is <bytes>

    Examples:
      | example         | text           | bytes      |
      | mebibytes       | "512.3MiB"     | 537185484  |
      | gibibytes       | "1.5GiB"       | 1610612736 |
      | kibibytes       | "4KiB"         | 4096       |
      | kilobytes       | "12kB"         | 12000      |
      | upper kilobytes | "1.2KB"        | 1200       |
      | megabytes       | "3.25MB"       | 3250000    |
      | bytes           | "0B"           | 0          |
      | bare number     | "17"           | 17         |
      | padded          | " 2GiB "       | 2147483648 |
      | no number       | "MiB"          | absent     |
      | unknown unit    | "3 parsecs"    | absent     |
      | two points      | "1.2.3MiB"     | absent     |
      | placeholder     | "--"           | absent     |

  Scenario Outline: tests: a percentage reads as a number
    Given Docker CPU field is <text>
    When the telemetry parser converts the CPU percentage
    Then the parsed CPU percentage is <percent>

    Examples:
      | example       | text      | percent |
      | plain         | "12.34%"  | 12.34   |
      | over one core | "250.00%" | 250     |
      | zero          | "0.00%"   | 0       |
      | placeholder   | "--"      | absent  |
      | no sign       | "12.34"   | absent  |

  Scenario: tests: docker stats reads by container name
    Given Docker stats output is:
      """
      {"BlockIO":"0B / 0B","CPUPerc":"12.34%","Container":"k1","ID":"a","MemPerc":"3.2%","MemUsage":"512MiB / 15.6GiB","Name":"k1","NetIO":"1kB / 2kB","PIDs":"40"}
      {"CPUPerc":"--","MemUsage":"-- / --","Name":"k2"}
      not json
      """
    When the telemetry parser reads stats by container name
    Then k1 has CPU 12.34 percent and 512 MiB while k2 has unavailable readings

  Scenario: tests: a body reads as controller progress
    Given the broker progress scrape is:
      """
      # HELP krabka_broker_active_controller 1 if this broker is the raft (controller) leader.
      # TYPE krabka_broker_active_controller gauge
      krabka_broker_active_controller 1
      krabka_broker_controller_leader_changes_total 3
      krabka_broker_raft_current_state{state="candidate"} 0
      krabka_broker_raft_current_state{state="leader"} 1
      krabka_broker_raft_current_epoch 7
      krabka_broker_raft_high_watermark 1.2e3
      krabka_broker_metadata_last_applied_offset 1199
      krabka_broker_metadata_last_applied_offset_extra 5
      krabka_broker_metadata_last_committed_offset 1200
      krabka_broker_metadata_last_applied_timestamp_ms 1700000000000
      krabka_broker_metadata_last_applied_lag_ms 480
      krabka_broker_metadata_lag_records 1
      krabka_broker_under_replicated_partitions 2
      krabka_broker_under_min_isr_partition_count 0
      krabka_broker_offline_partitions_count 0
      """
    When the telemetry parser reads controller progress
    Then controller progress retains the role epoch offsets lag and partition health

  Scenario: tests: a body without the series reads as unknown
    Given the scrape contains only a comment and no broker series
    When the telemetry parser reads controller progress
    Then all controller progress fields are unknown

  Scenario Outline: tests: a gauge is its first sample
    Given gauge exposition is <body>
    When the telemetry parser reads the first sample of gauge m
    Then the gauge reading is <value>

    Examples:
      | example      | body           | value  |
      | integer      | "m 42"         | 42     |
      | negative     | "m -1"         | -1     |
      | float        | "m 2.6"        | 3      |
      | labelled     | "m{a="b"} 9"   | 9      |
      | prefix only  | "m_other 9"    | absent |
      | comment      | "# m 9"        | absent |
      | not a number | "m NaN"        | absent |

  Scenario: An absent gauge has no reading
    Given the gauge exposition is empty
    When the telemetry parser reads the first sample of gauge m
    Then the gauge reading is absent

  Scenario Outline: tests: the raft state is the label that reads one
    Given raft leader gauge is <leader> and follower gauge is <follower>
    When the telemetry parser selects the raft state label whose value is one
    Then the observed raft state is <state>

    Examples:
      | example  | leader | follower | state    |
      | follower | 0      | 1        | follower |
      | none set | 0      | absent   | absent   |

  Scenario: A broker controller gauge does not identify the raft state
    Given the scrape only reports an inactive broker controller gauge
    When the telemetry parser selects the raft state label whose value is one
    Then the observed raft state is absent

  Scenario: tests: rows fold into peaks gaps and controller changes
    Given node telemetry observations are:
      | tick | node | memory | disk | lag | changes | controller | answered |
      | 0    | 1    | 100    | 10   | 0   | 4       | true       | true     |
      | 0    | 2    | 200    | 20   | 2   | 1       | false      | true     |
      | 1    | 1    | absent | 15   | 0   | 0       | false      | false    |
      | 1    | 2    | 150    | 30   | 5   | 2       | true       | true     |
      | 2    | 1    | 50     | 12   | 1   | 1       | false      | true     |
      | 2    | 2    | 180    | 25   | 0   | 2       | true       | true     |
    When the telemetry observations are summarized
    Then the summary records peaks gaps counter resets and the controller handoff

  Scenario: tests: lag before the first record is not counted
    Given the first metadata sample has no applied record despite a lag reading
    When the telemetry observations are summarized
    Then pre-record metadata lag is excluded from the summary
