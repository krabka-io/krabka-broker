Feature: Runtime coordinator bounds and rack tag validation

  Scenario: defaults
    Given the runtime coordinator properties are
      """
      """
    When the broker validates and applies the coordinator properties
    Then the entire Share group policy equals Kafka defaults except
      | field | value |

  Scenario: record lock limit at its floor
    Given the runtime coordinator properties are
      """
      share_group_partition_max_record_locks = 100
      """
    When the broker validates and applies the coordinator properties
    Then the entire Share group policy equals Kafka defaults except
      | field | value |
      | max_inflight_records | 100 |

  Scenario: record lock limit below 100
    Given the runtime coordinator properties are
      """
      share_group_partition_max_record_locks = 99
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_partition_max_record_locks: must be within 100..=10000
      """

  Scenario: record lock limit above 10000
    Given the runtime coordinator properties are
      """
      share_group_partition_max_record_locks = 10001
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_partition_max_record_locks: must be within 100..=10000
      """

  Scenario: record lock limit above its maximum
    Given the runtime coordinator properties are
      """
      share_group_partition_max_record_locks = 5000
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_max_partition_max_record_locks: must be greater than or equal to share_group_partition_max_record_locks
      """

  Scenario: record lock limit raised with its maximum
    Given the runtime coordinator properties are
      """
      share_group_partition_max_record_locks = 5000
      share_group_max_partition_max_record_locks = 5000
      """
    When the broker validates and applies the coordinator properties
    Then the entire Share group policy equals Kafka defaults except
      | field | value |
      | max_inflight_records | 5000 |
      | max_partition_max_record_locks | 5000 |

  Scenario: record lock limit below its minimum
    Given the runtime coordinator properties are
      """
      share_group_partition_max_record_locks = 200
      share_group_min_partition_max_record_locks = 300
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_partition_max_record_locks: must be greater than or equal to share_group_min_partition_max_record_locks
      """

  Scenario: delivery count limit of 1
    Given the runtime coordinator properties are
      """
      share_group_delivery_count_limit = 1
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_delivery_count_limit: must be within 2..=10
      """

  Scenario: delivery count limit of 11
    Given the runtime coordinator properties are
      """
      share_group_delivery_count_limit = 11
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_delivery_count_limit: must be within 2..=10
      """

  Scenario: delivery count limit above its maximum
    Given the runtime coordinator properties are
      """
      share_group_delivery_count_limit = 8
      share_group_max_delivery_count_limit = 6
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_max_delivery_count_limit: must be greater than or equal to share_group_delivery_count_limit
      """

  Scenario: maximum delivery count limit of 25
    Given the runtime coordinator properties are
      """
      share_group_max_delivery_count_limit = 25
      """
    When the broker validates and applies the coordinator properties
    Then the entire Share group policy equals Kafka defaults except
      | field | value |
      | max_delivery_count_limit | 25 |

  Scenario: record lock duration below 1s
    Given the runtime coordinator properties are
      """
      share_group_record_lock_duration = "999ms"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_record_lock_duration: must be within 1000ms..=3600000ms
      """

  Scenario: record lock duration above its maximum
    Given the runtime coordinator properties are
      """
      share_group_record_lock_duration = "90s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_max_record_lock_duration: must be greater than or equal to share_group_record_lock_duration
      """

  Scenario: record lock duration below its minimum
    Given the runtime coordinator properties are
      """
      share_group_record_lock_duration = "10s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_record_lock_duration: must be greater than or equal to share_group_min_record_lock_duration
      """

  Scenario: record lock duration and its minimum lowered
    Given the runtime coordinator properties are
      """
      share_group_record_lock_duration = "10s"
      share_group_min_record_lock_duration = "5s"
      """
    When the broker validates and applies the coordinator properties
    Then the entire Share group policy equals Kafka defaults except
      | field | value |
      | record_lock_duration | 10000 |
      | min_record_lock_duration | 5000 |

  Scenario: minimum record lock duration above 30s
    Given the runtime coordinator properties are
      """
      share_group_min_record_lock_duration = "31s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_min_record_lock_duration: must be within 1000ms..=30000ms
      """

  Scenario: session timeout above its maximum
    Given the runtime coordinator properties are
      """
      share_group_session_timeout = "61s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_session_timeout: must be less than or equal to share_group_max_session_timeout
      """

  Scenario: session bounds widened
    Given the runtime coordinator properties are
      """
      share_group_min_session_timeout = "10s"
      share_group_max_session_timeout = "2min"
      share_group_session_timeout = "90s"
      """
    When the broker validates and applies the coordinator properties
    Then the entire Share group policy equals Kafka defaults except
      | field | value |
      | min_session_timeout | 10000 |
      | max_session_timeout | 120000 |
      | session_timeout | 90000 |

  Scenario: heartbeat interval below its minimum
    Given the runtime coordinator properties are
      """
      share_group_heartbeat_interval = "4s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_heartbeat_interval: must be greater than or equal to share_group_min_heartbeat_interval
      """

  Scenario: heartbeat bounds inverted
    Given the runtime coordinator properties are
      """
      share_group_min_heartbeat_interval = "20s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_max_heartbeat_interval: must be greater than or equal to share_group_min_heartbeat_interval
      """

  Scenario: heartbeat interval not below the session timeout
    Given the runtime coordinator properties are
      """
      share_group_min_session_timeout = "10s"
      share_group_session_timeout = "15s"
      share_group_heartbeat_interval = "15s"
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_heartbeat_interval: must be less than share_group_session_timeout
      """

  Scenario: share group of 1001 members
    Given the runtime coordinator properties are
      """
      share_group_max_size = 1001
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: share_group_max_size: must be within 1..=1000
      """

  Scenario: Rack tag parsing for []
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = []
      """
    When the broker validates and applies the coordinator properties
    Then the Streams rack tags are exactly []

  Scenario: Rack tag parsing for [""]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = [""]
      """
    When the broker validates and applies the coordinator properties
    Then the Streams rack tags are exactly []

  Scenario: Rack tag parsing for ["zone"]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = ["zone"]
      """
    When the broker validates and applies the coordinator properties
    Then the Streams rack tags are exactly ["zone"]

  Scenario: Rack tag parsing for [" zone ", "rack"]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = [" zone ", "rack"]
      """
    When the broker validates and applies the coordinator properties
    Then the Streams rack tags are exactly ["zone", "rack"]

  Scenario: Rack tag parsing for ["zone,rack"]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = ["zone,rack"]
      """
    When the broker validates and applies the coordinator properties
    Then the Streams rack tags are exactly ["zone", "rack"]

  Scenario: Rack tag parsing for ["zone", ""]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = ["zone", ""]
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: streams_group_rack_aware_assignment_tags: Configuration 'group.streams.rack.aware.assignment.tags' values must not be empty.
      """

  Scenario: Rack tag parsing for ["zone", "rack", "zone"]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = ["zone", "rack", "zone"]
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: streams_group_rack_aware_assignment_tags: group.streams.rack.aware.assignment.tags must not contain duplicate tag keys.
      """

  Scenario: Rack tag parsing for ["zone", "zone", ""]
    Given the runtime coordinator properties are
      """
      streams_group_rack_aware_assignment_tags = ["zone", "zone", ""]
      """
    When the broker validates and applies the coordinator properties
    Then coordinator properties are refused with
      """
      invalid config: streams_group_rack_aware_assignment_tags: Configuration 'group.streams.rack.aware.assignment.tags' values must not be empty.
      """
