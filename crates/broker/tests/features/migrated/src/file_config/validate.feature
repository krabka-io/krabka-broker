Feature: Metadata log file properties retain Kafka byte and millisecond domains

  Scenario: metadata_log_segment_bytes = 8MiB
    Given the runtime metadata property is
      """
      metadata_log_segment_bytes = "8MiB"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with segment_size_bytes set to 8388608

  Scenario: metadata_log_segment_bytes = 2147483647B
    Given the runtime metadata property is
      """
      metadata_log_segment_bytes = "2147483647B"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with segment_size_bytes set to 2147483647

  Scenario: metadata_log_segment_bytes = 8388607B
    Given the runtime metadata property is
      """
      metadata_log_segment_bytes = "8388607B"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_bytes with a must constraint

  Scenario: metadata_log_segment_bytes = 0B
    Given the runtime metadata property is
      """
      metadata_log_segment_bytes = "0B"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_bytes with a must constraint

  Scenario: metadata_log_segment_bytes = 2147483648B
    Given the runtime metadata property is
      """
      metadata_log_segment_bytes = "2147483648B"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_bytes with a must constraint

  Scenario: metadata_log_segment_bytes = 8388608.5B
    Given the runtime metadata property is
      """
      metadata_log_segment_bytes = "8388608.5B"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_bytes with a must constraint

  Scenario: metadata_log_segment_roll_interval = 1ms
    Given the runtime metadata property is
      """
      metadata_log_segment_roll_interval = "1ms"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with segment_roll_interval_ms set to 1

  Scenario: metadata_log_segment_roll_interval = 7d
    Given the runtime metadata property is
      """
      metadata_log_segment_roll_interval = "7d"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with segment_roll_interval_ms set to 604800000

  Scenario: metadata_log_segment_roll_interval = 0ms
    Given the runtime metadata property is
      """
      metadata_log_segment_roll_interval = "0ms"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_roll_interval with a must constraint

  Scenario: metadata_log_segment_roll_interval = 250us
    Given the runtime metadata property is
      """
      metadata_log_segment_roll_interval = "250us"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_roll_interval with a must constraint

  Scenario: metadata_max_retention_bytes = 0B
    Given the runtime metadata property is
      """
      metadata_max_retention_bytes = "0B"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_retention_size_bytes set to 0

  Scenario: metadata_max_retention_bytes = 1GiB
    Given the runtime metadata property is
      """
      metadata_max_retention_bytes = "1GiB"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_retention_size_bytes set to 1073741824

  Scenario: metadata_max_retention_bytes = 1.5B
    Given the runtime metadata property is
      """
      metadata_max_retention_bytes = "1.5B"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_retention_bytes with a must constraint

  Scenario: metadata_max_retention_bytes = 9223372036854775808B
    Given the runtime metadata property is
      """
      metadata_max_retention_bytes = "9223372036854775808B"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_retention_bytes with a must constraint

  Scenario: metadata_max_retention = 0ms
    Given the runtime metadata property is
      """
      metadata_max_retention = "0ms"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_retention_ms set to 0

  Scenario: metadata_max_retention = 30d
    Given the runtime metadata property is
      """
      metadata_max_retention = "30d"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_retention_ms set to 2592000000

  Scenario: metadata_max_retention = 250us
    Given the runtime metadata property is
      """
      metadata_max_retention = "250us"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_retention with a must constraint

  Scenario: metadata_max_retention = 1.5ms
    Given the runtime metadata property is
      """
      metadata_max_retention = "1.5ms"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_retention with a must constraint

  Scenario: metadata_max_idle_interval = 0ms
    Given the runtime metadata property is
      """
      metadata_max_idle_interval = "0ms"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_idle_interval_ms set to 0

  Scenario: metadata_max_idle_interval = 500ms
    Given the runtime metadata property is
      """
      metadata_max_idle_interval = "500ms"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_idle_interval_ms set to 500

  Scenario: metadata_max_idle_interval = 2147483647ms
    Given the runtime metadata property is
      """
      metadata_max_idle_interval = "2147483647ms"
      """
    When the broker validates and applies the metadata property
    Then the entire metadata log policy equals defaults with max_idle_interval_ms set to 2147483647

  Scenario: metadata_max_idle_interval = 2147483648ms
    Given the runtime metadata property is
      """
      metadata_max_idle_interval = "2147483648ms"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_idle_interval with a must constraint

  Scenario: metadata_max_idle_interval = 250us
    Given the runtime metadata property is
      """
      metadata_max_idle_interval = "250us"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_idle_interval with a must constraint

  Scenario: metadata_max_idle_interval = -1ms
    Given the runtime metadata property is
      """
      metadata_max_idle_interval = "-1ms"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_idle_interval with a must constraint

  Scenario: metadata_max_retention = -1ms
    Given the runtime metadata property is
      """
      metadata_max_retention = "-1ms"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_max_retention with a must constraint

  Scenario: metadata_log_segment_roll_interval = -1ms
    Given the runtime metadata property is
      """
      metadata_log_segment_roll_interval = "-1ms"
      """
    When the broker validates and applies the metadata property
    Then metadata policy refuses metadata_log_segment_roll_interval with a must constraint
