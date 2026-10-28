Feature: Share coordinator runtime bounds and complete applied policy

  Scenario: defaults
    Given the Share coordinator runtime properties are
      """
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
    And the Share coordinator load buffer has 5242880 bytes

  Scenario: snapshot cadence at its ceiling
    Given the Share coordinator runtime properties are
      """
      share_snapshot_update_records_per_snapshot = 500
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | snapshot_update_records_per_snapshot | 500 |

  Scenario: snapshot cadence of zero
    Given the Share coordinator runtime properties are
      """
      share_snapshot_update_records_per_snapshot = 0
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | snapshot_update_records_per_snapshot | 0 |

  Scenario: snapshot cadence above 500
    Given the Share coordinator runtime properties are
      """
      share_snapshot_update_records_per_snapshot = 501
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: write timeout
    Given the Share coordinator runtime properties are
      """
      share_coordinator_write_timeout = "250ms"
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | write_timeout | 250 |

  Scenario: zero write timeout
    Given the Share coordinator runtime properties are
      """
      share_coordinator_write_timeout = "0ms"
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: prune interval
    Given the Share coordinator runtime properties are
      """
      share_state_prune_interval = "1s"
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | state_topic_prune_interval | 1000 |

  Scenario: cold snapshot interval above the INT ceiling
    Given the Share coordinator runtime properties are
      """
      share_cold_partition_snapshot_interval = "2147483648ms"
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: load buffer size
    Given the Share coordinator runtime properties are
      """
      share_coordinator_load_buffer_size = "2MiB"
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
    And the Share coordinator load buffer has 2097152 bytes

  Scenario: zstd state topic codec
    Given the Share coordinator runtime properties are
      """
      share_state_compression_codec = 4
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | state_topic_compression_codec | zstd |

  Scenario: unknown state topic codec
    Given the Share coordinator runtime properties are
      """
      share_state_compression_codec = 5
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: negative state topic codec
    Given the Share coordinator runtime properties are
      """
      share_state_compression_codec = -1
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: four coordinator threads
    Given the Share coordinator runtime properties are
      """
      share_coordinator_threads = 4
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | threads | 4 |

  Scenario: zero coordinator threads
    Given the Share coordinator runtime properties are
      """
      share_coordinator_threads = 0
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: adaptive append linger
    Given the Share coordinator runtime properties are
      """
      share_coordinator_append_linger_ms = -1
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |

  Scenario: zero append linger
    Given the Share coordinator runtime properties are
      """
      share_coordinator_append_linger_ms = 0
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | append_linger | 0 |

  Scenario: append linger below -1
    Given the Share coordinator runtime properties are
      """
      share_coordinator_append_linger_ms = -2
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: cached buffer at its floor
    Given the Share coordinator runtime properties are
      """
      share_coordinator_cached_buffer_max_bytes = "512KiB"
      """
    When the broker parses and applies the Share coordinator properties
    Then the complete Share coordinator policy equals defaults except
      | field | value |
      | cached_buffer_max_bytes | 524288 |

  Scenario: cached buffer below 512KiB
    Given the Share coordinator runtime properties are
      """
      share_coordinator_cached_buffer_max_bytes = "524287B"
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused

  Scenario: cached buffer above the INT ceiling
    Given the Share coordinator runtime properties are
      """
      share_coordinator_cached_buffer_max_bytes = "2GiB"
      """
    When the broker parses and applies the Share coordinator properties
    Then the Share coordinator policy is refused
