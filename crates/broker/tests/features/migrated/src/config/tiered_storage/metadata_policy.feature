Feature: Topic metadata transport and snapshots honor configured policy

  Scenario: A default topic metadata policy is usable and follows Kafka defaults
    Given the topic metadata manager uses default settings
    When the topic metadata manager policy is validated
    Then the topic metadata manager policy is accepted
    And the topic metadata policy retains Kafka defaults for topology, snapshots, transport and security

  Scenario: Explicit valid transport and snapshot limits are accepted together
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | topic_create_timeout_ms | 45000 |
      | fetch_max_wait_ms | 750 |
      | fetch_max_bytes | 2097152 |
      | fetch_retry_backoff_ms | 300 |
      | event_queue_capacity | 2048 |
      | snapshot_interval_ms | 90000 |
      | min_isr | 1 |
    When the topic metadata manager policy is validated
    Then the topic metadata manager policy is accepted

  Scenario: Refuse fetch_max_wait_ms=0
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | fetch_max_wait_ms | 0 |
    When the topic metadata manager policy is validated
    Then the policy refusal names fetch_max_wait

  Scenario: Refuse min_isr=0
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | min_isr | 0 |
    When the topic metadata manager policy is validated
    Then the policy refusal names min_isr

  Scenario: Refuse snapshot_interval_ms=0
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | snapshot_interval_ms | 0 |
    When the topic metadata manager policy is validated
    Then the policy refusal names snapshot_interval

  Scenario: Refuse snapshot_interval_ms=infinity
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | snapshot_interval_ms | infinity |
    When the topic metadata manager policy is validated
    Then the policy refusal names snapshot_interval

  Scenario: Retain the configured snapshot location and cadence
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | bootstrap | 127.0.0.1:9092 |
      | snapshot_interval_ms | 60000 |
      | snapshot_dir | /data/remote-log-metadata |
    When the topic metadata manager policy is validated
    Then the topic metadata manager policy is accepted
    And metadata snapshots use a one minute interval and directory /data/remote-log-metadata

  Scenario: A local one-replica metadata connection does not need security
    Given the topic metadata manager has these policy overrides:
      | field | value |
      | bootstrap | 127.0.0.1:9092 |
      | num_partitions | 1 |
      | replication | 1 |
      | snapshot_dir | /data/remote-log-metadata |
    When the topic metadata manager policy is validated
    Then the metadata connection requires no security configuration
