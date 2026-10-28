Feature: Remote storage file policy reaches broker runtime configuration

  Scenario: Kafka metadata defaults retain transport and snapshot bounds
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      [remote_storage.kafka_metadata]
      bootstrap = "127.0.0.1:9092"
      """
    When the remote storage file is parsed and applied to broker defaults
    Then the complete Kafka metadata transport policy is
      | field | value |
      | bootstrap | 127.0.0.1:9092 |
      | num_partitions | 50 |
      | replication | 3 |
      | min_isr | 2 |
      | topic_create_timeout_ms | 30000 |
      | fetch_max_wait_ms | 500 |
      | fetch_max_bytes | 1048576 |
      | fetch_retry_backoff_ms | 200 |
      | event_queue_capacity | 1024 |
      | snapshot_interval_ms | 60000 |

  Scenario: Explicit Kafka metadata settings replace every transport bound
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      [remote_storage.kafka_metadata]
      bootstrap = "broker-0:9094"
      num_partitions = 8
      replication = 1
      min_isr = 1
      topic_create_timeout = "45s"
      fetch_max_wait = "750ms"
      fetch_max_bytes = "2MiB"
      fetch_retry_backoff = "300ms"
      event_queue_capacity = 2048
      snapshot_interval = "90s"
      """
    When the remote storage file is parsed and applied to broker defaults
    Then the complete Kafka metadata transport policy is
      | field | value |
      | bootstrap | broker-0:9094 |
      | num_partitions | 8 |
      | replication | 1 |
      | min_isr | 1 |
      | topic_create_timeout_ms | 45000 |
      | fetch_max_wait_ms | 750 |
      | fetch_max_bytes | 2097152 |
      | fetch_retry_backoff_ms | 300 |
      | event_queue_capacity | 2048 |
      | snapshot_interval_ms | 90000 |

  Scenario Outline: Invalid metadata policy reports the property that failed
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      [remote_storage.kafka_metadata]
      <field> = <value>
      """
    When the remote storage file is parsed and applied to broker defaults
    Then remote policy application refuses property <field>

    Examples:
      | field | value |
      | min_isr | 0 |
      | topic_create_timeout | "0ms" |
      | topic_create_timeout | "0.5ms" |
      | topic_create_timeout | "2147483648ms" |
      | fetch_max_wait | "0ms" |
      | fetch_max_bytes | "0B" |
      | fetch_max_bytes | "0.5B" |
      | fetch_max_bytes | "2147483648B" |
      | fetch_retry_backoff | "0ms" |
      | event_queue_capacity | 0 |
      | snapshot_interval | "0s" |

  Scenario: Remote readers retain Kafka thread and cache defaults
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      """
    When the remote storage file is parsed and applied to broker defaults
    Then the remote reader has 10 threads and 100 pending tasks with 1073741824 index cache bytes

  Scenario: Sweep workers retain Kafka thread defaults
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      """
    When the remote storage file is parsed and applied to broker defaults
    Then remote copying uses 10 threads and expiration uses 10 threads

  Scenario: Explicit sweep worker counts reach broker configuration
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      copier_threads = 32
      expiration_threads = 4
      """
    When the remote storage file is parsed and applied to broker defaults
    Then remote copying uses 32 threads and expiration uses 4 threads

  Scenario: Explicit in-memory metadata opts out of topic-backed storage
    Given the remote storage TOML is
      """
      [remote_storage]
      storage_dir = "/tmp/tier"
      [remote_storage.kafka_metadata]
      in_memory = true
      """
    When the remote storage file is parsed and applied to broker defaults
    Then remote metadata is held in memory
