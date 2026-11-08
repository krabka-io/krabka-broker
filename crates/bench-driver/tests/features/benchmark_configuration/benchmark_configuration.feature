Feature: Operator benchmark configuration and recorded measurements
  Scenario Outline: Broker pod selectors distinguish the benchmark stacks
    Given benchmark stack <stack>
    When the benchmark selects broker pods
    Then the broker pod selector is <selector>

    Examples:
      | case           | stack  | selector     |
      | Krabka brokers | Krabka | ^demo-broker |
      | Kafka brokers  | Kafka  | ^demo-kafka- |

  Scenario Outline: Acknowledgement settings select the matching producer contract
    Given benchmark acknowledgements <acks>
    When benchmark acknowledgements configure the producer
    Then the producer waits for <replicas> replicas

    Examples:
      | case                               | acks   | replicas |
      | no acknowledgement                 | none   | zero     |
      | the leader alone acknowledges      | leader | one      |
      | every in-sync replica acknowledges | all    | all      |

  Scenario Outline: Compression settings select the matching producer codec
    Given benchmark compression <codec>
    When benchmark compression configures the producer
    Then the producer compression codec is <codec>

    Examples:
      | case             | codec  |
      | no compression   | none   |
      | the gzip codec   | gzip   |
      | the snappy codec | snappy |
      | the lz4 codec    | lz4    |
      | the zstd codec   | zstd   |

  Scenario: Omitting compression leaves the producer uncompressed
    Given benchmark compression is omitted
    When benchmark compression configures the producer
    Then the benchmark compression default is none
    And the producer compression codec is none

  Scenario: Operators can specify sizes and timings with units
    Given benchmark scenario YAML
      """
      name: small-msg-saturate
      mode_tag: ci
      msg_size: 100B
      partitions: 6
      replication_factor: 1
      producers: 1
      consumers: 1
      mode:
        kind: saturate
      acks: leader
      compression: none
      linger: 5ms
      batch_size: 16KiB
      duration: 60s
      warmup: 10s
      """
    When the operator benchmark document is loaded
    Then small-msg-saturate uses six partitions of 100-byte records
    And the benchmark uses the standard batch and measurement timings

  Scenario: A minimal benchmark receives runnable defaults
    Given benchmark scenario YAML
      """
      name: bare
      mode:
        kind: saturate
      """
    When the operator benchmark document is loaded
    Then omitted benchmark fields use 1-KiB messages and no failover
    And the benchmark uses the standard batch and measurement timings

  Scenario: A nonzero message size needs a unit
    Given a saturation benchmark with msg_size set to 100
    When the operator benchmark document is loaded
    Then the benchmark is refused with missing unit

  Scenario Outline: Unrunnable benchmark magnitudes are rejected before execution
    Given a saturation benchmark with <field> set to <value>
    When the operator benchmark document is loaded
    Then the benchmark is refused with <requirement>

    Examples:
      | case                           | field               | value  | requirement               |
      | a negative message size        | msg_size            | -1B    | a positive size           |
      | a zero message size            | msg_size            | 0      | a positive size           |
      | a negative key size            | key_size            | -1B    | a size of zero or more    |
      | a negative batch size          | batch_size          | -16KiB | a positive size           |
      | a zero batch size              | batch_size          | 0      | a positive size           |
      | a negative linger              | linger              | -5ms   | an extent of zero or more |
      | a negative duration            | duration            | -60s   | a positive extent         |
      | a zero duration                | duration            | 0      | a positive extent         |
      | a negative warmup              | warmup              | -10s   | an extent of zero or more |
      | a negative failover kill delay | failover.kill_after | -4s    | an extent of zero or more |

  Scenario Outline: A paced producer requires a positive event rate
    Given a fixed-rate benchmark paced at <rate>
    When the operator benchmark document is loaded
    Then the benchmark is refused with a positive rate

    Examples:
      | case            | rate |
      | a negative rate | -1/s |
      | a zero rate     | 0    |

  Scenario: Zero-valued options remain valid when they describe runnable work
    Given benchmark scenario YAML
      """
      name: bounds
      mode:
        kind: saturate
      key_size: 0
      linger: 0
      warmup: 0
      failover:
        kill_after: 0
      """
    When the operator benchmark document is loaded
    Then zero disables keys linger and warmup while scheduling an immediate kill

  Scenario: A fixed event rate is read from the operator document
    Given benchmark scenario YAML
      """
      name: fixed-rate
      mode:
        kind: fixed_rate
        rate: 20000/s
      """
    When the operator benchmark document is loaded
    Then the benchmark produces 20000 events per second

  Scenario: Failover timing selects the partition-zero leader by default
    Given benchmark scenario YAML
      """
      name: failover
      mode:
        kind: saturate
      failover:
        kill_after: 4s
      """
    When the operator benchmark document is loaded
    Then the partition-zero leader is killed four seconds into the benchmark

  Scenario: Saving a benchmark retains its human-readable units
    Given a benchmark with explicit sizes timings and a fixed event rate
    When the benchmark is saved to YAML and loaded again
    Then the benchmark keeps its values and displays explicit units

  Scenario: A run document can be saved without changing its measurements
    Given a benchmark run with distinctive throughput latency and resource measurements
    When the benchmark run is saved to JSON and loaded again
    Then the benchmark run retains every recorded value

  Scenario: Run JSON retains exact integer measurements
    Given a benchmark run with distinctive throughput latency and resource measurements
    When the benchmark run is saved to JSON and loaded again
    Then measurements retain exact base-unit integers in JSON

  Scenario: Missing optional measurements remain missing after reload
    Given a benchmark run without JVM memory page-cache and startup measurements
    When the benchmark run is saved to JSON and loaded again
    Then unmeasured optional fields stay absent from the run document
    And the benchmark run retains every recorded value
