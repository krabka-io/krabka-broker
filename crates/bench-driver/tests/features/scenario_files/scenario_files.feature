Feature: Operator benchmark scenario documents
  Scenario Outline: A shipped scenario describes a runnable benchmark
    Given benchmark scenario <filename>
    When the benchmark YAML is loaded
    And every loaded benchmark is serialized and reparsed
    Then every benchmark is runnable
    And every benchmark round trips through YAML

    Examples:
      | filename |
      | datarate-512k.yaml |
      | endurance.yaml |
      | failover.yaml |
      | fan-out.yaml |
      | firehose.yaml |
      | fixed-rate-latency.yaml |
      | high-partition-fanout.yaml |
      | high-partition-latency.yaml |
      | high-partition-saturate.yaml |
      | large-msg.yaml |
      | mixed-acks.yaml |
      | small-msg-saturate.yaml |

  Scenario: Newly checked in scenarios join the validated corpus automatically
    Given all checked in benchmark scenarios
    When the benchmark YAML is loaded
    And every loaded benchmark is serialized and reparsed
    Then every benchmark is runnable
    And every benchmark round trips through YAML
