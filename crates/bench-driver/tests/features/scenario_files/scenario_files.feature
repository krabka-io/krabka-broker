Feature: Operator benchmark scenarios

  Scenario: Load datarate-512k.yaml
    Given benchmark scenario datarate-512k.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load endurance.yaml
    Given benchmark scenario endurance.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load failover.yaml
    Given benchmark scenario failover.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load fan-out.yaml
    Given benchmark scenario fan-out.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load firehose.yaml
    Given benchmark scenario firehose.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load fixed-rate-latency.yaml
    Given benchmark scenario fixed-rate-latency.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load high-partition-fanout.yaml
    Given benchmark scenario high-partition-fanout.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load high-partition-latency.yaml
    Given benchmark scenario high-partition-latency.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load high-partition-saturate.yaml
    Given benchmark scenario high-partition-saturate.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load large-msg.yaml
    Given benchmark scenario large-msg.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load mixed-acks.yaml
    Given benchmark scenario mixed-acks.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Load small-msg-saturate.yaml
    Given benchmark scenario small-msg-saturate.yaml
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML

  Scenario: Every checked in scenario remains covered
    Given all checked in benchmark scenarios
    When the benchmark YAML is loaded
    Then every benchmark is runnable
    And every benchmark round trips through YAML
