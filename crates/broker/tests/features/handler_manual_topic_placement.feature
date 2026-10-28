Feature: Explicit topic replica assignment validation

  Scenario Outline: Reject invalid manual assignments with actionable Kafka diagnostics
    Given a topic with numPartitions <partitions>, replication factor <factor> and explicit partition replicas <replicas>
    When manual placement validates against registered brokers 1 and 2
    Then manual placement refuses with error <code> and message "<message>"

    Examples:
      | partitions | factor | replicas | code | message |
      | -1 | 2 | 0:1,2 | 42 | A manual partition assignment was specified, but replication factor was not set to -1. |
      | 1 | -1 | 0:1,2 | 42 | A manual partition assignment was specified, but numPartitions was not set to -1. |
      | -1 | -1 | 0:1,2;0:2,1 | 39 | Found multiple manual partition assignments for partition 0 |
      | -1 | -1 | 0: | 39 | The manual partition assignment includes an empty replica list. |
      | -1 | -1 | 0:9,1,7 | 39 | The manual partition assignment includes broker 7, but no such broker is registered. |
      | -1 | -1 | 0:2,1,2 | 39 | The manual partition assignment includes the broker 2 more than once. |
      | -1 | -1 | 0:1,2;1:1 | 39 | The manual partition assignment includes a partition with 1 replica(s), but this is not consistent with previous partitions, which have 2 replica(s). |
      | -1 | -1 | 0:1;2:2 | 39 | partitions should be a consecutive 0-based integer sequence |
