Feature: CreatePartitions checks manual assignments against the topic and registered brokers
  Background:
    Given brokers 0 1 and 2 are registered and the topic replication factor is 2

  Scenario Outline: Invalid explicit replicas preserve Kafka placement diagnostics
    Given new partitions have explicit replica lists <lists>
    When CreatePartitions validates the explicit assignments
    Then the assignment is refused as invalid replica assignment with message <message>
    Examples:
      | lists     | message                                                                                                                                          |
      | 0,1,2     | The manual partition assignment includes a partition with 3 replica(s), but this is not consistent with previous partitions, which have 2 replica(s). |
      | 1,1       | The manual partition assignment includes the broker 1 more than once.                                                                             |
      | 0,9       | The manual partition assignment includes broker 9, but no such broker is registered.                                                              |
      | 0,-1      | The manual partition assignment includes broker -1, but no such broker is registered.                                                             |
      | 0,1;empty | The manual partition assignment includes an empty replica list.                                                                                   |
