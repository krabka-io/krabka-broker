Feature: Replication fetch requests separate response and per-partition budgets
  Kafka's default response budget leaves room for several partitions so a full
  batch from one partition does not consume the entire leader response.

  Scenario: Build the initial follower fetch with Kafka's default byte limits
    Given a follower replicates a partition with default replication fetch limits
    When the follower builds its initial partition fetch request
    Then the fetch response budget is 10485760 bytes and its partition budget is 1048576 bytes
