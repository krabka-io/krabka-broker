Feature: Physical replica directory moves preserve partition data and are idempotent

  Scenario: Moving both partitions empties the source and repeated moves succeed
    Given a two-log-directory broker hosts topic t with 2 partitions
    When AlterReplicaLogDirs moves topic t partitions 0,1 to the opposite directory
    Then partitions 0,1 acknowledge success, DescribeLogDirs places them in the destination, the source is empty and no future directory remains
    When the client repeats the move of t partition 0 to the same destination
    Then the repeated directory move is an idempotent success for partition 0

  Scenario: A physical directory move preserves every stored record
    Given a two-log-directory broker hosts topic t with 1 partitions
    And topic t partition 0 contains the fifty records v0 through v49
    When AlterReplicaLogDirs moves topic t partitions 0 to the opposite directory
    Then partitions 0 acknowledge success, DescribeLogDirs places them in the destination, the source is empty and no future directory remains
    When a new earliest-offset consumer reads t from the destination directory
    Then the consumer receives exactly the fifty original records v0 through v49
