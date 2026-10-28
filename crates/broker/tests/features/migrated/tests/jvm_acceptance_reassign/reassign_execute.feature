Feature: Reassignment plan generation, execution and extension
  Scenario: jvm kafka reassign partitions end to end
    Given a replicated SASL topic krabka-reassign-itest contains record before-reassignment
    When the JVM executes a replica move retaining the leader and adding the unassigned broker
    Then the added replica catches up with real records, the target assignment completes, and the removed replica directory is pruned
    When the JVM verifies the executed reassignment plan
    Then the verification output reports complete

  Scenario: reassign partitions generate produces a usable plan on both
    Given Kafka and Krabka each have single-replica topic krabka-generate-itest with 3 partitions
    When the JVM generates reassignment plans using broker list 1 on both brokers
    Then both current and proposed plans cover every topic partition exactly with one distinct replica from broker 1

  Scenario: reassign partitions additional keeps the reassignment already running
    Given topic krabka-additional-itest has two partitions on broker 1 and partition 0 is moving to an offline registered target
    When the JVM executes a no-op assignment for partition 1 with additional
    Then the additional command succeeds and partition 0 still has adding replicas
