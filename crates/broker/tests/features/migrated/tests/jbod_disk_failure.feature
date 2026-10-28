Feature: Runtime log directory failures isolate storage faults and inform the controller
  Scenario: A directory failure rejects its partitions while the healthy directory still accepts writes
    Given topic kip112-offline has six partitions spread across primary and extra log directories
    When the extra log directory goes offline
    Then the registered online directory transitions offline
    When a client produces to a partition in the extra directory
    Then the partition produce returns Kafka error code 56
    When a client produces to a partition in the primary directory
    Then the partition produce returns Kafka error code 0

  Scenario: Losing the only log directory requests broker shutdown
    Given a broker has exactly one registered log directory and its shutdown signal is observed
    When the broker's only log directory goes offline
    Then the registered online directory transitions offline
    And within fifteen seconds the broker latches its self shutdown signal

  Scenario: A controller accepts and echoes a live broker directory assignment
    Given a controller leader broker has topic kip112-assign with two partitions
    When broker one assigns partition zero to directory UUID hex cafebabe
    Then the assignment echoes a directory and reports success at both response and partition levels

  Scenario: A controller accepts an offline directory heartbeat with no alternative ISR
    Given a single broker has registered its live epoch and become controller leader
    When broker one heartbeats with offline directory UUID hex dead1234
    Then the controller accepts the offline directory heartbeat without an ISR alternative
