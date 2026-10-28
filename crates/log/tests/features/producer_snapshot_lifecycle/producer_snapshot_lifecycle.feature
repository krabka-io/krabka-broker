Feature: Kafka producer snapshot lifecycle across log mutations
  Scenario Outline: Rolling closing truncating and resetting leave the required snapshots
    Given three two-record segments from producer 42 with sequences 0 2 and 4
    When log lifecycle action <action> is applied and the log is reopened
    Then producer snapshots remain at offsets <offsets>
    And reopening restores the complete producer state present before shutdown

    Examples:
      | action                      | offsets |
      | drop                        | 2,4     |
      | close                       | 2,4,6   |
      | truncate-to-2               | 2       |
      | trim-to-2-then-truncate-to-4 | 4       |
      | reset-to-10                 | 10      |
