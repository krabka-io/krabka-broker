Feature: Kafka producer snapshot lifecycle across log mutations
  Scenario Outline: Rolling closing truncating and resetting leave the required snapshots
    Given three two-record segments from producer 42 with sequences 0 2 and 4
    When log lifecycle action <action> is applied and the log is reopened
    Then producer snapshots remain at offsets <offsets>
    And reopening restores the complete producer state present before shutdown

    Examples:
      | case                                     | action                       | offsets |
      | an unclean shutdown keeps roll snapshots | drop                         | 2,4     |
      | a clean close adds an end snapshot       | close                        | 2,4,6   |
      | truncation drops later snapshots         | truncate-to-2                | 2       |
      | trimming drops earlier snapshots         | trim-to-2-then-truncate-to-4 | 4       |
      | a reset leaves one snapshot at the start | reset-to-10                  | 10      |
