Feature: src kafka log

  Scenario: tests: metadata requests name only the metadata topic without auto creation
    Then kafka log tests: metadata requests name only the metadata topic without auto creation

  Scenario: tests: open read only rejects invalid policy before connecting
    Then kafka log tests: open read only rejects invalid policy before connecting

  Scenario Outline: tests: bootstrap resolution names the address it could not use
    Then kafka log tests: bootstrap resolution names the address it could not use for <example>

    Examples:
      | example         |
      | empty           |
      | separators only |
      | missing port    |

  Scenario: tests: bootstrap resolution keeps valid addresses in order
    Then kafka log tests: bootstrap resolution keeps valid addresses in order

  Scenario: tests: start rejects invalid policy before connecting
    Then kafka log tests: start rejects invalid policy before connecting

  Scenario: tests: keyed tombstone record is partitioned and preserves the null value
    Then kafka log tests: keyed tombstone record is partitioned and preserves the null value

  Scenario Outline: tests: keyed record rejects out of range partitions
    Then kafka log tests: keyed record rejects out of range partitions for <example>

    Examples:
      | example     |
      | negative    |
      | upper bound |
