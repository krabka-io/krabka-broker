Feature: Kafka RE2J client pattern admission
  Client patterns keep Kafka's string bound before translation and its inline
  flag dialect, while quoted or character-class text never becomes a flag.

  Scenario: Quoting overhead cannot hide an oversized Kafka string
    Given a quoted RE2J pattern contains the Kafka-limit number of literal x characters
    When the broker compiles the pattern for a full Kafka match
    Then the pattern is rejected for exceeding the Kafka string limit

  Scenario: An actual unsupported inline flag keeps Kafka's error wording
    Given the client-supplied RE2J pattern is:
      """
      (?x)txn
      """
    When the broker compiles the pattern for a full Kafka match
    Then pattern compilation reports Kafka's invalid or unsupported Perl syntax message

  Scenario: Flag-shaped text inside a character class remains literal
    Given the client-supplied RE2J pattern is:
      """
      [(?x)]
      """
    When the broker scans the pattern for unsupported inline flags
    Then no unsupported inline flag is reported

  Scenario: An escaped parenthesis cannot introduce an inline flag
    Given the client-supplied RE2J pattern is:
      """
      \(?x)
      """
    When the broker scans the pattern for unsupported inline flags
    Then no unsupported inline flag is reported
