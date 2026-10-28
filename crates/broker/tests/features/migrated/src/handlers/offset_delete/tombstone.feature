Feature: src handlers offset delete tombstone

  Scenario Outline: tests: write errors map as kafka handle operation exception
    Then handlers offset delete tombstone tests: write errors map as kafka handle operation exception for <example>

    Examples:
      | example               |
      | network exception     |
      | unknown topic         |
      | corrupt message       |
      | not enough replicas   |
      | request timed out     |
      | not leader            |
      | kafka storage error   |
      | message too large     |
      | record list too large |
      | unknown server error  |
