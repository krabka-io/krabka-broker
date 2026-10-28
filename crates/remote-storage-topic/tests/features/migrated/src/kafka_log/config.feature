Feature: src kafka log config

  Scenario: tests: config defaults match kafka
    Then kafka log config tests: config defaults match kafka

  Scenario: tests: client tunables convert to their kafka wire images
    Then kafka log config tests: client tunables convert to their kafka wire images

  Scenario: tests: config accepts custom transport policy
    Then kafka log config tests: config accepts custom transport policy

  Scenario Outline: tests: config rejects invalid transport policy
    Then kafka log config tests: config rejects invalid transport policy for <example>

    Examples:
      | example                       |
      | create timeout zero           |
      | create timeout submillisecond |
      | create timeout infinite       |
      | create timeout too large      |
      | fetch wait zero               |
      | fetch wait submillisecond     |
      | fetch wait infinite           |
      | fetch wait too large          |
      | fetch bytes zero              |
      | fetch bytes fractional        |
      | fetch bytes infinite          |
      | fetch bytes too large         |
      | retry backoff zero            |
      | retry backoff infinite        |

  Scenario Outline: tests: min isr must be at least one
    Then kafka log config tests: min isr must be at least one for <example>

    Examples:
      | example                    |
      | one                        |
      | two                        |
      | zero                       |
      | negative                   |
      | unset uses cluster default |

  Scenario: tests: metadata event queue capacity rejects zero
    Then kafka log config tests: metadata event queue capacity rejects zero

  Scenario: tests: config carries client resource policy
    Then kafka log config tests: config carries client resource policy

  Scenario: tests: config carries security
    Then kafka log config tests: config carries security
