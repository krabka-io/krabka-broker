Feature: src file config remote storage

  Scenario: tests: kafka metadata section parses with defaults
    Then file config remote storage tests: kafka metadata section parses with defaults

  Scenario: tests: kafka metadata section honors overrides
    Then file config remote storage tests: kafka metadata section honors overrides

  Scenario Outline: tests: kafka metadata section rejects invalid policy
    Then file config remote storage tests: kafka metadata section rejects invalid policy for <example>

    Examples:
      | example                         |
      | min isr                         |
      | topic create timeout zero       |
      | topic create timeout fractional |
      | topic create timeout overflow   |
      | fetch max wait zero             |
      | fetch max bytes zero            |
      | fetch max bytes fractional      |
      | fetch max bytes overflow        |
      | fetch retry backoff zero        |
      | event queue capacity zero       |
      | snapshot interval zero          |

  Scenario: tests: reader bounds and index cache size default to kafkas values
    Then file config remote storage tests: reader bounds and index cache size default to kafkas values

  Scenario Outline: tests: sweep bounds default to kafkas thread pool sizes and are overridable
    Then file config remote storage tests: sweep bounds default to kafkas thread pool sizes and are overridable for <example>

    Examples:
      | example    |
      | defaults   |
      | overridden |

  Scenario: tests: kafka metadata in memory true opts out to in memory rlmm
    Then file config remote storage tests: kafka metadata in memory true opts out to in memory rlmm
