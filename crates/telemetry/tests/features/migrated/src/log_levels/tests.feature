Feature: src log levels tests

  Scenario Outline: kafka level names round trip
    Then log levels tests: kafka level names round trip for <example>

    Examples:
      | example |
      | fatal   |
      | error   |
      | warn    |
      | info    |
      | debug   |
      | trace   |

  Scenario Outline: kafka level names reject non levels
    Then log levels tests: kafka level names reject non levels for <example>

    Examples:
      | example   |
      | lowercase |
      | off       |
      | empty     |
