Feature: src handlers fetch throttle

  Scenario Outline: tests: an empty fetch response is charged kafkas size
    Then handlers fetch throttle tests: an empty fetch response is charged kafkas size for <example>

    Examples:
      | example |
      | v0      |
      | v1      |
      | v4      |
      | v7      |
      | v12     |
