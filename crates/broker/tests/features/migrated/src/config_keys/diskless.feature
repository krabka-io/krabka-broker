Feature: src config keys diskless

  Scenario Outline: tests: resolve diskless reads a kafka boolean
    Then config keys diskless tests: resolve diskless reads a kafka boolean for <example>

    Examples:
      | example         |
      | no override map |
      | missing key     |
      | false value     |
      | uppercase true  |
      | true value      |
