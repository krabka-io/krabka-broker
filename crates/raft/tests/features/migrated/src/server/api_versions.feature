Feature: src server api versions

  Scenario Outline: tests: an api kafka 4 3 1 lacks follows unstable api versions enable
    Then server api versions tests: an api kafka 4 3 1 lacks follows unstable api versions enable for <example>

    Examples:
      | example  |
      | disabled |
      | enabled  |

  Scenario: tests: api versions response runs kafka request checks
    Then server api versions tests: api versions response runs kafka request checks

  Scenario: tests: feature fields match kafka on the controller listener
    Then server api versions tests: feature fields match kafka on the controller listener

  Scenario Outline: tests: a fresh controller advertises kafkas minimum metadata version
    Then server api versions tests: a fresh controller advertises kafkas minimum metadata version for <example>

    Examples:
      | example         |
      | kraft version 0 |
      | kraft version 1 |
