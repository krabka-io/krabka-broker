Feature: src kafka log topic

  Scenario Outline: tests: creation configs follow kafkas metadata topic
    Then kafka log topic tests: creation configs follow kafkas metadata topic for <example>

    Examples:
      | example          |
      | defaults         |
      | min isr override |
      | compacted        |

  Scenario: tests: unset min isr leaves the cluster default
    Then kafka log topic tests: unset min isr leaves the cluster default
