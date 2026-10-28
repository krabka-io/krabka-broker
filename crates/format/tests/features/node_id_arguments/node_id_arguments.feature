Feature: Formatter node IDs follow Kafka's integer range
  Scenario Outline: A nonnegative signed integer is a valid node ID
    Given formatter node ID text <input>
    When the formatter node ID is parsed
    Then the formatter node ID equals <value>

    Examples:
      | input      | value      |
      | 7          | 7          |
      | 0          | 0          |
      | 2147483647 | 2147483647 |

  Scenario: Surrounding whitespace does not change a node ID
    Given formatter node ID 7 surrounded by spaces
    When the formatter node ID is parsed
    Then the formatter node ID equals 7

  Scenario Outline: Invalid node IDs retain Kafka's diagnostic messages
    Given formatter node ID text <input>
    When the formatter node ID is parsed
    Then the formatter node ID error is <error>

    Examples:
      | input      | error                                              |
      | 2147483648 | node id: number too large to fit in target type     |
      | -1         | You must specify a valid non-negative node ID.     |
      | 1.0        | node id: invalid digit found in string             |
      | 0x7        | node id: invalid digit found in string             |

  Scenario: An empty node ID has an explicit parsing error
    Given an empty formatter node ID
    When the formatter node ID is parsed
    Then the formatter node ID error is node id: cannot parse integer from empty string
