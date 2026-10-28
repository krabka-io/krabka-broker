Feature: tests soak contract transitions

  Scenario Outline: tests: a reassignment drains the target onto the spare node
    Then soak contract transitions tests: a reassignment drains the target onto the spare node for <example>

    Examples:
      | example                 |
      | follower moved in place |
      | leader moved in place   |
      | not held                |
      | nowhere to go           |

  Scenario Outline: tests: a partition is on its plan in the planned order
    Then soak contract transitions tests: a partition is on its plan in the planned order for <example>

    Examples:
      | example       |
      | same order    |
      | another order |
      | not yet       |

  Scenario: tests: each issue comes from a new ca and trusts every earlier one
    Then soak contract transitions tests: each issue comes from a new ca and trusts every earlier one
