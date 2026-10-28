Feature: src txn state

  Scenario Outline: tests: kafka status round trips all states
    Then txn state tests: kafka status round trips all states for <example>

    Examples:
      | example         |
      | empty           |
      | ongoing         |
      | prepare commit  |
      | prepare abort   |
      | complete commit |
      | complete abort  |
      | dead            |

  Scenario Outline: tests: kafka status values match kafka
    Then txn state tests: kafka status values match kafka for <example>

    Examples:
      | example |
      | ongoing |
      | empty   |
      | dead    |

  Scenario Outline: tests: kafka status values outside model are absent
    Then txn state tests: kafka status values outside model are absent for <example>

    Examples:
      | example             |
      | prepare epoch fence |
      | below range         |
