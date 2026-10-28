Feature: Replica throttle lists accept Kafka list spellings
  Whitespace, wildcard entries and empty list segments keep Kafka's meaning.

  Scenario: Whitespace selects no throttled replicas
    Given the throttled replica configuration is JSON " "
    When the broker parses the throttled replica list
    Then the throttled replica selection is empty

  Scenario: A trimmed wildcard selects every replica
    Given the throttled replica configuration is JSON " * "
    When the broker parses the throttled replica list
    Then the throttled replica selection includes every replica

  Scenario Outline: Explicit pairs retain order while ignoring list padding
    Given the throttled replica configuration is JSON <value>
    When the broker parses the throttled replica list
    Then the selected partition and broker pairs are JSON <pairs>

    Examples:
      | value | pairs |
      | " 0:1 , 2:3 " | [[0,1],[2,3]] |
      | "0:1,,1:2" | [[0,1],[1,2]] |
