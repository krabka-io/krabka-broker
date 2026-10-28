Feature: src retention tests barrier cut expiry is exact and fails closed at extremes

  Scenario Outline: barrier cut expiry is exact and fails closed at extremes
    Then retention tests barrier cut expiry is exact and fails closed at extremes: barrier cut expiry is exact and fails closed at extremes for <example>

    Examples:
      | example                |
      | before expiry          |
      | at expiry              |
      | after expiry           |
      | no retained cuts       |
      | negative retained cuts |
      | minimum published      |
      | minimum successor      |
      | maximum published      |

  Scenario: local prefix matches kafkas delete old segments passes
    Then retention tests barrier cut expiry is exact and fails closed at extremes: local prefix matches kafkas delete old segments passes

  Scenario: remote prefix matches kafkas remote log retention handler
    Then retention tests barrier cut expiry is exact and fails closed at extremes: remote prefix matches kafkas remote log retention handler
