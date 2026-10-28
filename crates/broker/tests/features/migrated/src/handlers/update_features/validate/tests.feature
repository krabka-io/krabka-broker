Feature: src handlers update features validate tests

  Scenario Outline: rows validate as kafkas update feature
    Then handlers update features validate tests: rows validate as kafkas update feature for <example>

    Examples:
      | example                                     |
      | empty request                               |
      | repeated name keeps last row                |
      | unknown feature above zero                  |
      | unknown feature at zero                     |
      | level zero downgrade of unfinalized feature |
      | unknown upgrade type                        |
      | negative level                              |
      | above local range                           |
      | downgrade without downgrade type            |
      | downgrade type raises level                 |
      | dependency uses proposed levels             |
      | unmet dependency                            |

  Scenario Outline: kraft version rows follow kafka
    Then handlers update features validate tests: kraft version rows follow kafka for <example>

    Examples:
      | example                     |
      | upgrade                     |
      | upgrade to current          |
      | downgrade type raises level |
      | above supported range       |

  Scenario: enabling elr writes kafkas safety config records
    Then handlers update features validate tests: enabling elr writes kafkas safety config records

  Scenario: krabka version finalizes as kafka finalizes a non metadata feature
    Then handlers update features validate tests: krabka version finalizes as kafka finalizes a non metadata feature
