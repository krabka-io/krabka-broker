Feature: src ids

  Scenario Outline: tests: parse and render match kafka uuid
    Then ids tests: parse and render match kafka uuid for <example>

    Examples:
      | example                |
      | kafka cluster          |
      | kafka directory        |
      | reserved zero          |
      | reserved one           |
      | ignored low bits       |
      | rounded low bits       |
      | padded                 |
      | hyphenated is too long |
      | unicode too long       |
      | twenty four chars      |
      | empty                  |
      | short                  |
      | bad padding length     |
      | twenty one chars       |
      | invalid plus           |
      | bad padding            |
      | non ascii              |

  Scenario: tests: serde uses the kafka string
    Then ids tests: serde uses the kafka string

  Scenario: tests: random ids obey kafka generation rules
    Then ids tests: random ids obey kafka generation rules

  Scenario Outline: tests: reservations match kafka
    Then ids tests: reservations match kafka for <example>

    Examples:
      | example          |
      | zero             |
      | one              |
      | two              |
      | ninety nine      |
      | one hundred      |
      | low bits max     |
      | high bits one    |
      | high and low one |

  Scenario: tests: dash check matches kafka for every first byte
    Then ids tests: dash check matches kafka for every first byte
