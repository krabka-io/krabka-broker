Feature: src connection limiter settings

  Scenario Outline: tests: overrides parse as kafka parses them
    Then connection limiter settings tests: overrides parse as kafka parses them for <example>

    Examples:
      | example                   |
      | empty                     |
      | one                       |
      | spaced                    |
      | ipv6 splits on last colon |
      | trailing comma            |
      | last duplicate wins       |
      | negative refuses all      |
      | leading comma             |
      | no colon                  |
      | not a number              |
      | bad host                  |
