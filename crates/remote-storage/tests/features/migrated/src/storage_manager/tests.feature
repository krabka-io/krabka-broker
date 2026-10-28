Feature: src storage manager tests

  Scenario Outline: index suffixes match kafka
    Then storage manager tests: index suffixes match kafka for <example>

    Examples:
      | example           |
      | offset            |
      | timestamp         |
      | producer snapshot |
      | leader epoch      |
      | transaction       |

  Scenario: local tiered storage names match kafka
    Then storage manager tests: local tiered storage names match kafka

  Scenario: object store key components match kafka
    Then storage manager tests: object store key components match kafka

  Scenario: parses the pinned kafka names
    Then storage manager tests: parses the pinned kafka names

  Scenario Outline: decode kafka uuid rejects wrong length base64
    Then storage manager tests: decode kafka uuid rejects wrong length base64 for <example>

    Examples:
      | example      |
      | empty        |
      | short        |
      | long         |
      | invalid tail |
