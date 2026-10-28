Feature: src clean shutdown tests

  Scenario: the proof holds kafkas bytes
    Then clean shutdown tests: the proof holds kafkas bytes

  Scenario Outline: decode refuses documents that are not kafkas
    Then clean shutdown tests: decode refuses documents that are not kafkas for <example>

    Examples:
      | example         |
      | bare epoch      |
      | missing version |
      | missing epoch   |
      | null epoch      |
      | junk            |
      | empty           |
