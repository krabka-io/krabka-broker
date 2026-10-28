Feature: src format quorum

  Scenario: tests: parses initial controller spec with a kafka directory id
    Then format quorum tests: parses initial controller spec with a kafka directory id

  Scenario Outline: tests: a kraft version level out of range is kafkas no feature error
    Then format quorum tests: a kraft version level out of range is kafkas no feature error for <example>

    Examples:
      | example              |
      | below range          |
      | unsupported in range |
      | above range          |

  Scenario Outline: tests: the voter endpoints carry the controller listener name
    Then format quorum tests: the voter endpoints carry the controller listener name for <example>

    Examples:
      | example                            |
      | default listener name              |
      | explicit uppercase listener name   |
      | normalized lowercase listener name |
      | explicit quorum default name       |
