Feature: src segment dump log tests

  Scenario Outline: the append paths write the indexes kafka dump log verifies
    Then segment dump log tests: the append paths write the indexes kafka dump log verifies for <example>

    Examples:
      | example         |
      | encoding append |
      | verbatim append |
