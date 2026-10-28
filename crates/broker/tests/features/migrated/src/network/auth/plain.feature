Feature: src network auth plain

  Scenario: tests: every plain refusal answers kafkas message
    Then network auth plain tests: every plain refusal answers kafkas message

  Scenario Outline: tests: malformed bytes match what kafka decodes them to
    Then network auth plain tests: malformed bytes match what kafka decodes them to for <example>

    Examples:
      | example  |
      | username |
      | password |
      | authzid  |
