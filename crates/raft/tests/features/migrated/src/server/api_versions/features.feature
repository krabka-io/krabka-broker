Feature: src server api versions features

  Scenario Outline: tests: supported features follow kafkas minimums and alter level zero
    Then server api versions features tests: supported features follow kafkas minimums and alter level zero for <example>

    Examples:
      | example     |
      | stable v0   |
      | stable v3   |
      | stable v4   |
      | stable v5   |
      | unstable v0 |
      | unstable v3 |
      | unstable v4 |
      | unstable v5 |
