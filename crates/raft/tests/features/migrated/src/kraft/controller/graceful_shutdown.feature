Feature: src kraft controller graceful shutdown

  Scenario Outline: tests: the window is kafkas five seconds or two and a half fetch timeouts
    Then kraft controller graceful shutdown tests: the window is kafkas five seconds or two and a half fetch timeouts for <example>

    Examples:
      | example            |
      | short test timeout |
      | kafka default      |
      | longer than kafka  |
