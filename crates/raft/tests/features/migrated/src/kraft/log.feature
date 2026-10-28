Feature: src kraft log

  Scenario Outline: tests: logview end offset for epoch follows kafka
    Then kraft log tests: logview end offset for epoch follows kafka for <example>

    Examples:
      | example               |
      | epoch older than log  |
      | completed prior epoch |
      | between held epochs   |
      | current epoch         |
      | newer than log        |
