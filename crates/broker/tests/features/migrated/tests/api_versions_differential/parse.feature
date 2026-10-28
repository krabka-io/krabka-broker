Feature: tests api versions differential parse

  Scenario: tests: reads every row kind of a single broker block
    Then api versions differential parse tests: reads every row kind of a single broker block

  Scenario: tests: advertised keeps the version ranges
    Then api versions differential parse tests: advertised keeps the version ranges

  Scenario: tests: rows come back sorted by api key
    Then api versions differential parse tests: rows come back sorted by api key

  Scenario: tests: rejects outputs that are not one broker
    Then api versions differential parse tests: rejects outputs that are not one broker

  Scenario: tests: rejects a row whose range does not parse
    Then api versions differential parse tests: rejects a row whose range does not parse
