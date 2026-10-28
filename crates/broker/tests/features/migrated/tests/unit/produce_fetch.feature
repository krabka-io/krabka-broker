Feature: tests unit produce fetch

  Scenario: produce assigns base offsets
    Then unit produce fetch: produce assigns base offsets

  Scenario: produce without a topic id returns unknown topic id
    Then unit produce fetch: produce without a topic id returns unknown topic id

  Scenario: produce then fetch round trip
    Then unit produce fetch: produce then fetch round trip

  Scenario: list offsets earliest and latest
    Then unit produce fetch: list offsets earliest and latest
