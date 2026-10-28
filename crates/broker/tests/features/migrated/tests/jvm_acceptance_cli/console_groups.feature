Feature: tests jvm acceptance cli console groups

  Scenario: console consumer with group round trip
    Then jvm acceptance cli console groups: console consumer with group round trip

  Scenario: console consumer with static membership
    Then jvm acceptance cli console groups: console consumer with static membership

  Scenario: cooperative sticky kafka console consumer
    Then jvm acceptance cli console groups: cooperative sticky kafka console consumer
