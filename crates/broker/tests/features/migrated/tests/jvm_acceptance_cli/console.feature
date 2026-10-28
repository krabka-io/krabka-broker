Feature: tests jvm acceptance cli console

  Scenario: console producer round trip
    Then jvm acceptance cli console: console producer round trip

  Scenario: rust producer to console consumer
    Then jvm acceptance cli console: rust producer to console consumer

  Scenario: console consumer prints log append time
    Then jvm acceptance cli console: console consumer prints log append time
