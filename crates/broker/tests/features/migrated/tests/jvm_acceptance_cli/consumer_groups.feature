Feature: tests jvm acceptance cli consumer groups

  Scenario: kafka consumer groups list describe
    Then jvm acceptance cli consumer groups: kafka consumer groups list describe

  Scenario: kafka consumer groups delete offsets
    Then jvm acceptance cli consumer groups: kafka consumer groups delete offsets

  Scenario: reset offsets modes match apache kafka
    Then jvm acceptance cli consumer groups: reset offsets modes match apache kafka

  Scenario: group delete and reset refusals match apache kafka
    Then jvm acceptance cli consumer groups: group delete and reset refusals match apache kafka
