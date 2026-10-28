Feature: tests unit topic admin

  Scenario: create then delete topic round trip
    Then unit topic admin: create then delete topic round trip

  Scenario: create topic with zero partitions errors
    Then unit topic admin: create topic with zero partitions errors

  Scenario: duplicate create returns topic already exists
    Then unit topic admin: duplicate create returns topic already exists

  Scenario: create topics rf too high returns invalid replication factor
    Then unit topic admin: create topics rf too high returns invalid replication factor
