Feature: tests admin handlers admin describe cluster

  Scenario: describe cluster lists brokers
    Then admin handlers admin describe cluster: describe cluster lists brokers

  Scenario: describe cluster endpoint type controllers is rejected
    Then admin handlers admin describe cluster: describe cluster endpoint type controllers is rejected
