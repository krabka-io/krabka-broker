Feature: tests admin handlers admin alter configs

  Scenario: alter configs round trip
    Then admin handlers admin alter configs: alter configs round trip

  Scenario: alter configs rejects unknown key
    Then admin handlers admin alter configs: alter configs rejects unknown key

  Scenario: min insync replicas is clamped to the replica count for acks all
    Then admin handlers admin alter configs: min insync replicas is clamped to the replica count for acks all
