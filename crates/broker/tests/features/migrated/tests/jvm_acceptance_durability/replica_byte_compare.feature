@docker
Feature: Replicated record segments remain byte compatible across all brokers
  Scenario: All replicas store records with identical Kafka dump log renderings
    Given a three broker cluster hosts a replication factor three topic ready for all replica acknowledgements
    When the stock console producer appends a hundred numbered records with all replica acknowledgements
    Then the all replica producer succeeds
    When all three local logs reach a hundred records and kafka dump log reads each first segment
    Then all three existing segments dump successfully with byte identical renderings
