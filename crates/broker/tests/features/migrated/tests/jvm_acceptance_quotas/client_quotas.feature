@docker
Feature: Stock Kafka quota administration round trips through committed metadata
  Scenario: A user producer byte quota is visible and can be removed from committed metadata
    Given three registered SASL brokers accept the administrator and these extra PLAIN users
      | username | password |
      | alice | alice-secret |
    And the operator addresses users entity alice recorded as metadata entity type user
    When the administrator sets quota producer_byte_rate to 1024
    Then the quota alteration command succeeds
    When the administrator describes the addressed quota entity
    Then the successful quota report includes producer_byte_rate=1024
    When the administrator deletes the configured quota key
    Then the deletion succeeds and the committed metadata image no longer contains that quota

  Scenario: An IP connection creation quota is visible and can be removed from committed metadata
    Given three registered SASL brokers accept the administrator and these extra PLAIN users
      | username | password |
    And the operator addresses ips entity 127.0.0.1 recorded as metadata entity type ip
    When the administrator sets quota connection_creation_rate to 2.0
    Then the quota alteration command succeeds
    When the administrator describes the addressed quota entity
    Then the successful quota report includes connection_creation_rate=2
    When the administrator deletes the configured quota key
    Then the deletion succeeds and the committed metadata image no longer contains that quota

  Scenario: A user controller mutation quota is visible and can be removed from committed metadata
    Given three registered SASL brokers accept the administrator and these extra PLAIN users
      | username | password |
      | alice | alice-secret |
    And the operator addresses users entity alice recorded as metadata entity type user
    When the administrator sets quota controller_mutation_rate to 2.0
    Then the quota alteration command succeeds
    When the administrator describes the addressed quota entity
    Then the successful quota report includes controller_mutation_rate=2
    When the administrator deletes the configured quota key
    Then the deletion succeeds and the committed metadata image no longer contains that quota
