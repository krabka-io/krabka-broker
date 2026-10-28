@docker
Feature: Administrators manage SCRAM credentials with stock Kafka tools
  Scenario: Administrators inspect a provisioned SCRAM credential
    Given a three broker SASL cluster accepts the administrator's PLAIN identity
    When the administrator provisions alice with SCRAM SHA 512 and 4096 iterations
    Then the credential alteration succeeds
    When the administrator describes alice's credentials with the stock config tool
    Then the credential report includes SCRAM SHA 512
