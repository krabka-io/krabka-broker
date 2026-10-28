@docker
Feature: SCRAM clients authenticate and exchange records over TLS
  Scenario: Stock SCRAM clients exchange records over trusted TLS
    Given alice has a SCRAM SHA 512 credential and a topic on a trusted SASL SSL broker
    When alice's encrypted console producer writes ten numbered records
    Then the SCRAM authenticated encrypted producer succeeds
    When alice's encrypted console consumer reads ten records from the beginning
    Then the SCRAM authenticated encrypted consumer succeeds and emits all ten records
