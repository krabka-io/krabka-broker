Feature: Compare actual client API advertisements using the same JVM tool
  Scenario: krabka advertises exactly the api catalog
    Given a PLAINTEXT Krabka client listener without a metrics receiver
    When the JVM API versions tool reads the Krabka client listener
    Then the complete advertised Krabka table equals the client API catalog

  Scenario: divergence from real kafka matches the expectation
    Given a PLAINTEXT Krabka client listener without a metrics receiver
    And a stock Kafka broker from the pinned oracle image
    When the JVM API versions tool reads the Krabka client listener
    And the same JVM API versions tool reads the stock Kafka broker
    And the advertised tables are compared with the reviewed divergence expectation
    Then every advertised API has a canonical name and the full divergence equals the reviewed report

  Scenario: the client listener prints the same api key set as kafka
    Given a PLAINTEXT Krabka client listener without a metrics receiver
    And a stock Kafka broker from the pinned oracle image
    When the JVM API versions tool reads the Krabka client listener
    And the same JVM API versions tool reads the stock Kafka broker
    Then both client listeners advertise exactly the same API key set
