@docker
Feature: JVM authentication across record-production and consumption workflows
  Scenario: The stock PLAIN client creates produces and consumes under its configured identity
    Given PLAIN user alice has password wonderland on topic krabka-sasl-plain-itest
    When the authenticated JVM tool creates its topic
    Then authenticated topic creation succeeds
    When the authenticated console producer writes msg-0 through msg-9
    Then the authenticated producer exits successfully
    When the authenticated console consumer reads ten records
    Then the authenticated consumer succeeds and returns msg-0 through msg-9

  Scenario: The unsecured OAUTHBEARER client uses its token principal for the record workflow
    Given an unsecured OAUTHBEARER token identifies admin on topic krabka-sasl-oauthbearer-itest
    When the authenticated JVM tool creates its topic
    Then authenticated topic creation succeeds
    When the authenticated console producer writes msg-0 through msg-9
    Then the authenticated producer exits successfully
    When the authenticated console consumer reads ten records
    Then the authenticated consumer succeeds and returns msg-0 through msg-9

  Scenario: Provisioned SCRAM-SHA-512 credentials authenticate a complete JVM record round trip
    Given SCRAM-SHA-512 credentials for alice permit Read and Write on topic krabka-sasl-scram-itest
    When the authenticated console producer writes msg-0 through msg-9
    Then the authenticated producer exits successfully
    When the authenticated console consumer reads ten records
    Then the authenticated consumer succeeds and returns msg-0 through msg-9

  Scenario: Provisioned SCRAM-SHA-256 credentials authenticate a complete JVM record round trip
    Given SCRAM-SHA-256 credentials for alice permit Read and Write on topic krabka-sasl-scram256-itest
    When the authenticated console producer writes msg-0 through msg-9
    Then the authenticated producer exits successfully
    When the authenticated console consumer reads ten records
    Then the authenticated consumer succeeds and returns msg-0 through msg-9

  Scenario: The JVM producer continues writing across multiple SCRAM reauthentication windows
    Given a SCRAM-SHA-512 writer has a two-second reauthentication window on topic krabka-sasl-scram-reauth-itest
    When the JVM verifiable producer sends ten records at one record per second
    Then all ten records are acknowledged without a send error across reauthentication windows
