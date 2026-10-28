@docker
Feature: Client certificate principal mappings determine authorization for stock JVM clients
  Scenario: A JVM client certificate gains privileges through the configured DN mapping
    Given the fixture client certificate is trusted and its mapped short name is the only data listener superuser
    When the certificate authenticated stock admin creates a topic and its producer writes msg zero
    Then both privileged topic creation and record production succeed under the mapped identity
    When the certificate authenticated stock consumer reads the first record
    Then the authorized consumer succeeds and receives msg zero
