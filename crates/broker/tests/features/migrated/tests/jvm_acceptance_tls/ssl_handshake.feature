@docker
Feature: Stock JVM clients discover APIs through an SSL only listener
  Scenario: A JVM client discovers broker APIs over trusted TLS
    Given an SSL only broker and a JVM truststore containing its development certificate
    When the stock broker API versions tool connects through the encrypted listener
    Then TLS negotiation and API discovery complete successfully
