@docker
Feature: Delegation tokens authenticate stock Kafka producers
  Scenario: An administrator issues uses inspects and expires a delegation token
    Given three registered SASL brokers share the delegation token master key
    When the administrator creates a token with the broker default maximum lifetime
    Then the issued token has a nonempty identifier and HMAC
    When a SCRAM SHA 256 token authenticated producer writes hello to the administrator's topic
    Then the token authenticated producer succeeds
    When the administrator describes tokens owned by User admin
    Then the owner's token listing includes the issued identifier
    When the administrator expires the token immediately using its HMAC
    Then the expiration command succeeds
