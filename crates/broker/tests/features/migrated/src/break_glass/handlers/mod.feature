Feature: Break-glass Kafka principal identity
  Approval and audit identities use Kafka's typed principal string instead of
  dropping the User prefix from an authenticated operator's name.

  Scenario: An authenticated operator keeps Kafka's User principal prefix
    Given a break-glass request is authenticated as principal alice
    When the handler derives the request's Kafka principal string
    Then the break-glass principal string is User:alice
