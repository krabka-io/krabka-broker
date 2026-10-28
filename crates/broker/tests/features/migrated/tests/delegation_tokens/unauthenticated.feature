Feature: Neither plaintext nor one way TLS connections can mint tokens anonymously
  Scenario Outline: unauthenticated create delegation token is refused
    Given an unauthenticated <protocol> token listener has configured super user <super_user>
    When a connection with no SASL identity or client certificate creates a token with lifetime -1
    Then the anonymous token request is refused with error 64
    Examples:
      | protocol | super_user |
      | PLAINTEXT | none |
      | PLAINTEXT | operator |
      | SSL | none |
      | SSL | operator |
