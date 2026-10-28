Feature: Forwarded Kafka principal admission
  KIP-590 forwarding requires a readable version-zero principal. Null, empty,
  unknown-version, and truncated principals are never mistaken for an identity.

  Scenario: A null forwarded principal is refused
    Given the Envelope request principal is null
    When the controller deserializes the forwarded Kafka principal
    Then the forwarded principal is rejected as PrincipalDeserializationFailure

  Scenario: An empty serialized principal is refused
    Given the serialized Envelope principal has these hexadecimal bytes:
      """
      """
    When the controller deserializes the forwarded Kafka principal
    Then the forwarded principal is rejected as PrincipalDeserializationFailure

  Scenario: A one-byte version prefix is refused
    Given the serialized Envelope principal has these hexadecimal bytes:
      """
      00
      """
    When the controller deserializes the forwarded Kafka principal
    Then the forwarded principal is rejected as PrincipalDeserializationFailure

  Scenario: An unsupported principal schema version is refused
    Given the serialized Envelope principal has these hexadecimal bytes:
      """
      00 01 05 55 73 65 72 06 61 6c 69 63 65 00 00
      """
    When the controller deserializes the forwarded Kafka principal
    Then the forwarded principal is rejected as PrincipalDeserializationFailure

  Scenario: A principal body truncated in its first string is refused
    Given the serialized Envelope principal has these hexadecimal bytes:
      """
      00 00 05 55
      """
    When the controller deserializes the forwarded Kafka principal
    Then the forwarded principal is rejected as PrincipalDeserializationFailure
