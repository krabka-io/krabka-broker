Feature: Kafka Envelope failure codes
  A forwarding failure retains the Kafka code its own controller would return.

  Scenario Outline: Envelope failures retain their exact Kafka wire error code
    Given the Envelope failure is <failure>
    When the Envelope failure is translated to its Kafka wire code
    Then the Envelope wire error code is <code>

    Examples:
      | failure                         | code |
      | ClusterAuthorizationFailed      | 31   |
      | PrincipalDeserializationFailure | 97   |
      | UnsupportedVersion              | 35   |
      | InvalidRequest                  | 42   |
