Feature: SCRAM stored credentials enforce Kafka's iteration floor after authorization identity
  Both SCRAM algorithms refuse credentials below 4096 iterations, while checking
  authorization identity before exposing credential strength failures.

  Scenario Outline: Refuse a stored weak credential
    Given the <mechanism> credential store contains weak at 4095 iterations and floor at 4096
    And the SCRAM client authenticates user weak with authorization id unset
    When the SCRAM client completes every round the broker permits using the stored password
    Then the first SCRAM round refuses the credential with generic authentication text

    Examples:
      | mechanism |
      | SCRAM-SHA-256 |
      | SCRAM-SHA-512 |

  Scenario Outline: Authenticate a credential at Kafka's iteration floor
    Given the <mechanism> credential store contains weak at 4095 iterations and floor at 4096
    And the SCRAM client authenticates user floor with authorization id unset
    When the SCRAM client completes every round the broker permits using the stored password
    Then the floor user is authenticated through the configured SCRAM mechanism without a token

    Examples:
      | mechanism |
      | SCRAM-SHA-256 |
      | SCRAM-SHA-512 |

  Scenario Outline: Authorization identity mismatch takes precedence over weak iterations
    Given the <mechanism> credential store contains weak at 4095 iterations and floor at 4096
    And the SCRAM client authenticates user weak with authorization id mallory
    When the SCRAM client completes every round the broker permits using the stored password
    Then the first SCRAM round refuses the authorization id with Authentication failed: Client requested an authorization id that is different from username

    Examples:
      | mechanism |
      | SCRAM-SHA-256 |
      | SCRAM-SHA-512 |
