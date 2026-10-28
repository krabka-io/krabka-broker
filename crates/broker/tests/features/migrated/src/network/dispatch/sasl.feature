Feature: SASL dispatch diagnostics use Kafka API and principal vocabulary
  Operators can join authentication audit rows to other privileged action rows
  and recognize Kafka API names in exception text.

  Scenario Outline: An exception names Kafka API <key>
    Given a SASL dispatch exception concerns Kafka API key <key>
    When the Kafka API name is rendered for the SASL exception
    Then the exception API name is <name>

    Examples:
      | key | name |
      | 0 | PRODUCE |
      | 2 | LIST_OFFSETS |
      | 23 | OFFSET_FOR_LEADER_EPOCH |
      | 50 | DESCRIBE_USER_SCRAM_CREDENTIALS |

  Scenario: A resolved SCRAM principal joins other Kafka user audit rows
    Given user alice authenticated through SCRAM-SHA-512
    When the resolved principal is rendered for authentication audit
    Then the audit principal is User:alice with authentication method SaslScramSha512
