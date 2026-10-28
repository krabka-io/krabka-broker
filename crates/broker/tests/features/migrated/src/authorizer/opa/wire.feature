Feature: OPA policy actions use Kafka ACL operation names

  Scenario Outline: Render Kafka <operation> for a policy
    Given the OPA authorization action is Kafka operation <operation>
    When the operation is rendered for an OPA Kafka plugin policy
    Then the policy operation name is <name>

    Examples:
      | operation | name |
      | Read | READ |
      | Write | WRITE |
      | TwoPhaseCommit | TWO_PHASE_COMMIT |
