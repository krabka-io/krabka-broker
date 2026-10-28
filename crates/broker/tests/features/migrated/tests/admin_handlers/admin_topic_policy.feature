Feature: Enforcing operator topic policy on creates and changes

  Scenario Outline: create topics below the replication floor is a policy violation
    Given topic policy requires at least 3 replicas
    When the operator creates topic policy-rf with 1 replica and validate-only <validate_only>
    Then topic creation is refused with POLICY_VIOLATION naming replication factor 1 and floor 3
    And no topic named policy-rf is committed

    Examples:
      | validate_only |
      | false         |
      | true          |

  Scenario: a topic the policy allows is created and validate only commits nothing
    Given topic policy permits 1 replica and at most 4 partitions
    When the operator creates topic policy-dry-run with 1 replica and validate-only true
    Then topic creation succeeds
    And no topic named policy-dry-run is committed
    When the operator creates topic policy-ok with 1 replica and validate-only false
    Then topic creation succeeds
    And topic policy-ok has one partition and is committed

  Scenario: altering a topic to a forbidden config is a policy violation
    Given topic policy forbids unclean.leader.election.enable=true
    And topic policy-alter already exists with one partition
    When the operator sets policy-alter unclean.leader.election.enable to true
    Then the configuration change is refused with POLICY_VIOLATION naming the forbidden key
    When the operator sets policy-alter unclean.leader.election.enable to false
    Then the configuration change succeeds
