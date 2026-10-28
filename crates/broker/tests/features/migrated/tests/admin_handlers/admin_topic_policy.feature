Feature: tests admin handlers admin topic policy

  Scenario: create topics below the replication floor is a policy violation
    Then admin handlers admin topic policy: create topics below the replication floor is a policy violation

  Scenario: a topic the policy allows is created and validate only commits nothing
    Then admin handlers admin topic policy: a topic the policy allows is created and validate only commits nothing

  Scenario: altering a topic to a forbidden config is a policy violation
    Then admin handlers admin topic policy: altering a topic to a forbidden config is a policy violation
