Feature: The formatter verifies the cluster and node of existing log directories
  Scenario: No formatted directories leaves the cluster unknown
    Given the formatter expects cluster discover and node 1
    And the formatter finds these formatted directories
      | path | cluster | node | directory |
    When the formatter verifies the existing directory ensemble
    Then the formatter ensemble cluster is unknown

  Scenario: A configured cluster ID remains available without formatted directories
    Given the formatter expects cluster 193 and node 1
    And the formatter finds these formatted directories
      | path | cluster | node | directory |
    When the formatter verifies the existing directory ensemble
    Then the formatter ensemble cluster is 193

  Scenario: Existing directories supply one consistent cluster ID
    Given the formatter expects cluster discover and node 1
    And the formatter finds these formatted directories
      | path | cluster | node | directory |
      | /a   | 193     | 1    | 4096      |
      | /b   | 193     | 1    | 8192      |
    When the formatter verifies the existing directory ensemble
    Then the formatter ensemble cluster is 193

  Scenario: A configured cluster cannot claim another cluster's directory
    Given the formatter expects cluster 194 and node 1
    And the formatter finds these formatted directories
      | path | cluster | node | directory |
      | /a   | 193     | 1    | 4096      |
    When the formatter verifies the existing directory ensemble
    Then the formatter refuses a different cluster ID

  Scenario: A node cannot claim another node's directory
    Given the formatter expects cluster 193 and node 1
    And the formatter finds these formatted directories
      | path | cluster | node | directory |
      | /a   | 193     | 2    | 4096      |
    When the formatter verifies the existing directory ensemble
    Then the formatter refuses another node's log directory
