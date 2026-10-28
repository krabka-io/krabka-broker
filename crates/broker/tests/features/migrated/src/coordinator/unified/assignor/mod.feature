Feature: Consumer subscription shape chooses the Kafka uniform assignment builder
  Scenario: No members
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
    When the coordinator classifies the group subscription shape
    Then the group uses the homogeneous assignment builder
  Scenario: Members without subscriptions
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | none | absent | none |
      | none | absent | none |
    When the coordinator classifies the group subscription shape
    Then the group uses the homogeneous assignment builder
  Scenario: Identical names
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | a,b | absent | none |
      | a,b | absent | none |
    When the coordinator classifies the group subscription shape
    Then the group uses the homogeneous assignment builder
  Scenario: Different names
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | a,b | absent | none |
      | a | absent | none |
    When the coordinator classifies the group subscription shape
    Then the group uses the heterogeneous assignment builder
  Scenario: Missing topics still count in named subscriptions
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | a,b | absent | none |
      | a,b,missing | absent | none |
    When the coordinator classifies the group subscription shape
    Then the group uses the heterogeneous assignment builder
  Scenario: One shared expression
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | none | t.* | a,b |
      | none | t.* | a,b |
    When the coordinator classifies the group subscription shape
    Then the group uses the homogeneous assignment builder
  Scenario: An unresolved shared expression
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | none | t.* | none |
      | none | t.* | none |
    When the coordinator classifies the group subscription shape
    Then the group uses the homogeneous assignment builder
  Scenario: Distinct expressions resolving to identical topics
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | none | t.* | a,b |
      | none | [ab] | a,b |
    When the coordinator classifies the group subscription shape
    Then the group uses the heterogeneous assignment builder
  Scenario: An expression and identical named topics
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | none | t.* | a,b |
      | a,b | absent | none |
    When the coordinator classifies the group subscription shape
    Then the group uses the heterogeneous assignment builder
  Scenario: A shared expression with an additional named topic
    Given consumer members subscribe by names and regular expressions
      | names | expression | resolved |
      | a | t.* | a,b |
      | none | t.* | a,b |
    When the coordinator classifies the group subscription shape
    Then the group uses the heterogeneous assignment builder
