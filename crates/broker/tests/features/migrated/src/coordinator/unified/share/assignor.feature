Feature: Share assignment balances concrete subscriptions eligible partitions and prior ownership
  Scenario: Kafka Share assignment 1 no members
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 3 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
  Scenario: Kafka Share assignment 2 no subscribed topic
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 3 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [] | {} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {} |
  Scenario: Kafka Share assignment 3 three single-partition topics balance over three members
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 1 | {} |
      | 2 | 1 | {} |
      | 3 | 1 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1,2,3] | {} |
      | B | absent | [1,2,3] | {} |
      | C | absent | [1,2,3] | {} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[0]} |
      | B | {"2":[0]} |
      | C | {"3":[0]} |
  Scenario: Kafka Share assignment 4 more members than partitions share
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 1 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1] | {} |
      | B | absent | [1] | {} |
      | C | absent | [1] | {} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[0]} |
      | B | {"1":[0]} |
      | C | {"1":[0]} |
  Scenario: Kafka Share assignment 5 a joining member takes partitions and the rest stay
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 6 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1] | {"1":[0,1,2]} |
      | B | absent | [1] | {"1":[3,4,5]} |
      | C | absent | [1] | {} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[1,2]} |
      | B | {"1":[4,5]} |
      | C | {"1":[0,3]} |
  Scenario: Kafka Share assignment 6 an unsubscribed topic leaves the assignment
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 1 | {} |
      | 2 | 1 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [2] | {"1":[0],"2":[0]} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"2":[0]} |
  Scenario: Kafka Share assignment 7 only assignable partitions are assigned
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 3 | {} |
      | 3 | 2 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1,3] | {} |
      | B | absent | [1,3] | {} |
    And the Share assignable partition set is {"1":[0,2]}
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[0]} |
      | B | {"1":[2]} |
  Scenario: Kafka Share assignment 8 Kafka's heterogeneous case with a non-assignable topic
    Given the Share subscription builder is heterogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 3 | {} |
      | 2 | 3 | {} |
      | 3 | 2 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1,2] | {} |
      | B | absent | [3] | {} |
      | C | absent | [2,3] | {} |
    And the Share assignable partition set is {"1":[0,1,2],"2":[0,1,2]}
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[0,1,2],"2":[0,2]} |
      | B | {} |
      | C | {"2":[1]} |
  Scenario: Kafka Share assignment 9 a heterogeneous group balances each topic by itself
    Given the Share subscription builder is heterogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 2 | {} |
      | 2 | 2 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1] | {} |
      | B | absent | [2] | {} |
      | C | absent | [1,2] | {} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[0]} |
      | B | {"2":[0]} |
      | C | {"1":[1],"2":[1]} |
  Scenario: Kafka Share assignment 10 a repeated member counts twice, as in Kafka (homogeneous)
    Given the Share subscription builder is homogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 2 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1] | {"1":[0]} |
      | B | absent | [1] | {"1":[0]} |
      | C | absent | [1] | {"1":[0]} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[1]} |
      | B | {"1":[0]} |
      | C | {"1":[0]} |
  Scenario: Kafka Share assignment 11 a repeated member counts twice, as in Kafka (heterogeneous)
    Given the Share subscription builder is heterogeneous
    And the Share topic partitions and replica racks are
      | topic | partitions | replica_racks |
      | 1 | 2 | {} |
      | 2 | 1 | {} |
    And the Share members have these subscriptions and previous assignments
      | member | rack | subscriptions | previous |
      | A | absent | [1] | {"1":[0]} |
      | B | absent | [1] | {"1":[0]} |
      | C | absent | [1,2] | {"1":[0]} |
    And the Share assignable partition set is null
    When the Share simple assignor balances eligible partitions and retains prior ownership
    Then the complete Share assignment is
      | member | partitions |
      | A | {"1":[1]} |
      | B | {"1":[0]} |
      | C | {"1":[0],"2":[0]} |
