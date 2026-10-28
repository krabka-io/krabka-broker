Feature: Describe topic partitions preserves sorting authorization membership and pagination

  Scenario: named request returns listed topics with partitions
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | alpha | 2 |
      | beta | 1 |
    When the client describes topics alpha,beta with partition limit 2000
    Then alpha then beta include all partition rows and leaders without a cursor

  Scenario: fetch all returns topics in alphabetical order
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | gamma | 1 |
      | alpha | 1 |
      | beta | 1 |
    When the client describes topics all with partition limit 2000
    Then all user topics are alphabetically alpha beta gamma

  Scenario: unknown topic in named request returns error row
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | real-topic | 1 |
    When the client describes topics ghost,real-topic with partition limit 2000
    Then ghost has error 3 and no partitions while real-topic is still described

  Scenario: elr lists are empty not null for jvm 3 8 admin compatibility
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | t | 1 |
    When the client describes topics t with partition limit 2000
    Then both ELR lists are present empty lists rather than null

  Scenario: a feature downgrade empties the reported elr
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | t | 1 |
    And ELR version 1 is finalized and t partition 0 has eligible nodes 2 and 3
    When the client describes topics t with partition limit 2000
    Then t partition 0 reports eligible replicas 2 and 3 before downgrade
    When the admin safely downgrades ELR version to 0
    Then the ELR feature downgrade succeeds
    When the client retries describing t until the clearing batch is applied
    Then both ELR lists are empty and the finalized ELR feature is absent

  Scenario: topic authorized operations populated for super user
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | t | 1 |
    When the client describes topics t with partition limit 2000
    Then the topic row succeeds and carries the full authorized operations mask

  Scenario: pagination caps response at partition limit and returns next cursor
    Given an allow-all broker contains topics created in this order:
      | name | partitions |
      | big | 5 |
    When the client describes topics big with partition limit 3
    Then the first page contains three big partitions and a cursor for big partition 3
    When the client resumes from the exact returned cursor with limit 2000
    Then the resumed page contains exactly partitions 3 and 4 without another cursor
