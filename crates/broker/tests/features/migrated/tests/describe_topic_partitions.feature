Feature: tests describe topic partitions

  Scenario: named request returns listed topics with partitions
    Then describe topic partitions: named request returns listed topics with partitions

  Scenario: fetch all returns topics in alphabetical order
    Then describe topic partitions: fetch all returns topics in alphabetical order

  Scenario: unknown topic in named request returns error row
    Then describe topic partitions: unknown topic in named request returns error row

  Scenario: elr lists are empty not null for jvm 3 8 admin compatibility
    Then describe topic partitions: elr lists are empty not null for jvm 3 8 admin compatibility

  Scenario: a feature downgrade empties the reported elr
    Then describe topic partitions: a feature downgrade empties the reported elr

  Scenario: topic authorized operations populated for super user
    Then describe topic partitions: topic authorized operations populated for super user

  Scenario: pagination caps response at partition limit and returns next cursor
    Then describe topic partitions: pagination caps response at partition limit and returns next cursor
