Feature: Offset administration resets empty share groups and fences live members

  Scenario: alter resets empty group
    Given an empty share group g1 with 6 records on t partition 0
    When an administrator resets empty g1 to offset 5, retrying while its persister settles
    Then both the group and partition offset-reset responses succeed
    When DescribeShareGroupOffsets observes the reset start offset
    Then the described share start offset is 5
    When a member joins reset g1 and acquires its first available records
    Then the member first acquires offset 5

  Scenario: alter non empty group fenced
    Given an empty share group g1 with 3 records on t partition 0
    And a live share member has joined g1 and initialized its state
    When an administrator tries to reset occupied g1 to offset 1
    Then the offset reset is refused at group level with NON_EMPTY_GROUP
