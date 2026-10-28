Feature: Admin discovers consumer groups and their committed positions
  Scenario: lists groups and committed offsets
    Given t1 has one record v and an Earliest consumer in group g1
    When group g1 polls its first record within 30 seconds
    Then exactly t1 partition 0 offset 0 with value v was consumed
    When group g1 commits its consumed position synchronously
    And the admin lists consumer groups across the cluster
    Then the group listing contains g1
    When the admin reads g1 committed offsets
    Then g1 has committed next offset 1 for t1 partition 0
