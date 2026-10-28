Feature: Share offset description preserves feature, ACL and unknown-topic response shape

  Scenario: Disabled API retains every requested group
    Given a share offsets broker with API enabled false and group access allowed true
    When DescribeShareGroupOffsets requests these groups and partitions
      | group | topic | partitions |
      | g1 | t1 | 0 |
      | g2 | t2 | 1 |
    Then the complete share offset response contains these group rows
      | group | code | topic | partitions |
      | g1 | 35 | none | none |
      | g2 | 35 | none | none |

  Scenario: Group ACL denial preserves its identity
    Given a share offsets broker with API enabled true and group access allowed false
    When DescribeShareGroupOffsets requests these groups and partitions
      | group | topic | partitions |
      | g1 | missing | 0 |
    Then the complete share offset response contains these group rows
      | group | code | topic | partitions |
      | g1 | 30 | none | none |

  Scenario: Missing topics report uninitialized data without errors
    Given a share offsets broker with API enabled true and group access allowed true
    When DescribeShareGroupOffsets requests these groups and partitions
      | group | topic | partitions |
      | g1 | missing-topic | 3,5 |
    Then the complete share offset response contains these group rows
      | group | code | topic | partitions |
      | g1 | 0 | missing-topic | 3,5 |
