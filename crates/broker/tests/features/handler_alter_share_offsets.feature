Feature: Share offset resets honor feature gates and Read authorization

  Scenario: Disabled API refuses the whole reset
    Given a share offset reset broker with API enabled false and group access allowed true
    When alice resets share group g1, topic missing, partitions 0 to start offset 42
    Then the full share reset response has top-level code 35 and these unknown-topic partitions
      | topic | partitions | code | message |

  Scenario: Denied group refuses the whole reset
    Given a share offset reset broker with API enabled true and group access allowed false
    When alice resets share group g1, topic missing, partitions 0 to start offset 42
    Then the full share reset response has top-level code 30 and these unknown-topic partitions
      | topic | partitions | code | message |

  Scenario: Unknown topic preserves partition identities
    Given a share offset reset broker with API enabled true and group access allowed true
    When alice resets share group g1, topic missing-topic, partitions 3,5 to start offset 42
    Then the full share reset response has top-level code 0 and these unknown-topic partitions
      | topic | partitions | code | message |
      | missing-topic | 3,5 | 3 | This server does not host this topic-partition. |

  Scenario: Group Read alone permits offset reset
    Given alice has group Read true, group Alter false, topic Read true and acl-topic exists true
    When alice resets share group g-acl, topic acl-topic, partitions 0 to start offset 42
    Then the share reset has top-level code 0 and partition code 0

  Scenario: Group Alter without Read refuses reset and preserves stored state
    Given alice has group Read false, group Alter true, topic Read true and acl-topic exists true
    When alice resets share group g-acl, topic acl-topic, partitions 0 to start offset 42
    Then the share reset has top-level code 30 and partition code none

  Scenario: Topic Read denial preserves stored state
    Given alice has group Read true, group Alter false, topic Read false and acl-topic exists true
    When alice resets share group g-acl, topic acl-topic, partitions 0 to start offset 42
    Then the share reset has top-level code 0 and partition code 29

  Scenario: Authorized unknown topic reports missing partition
    Given alice has group Read true, group Alter false, topic Read true and acl-topic exists false
    When alice resets share group g-acl, topic acl-topic, partitions 0 to start offset 42
    Then the share reset has top-level code 0 and partition code 3
