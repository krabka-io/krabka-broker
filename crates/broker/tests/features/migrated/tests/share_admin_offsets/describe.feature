Feature: Share offset descriptions report start positions, lag, missing topics, and feature availability

  Scenario: describe reflects spso after consume
    Given g1's member has acquired the three records on t partition 0
    Then the described-group fixture acquired all three offsets
    When the described-group member accepts offsets 0 through 2
    And DescribeShareGroupOffsets waits for g1's start offset to reach 3
    Then g1 describes topic t successfully with start offset 3 and lag zero

  Scenario: describe unknown topic
    Given a fresh share-enabled broker for offset description
    And the description broker's share-state persister is ready for g1
    When g1's offsets for topic nonexistent partition 0 are described
    Then the unknown topic description succeeds with zero topic ID and missing start and lag

  Scenario: admin offsets rejected when share disabled
    Given a fresh share-enabled broker for offset description
    And the description broker holds topic t
    When g1's offsets for topic t partition 0 are described
    Then share offset description is available
    When share.version is safely downgraded to zero
    Then the share-version downgrade succeeds
    When g1's offsets for topic t partition 0 are described
    Then share offset description is refused with UNSUPPORTED_VERSION
