Feature: A follower learns shared tier metadata before becoming the leader

  Scenario: tiered storage metadata sharing via survivor
    Given three registered brokers share a local archive and RF3 topic metadata, while an RF2 tiered data partition spans brokers 1 and 2
    When 160 acknowledged records are produced and their sealed segments are copied to the shared archive
    Then the follower evicts all sealed local segments while the original leader is still running
    When eight seconds allow the follower's metadata consumer to catch up and the original leader shuts down
    And the surviving quorum elects the follower and its metadata manager settles for three seconds
    And a consumer fetches all 160 records from the survivor starting at offset zero
    Then the survivor serves at least 160 records using metadata learned before its election
