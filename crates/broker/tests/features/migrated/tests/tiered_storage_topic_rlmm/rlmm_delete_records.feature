Feature: DeleteRecords moves the global remote-log floor and frees breached segments

  Scenario: delete records puts the tiered prefix out of range and frees it
    Given a topic-backed tiered topic has 80 records, 1-KiB segments, one byte local retention and unlimited total retention
    When Fetch reads the remote offset zero before moving the global floor
    Then the first fetched record value is test-record-0
    When DeleteRecords moves partition 0 global floor to 40
    Then DeleteRecords succeeds for exactly one partition with low watermark 40
    When Fetch reads offset zero and offset forty after the deletion
    Then offset zero is OFFSET_OUT_OF_RANGE with log start -1 while offset forty remains readable
    And remote retention frees every previously archived segment breached by the floor
