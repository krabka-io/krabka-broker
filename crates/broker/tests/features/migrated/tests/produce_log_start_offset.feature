Feature: Produce responses report the partition's actual log start offset
  Scenario: An appended record reports the real trimmed start of its partition
    Given the orders partition contains four acknowledged records
    When DeleteRecords trims the partition to offset three
    Then the trim acknowledges low watermark three
    When a nonidempotent record is appended to the trimmed partition
    Then the accepted append reports base offset four and log start offset three

  Scenario: An idempotent retry reports the same trimmed start without appending
    Given the orders partition contains four acknowledged records
    When DeleteRecords trims the partition to offset three
    Then the trim acknowledges low watermark three
    When producer 777 appends its first record to the trimmed partition
    Then the accepted append reports base offset four and log start offset three
    When producer 777 retries the identical epoch and sequence
    Then the retry preserves the assigned offset timestamp and trimmed log start
