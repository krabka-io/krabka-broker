Feature: Admin record deletion advances the readable log boundary
  Scenario: delete records truncates wal and maps outcome
    Given wal partition 0 contains 100 flushed records on a standalone broker
    When the admin deletes wal partition 0 records before offset 50
    Then DeleteRecords returns exactly one successful wal outcome with low watermark 50
    When a reader fetches wal partition 0 at offset 0
    Then the fetch fails with OFFSET_OUT_OF_RANGE error 1
