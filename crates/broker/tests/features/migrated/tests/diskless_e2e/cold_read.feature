Feature: Serve flushed diskless records and enforce object-tier deletion

  Scenario: trimmed diskless offsets are served from the object store
    Given a diskless quorum with 24 acknowledged diskless-e2e-record payloads and flush interval 100 milliseconds
    When the published flush frontier trims the leader's local committed prefix
    Then the flush frontier and high watermark are 24 and the local log start exceeds 0
    When the diskless leader fetches 24 records from offset 0 with a 90 second deadline
    Then the cold read contains exactly the produced 24 payloads at contiguous offsets from 0
    And the cold-read clients and cluster are shut down

  Scenario: deleted diskless records leave the object store unreadable
    Given a diskless quorum with 24 acknowledged diskless-e2e-record payloads and flush interval 100 milliseconds
    When the published flush frontier trims the leader's local committed prefix
    Then the flush frontier and high watermark are 24 and the local log start exceeds 4
    When the diskless leader fetches 24 records from offset 0 with a 90 second deadline
    Then the cold read contains exactly the produced 24 payloads at contiguous offsets from 0
    And the earliest diskless offset is 0
    When DeleteRecords removes diskless offsets below 4
    Then the delete low watermark is 4 and deleted fetch errors are [1,1]
    And the earliest diskless offset is 4
    When the diskless leader fetches 20 records from offset 4 with a 90 second deadline
    Then the retained cold read starts exactly at offset 4
    And the cold-read clients and cluster are shut down
