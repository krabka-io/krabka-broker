Feature: Committed Fetch and share visibility agree across physical local-to-remote eviction

  Scenario: read committed reads of a transactional range agree across local eviction
    Given a tiered partition has 1-KiB segments and retains all local segments
    And five interleaved commit-abort rounds each write three two-record batches and one plain record, followed by eight abort batches, two open batches and two plain tail records
    When the remote tier copies every segment eligible below the open transaction's LSO
    Then the tier holds a-last data while its abort marker stays local and local start remains zero
    When Fetch and a fresh committed share group read the range before local eviction
    Then both pre-eviction readers see exactly committed and plain records below the open LSO with complete abort metadata
    When all copied prefix segments are evicted from local disk
    And Fetch and a different committed share group read the same range after eviction
    Then post-eviction visibility matches both original readers and most returned records are remote-only
    When the open transaction commits and both readers continue beyond the old LSO
    Then the new LSO includes the committed open transaction and plain tail while reporting the late abort
