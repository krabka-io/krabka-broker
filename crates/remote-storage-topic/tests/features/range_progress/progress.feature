Feature: Bounded metadata range progress
  Scenario Outline: Batch progress determines whether to continue stop or fail
    Given a range at offset 4 ending before 10 receives next batch offset <next>
    When the range read chooses its next offset
    Then range progress is <expected>
    Examples:
      | next | expected      |
      | 5    | continue at 5 |
      | 8    | continue at 8 |
      | 10   | done          |
      | 12   | done          |
      | none | stalled       |
      | 4    | stalled       |
      | 3    | stalled       |

  Scenario Outline: Retriable server errors allow leader refresh
    Given the range fetch failed with server error <code>
    When the range reader decides whether to retry
    Then retrying the range fetch is <expected>
    Examples:
      | code | expected |
      | 6    | true     |
      | 74   | true     |
      | 75   | true     |
      | 5    | true     |
      | 9    | true     |
      | 56   | true     |
      | 78   | true     |
      | 100  | true     |
      | -1   | true     |
      | 1    | false    |
      | 2    | false    |
      | 29   | false    |
      | 35   | false    |

  Scenario Outline: Transport recovery is distinct from invalid configuration
    Given the range fetch failed with <failure>
    When the range reader decides whether to retry
    Then retrying the range fetch is <expected>
    Examples:
      | failure            | expected |
      | disconnected       | true     |
      | timeout            | true     |
      | connection reset   | true     |
      | connection refused | true     |
      | invalid config     | false    |

  Scenario: Range filtering preserves offsets keys and tombstones
    Given a fetched page has records before inside and after offsets 4 through 6 including a tombstone at 6
    When the page is filtered to partition 1 offsets 4 through 6
    Then only offset 4 and the tombstone at 6 are delivered with their original keys
