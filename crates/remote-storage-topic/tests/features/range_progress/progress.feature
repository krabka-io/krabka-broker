Feature: Bounded metadata range progress
  Scenario Outline: Batch progress determines whether to continue stop or fail
    Given a range at offset 4 ending before 10 receives next batch offset <next>
    When the range read chooses its next offset
    Then range progress is <expected>

    Examples:
      | case                        | next | expected      |
      | one batch of progress       | 5    | continue at 5 |
      | several batches of progress | 8    | continue at 8 |
      | exactly the range end       | 10   | done          |
      | past the range end          | 12   | done          |
      | no next batch               | none | stalled       |
      | no progress                 | 4    | stalled       |
      | a step backwards            | 3    | stalled       |

  Scenario Outline: Retriable server errors allow leader refresh
    Given the range fetch failed with server error <code>
    When the range reader decides whether to retry
    Then retrying the range fetch is <expected>

    Examples:
      | case                       | code | expected |
      | not leader or follower     | 6    | true     |
      | fenced leader epoch        | 74   | true     |
      | unknown leader epoch       | 75   | true     |
      | leader not available       | 5    | true     |
      | replica not available      | 9    | true     |
      | Kafka storage error        | 56   | true     |
      | offset not available       | 78   | true     |
      | unknown topic ID           | 100  | true     |
      | unknown server error       | -1   | true     |
      | offset out of range        | 1    | false    |
      | corrupt message            | 2    | false    |
      | topic authorization failed | 29   | false    |
      | unsupported version        | 35   | false    |

  Scenario Outline: Transport recovery is distinct from invalid configuration
    Given the range fetch failed with <failure>
    When the range reader decides whether to retry
    Then retrying the range fetch is <expected>

    Examples:
      | case                 | failure            | expected |
      | a dropped connection | disconnected       | true     |
      | a request timeout    | timeout            | true     |
      | a reset connection   | connection reset   | true     |
      | a refused connection | connection refused | true     |
      | an invalid client    | invalid config     | false    |

  Scenario: Range filtering preserves offsets keys and tombstones
    Given a fetched page has records before inside and after offsets 4 through 6 including a tombstone at 6
    When the page is filtered to partition 1 offsets 4 through 6
    Then only offset 4 and the tombstone at 6 are delivered with their original keys
