Feature: Share fetch payloads match their acquired offsets across on-disk batch boundaries

  Scenario: acquire past leading batch returns bytes
    Given a share member has a leading three-record batch available
    When the member acquires and archives offsets zero through two
    And a new single-record v0 batch is appended at offset three
    And the member fetches the record beyond the archived leading batch
    Then offset three is acquired alone and carries the decodable v0 payload

  Scenario: fragmented window records match acquired offsets
    Given a share member has three distinct single-record batches v0,v1,v2 available
    When the member acquires all three records and accepts only middle offset one
    And the member releases the outer offsets zero and two
    And the member fetches until both disjoint outer offsets are reacquired
    Then the acquired offsets are exactly zero and two and the record bytes exclude offset one's v1
