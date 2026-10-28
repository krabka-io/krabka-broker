Feature: Restored aborted-transaction index validity
  Abort markers are strictly ordered and belong to the owning segment. Transaction
  starts may precede a segment roll but cannot follow their marker or be negative.

  Scenario: A single abort belongs to its segment
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 7 | 100 | 102 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: true

  Scenario: Sequential aborts remain in marker order
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 7 | 100 | 102 |
      | 8 | 103 | 104 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: true

  Scenario: Interleaved transactions are ordered by abort marker rather than transaction start
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 2 | 105 | 108 |
      | 1 | 100 | 110 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: true

  Scenario: A transaction may begin before its segment rolls
    Given the restored segment spans offsets 3 through 4
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 1000 | 0 | 3 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: true

  Scenario: Regressing abort markers invalidate an index
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 1 | 100 | 110 |
      | 2 | 105 | 108 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false

  Scenario: Repeated abort markers invalidate an index
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 1 | 100 | 104 |
      | 2 | 101 | 104 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false

  Scenario: An abort marker before the segment is invalid
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 7 | 90 | 99 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false

  Scenario: An abort marker beyond the segment is invalid
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 7 | 100 | 111 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false

  Scenario: A transaction cannot start after its own abort marker
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 7 | 105 | 104 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false

  Scenario: A negative transaction start is invalid
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | 7 | -1 | 104 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false

  Scenario: A negative producer identity is invalid
    Given the restored segment spans offsets 100 through 110
    And the archived transaction index contains these entries in order:
      | producer_id | transaction_start | abort_marker |
      | -1 | 100 | 102 |
    When the restored transaction index is validated in marker order
    Then the restored transaction index is valid: false
