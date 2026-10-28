Feature: Restored record coordinates and rewritten batch headers
  Timestamp type controls how record deltas are interpreted. Rewritten data and
  control batches must retain legal offsets and complete Kafka producer identities.

  Scenario: Create-time records add the producer timestamp delta
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | create-time | 100 | 110 |
    When a record with offset delta 1 and timestamp delta -5 is restored
    Then the restored record has offset 11 and timestamp 95

  Scenario: Log-append-time records use the batch maximum timestamp
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | log-append-time | 2000 | 1000 |
    When a record with offset delta 1 and timestamp delta 50 is restored
    Then the restored record has offset 11 and timestamp 1000

  Scenario: Log-append-time ignores producer timestamp-delta overflow
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | log-append-time | 9223372036854775807 | 1000 |
    When a record with offset delta 0 and timestamp delta 1 is restored
    Then the restored record has offset 10 and timestamp 1000

  Scenario: Create-time timestamp overflow above maximum is rejected
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | create-time | 9223372036854775807 | 9223372036854775807 |
    When a record with offset delta 0 and timestamp delta 1 is restored
    Then the restored record coordinates are rejected

  Scenario: Create-time timestamp overflow below minimum is rejected
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | create-time | -9223372036854775808 | 9223372036854775807 |
    When a record with offset delta 0 and timestamp delta -1 is restored
    Then the restored record coordinates are rejected

  Scenario: Create-time timestamp may exactly reach the minimum
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | create-time | -9223372036854775803 | 9223372036854775807 |
    When a record with offset delta 0 and timestamp delta -5 is restored
    Then the restored record has offset 10 and timestamp -9223372036854775808

  Scenario: Negative record offset deltas are rejected
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | create-time | 100 | 110 |
    When a record with offset delta -1 and timestamp delta 0 is restored
    Then the restored record coordinates are rejected

  Scenario: Record offsets beyond the batch span are rejected
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | 4 | create-time | 100 | 110 |
    When a record with offset delta 5 and timestamp delta 0 is restored
    Then the restored record coordinates are rejected

  Scenario: Negative batch spans are rejected
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 10 | -1 | create-time | 100 | 110 |
    When a record with offset delta 0 and timestamp delta 0 is restored
    Then the restored record coordinates are rejected

  Scenario: Absolute record offset overflow is rejected
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 9223372036854775807 | 4 | create-time | 100 | 110 |
    When a record with offset delta 1 and timestamp delta 0 is restored
    Then the restored record coordinates are rejected

  Scenario: Maximum representable record coordinates remain valid
    Given an archived record batch has this coordinate frame:
      | base_offset | last_delta | timestamp_type | base_timestamp | max_timestamp |
      | 9223372036854775807 | 0 | create-time | 9223372036854775807 | 9223372036854775807 |
    When a record with offset delta 0 and timestamp delta 0 is restored
    Then the restored record has offset 9223372036854775807 and timestamp 9223372036854775807

  Scenario: Non-idempotent data retains the full offset span
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | 2 | false | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch advances the restore frontier to 13

  Scenario: An emptied idempotent data batch retains its offset span
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | 0 | false | false | 7 | 2 | 4 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch advances the restore frontier to 13

  Scenario: Transactional data includes a complete producer identity
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | 3 | false | true | 7 | 2 | 4 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch advances the restore frontier to 13

  Scenario: A transaction marker has one offset and no base sequence
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 0 | 1 | true | true | 7 | 2 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch advances the restore frontier to 11

  Scenario: A compacted transaction marker may have no records
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 0 | 0 | true | true | 7 | 2 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch advances the restore frontier to 11

  Scenario: A barrier control batch has no transactional producer
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 0 | 0 | 1 | true | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch advances the restore frontier to 1

  Scenario: Control batches cannot span multiple offsets
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 1 | 1 | true | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Control batches cannot contain two records
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 0 | 2 | true | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Transaction markers cannot carry a base sequence
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 0 | 1 | true | true | 7 | 2 | 4 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Transactional markers require a producer identity
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 0 | 1 | true | true | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Transactional data requires a producer identity
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | 2 | false | true | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: A partial sentinel producer identity is rejected
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | 2 | false | false | -1 | 0 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: A producer identity without an epoch is rejected
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | 2 | false | false | 7 | -1 | 4 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Negative rewritten batch spans are rejected
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 0 | -1 | 1 | false | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Negative rewritten record counts are rejected
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 10 | 2 | -1 | false | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: Negative rewritten base offsets are rejected
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | -1 | 0 | 1 | false | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected

  Scenario: A rewritten frontier past maximum offset is rejected
    Given a rewritten batch has this header:
      | base_offset | last_delta | record_count | control | transactional | producer_id | producer_epoch | base_sequence |
      | 9223372036854775807 | 0 | 0 | false | false | -1 | -1 | -1 |
    When the restore writer validates the rewritten batch header
    Then the rewritten batch header is rejected
