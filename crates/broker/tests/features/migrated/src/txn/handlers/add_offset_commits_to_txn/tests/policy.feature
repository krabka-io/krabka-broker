Feature: Kafka AddOffsets validation ordering

  Scenario Outline: Decide whether offsets partition 4 needs a transaction record
    Given transaction metadata and AddOffsets request <input>
    When the AddOffsets policy checks offsets partition 4
    Then the AddOffsets decision is <expected>

    Examples:
      | input | expected |
      | {"state":"Ongoing","partitions":[],"held_id":99,"next_id":8,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 51 |
      | {"state":"Ongoing","partitions":[],"held_id":7,"next_id":-1,"requested_id":8,"requested_epoch":3,"transactional_id":"tid-offsets"} | 49 |
      | {"state":"Ongoing","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":2,"transactional_id":"tid-offsets"} | 90 |
      | {"state":"PrepareCommit","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 51 |
      | {"state":"PrepareAbort","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 51 |
      | {"state":"Ongoing","partitions":[4],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 0 |
      | {"state":"Ongoing","partitions":[5],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | append |
      | {"state":"Empty","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | append |
      | {"state":"CompleteCommit","partitions":[4],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | append |
      | {"state":"CompleteAbort","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | append |
