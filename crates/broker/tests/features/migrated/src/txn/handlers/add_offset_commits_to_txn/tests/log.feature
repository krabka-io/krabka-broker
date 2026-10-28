Feature: Durable transaction offsets registration

  Scenario Outline: Durable AddOffsets decisions
    Given a leading transaction log seeded for AddOffsets <input>
    When AddOffsets registers offsets partition 4 through the coordinator
    Then AddOffsets returns code <code> and log mutation <mutation>

    Examples:
      | input | code | mutation |
      | {"state":"Empty","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 0 | Ongoing-with-start-time |
      | {"state":"Ongoing","partitions":[4],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 0 | unchanged-entry |
      | {"state":"PrepareCommit","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":"tid-offsets"} | 51 | unchanged-entry |
      | {"state":"Ongoing","partitions":[],"held_id":7,"next_id":-1,"requested_id":8,"requested_epoch":3,"transactional_id":"tid-offsets"} | 49 | unchanged-entry |
      | {"state":"Ongoing","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":2,"transactional_id":"tid-offsets"} | 90 | unchanged-entry |
      | {"state":"Ongoing","partitions":[],"held_id":7,"next_id":-1,"requested_id":7,"requested_epoch":3,"transactional_id":""} | 42 | unchanged-entry |
