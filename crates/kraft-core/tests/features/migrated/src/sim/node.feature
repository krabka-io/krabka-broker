Feature: Playground scheduler timer precision
  Voter-specific election staggering must survive conversion to the scheduler's
  integer-millisecond clock; every voter shares the 300 ms heartbeat period.

  Scenario Outline: Voter timers preserve whole-millisecond election staggering
    Given playground voter <node_id>
    When its election and heartbeat deadlines are converted to scheduler milliseconds
    Then its election timer is <election_ms> ms and its heartbeat timer is 300 ms

    Examples:
      | case                                   | node_id | election_ms |
      | the first voter waits one stagger      | 1       | 1050        |
      | the second voter waits two staggers    | 2       | 1100        |
      | the third voter waits three staggers   | 3       | 1150        |
      | a voter id beyond the cluster size     | 7       | 1350        |
